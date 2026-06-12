<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# paimon-rust Parquet 多层下推补齐 — 实施方案

## Context

`docs/pk-read-rust-vs-java-capabilities.md` P7 标记 ❓ — "Parquet 多层下推深度未审计"。事实摸底（master HEAD `029a159`）已完成，paimon-rust 当前 Parquet 读路径已有：

- **Manifest stats prune**：`crates/paimon/src/table/stats_filter.rs`（按 manifest 文件元数据淘汰整文件）。
- **Row-group STATISTICS prune**：`crates/paimon/src/arrow/format/parquet.rs:632-670` 的 `build_predicate_row_selection`，通过 `ParquetRecordBatchStreamBuilder::with_row_selection` 跳过整 row group。
- **Per-row RowFilter**：`parquet.rs:212-279`，F7 stage 1/2/3 全部 16 个 op 已上线（commit `c36bf05`）。

对齐 Java `paimon-format/.../parquet/ParquetReaderFactory.java:91-103` + 内嵌 parquet-hadoop `RowGroupFilter`（`STATISTICS / DICTIONARY / BLOOMFILTER / COLUMN_INDEX` 4 层），paimon-rust 当前**未在读路径显式利用 Parquet COLUMN_INDEX、BLOOMFILTER、DICTIONARY 三类 parquet 内部索引**：

- `ArrowReaderOptions` 默认 `PageIndexPolicy::Skip`（`parquet-58.3.0/src/file/metadata/reader.rs:85-94`），metadata 不加载 page index；
- `ParquetRecordBatchStreamBuilder::get_row_group_column_bloom_filter` 在仓库零调用；
- 无 dictionary filter helper（arrow-rs 没提供等价 Java parquet-hadoop `DictionaryFilter`，需自己实现）。

**目标**：补齐 Page-Index、Bloom Filter、Dictionary Filter 三类 parquet 内部 pruning 能力（Stage 1+2 必做、Stage 3 follow-up）。完成后 P7 在 capabilities 矩阵从 ❓ → ✓。

## 优先级 + Stage 拆分

按 ROI 排序。三 stage 都复用现有 `predicate_stats::data_leaf_may_match` + `StatsAccessor` trait（`crates/paimon/src/predicate_stats.rs:21-28`）—— 把"row-group stats"换成"page stats / bloom 探针 / dict 集合"即可，**不引入新谓词求值路径**。

### Stage 1（必做、高 ROI、低复杂度）— Page-Index page-level prune

**目标**：让 Parquet metadata 加载 ColumnIndex / OffsetIndex 后，基于 page-level min/max/null_count 构造额外 RowSelection，与现有 row-group stats RowSelection、外部 row_ranges RowSelection 取交集。宽 row-group 文件（行数 100w+）IO 层就少读未命中 page。

**适用范围（不是"对全部 16 op 适用"）**：仅对可由 page min/max/null_count 安全判断的谓词剪枝；遇到以下任一情况都必须 fail-open（保留 page，后续 per-row filter 兜底）：

- 文件没 page index / offset index（老文件、写端未生成）；
- 该 page 是 null page（`null_pages[i] == true`）；
- `boundary_order` 不可用 / 类型转换失败；
- 谓词本身在 stats 上无法安全判断（与现有 `predicate_stats::data_leaf_may_match` 的 fail-open 矩阵一致 —— `Contains` / `EndsWith` / 一般 `Like` / `NotEq` / `NotIn` / `NotBetween` 多数情况下只能 fail-open）。

设计原则：**不能因为 page stats 不可用而错误 skip page**。复用 `predicate_stats::data_leaf_may_match` 的现有保守语义即可。

**关键 API**（parquet-58.3.0）：

- `ParquetRecordBatchStreamBuilder::new_with_options(reader, options)` — 替换当前 `ParquetFormatReader::read_batch_stream` 内 `parquet.rs:155` 的无参 `new`。
- `ArrowReaderOptions::with_page_index_policy(PageIndexPolicy::Optional)`（`arrow_reader/mod.rs:633`）— 一次设置同时打开 column_index + offset_index；`Optional` 让没 index 的老文件 fallthrough（**`Required` 会返回 error，不是 panic**，见 `parquet-58.3.0/src/file/metadata/reader.rs:85-94`）。
- `ParquetMetaData::column_index() / offset_index()`（`file/metadata/mod.rs:156-169`）取 `Vec<Vec<…>>`，索引为 `[row_group][column]`。
- `ColumnIndex` 含 `null_pages: Vec<bool>` / `boundary_order: BoundaryOrder` / `null_counts: Option<Vec<i64>>`（`column_index.rs:40-46`）；min/max 通过 `ColumnIndexMetaData` enum 按类型分支（INT32 / INT64 / BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY 等，见 `column_index.rs:560-580`）。
- `OffsetIndexMetaData::page_locations() -> &Vec<PageLocation>`（`offset_index.rs:64-67`）。`PageLocation` 只有 `first_row_index / offset / compressed_page_size`，**无 `rows` 字段**；每页行数需相邻 page 推：`page_end = next_page.first_row_index`，最后一页 `page_end = row_group.num_rows()`。
- 输出 `RowSelection::from_consecutive_ranges(iter, total_rows)`（`selection.rs:164`）→ `intersection`（`selection.rs:414`）与 row-group RowSelection 合并。

**改动清单**（全部在 `crates/paimon/src/arrow/format/parquet.rs`）：

1. `ParquetFormatReader::read_batch_stream`（`parquet.rs:142-205`）：构 `ArrowReaderOptions`，按 `core_options` flag 决定是否 `with_page_index_policy(Optional)`，改用 `new_with_options`。
2. 新增 `build_predicate_page_selection(metadata, row_groups, predicates, file_fields) -> Option<RowSelection>`：对每个 row group 拿 `ColumnIndex / OffsetIndex`，per-page 跑 `predicates_may_match_with_schema`，把命中 page 的 `[page_first_row_index, page_end)` 收成 `Vec<Range<usize>>` 过 `from_consecutive_ranges`。null page 一律 keep（fail-open 给 row filter 处理）；缺 ColumnIndex 整体返 `None`（不剪）。
3. 新增 `ParquetPageStats { column_index_metadata, offset_index, page_idx, … }: StatsAccessor`：**不能复用 `parquet_stats_to_datum`**（输入是 footer 的 `ParquetStatistics`，不是 page index 的 `ColumnIndexMetaData`）—— 需新增按 enum 分支提取 page min/max → `Datum` 的 helper。
4. `parquet.rs:188-198` 的 RowSelection 合并链上加 `intersect_optional_row_selections(predicate_row_selection, page_selection)`。
5. `crates/paimon/src/spec/core_options.rs` 加 `read.parquet.page-index.enabled`，**默认 on**（对齐 Java 行为；同时给 escape hatch）。

**Trade-off**：选 `Optional` 而非 `Required` policy — 老文件无 page index 时不报错、自然 fallthrough 到 row-group 级；这是写端是否生成 page index 的兼容兜底。

**Acceptance**：

- 单测分两类：
  - **可剪枝 op**（Eq / Lt / LtEq / Gt / GtEq / Between / IsNull / IsNotNull / StartsWith / 可解析 prefix 的 Like）：mock metadata + 已知 page min/max → 断言 RowSelection 与手算一致。
  - **保守 op**（NotEq / NotIn / EndsWith / Contains / 一般 Like / NotBetween）：断言全 page 保留（fail-open），不被错误 skip。
- Boundary cases：空 page index → 返 `None`；null page → 保留；page min==max（dict-encoded 列特征）→ 等价 Eq 比较。
- 集成：`parquet.rs::tests` 写 1 row group / 8 page 小 fixture，跑 Eq / Lt / Between，断言读到的行数 = 手算 page 命中数。
- `cargo test -p paimon --lib arrow::format::parquet`。

### Stage 2（推荐、中 ROI、中复杂度）— Bloom Filter row-group prune

**目标**：对 Eq / In leaf 在 row-group 级（per-row filter 之前）调 bloom 过滤；高基数列点查（PK 字段）大幅减 IO。

**关键 API**：

- `ParquetRecordBatchStreamBuilder::get_row_group_column_bloom_filter(rg, col).await -> Option<Sbbf>`（`async_reader/mod.rs:511`，仓库零调用）。
- `Sbbf::check<T: AsBytes>(value) -> bool`（`bloom_filter/mod.rs:556`）—— bloom 是不存在性证明：`false` ⇒ 一定不存在（可 skip），`true` ⇒ 可能存在（继续）。
- 无 `with_bloom_filter` toggle，全部手写。

**改动**（`parquet.rs`）：

1. 新增 `bloom_check_row_groups(builder, row_groups, predicates, file_fields).await -> HashSet<rg_idx>` 返回"可被 skip 的 row group 集合"。
2. 对每 rg + 每 Eq/In leaf：`get_row_group_column_bloom_filter` → `None` fall-open；`Some(sbbf)` → literal 转 bytes（int LE / string UTF-8）后 `Sbbf::check`；In 谓词所有 literal 都 false 才 skip 整 rg，Eq 一个 false 即 skip。
3. 在 `build_predicate_row_selection` 输出前合并：bloom 标记 skip 的 row group 输出 `RowSelector::skip(rg.num_rows())`，与 stats prune 取并集。
4. `core_options.rs` 加 `read.parquet.bloom-filter.enabled`，**默认 off**（paimon-rust 当前 Parquet writer `ParquetFormatWriter::new`（`parquet.rs:65-73`）只设 `set_compression(codec)`，**未配置 parquet bloom filter 写入** —— 现存 paimon-rust 写出的文件普遍无 bloom；默认 off 避免无 bloom 文件白付一轮 IO。注意：这跟 `crates/paimon/src/btree/writer.rs:182-183` 的 b-tree global index bloom todo 是两件事，b-tree bloom 不影响 Parquet 文件内部 bloom）。

**Trade-off**：bloom 是 async IO（一 rg 一次拉 bloom 结构），不像 stats 从 footer 零开销读。默认 off 让 user 知情时手动开。

**类型 / op 矩阵**：

| op | bloom 适用 |
|---|---|
| Eq / In | ✓ |
| NotEq / NotIn | ✗（bloom 不能反向证明存在） |
| Lt / LtEq / Gt / GtEq / Between / NotBetween | ✗（区间无法 bloom 探） |
| StartsWith / EndsWith / Contains / Like | ✗ |
| IsNull / IsNotNull | ✗ |

未匹配 op fall-open 至 row-group stats + per-row filter。

**Acceptance**：

- 单测:手构 `Sbbf::new_with_ndv_fpp` → 插入已知 keys → Eq 谓词命中/不命中两组断言 row group skip。
- 集成：用 `WriterProperties::set_bloom_filter_enabled(true)` 写一个**带 bloom** 的 parquet fixture（与正式写端无关的一次性 fixture），跑 Eq 命中/不命中两组断言。
- `cargo test -p paimon --lib arrow::format::parquet`。

### Stage 3（follow-up、低 ROI、高复杂度）— Dictionary filter

**目标**：dict-encoded 列读 dict page，与谓词字面量集合求交；全否 → skip 整 row group。低基数列（status / region）效果好。

**复杂度**：dict page 不在 footer，需按 ColumnChunk `dictionary_page_offset` 读字节流 → 解码 → 与 literals 求交。arrow-rs 没有等价 helper（Java 端 parquet-hadoop 内置 `DictionaryFilter`），全部手写。

**Trade-off**：先 Stage 1+2 落地。Stage 3 命中率取决于写端 dict encoding 行为，未审计前不划算。**默认 off**，新增 `read.parquet.dictionary-filter.enabled`。

## 验证（端到端）

1. **单测**：见每 stage acceptance。
2. **集成 A/B**：扩 `crates/paimon/examples/read_local_demo.rs` 加 `--parquet-page-index <on|off>` / `--parquet-bloom <on|off>` flag，对真实表跑同一谓词，输出读到行数 + `drain_ms`。同 split 同谓词 page-index off→on 应明显 skip 增多（row group 100w 行 / 8-16 page → page-level 选择率细于 row-group level）。
3. **回归**：`cargo test -p paimon` / `cargo test -p paimon-datafusion --lib filter_pushdown` 全绿；现有 16 op row filter 单测不受影响。

## 不在范围

- **写端 page index / bloom filter 生成**：当前 `ParquetFormatWriter::new`（`parquet.rs:65-73`）`WriterProperties::builder()` 只设 compression，未启用 `set_bloom_filter_enabled` / `set_column_bloom_filter_enabled`（API 见 `parquet-58.3.0/src/file/properties.rs`）；page index 是否默认生成需另行审计。写端启用是另一专项；本方案读端无 bloom / 无 page index 时 fall-open（不剪而非报错）。
- **DataFusion adapter 层下推**：F7 已完成，本方案在 reader 内层加深下推，DF 自动受益。
- **ORC predicate pushdown**：`crates/paimon/src/arrow/format/orc.rs` 当前完全无下推，是另一独立专项。
- **F6 file-index reader**（bloom / hash / BSI / range / bitmap sidecar）：是 paimon-spec 内的 sidecar 索引，不在 parquet 文件内部，与本方案正交。

## 关联工作

- **P7**（本方案）：完成后 [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) 矩阵从 ❓ → ✓。
- 复用基础：`predicate_stats::StatsAccessor` trait + `data_leaf_may_match`（Stage 1+2+3 都用同一抽象，把不同 stats 源插入即可）。

## Follow-up

- Stage 3 dictionary filter（依赖 Stage 1+2 落地）。
- Parquet writer 启用 page index + bloom filter 生成（`ParquetFormatWriter::new` 的 `WriterProperties` 加 `set_bloom_filter_enabled` / page index toggle，让 Stage 1+2 实战命中率上去）。
- ORC predicate pushdown（同设计模式：footer stats → page stats → row filter）。
