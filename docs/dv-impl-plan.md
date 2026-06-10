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

# paimon-rust 实现 deletion vector 读路径 —— 实施方案

<!-- SECTION-OVERVIEW -->

## 总览

paimon-rust 对 deletion vector (DV) 的支持目前只在 raw 路径（`DataFileReader`）就位 —— `crates/paimon/src/table/data_file_reader.rs` 已经把 `DeletionVectorFactory`（line 96 起）接到 parquet `with_row_selection`（`merge_row_selection`，line 211 / 329 / 344），DV 在 parquet 层就剥掉删除行。但 **PK MoR 路径（`KeyValueFileReader`）在 `crates/paimon/src/table/kv_file_reader.rs:271-275` 直接 `Error::Unsupported` 拒绝带 DV 的 split**，且 `read_single_file_stream` 调用处对 `dv` 参数硬传 `None`。后果：任何 `deletion-vectors.enabled=true` 的 PK 表只要 split 含 L0 文件，整次读取必崩。

叠加：
- `Bitmap64DeletionVector`（64-bit row id，Iceberg 兼容格式）完全没识别（C6）；
- `deletion-vectors.read-mode` 选项（`PERFORMANCE` / `FRESHNESS`）被静默忽略（C7）；
- L0 文件上 value-stats 裁剪不安全（C4，与 read-mode 联动）。

构成一组关联的读路径缺口，**全部都是 capabilities 文档标 HIGH 的项**。

本方案覆盖 `pk-read-rust-vs-java-capabilities.md` 中以下条目：

| 文档条目 | 严重程度 | 本方案对应阶段 |
|---|---|---|
| C2 — KV reader 拒 DV split | C-HIGH | Stage 1 |
| C4 — L0 value-stats pruning 错误 | C-HIGH | Stage 3（同步修） |
| C6 — Bitmap64 DV 不识别 | C-HIGH | Stage 2 |
| C7 — `deletion-vectors.read-mode` 忽略 | C-HIGH | Stage 3 |
| F9 — LookupMerge L0+DV 快路径 | F | Stage 4a / 4b |
| C5 — raw 路径不剥 DELETE / UPDATE_BEFORE | C-HIGH | Stage 4a 顺手修 |

**不在 scope**：DV 写路径（`BucketedDvMaintainer` 等价物 / compaction 时生产 DV / writer 配 DV 启用）。capabilities 文档未列写侧 gap，且 `kv_file_writer.rs` 等处对 DV+PartialUpdate 主动 reject —— 写侧调通后续单立 plan。



<!-- SECTION-JAVA-PIPELINE -->

## Java 等价路径速览（参照实现 anchor）

来自针对 paimon-java master 的 audit。以下 anchor 直接给到本方案要 mirror 的入口。

### 1. Per-file DV apply（在 sort-merge 之前）

- **`paimon-core/.../io/KeyValueFileReaderFactory.java:173-177`**：在构建完 per-file row reader 后，查 `dvFactory.create(file.fileName())`；若 DV 存在且非空，用 `ApplyDeletionVectorReader` 包一层，**再** 喂给 `KeyValueDataFileRecordReader`，最后才进 sort-merge。
- **`paimon-core/.../deletionvectors/ApplyDeletionFileRecordIterator.java:64-74`**：行级 `deletionVector.isDeleted(returnedPosition())`。`returnedPosition()` 来自底层 parquet/orc reader 的 `FileRecordIterator`（绝对 row position 跨 row-group），**不是** 内部计数器。
- **`paimon-core/.../deletionvectors/DeletionVector.java:152-169`**：DV factory 两个变体 —— `factory(BucketedDvMaintainer)`（writer 侧）/ `factory(FileIO, files, deletionFiles)`（reader 侧）。Rust 已经有 reader 侧等价的 `crates/paimon/src/deletion_vector/factory.rs::DeletionVectorFactory::new`。

**Rust 走更优的方案**：`crates/paimon/src/table/data_file_reader.rs:344` 用 `dv_to_non_deleted_ranges`（line 353）把 DV 转成 row range list，喂给 parquet `with_row_selection` —— 跳过解码而非解码后过滤。本方案让 KV 路径走同样模式，不需要 row-by-row `is_deleted` 检查。

### 2. DV format dispatch（32-bit vs 64-bit bitmap）

- **`paimon-core/.../deletionvectors/DeletionVector.java:100-145`**：先读 `bitmapLength`，再读 `int magicNumber`。按 magic 分派：
  - `BitmapDeletionVector.MAGIC_NUMBER = 1581511376`（**BE int32** 比较）→ RoaringBitmap32，最大 2^31-1 行
  - `toLittleEndianInt(magic) == Bitmap64DeletionVector.MAGIC_NUMBER (1681511377)` → `OptimizedRoaringBitmap64`，64-bit 行 id，**LE 字节序** payload
- **`Bitmap64DeletionVector.java:38-167`**：layout `[length:int32 BE][magic:int32 LE][bitmap LE bytes][crc32:int32]`；CRC 跳过不校验（与 Bitmap32 一致）。
- 两种格式共用 `DeletionVectorsIndexFile` 容器：`[version:byte=1][ for each entry: int32 length, int32 magic, bitmap-bytes, int32 crc ]`（`DeletionVectorsIndexFile.java:163-172`）。

### 3. `deletion-vectors.read-mode` (PERFORMANCE / FRESHNESS)

- **`paimon-api/.../CoreOptions.java:1880-1889`**：option 声明，默认 `PERFORMANCE`。
- **`paimon-core/.../KeyValueFileStoreScan.java:154-162`**：plan 时若 L0 entry 跑 value-stats 裁剪，**仅 FRESHNESS 模式允许**；其它模式 throw 或 strip。
- **`paimon-core/.../table/source/DataTableBatchScan.java:71-76`**：PK + `batchScanSkipLevel0` 分支：FRESHNESS → 仅 `enableValueFilter()`（保 L0）；PERFORMANCE → `withLevelFilter(level -> level > 0).enableValueFilter()`（跳 L0）。
- `DataTableStreamScan.java:163-172` 在 streaming bootstrap 路径上做同样的分派。
- `MergeFileSplitRead` **不直接** 读 `read-mode`；它只对 planner 喂进来的文件做 sort-merge + per-file DV apply。所以读端实现的 contract 是：planner 已按 mode 决定 L0 是否在 split 里。

### 4. DV 与 LookupMerge 的关系

⚠️ **澄清 capabilities 文档 F9**：DV 和 LookupMerge 在 Java 是 **两个独立优化**，文档把它们混在一起描述了：

- **DV-PERFORMANCE**：planner 直接不交 L0 给 reader，L1+ 已 DV-applied + 非重叠 → 可走 raw read（绕过 sort-merge）。这是 F9 描述里 "level≥1 文件已全部应用 DV，可绕过 sort-merge 走 raw read" 的含义。
- **LookupMergeFunction**：runtime 用 L0 hash + L1+ 点查取代 sort-merge，由 `mfFactory instanceof LookupMergeFunction.Factory` 选中（`MergeFileSplitRead.java:170-171`），驱动条件是 `changelog-producer=lookup` 或 `force-lookup=true`，**与 `DV_READ_MODE` 无关**。主要服务流式 changelog 生成。

批读用例下，DV-PERFORMANCE 的 raw-read 短路（Stage 4a）就够拿到 F9 的吞吐收益；LookupMergeFunction 全套（Stage 4b）只在确有流式 changelog 需求时才需要。

### 5. 索引文件加载

- 多文件批量：`DeletionVectorsIndexFile.readAllDeletionVectors(IndexFileMeta)` (`paimon-core/.../deletionvectors/DeletionVectorsIndexFile.java:73-96`)，开 index blob 一次，按 `dvRanges()`（`LinkedHashMap<dataFileName, DeletionVectorMeta>`）逐项读。
- 按 split partial：`readDeletionVector(Map<String, DeletionFile>)` (line 105-127)。
- Rust 已有 `crates/paimon/src/spec/avro/index_manifest_entry_decode.rs` 解码 index manifest entry，`DeletionVectorMeta` 已就绪。

### 6. Empty-file / all-deleted 短路

Java **没有** plan-time "all rows deleted → skip file" 优化（`DataFileMeta.deleteRowCount()` 在 plan 阶段只用于 limit-pushdown 的反向短路：`KeyValueFileStoreScan.java:280-294`，遇到 `deleteRowCount > 0` 时 limit-prune **bail out**）。`KeyValueFileReaderFactory.java:174` 仅在 `dv.isEmpty()` 时跳过 wrap，不跳过文件本身。**Rust 此点跟 Java 一致即可**，不引入新优化。



<!-- SECTION-STAGES -->

## 实施分阶段

每阶段独立 PR-able，建议按顺序推进。

### Stage 1 — KV reader DV wiring (C2)

**目标**：让 `kv_file_reader.rs` 能消费带 DV 的 split。复用 `data_file_reader.rs` 已验证的 DV→parquet-row-selection 模式，**不动** `MergeRow` / `SortMergeReader` —— DV 在 parquet 层就剥掉删除行，sort-merge 看到的是过滤后的行。

**改动**：

- `crates/paimon/src/table/kv_file_reader.rs`
  - 删除 line 271-275 的 `Error::Unsupported`：
    ```rust
    Err(Error::Unsupported {
        message: "KeyValueFileReader does not support deletion vectors".to_string(),
    })
    ```
  - 在 `read()` 入口（紧邻 file_streams 构造之前）建：
    ```rust
    let dv_factory = DeletionVectorFactory::new(
        file_io.clone(),
        split.data_files(),
        split.data_deletion_files(),
    );
    ```
  - file 循环里查 `dv_factory.get(file_meta.file_name())`，把 `Option<Arc<DeletionVector>>` 透传给 `read_single_file_stream` —— 替换当前调用处（line ~330）的硬 `None`
  - `read_single_file_stream` 加 `dv: Option<Arc<DeletionVector>>` 参数（mirror `data_file_reader.rs:147-154` 签名），通过 `merge_row_selection`（复用 `data_file_reader.rs:211 / 329` 模式）转 row-ranges 喂 parquet
- `crates/paimon/src/deletion_vector/mod.rs`：把 `data_file_reader.rs:329` `merge_row_selection` 和 line 353 `dv_to_non_deleted_ranges` 抽到本 module 作公共 helper（KV / raw 两路共用）。`data_file_reader.rs` 改为 re-export。

**Verification**：
- 单测（`kv_file_reader.rs` `#[cfg(test)] mod tests`）：构造 1 个含 4 行的 parquet 文件 + DV bitmap{1,3} → KV reader 输出 row 0 / row 2
- 集成测试（新文件 `crates/integrations/datafusion/tests/dv_pk_tables.rs`）：DataFusion SQL 写 PK 表 with `deletion-vectors.enabled=true`，UPDATE 一行后 SELECT，验证旧值不出现
- 不引入新 unit / integration 之外的依赖；现有 716+ lib 测全过

### Stage 2 — Bitmap64 DV (C6)

**目标**：识别 Java `Bitmap64DeletionVector` 写出的 64-bit row-position DV 文件。当前 Rust 只能解 32-bit；Java 写出的 64-bit DV 文件 paimon-rust 当 invalid magic 报错。

**改动**：

- `crates/paimon/src/deletion_vector/core.rs`
  - 当前 `pub struct DeletionVector { bitmap: Arc<RoaringBitmap> }` (line 27-30) 改为枚举：
    ```rust
    pub enum DeletionVector {
        Bitmap32(Bitmap32DeletionVector),
        Bitmap64(Bitmap64DeletionVector),
    }
    ```
  - 公共 API 保留：`iter() -> Box<dyn Iterator<Item=u64>>`、`cardinality() -> u64`、`is_empty() -> bool`，分派到内部 impl
  - `read_from_bytes`（line 82-147）改成 magic dispatch：先读 4 字节 length（BE int32）+ 4 字节 magic；按 "BE 比较 32-bit magic `1581511376`" / "LE 比较 64-bit magic `1681511377`" 选 impl
  - 调用方（`factory.rs`、`data_file_reader.rs`、Stage 1 新 KV 路径）按 enum 用，逻辑不变
- 新文件 `crates/paimon/src/deletion_vector/bitmap64.rs`
  - `pub struct Bitmap64DeletionVector { bitmap: roaring::RoaringTreemap }`
  - `MAGIC_NUMBER: u32 = 1681511377`（按 LE 比较）
  - `deserialize_from_bitmap_data_bytes(bytes) -> Result<Self>` 与 Java `Bitmap64DeletionVector.java:38-167` 字节对齐：`[length:int32 BE][magic:int32 LE][bitmap LE bytes][crc32:int32]`，CRC 跳过不校验（与 Bitmap32 现状一致）
- `crates/paimon/Cargo.toml`：确认 `roaring` 已含 `RoaringTreemap`（roaring 0.10+ 默认含；如未启用对应 feature 则补）
- `crates/paimon/src/spec/core_options.rs`
  - 加 `DELETION_VECTORS_BITMAP64_OPTION = "deletion-vectors.bitmap64"`
  - `pub fn deletion_vectors_bitmap64(&self) -> bool`，默认 false
  - **仅供 schema 信息记录 / 未来写侧选 format**；read 端 magic dispatch 已可识别两种格式，与该 option 无关

**Verification**：
- 黄金字节测试：把 Java 写出的 32-bit + 64-bit DV 文件二进制 hex 拷到 `crates/paimon/tests/fixtures/dv/`（如缺，本地无 Java 环境时依据 Java 字节布局手工构造一个最小例子并加 `// TODO: replace with real Java-generated fixture` 注释）
- 单测：`Bitmap64DeletionVector::deserialize` round-trip 5 个 row id（含 > 2^32 的）→ `iter()` 返回相同集合
- magic dispatch 单测：BE 32-bit magic / LE 64-bit magic / 无效 magic 三条 case；验证 enum 派发结果

### Stage 3 — read-mode + L0 routing (C7 + C4)

**目标**：识别 `deletion-vectors.read-mode`，让 plan 端按 mode 决定是否保留 L0 + 是否对 L0 跑 value-stats 裁剪。同步修 C4（非 DV 模式下 L0 value-stats 裁剪不安全）。

**改动**：

- `crates/paimon/src/spec/core_options.rs`
  - 新 enum:
    ```rust
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DvReadMode {
        Performance,
        Freshness,
    }
    ```
  - `DELETION_VECTORS_READ_MODE_OPTION = "deletion-vectors.read-mode"`
  - `pub fn deletion_vectors_read_mode(&self) -> Result<DvReadMode>`，默认 `Performance`（与 Java `CoreOptions.java:1880-1889` 一致）
- `crates/paimon/src/table/table_scan.rs:287` `should_skip_level_zero_for_scan`
  - 签名加 `dv_read_mode: DvReadMode` 参数
  - 决策矩阵：

    | merge_engine | dv_enabled | dv_read_mode | skip_level_zero |
    |---|---|---|---|
    | * | false | * | false（保 L0）|
    | FirstRow | * | * | true |
    | * | true | Performance | true |
    | * | true | Freshness | false（保 L0；依赖 Stage 1 让 KV reader 能处理 L0+DV）|
- `crates/paimon/src/table/table_scan.rs:175` value-stats pruning（`data_file_matches_predicates` 调用点）
  - 在 PK + (DV-enabled 且非 FRESHNESS) 情况下，对 L0 entry 跳过 value 范围裁剪（mirror Java `KeyValueFileStoreScan.java:154-162`）
  - **C4 同步修复**：非 DV 模式下，PK + Deduplicate / PartialUpdate 也跳 L0 value-stats（capabilities 文档 C4 描述的 "ghost 旧值" 风险）—— Java 是默认行为，Rust 当前漏了
  - 抽 helper `pub(crate) fn is_l0_value_stats_safe(merge_engine: MergeEngine, dv_enabled: bool, dv_read_mode: DvReadMode) -> bool`，只在 FRESHNESS 模式返回 true

**Verification**：
- 单测：`should_skip_level_zero_for_scan` 矩阵覆盖（5+ 组合）；`is_l0_value_stats_safe` 矩阵
- 集成测试（在 Stage 1 的 `dv_pk_tables.rs` 内）：FRESHNESS 模式下读出的 L0 行 + DV 一致；PERFORMANCE 模式下 plan 输出不含 L0
- C4 回归测试：非 DV PK Dedup 表，构造 PK 重叠 + value 不同的多个 L0 文件 + value 谓词；旧（C4 buggy）路径返回过期值，新路径正确

### Stage 4a — DV-PERFORMANCE raw-read 短路 (F9 批读核心收益)

**目标**：DV-PERFORMANCE 模式下，全 L1+ split 绕过 sort-merge 走 raw read（DataFileReader 已支持 DV）。**实质上把 capabilities 文档 F9 的批读收益拿到了**，无需 LookupMergeFunction。

**改动**：

- `crates/paimon/src/table/table_read.rs:82-97` 的 dispatch
  - PK + DV-enabled + read-mode=PERFORMANCE + split 内全部 level ≥ 1 → 走 `read_raw`（`DataFileReader`，已支持 DV）
  - 其它情况保持现有 PK MoR 路径（Stage 1 已让 KV reader 能处理 DV + L0）
- 顺手补 `read_raw` 路径的 `_VALUE_KIND` DELETE / UPDATE_BEFORE 行后过滤（**C5 — capabilities 文档单列**，本 stage 顺手修，因为 raw 路径开始服务更多 PK 用例）
  - `crates/paimon/src/table/data_file_reader.rs` 加 post-decode `RowKind::is_add()` filter（同 Java `DropDeleteReader`）
  - 默认 `forceKeepDelete=false` —— 与 Java `MergeFileSplitRead.java:177-180` 一致

**Verification**：
- 集成：DV-PERFORMANCE + 全 L1+ split → 验证走 raw 路径（vs 走 sort-merge）。可用 metric 计数器或 trace 钩子辅助断言
- C5 回归：raw 路径读到含 DELETE 行的 L1+ 文件，DELETE 不出现在结果集
- Benchmark：与 sort-merge 路径吞吐对比；DV-PERFORMANCE + 大量 L1+ 数据时吞吐应有显著提升

### Stage 4b — LookupMergeFunction 全套 (F9 流式收益，建议 follow-up)

**前置条件**：仅在确有 `changelog-producer=lookup` / `force-lookup=true` 流式需求时启动；批读 PK 用例下 Stage 4a 已能拿到 F9 核心收益。

**目标**：服务 `changelog-producer=lookup` / `force-lookup=true` 流式 changelog 生成。

**改动梗概**（详化时再细化）：

- 新 `crates/paimon/src/table/lookup/{mod,lookup_levels,lookup_file,lookup_merge_function}.rs`
- `LookupLevels`：L0 文件全量 hash 进 in-memory 索引（PK → row）
- `LookupFile`：L1+ 文件按 PK 点查（基于 paimon 写入时按 PK 排序的事实，二分 / index 查）
- `LookupMergeFunction`：实现 `MergeFunction` trait，runtime 对每条 L1+ row 触发 L0 lookup 合并
- 选择逻辑：`new_merge_function` 在 `changelog-producer=lookup || force-lookup=true` 下返回 LookupMergeFunction

**Verification**：
- 移植 Java `LookupMergeFunctionTest` 单测
- E2E 验证 `changelog-producer=lookup` 表读结果与 Java 一致（依赖能跑 Java fixture）



<!-- SECTION-RISKS -->

## 关键风险 & 注意点

1. **DV row-id 与 parquet absolute row-position 对齐**：Java `ApplyDeletionVectorReader` 用 `iterator.returnedPosition()`（绝对 row position 跨 row-group）；Rust 走 parquet `with_row_selection`，需 DV row id 与 parquet 文件的绝对 row position 一致（**不是** 单 row-group 内偏移）。`data_file_reader.rs` 现有路径已验证模型，KV 路径同模式即可，**不需要重新调试这一层语义**。

2. **Empty DV 短路**：Java `KeyValueFileReaderFactory.java:174` 在 `dv.isEmpty()` 时跳过 wrap。Rust `dv_to_non_deleted_ranges` 在 empty DV 情况下应返回 full row range（不裁），等价行为。需单测覆盖 empty DV path。

3. **C4 与 C7 的语义边界**：Java 的 "L0 + value-stats 不安全" 逻辑在所有 PK + Deduplicate / PartialUpdate 下都成立，**不止 DV 模式**。本方案 Stage 3 顺手把 C4 也修了。如果担心改动牵涉过多现有 plan 测试，可以单独拆 Stage 3a (read-mode option only) / Stage 3b (C4 fix)。

4. **Bitmap64 endianness**：32-bit BE magic vs 64-bit LE magic 是 Java 历史包袱（Iceberg 兼容）。Rust magic dispatch 必须按 byte-order-aware 比较，**不能直接** `as i32`。Stage 2 单测要显式覆盖两种字节序。

5. **LookupMerge 真实需求确认**：F9 描述模糊。建议 Stage 4a 先 ship（小、覆盖批读），Stage 4b 等明确流式 changelog 需求再启动，避免大量 dead code。决策点放在 Stage 4a merge 之后。

6. **数据修复风险**：C4 修复后，老查询在某些 buggy 数据上可能 "由错变对"（之前由于过期值导致的结果数变化），用户察觉到行数 / 值变化。修复前后需保留观察日志，文档说明这是从过期值恢复到正确值，**而不是** 新引入的回归。

7. **`should_skip_level_zero_for_scan` 调用方更新**：现有 `table_scan.rs:493` 等调用点需要同步加 `dv_read_mode` 参数。Rust 编译器会强制所有调用点更新，但 Stage 3 PR 要确保新签名覆盖所有 5 个调用点（grep 全树确认）。

8. **`DeletionVector` enum 改造的兼容性**：当前 `pub struct DeletionVector` 是公开类型，外部 crate 可能引用了 `.bitmap()` 等方法。改 enum 后这些 API 会破坏。Stage 2 PR 要 grep workspace 内所有引用，提供过渡 API（如 `as_bitmap32()` 转 Option），或在 enum 上保留同名 method 派发到内部。



<!-- SECTION-FILE-LIST -->

## 文件改动清单（关键 anchor）

| 阶段 | 文件 | 关键行号 / 改动点 |
|---|---|---|
| 1 | `crates/paimon/src/table/kv_file_reader.rs` | 271-275（移 Unsupported），调用 `read_single_file_stream` 处（DV 透传） |
| 1 | `crates/paimon/src/deletion_vector/factory.rs` | 复用现状；`DeletionVectorFactory::new` 接 KV 路径 |
| 1 | `crates/paimon/src/deletion_vector/mod.rs` | 抽 `dv_to_non_deleted_ranges` + `merge_row_selection` 公共 helper |
| 1 | `crates/paimon/src/table/data_file_reader.rs` | 329 / 344 / 353（搬走或 re-export 给 KV 路径） |
| 1 | 新 `crates/integrations/datafusion/tests/dv_pk_tables.rs` | E2E 集成测试 |
| 2 | `crates/paimon/src/deletion_vector/core.rs` | 27-30（struct → enum），82-147（`read_from_bytes` magic dispatch） |
| 2 | 新 `crates/paimon/src/deletion_vector/bitmap64.rs` | 全部新增 |
| 2 | `crates/paimon/Cargo.toml` | 确认 `roaring` feature 含 `RoaringTreemap` |
| 2 | `crates/paimon/src/spec/core_options.rs` | `deletion_vectors_bitmap64()` getter |
| 2 | `crates/paimon/tests/fixtures/dv/` | 黄金字节 fixture |
| 3 | `crates/paimon/src/spec/core_options.rs` | `DvReadMode` enum + `deletion_vectors_read_mode()` getter |
| 3 | `crates/paimon/src/table/table_scan.rs` | 175（value-stats），287（`should_skip_level_zero_for_scan`），493（调用点） |
| 4a | `crates/paimon/src/table/table_read.rs` | 82-97（dispatch 加 raw-read 短路） |
| 4a | `crates/paimon/src/table/data_file_reader.rs` | 加 RowKind 后过滤（C5） |
| 4b | 新 `crates/paimon/src/table/lookup/...` | 全部（建议 follow-up） |



<!-- SECTION-OUT-OF-SCOPE -->

## 不在本计划范围内

- **DV 写路径**：`BucketedDvMaintainer` 等价物 / compaction 时生产 DV / writer 配 DV 启用。capabilities 文档未列写侧 gap，且 `crates/paimon/src/table/kv_file_writer.rs` 等处对 DV+PartialUpdate 主动 reject —— 写路径调通后续单立 plan。本方案默认 **paimon-java 写、paimon-rust 读** 的混合栈场景。
- **`changelog-producer=lookup` 全链路**：Stage 4b 占位但建议 follow-up，仅当确有流式 changelog 需求时启动。
- **paimon-rust 写出的 DV 给 paimon-java 读**：DV 写路径不在 scope，依赖前者完成。
- **DV index file 的 row-tracking 元数据**：当前 `paimon-rust` 用 `DeletionVectorMeta` 已能消费 Java 写出的 index manifest entry，本方案不再扩展 index manifest schema。

<!-- SECTION-VERIFICATION -->

## Verification（每阶段独立可验证）

| 阶段 | 验证 |
|---|---|
| 1 | 单测：4 行 parquet + DV bitmap{1,3} → 输出 row 0 / row 2；集成：DataFusion SQL 写 PK + DV 表，UPDATE 后旧值不出现 |
| 2 | 黄金字节：32-bit / 64-bit DV 文件 deserialize 后 bitmap 集合匹配；magic dispatch 单测覆盖 BE 32-bit / LE 64-bit / 无效 magic 三条 case；Bitmap64 序列化 + 反序列化 5 个 row id（含 > 2^32）round-trip |
| 3 | `should_skip_level_zero_for_scan` 5+ 矩阵；C4 回归：非 DV PK Dedup 表 + PK 重叠多 L0 + value 谓词，旧路径返过期值 / 新路径正确 |
| 4a | DV-PERFORMANCE + 全 L1+ split 走 raw 路径（trace 钩子 / metric 计数器辅助断言）；C5 回归：raw 路径含 DELETE 行的 L1+ 文件，DELETE 不出现；benchmark 与 sort-merge 路径吞吐对比 |
| 4b | 移植 Java `LookupMergeFunctionTest` 单测；E2E 与 Java 行级一致（依赖能跑 Java fixture） |

每阶段 PR merge 前要确保：
- `cargo test -p paimon --release --lib` 全过
- `cargo test -p paimon-datafusion --release` 全过
- 涉及 schema / option 改动时，`cargo test -p paimon --release --lib spec::core_options` 单独跑过

<!-- SECTION-EFFORT -->

## 工作量估计

| 阶段 | 估时 | 备注 |
|---|---|---|
| Stage 1 | 1 天 | KV reader DV wiring + 单测 + E2E 集成测 |
| Stage 2 | 半天 | Bitmap64 dispatch + 黄金字节测试 |
| Stage 3 | 半天 | read-mode + L0 stats fix + C4 回归测试 |
| Stage 4a | 1-2 天 | raw-read 短路 + C5 RowKind 后过滤 + benchmark |
| **本方案主体（Stage 1-3 + 4a）** | **3-4 工作日** | |
| Stage 4b | 1-2 周 | 单独 follow-up，建议视流式需求启动 |

<!-- SECTION-CROSS-REFS -->

## 与已有内部文档的交叉索引

| capabilities 文档条目 | 严重程度 | 本方案对应 | 关系 |
|---|---|---|---|
| C2 (KV reader 拒 DV split) | C-HIGH | Stage 1 | **直接修复** |
| C4 (L0 value-stats pruning 错误) | C-HIGH | Stage 3 | **同步修复**（Java 默认行为） |
| C5 (raw 路径不剥 DELETE) | C-HIGH | Stage 4a | **顺手修复**（Stage 4a 让 raw 路径开始服务更多 PK 用例，不修 C5 会暴露幽灵 DELETE 行） |
| C6 (Bitmap64 不识别) | C-HIGH | Stage 2 | **直接修复** |
| C7 (`deletion-vectors.read-mode` 忽略) | C-HIGH | Stage 3 | **直接修复** |
| F9 (LookupMerge L0+DV 快路径) | F | Stage 4a / 4b | **拆分** —— 4a 拿批读收益，4b 是流式 changelog（建议 follow-up） |

**与 `versioned-partial-update-impl-plan.md` 的关系**：VPU validation 已检查 ignore-mode → needLookup（`Schema::validate_versioned_partial_update`），DV 是 lookup capability 之一。**本方案 Stage 1 落地后**，VPU 表 `versioned-partial-update.ignore-mode.enabled=true` 配合 `deletion-vectors.enabled=true` 才能真正读取（之前会被 KV reader 直接 reject）。本方案 Stage 3 引入的 `deletion-vectors.read-mode` 也会让 VPU IGNORE-mode 文件在 PERFORMANCE / FRESHNESS 下行为可控。

**与 `pk-read-issues.md` 的关系**：本方案 Stage 1 关闭 issue 中 "DV 表无法读" 项；Stage 3 关闭 "L0 ghost 旧值" 项。

<!-- SECTION-REFERENCES -->

## 参考

### Java 关键源文件

- `paimon-core/.../io/KeyValueFileReaderFactory.java:173-177` —— `ApplyDeletionVectorReader` wrap 入口
- `paimon-core/.../deletionvectors/ApplyDeletionVectorReader.java:53-61` —— wrap 实现
- `paimon-core/.../deletionvectors/ApplyDeletionFileRecordIterator.java:64-74` —— 行级 `isDeleted` 循环
- `paimon-core/.../deletionvectors/DeletionVector.java:100-145` —— magic dispatch
- `paimon-core/.../deletionvectors/DeletionVector.java:152-169` —— factory 构造
- `paimon-core/.../deletionvectors/BitmapDeletionVector.java:34-115` —— 32-bit impl
- `paimon-core/.../deletionvectors/Bitmap64DeletionVector.java:38-167` —— 64-bit impl
- `paimon-core/.../deletionvectors/DeletionVectorsIndexFile.java:73-127` —— index file 读取
- `paimon-api/.../CoreOptions.java:1880-1889` —— `DELETION_VECTORS_READ_MODE` option 声明
- `paimon-core/.../KeyValueFileStoreScan.java:154-162` —— L0 + value-stats 安全性
- `paimon-core/.../table/source/DataTableBatchScan.java:71-76` —— PERFORMANCE / FRESHNESS plan-time 分派
- `paimon-core/.../operation/MergeFileSplitRead.java:170-181` —— LookupMerge 选择 + DV apply 入口
- `paimon-core/.../mergetree/DropDeleteReader.java:33-69` —— DELETE 行剥离

### Rust 现状关键 anchor

- `crates/paimon/src/deletion_vector/core.rs:18-147` —— 现有 32-bit DV 实现 + magic
- `crates/paimon/src/deletion_vector/factory.rs:39-76` —— DeletionVectorFactory 构造 + 加载
- `crates/paimon/src/table/data_file_reader.rs:96 / 211 / 329 / 344 / 353` —— raw 路径 DV wiring 模板（KV 路径要 mirror）
- `crates/paimon/src/table/kv_file_reader.rs:271-275` —— **本方案 Stage 1 删除点**
- `crates/paimon/src/table/table_scan.rs:175 / 287` —— **本方案 Stage 3 改动点**
- `crates/paimon/src/table/table_read.rs:82-97` —— **本方案 Stage 4a 改动点**
- `crates/paimon/src/spec/core_options.rs:20 / 242` —— `deletion-vectors.enabled` 已就绪
- `crates/paimon/src/spec/core_options.rs:59 / 381` —— `force-lookup` 已就绪（Stage 4b 用）
