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

# paimon-rust PK 表读取内存占用 Review

## 总览

本文记录 paimon-rust 当前在 **primary-key 表读取路径**上可定位的内存放大点，按优先级分级，每条附带「位置 / 现状 / 为什么放大 / 限制方向」。本文仅做代码 review 结论，不与 [`pk-read-issues.md`](./pk-read-issues.md) 中的正确性 / 性能 bug 重复，只关注**内存**维度。

走读范围：`table_scan → table_read → kv_file_reader → data_file_reader → sort_merge → arrow/parquet`，以及 `deletion_vector / spec / datafusion 集成`。

参考：

- [`pk-read-issues.md`](./pk-read-issues.md) — 已知正确性 / 性能问题（与本文互补）
- [`pk-read-rust-status.md`](./pk-read-rust-status.md) — 修复进度
- [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) — 功能对照

---

## P0 — 显著内存放大点（优先处理）

### P0-1. Plan 阶段所有 ManifestEntry 一次性 collect 到 `Vec`，并以 `buffered(64)` 预读

**位置**：`crates/paimon/src/table/table_scan.rs:133-213`（`read_all_manifest_entries`）+ `:669-677`（`plan_snapshot` 的 groups）

**现状**：

```rust
let all_entries: Vec<ManifestEntry> = futures::stream::iter(manifest_files)
    .map(|meta| async move {
        let content = input_file.read().await?;          // 整 manifest 文件读到 Bytes
        let entries = avro::from_manifest_bytes_filtered_shared(&content, ...);
        ...
    })
    .buffered(64)                                          // 同时 64 个 manifest 全字节驻留
    .try_collect::<Vec<_>>().await?
    .into_iter().flatten().collect();
```

**为什么放大**：

- 每个 manifest 文件 `input_file.read()` **全字节**到 `Bytes`；`buffered(64)` → 同时持有 64 个 manifest 字节缓冲 + 64 份解码后的 `Vec<ManifestEntry>`。每 manifest 几 MB ~ 几十 MB 的话，瞬时 GB 级。
- 收齐后 `Vec<ManifestEntry>` 整集合驻留；`merge_manifest_entries` 又过一遍 `HashSet<Identifier>` 去重；之后 `plan_snapshot:669` 的 `HashMap<(partition, bucket), (i32, Vec<DataFileMeta>)>` 把全部 `DataFileMeta`（含 `key_stats`/`value_stats`/`embedded_index`/`min_key`/`max_key`）继续保留到 split 切片结束。
- 每个 `DataFileMeta` 是大结构：`BinaryTableStats` 含 `min_values`/`max_values`/`null_counts`，宽列表 + 多文件下单文件元数据可达数 KB ~ 几十 KB；`embedded_index: Option<Vec<u8>>` 是 file index 的嵌入字节，最坏 MB 级。

**限制方向**：

1. `buffered(64)` 改为可配 option（如 `scan.manifest-parallelism`），默认降到 8 ~ 16；
2. 把 manifest 解码做成 streaming（`spec/avro` 的 `from_manifest_bytes_filtered_shared` 已支持过滤回调，再加一个 reader-based 版本，避免 `read().await` 整文件到 `Bytes`）；
3. split 切完后 reader 不再需要 `key_stats / value_stats / embedded_index / min_key / max_key`：构造一个精简的 `ReadFileMeta`（只保留 `file_name / file_size / row_count / level / first_row_id / schema_id / commit_snapshot_id / merge_mode`），把瘦身后的对象传给 reader。

---

### P0-2. K-way Sort-Merge 的 buffer 无上限，文件多 / PartialUpdate 时积累严重

**位置**：

- `crates/paimon/src/table/kv_file_reader.rs:323-385`（每个 split 的 `Vec<ArrowRecordBatchStream>`）
- `crates/paimon/src/table/sort_merge.rs:855-1015`（`cursors` + `batch_buffer` + `output_indices` + `same_key_rows`）

**现状**：

- 每个 split 内每个数据文件开一个 stream（`file_streams.push(stream)`，`kv_file_reader.rs:385`），同 split 文件越多，并行驻留的 batch 越多；
- `sort_merge.rs:880` `batch_buffer: Vec<BufferedBatch>` 持有 K 个 cursor 的当前 batch + 任何还被 `output_indices` 引用的旧 batch；只有每次 yield 输出 batch 时 `compact_batch_buffer` 清掉无引用项，**buffer 大小没有显式 cap**；
- `MergeResult::MaterializedRow` 路径（PartialUpdate / VersionedPartialUpdate / Aggregate）会**每 1 个 PK 产生 1 个单行 RecordBatch** push 到 `batch_buffer`（`sort_merge.rs:990`）。一个 1024 行输出 batch 触发 flush 之前可累积 1024 个单行 batch，分配热点；
- 每个 cursor 还持有 `arrow_row::Rows`（PK 列序列化形式，`sort_merge.rs:861`），与 batch 同生命周期；批宽时这份 Rows 可达 PK 列原大小的 0.5 ~ 1x。

**限制方向**：

1. 给同 split 的 K 加上限：实际由 `source.split.target-size` 间接控制，但目前默认 128 MB → 大文件场景 K 较小、小文件多场景 K 极大，建议加 `scan.max-files-per-split` 显式 cap；
2. PartialUpdate / VPU / Aggregate 的 `MaterializedRow` 路径改用**单个累积 BatchBuilder**（沿用 DataFusion 的做法）：每次 merge 完一组同 PK，把结果行直接 append 到一个共享 `RecordBatchBuilder`，不再 push 1 行 batch 到 `batch_buffer`。彻底消除 1024 个单行 batch 同时驻留的情况；
3. 对 `batch_buffer.len()` 加观测（debug log）+ 软上限（超过阈值强制提前 flush）。

---

### P0-3. `DataFileMeta` 在 reader 路径反复深拷贝

**位置**：

- `crates/paimon/src/table/kv_file_reader.rs:270` —— `let splits: Vec<DataSplit> = data_splits.to_vec();`
- `crates/paimon/src/table/kv_file_reader.rs:331` —— `for file_meta in split.data_files().to_vec()`
- 同样的二次 clone 模式也存在于 `data_file_reader.rs`

**为什么放大**：每个 split 的 `data_files: Vec<DataFileMeta>` 都被 `.to_vec()` clone 一份；外层 `splits.to_vec()` 又 clone 整个 split。stats / embedded_index / min_key / max_key 全部跟着复制。文件数多的表（哪怕单文件 meta 不大），整体 clone 开销可观，且这些字段在 reader 路径**完全用不上**（已经过 plan 阶段做完 stats pruning）。

**限制方向**：

1. `DataFileMeta` 改用 `Arc<DataFileMeta>` 共享（spec 层小改动，影响面可控），消除全部深 clone；
2. 或更彻底：把 reader 需要的字段抽成 `ReadFileMeta`（瘦身版），plan 阶段切完 split 后丢弃 stats / embedded_index 仅保留 `ReadFileMeta`。结合 P0-1 的 ReadFileMeta 思路。

---

## P1 — 中等放大点（按场景触发）

### P1-1. DeletionVector 在 split 整段生命周期常驻

**位置**：`crates/paimon/src/deletion_vector/factory.rs:30-57`，调用方 `crates/paimon/src/table/kv_file_reader.rs:309-320`

**现状**：`DeletionVectorFactory::new` 一次性把 split 内**所有数据文件的 DV** 读到 `HashMap<file_name, Arc<DeletionVector>>`，并跟随 reader stream 整个 split 生命周期常驻。RoaringBitmap 通常压缩较好，但 split 内文件多 + 高删除率的场景累积可观（100M 行 × 1% 删除 ≈ 1MB+ / file × N file）。

**限制方向**：

- 改成 lazy：只在打开对应 file 时才 `Self::read(file_io, df).await`，单文件 stream 结束后立即 drop（`Arc` 引用清零）；
- 或维持 eager，但加 `scan.dv-cache-bytes` cap，超过时 lazy。

---

### P1-2. DataFusion 集成不消费 `SessionConfig.batch_size`

**位置**：

- `crates/integrations/datafusion/src/physical_plan/scan.rs:132-174`（`PaimonTableScan::execute`）
- `crates/integrations/datafusion/src/table/mod.rs:196-209`（provider 只读 `target_partitions`）

**现状**：`execute()` 接到 `_context: Arc<TaskContext>` **直接丢弃**，paimon 内部 batch_size 完全由表的 `read.batch-size` option 决定（默认 1024）。用户在 DataFusion `SessionContext::with_batch_size(N)` 设置无效。

**影响**：

- 想全局调小 batch_size 来降单 batch 内存峰值，必须改写表 schema option（侵入持久化状态）；
- DataFusion 上层算子按 `execution.batch_size`（默认 8192）排管线，但 paimon 一直按 1024 输出，下游 batch 会被频繁拼接 / 重切，性能内存双损。

**限制方向**：在 `execute()` 里从 `_context.session_config().batch_size()` 取值，作为动态 override 喂给 `read_builder` —— 配套需要 P1-3 的动态 option 入口。

---

### P1-3. `read.batch-size` / `source.split.target-size` 没有动态 override 入口

**位置**：

- `crates/paimon/src/table/read_builder.rs`（无 `with_dynamic_options`）
- `crates/paimon/src/table/table_scan.rs:606`（直接走 `core_options.source_split_target_size()`，源是持久化 schema option）

**现状**：所有读 option 都从 `table.schema().options()` 取，要改只能 `ALTER TABLE`（`alter_option_demo.rs` 当前的绕路）。Java 提供 `FileStoreTable.copy(dynamicOptions)`，scan 时无副作用覆盖。

**限制方向**：在 `Table` / `ReadBuilder` 加 `with_dynamic_options(HashMap<String, String>)`，scan 时用 `schema().options() ⊕ dynamic` 喂 `CoreOptions::new`，仅作用于读 option（不影响 commit / schema）。

> 该问题已在 [`pk-read-issues.md`](./pk-read-issues.md) 问题 7 跟踪；本 review 把它列入 P1 是因为它是 P1-2 的前置依赖。

---

### P1-4. VersionedPartialUpdate 的 `mv_states` 单 PK 多版本累积

**位置**：`crates/paimon/src/table/versioned_partial_update.rs:161`

**现状**：`mv_states: HashMap<usize, BTreeMap<String, ArrayRef>>` —— 每个 multi-version 列对应一个 BTreeMap，key=version 字符串，value=单行 Arrow array。同一 PK 跨多版本时所有 (version → value) 全部驻留 BTreeMap，直到该 PK 组收齐才 materialise。VPU 表 + 单 PK 高频版本数 → 单 PK 即可累积巨量 ArrayRef。

**限制方向**：仅在 VPU 表场景生效。可加 `mv_states` 总条目软上限（超过 panic 或 truncate 老版本），并对外 expose metric 帮诊断。

---

## P2 — 可选 / 边缘

### P2-1. Parquet page index 默认开启，每文件 ColumnIndex / OffsetIndex 累积

**位置**：`crates/paimon/src/arrow/format/parquet.rs:173-180`，option `read.parquet.page-index.enabled` 默认 `true`（`core_options.rs:412`）

文件多 + 列多时，每个文件打开都 lazy fetch ColumnIndex + OffsetIndex 并驻留在 `ParquetMetaData`。已是 lazy，影响有限，但建议加文档说明：在表文件极多 + 列极多时可关闭以减少 metadata 内存。

### P2-2. 各类无界 cache（schema / manifest / global-index）

- `schema_manager.rs:48` `Arc<Mutex<HashMap<i64, Arc<TableSchema>>>>` —— 历史 schema 全保留
- `referenced_files.rs:93` `ManifestCache` —— 无 evict
- `global_index_scanner.rs:60` `reader_cache: Mutex<HashMap<String, BTreeIndexReader>>` —— 永远只 insert

长生命周期 session（DataFusion 持久化 Table 实例）下会涨。建议改 LRU（`moka` 或 `quick_cache`），单独提供 `cache.size` option。

### P2-3. `read.batch-size` 默认 1024 vs 8192 的 trade-off

Java 默认也是 1024。[`pk-read-issues.md`](./pk-read-issues.md) 问题 3 实测 8192 行/批显著快，但单 batch 行数 ↑ → 单 batch 内存峰值 ↑（`output_indices` cap、`Rows` 大小）。在 P0-2 没修之前，提默认值会让 buffer 峰值线性放大；**先修 P0-2 + P0-3，再考虑提默认**。注意问题 3 还有正确性 bug 待查（10M 表 1024 边界丢 3.75M 行）。

### P2-4. CrossPartition / DynamicBucket 全表 PK HashMap

`bucket_assigner_cross.rs:50` / `bucket_assigner_dynamic.rs:166-228` —— **写路径**初始化阶段全表 PK 加载到 HashMap。读路径不触发，但如果 reader 复用了 `with_scan_all_files()` 后再走写工作流会触发。本次 review **范围内不处理**，仅记录。

---

## 推荐处理顺序（每个独立 PR，方便 bisect）

1. **P0-3**（DataFileMeta Arc 化或 ReadFileMeta 瘦身）← 改动小、收益直接、影响面最局部，建议先做
2. **P0-1**（manifest streaming + buffered 可配 + plan 阶段释放 stats）← 一次大改，需要 spec/avro streaming reader 配合
3. **P0-2**（sort-merge `MaterializedRow` 改 BatchBuilder）← 中等改动，PartialUpdate / VPU / Aggregate 都受益
4. **P1-3**（dynamic options 入口）← P1-2 前置
5. **P1-2**（DataFusion `_context` 透传 batch_size）← 简单，做完 P1-3 后顺手完成
6. **P1-1**（DV lazy / cap）← 视实测 DV 占用决定优先级
7. **P1-4 / P2-** 长尾

---

## 验证方式

每个修改都用现有 jemalloc heap profile 对比前后内存峰值：

```bash
# 已有的 100M 行 8 commit PK MoR 表
MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_prefix:./jeprof \
    cargo run -p paimon --release --features jemalloc-profiling \
        --example read_local_demo -- \
        /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b

jeprof --collapsed ./target/release/examples/read_local_demo ./jeprof.*.heap \
    | flamegraph.pl > heap.svg
```

入口已就绪：`crates/paimon/src/alloc.rs`（jemalloc + `print_stats(label)` + `dump_heap_profile`），`examples/read_local_demo.rs:1129/1141/1145/1167` 已埋多处 stats 打点；环境变量 `PAIMON_MEM_STATS_INTERVAL_SECS` 控制周期采样。

每个 P0 改完后建议跑：

1. `read_local_demo` 的 `mor_primitive_100m_1b`（K-way 小，看 manifest / DataFileMeta clone）；
2. `read_local_demo` 的 `mor_primitive_100m_16b`（16 split 并发，看 DV、batch_buffer）；
3. PartialUpdate 表（专门看 P0-2 的 `MaterializedRow` 路径）。

对比 `print_stats` 的 `allocated / active / resident` 三个数 + flame graph。

---

## Out of scope

- 写路径 `bucket_assigner_*`、`cow_writer`、`data_file_writer` 的内存（用户问的是读）
- DataEvolution / append-only 表（用户问的是 PK）
- 正确性 bug（已在 [`pk-read-issues.md`](./pk-read-issues.md) 跟踪）
- compaction / spill 机制（Java 有但 Rust 还未实现，超出"内存限制"范围）

---

## 关键文件索引

读路径：

- `crates/paimon/src/table/table_scan.rs` —— plan 阶段，P0-1 主战场
- `crates/paimon/src/table/table_read.rs` —— PK / raw 分流入口
- `crates/paimon/src/table/kv_file_reader.rs` —— PK sort-merge 调度，P0-2 / P0-3
- `crates/paimon/src/table/data_file_reader.rs` —— 单文件读
- `crates/paimon/src/table/sort_merge.rs` —— K-way merge + buffer 管理，P0-2
- `crates/paimon/src/table/versioned_partial_update.rs` —— P1-4
- `crates/paimon/src/table/read_builder.rs` —— P1-3 入口
- `crates/paimon/src/spec/core_options.rs` —— option 总入口
- `crates/paimon/src/spec/data_file.rs` —— `DataFileMeta` 定义，P0-3
- `crates/paimon/src/deletion_vector/factory.rs` —— P1-1
- `crates/paimon/src/arrow/format/parquet.rs` —— P2-1
- `crates/integrations/datafusion/src/physical_plan/scan.rs` —— P1-2

诊断工具：

- `crates/paimon/src/alloc.rs`（jemalloc + heap profile）
- `crates/paimon/examples/read_local_demo.rs`（已埋点）
