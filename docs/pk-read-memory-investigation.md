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

# paimon-rust PK 读路径内存调研

## 背景

Doris 集成场景：单进程 16 并发读 paimon PK 表（embedding 表 `video_ann_small_v2`，~7M 行，FixedSizeList<float, N> 大列）。当前 paimon-rust 读取峰值 RSS 远超对应 Java 实现（Java 20 GB JVM 跑 16 并发不 OOM；Rust 默认参数峰值 ~336 GiB）。

本文记录从「接入 jemalloc → 定位瓶颈 → 尝试缓解 → 暂不修」的全过程，以及当前已落入仓库的能复现/A-B 工具与配置建议。

## 1. 排查工具落地

### 1.1 jemalloc + heap profiling 接入

PR：`feat(alloc): add optional jemalloc allocator + heap profiling hooks` 系列。

- workspace 加 `tikv-jemallocator` / `tikv-jemalloc-ctl` 0.6 版本（Linux-only optional dep，`target_os = "linux"` 隔离 macOS/Windows）。
- 新增 `paimon::alloc` 模块：暴露 `Jemalloc` 全局分配器、`print_stats` / `dump_heap_profile`。feature 关闭时是 no-op。
- 两个排查 example（`read_local_demo` / `validate_id_sum_demo`）顶部 `#[global_allocator]`、main 起止打 stats；`PAIMON_MEM_STATS_INTERVAL_SECS=N` 可周期打。
- 两个 feature：`jemalloc`（替换 allocator）、`jemalloc-profiling`（追加 `--enable-prof`）。

### 1.2 运行配方

构建 + 跑：

```bash
unset MALLOC_CONF   # 注意：MALLOC_CONF 会让 rustc 自身 abort
cargo build -p paimon --release \
  --features jemalloc-profiling,storage-hdfs \
  --example read_local_demo

_RJEM_MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:30,prof_prefix:./jeprof,prof_final:true" \
PAIMON_MEM_STATS_INTERVAL_SECS=10 \
./target/release/examples/read_local_demo \
  --parallelism 16 --read-mode freshness \
  viewfs://.../<table_path> \
  2>&1 | tee run.log
```

注意：tikv-jemallocator 给所有符号加 `_rjem_` 前缀，**环境变量必须是 `_RJEM_MALLOC_CONF` 而不是标准 `MALLOC_CONF`**，否则 jemalloc 不读它（但不报错，dump 文件不产生 — 这个坑卡了很久）。

诊断 jemalloc 是否真编进 prof：

```bash
_RJEM_MALLOC_CONF="abort_conf:true,prof:true" \
  ./target/release/examples/read_local_demo --help
# 若立刻 abort 并报 "Invalid conf pair: prof:true" → binary 没带 prof，cargo clean 重编
# 若走到 demo 自己的 arg 解析错 → prof 已编进
```

### 1.3 heap dump 分析脚本

`scripts/diagnose_heap.sh`：自动挑最大的 `.heap` 当 peak，跑 `jeprof --text/--tree/--svg`，输出到 `heap_diag/`，并过滤出 `paimon::` / `parquet::` 等模块的栈帧。

CentOS 7 上 jeprof 不能 yum 装（jemalloc 3.6.0 没附带），从上游拷一份即可：

```bash
curl -sSL https://raw.githubusercontent.com/jemalloc/jemalloc/5.3.0/bin/jeprof.in -o ~/bin/jeprof
sed -i 's/@JEMALLOC_VERSION@/5.3.0/g; s/@jemalloc_version@/5.3.0/g' ~/bin/jeprof
chmod +x ~/bin/jeprof
yum install -y graphviz   # dot，需要画 SVG 用
```

### 1.4 example 新增可调参数

- `--parallelism N`：替代原 `const PARALLELISM = 16`，同时驱动 split 并发 fan-out 和 tokio `worker_threads`。
- `--sort-merge-soft-cap N`：暴露 paimon 库 `read.sort-merge-buffer-soft-cap` 选项，per-run 通过 `Table::copy_with_options` 注入，不动 catalog。

## 2. 内存模型与定位

### 2.1 单 split 实测峰值定位

heap profile peak（315 GiB 时刻）的 paimon/parquet 栈帧分布：

```
99.7%  paimon::table::data_file_reader::DataFileReader::read_single_file_stream
99.9%  paimon::table::kv_file_reader::KeyValueFileReader::read
 72.5%  parquet::arrow::push_decoder::ParquetPushDecoder::try_decode
 38.4% + 33.6%  GenericRecordReader::read_records
 33.1% + 38.3%  GenericColumnReader::read_records
 29.3%  parquet::arrow::array_reader::byte_array::ByteArrayDecoder::read
 27.1%  parquet::arrow::async_reader::RequestState::begin_request
```

**结论**：内存堆在 parquet-rs 的 row-group / column-chunk 解码缓冲，不是 paimon 自己的逻辑。

### 2.2 写表方 row group 实测

`/home/relay/yuzhaojing/data-ba7abd75-...-9.parquet`：

```
row groups: 2
  RG[0]: 11689 rows, 244.4 MB (on-disk)
  RG[1]: 1311 rows,  26.8 MB
```

单行 ~21 KB（embedding 列 FixedSizeList<float> 压缩比很差，dict 命不中、zstd 也压不动）。**解码后 244 MB row group 占 ~10 GiB 内存**（膨胀 40-80×）。

### 2.3 Java vs Rust 的根本差异

| 维度 | Java parquet-mr | parquet-rs 58 |
|---|---|---|
| 解码颗粒度 | **page 级**（~1 page = ~1 MB / 列） | **column-chunk 级**（整个 row group / 列）|
| 单 stream 活跃状态 | ~1 page × M columns ≈ 几十 MB | ~1 row group × M columns ≈ ~10 GiB |
| async prefetch | 无，按需读 | 默认预取下一个 row group |
| 单 split N 文件 sort-merge | N × ~50 MB | N × ~10 GiB |

这是当前 parquet-rs 实现的**架构约束**：

- [arrow-rs#5523](https://github.com/apache/arrow-rs/issues/5523) — "parquet-rs always loads complete row groups in memory"
- [arrow-rs#5331](https://github.com/apache/arrow-rs/issues/5331) — "Add option to ParquetRecordBatchStream to limit memory usage"（feature request，未实现）

parquet-rs 58 引入了 `ParquetPushDecoder`，提供 caller-controlled fetch 和 `buffered_bytes()` 接口，但 paimon 当前用的是 `ParquetRecordBatchStreamBuilder`（pull 模式，内部仍按 row group 决策）。改成 push decoder + 应用层限流是改 paimon 库 +300 行的工作量。

`SerializedPageReader` 是公开 API 且 page 级（参考 [docs.rs](https://docs.rs/parquet/latest/parquet/file/serialized_reader/struct.SerializedPageReader.html)），但**不接 Arrow** — 要 page-level + Arrow 需要自己写 page → RecordBatch 拼装，相当于重写半个 `parquet::arrow::arrow_reader`，不在 paimon-rust 应当承担的范围。

## 3. 已验证的缓解手段

实测载体：`ks_hdp.video_ann_small_v2` (默认 row group ~270 MB) / `ks_hdp.video_ann_small_rowgroup_v2` (`file.block-size=32mb` 写入，实际 ~60 MB)，p16 + freshness。

| 配置 | 峰值 allocated | rows/s | batches |
|---|---|---|---|
| 270 MB RG, p16, baseline | 336 GiB | 30K | - |
| 270 MB RG, p4 | 78 GiB | 33K | - |
| 60 MB RG, p16, baseline | 108 GiB | 21K | 820 |
| 60 MB RG, p16, sc=4, batch=1024 | **62 GiB** | 29K | **595 万** ⚠ |
| 60 MB RG, p16, sc=8, batch=1024 | 65 GiB | **32K** | 595 万 ⚠ |
| 60 MB RG, p16, sc=8, batch=8192 | 106 GiB | 29K | 595 万 ⚠ |

### 3.1 减小 row group：`file.block-size=32mb`

最显著的杠杆（峰值 336 → 108 GiB）。注意几点：

- 选项 key 是 paimon 的 `file.block-size`，**不是** hadoop 标准的 `parquet.block.size`。Paimon 不读 hadoop 的 key。
- 实际落盘的 row group 字节 ≠ 设置值。embedding 列压缩比差，设 32 MB 实际看到 ~60 MB（writer 按未压缩内存估算切，列压缩后磁盘字节会膨胀）。
- ALTER 不会重写已有文件，要 `INSERT OVERWRITE` 或 `CALL sys.compact(...)` 触发。

### 3.2 降并发：`--parallelism N`

线性杠杆，但 Doris 集成约束 ≥16，所以这条只对排查 / 离线读有意义。

### 3.3 sort-merge soft-cap：`read.sort-merge-buffer-soft-cap=8`

`sc=8 batch=1024` 是当前最佳工作点：峰值 65 GiB、吞吐 32K rows/s。

**但 batches = 595 万**（每 batch 平均 ~1 行）— 这是 paimon-rust 当前 soft-cap 实现的副作用：cap 命中立即 yield 出当前累积的 output_indices，而不是攒到 batch_size 再 yield。

## 4. 尝试修 batches 副作用（未合入）

### 4.1 思路：cap-hit coalescing

让 cap 触发的 flush 不直接 yield，先把 interleave 出来的 RecordBatch 存到 `pending_partial: Vec<RecordBatch>`，行数累积到 batch_size 才 concat + yield 一次。`compact_batch_buffer` 提前跑（partial 是 interleave 出的 self-contained 数据，不再持有 source 引用），cap 的内存上限承诺保持不变。

实现 commit：`fix(sort-merge): coalesce cap-hit partials back to batch_size`（已 revert）。

### 4.2 实测结果

| 指标 | 旧 sc=8 batch=1024 | 新 sc=8 batch=1024 | 变化 |
|---|---|---|---|
| 峰值 | 65 GiB | 62 GiB | -5% ✅ |
| **batches** | **595 万** | **6393** | **-99.9%** ✅ |
| 吞吐 | 32K rows/s | **16K rows/s** | **-50%** ❌ |

batches 修对了，但**吞吐砍半**。

### 4.3 吞吐回归原因分析

每个 1024 行 batch 平均由 595万 / 6393 ≈ **931 个 partial concat 而成**。每个 partial ~1 行 × ~28 KB（embedding 列），concat 一次：

- 931 次小内存分配 + 释放
- 重新拷贝 ~28 MB 数据（每次 concat 都新建 column buffer）
- 加上 cap-hit 高频时 `build_output_interleave + compact_batch_buffer` 比旧路径多走一次列拷贝

旧实现虽然产 595 万小 batch，但每个 batch 的 dispatch 是 1 行 × 28 KB 的 Arc clone，便宜；新实现把这些拷贝集中到 sort-merge 这一层，从单机视角看反而更重。

### 4.4 真正的修法（暂未做）

cap-hit 时不 interleave 全列，只把 output_indices 涉及的 source 行通过 `arrow_select::take` 摘出来作为新 `BufferedBatch::Source` push 回 buffer，output_indices 重映射到 (new_idx, 0..N)。等 output_indices 满 batch_size 时一次性 interleave，避免 931 次 concat。

工作量：在 `sort_merge_stream` 里管理"已 take 但未 yield 的 staging buffer"，与 `materialized_slot` / `compact_batch_buffer` / `stream_batch_idx` 三套现有索引共存。约当前实现的 2-3 倍，并且要补 take-path 的测试。

**判断**：是否值得做取决于 Doris 集成的实测：
- 如果 Doris 那边发现 595 万 batch dispatch 是 CPU 瓶颈 → 必做
- 如果 Doris dispatch 不是瓶颈 → 当前 `sc=8 batch=1024` 旧实现的 32K rows/s 直接上车

## 5. 当前推荐参数（Doris 集成初始值）

```rust
// table options（写表方一次 ALTER + compact）
"file.block-size"                    = "32 mb"   // 重写表，强制 row group ≤ 60 MB
"deletion-vectors.read-mode"         = "freshness"   // 业务约束
"read.sort-merge-buffer-soft-cap"    = "8"       // sort-merge buffer 上限
"read.batch-size"                    = "1024"

// reader 侧
parallelism = 16   // Doris 线程约束
```

预期单进程稳态：~65 GiB 峰值、32K rows/s、595 万 batches。

## 6. 未来工作 / 已知 trade-off

按 ROI 排：

1. **`file.block-size` 调到 16 MB**：每次 ALTER + INSERT OVERWRITE 大表代价不小，但能再压一半峰值（~30-40 GiB）。需要写表方协调。
2. **Doris 集成实测验证 595 万 batch 是否真的是瓶颈**：决定要不要做 §4.4 的 take-staging 重构。
3. **paimon 库按 row group 切 stream**：单 stream 用 `ParquetRecordBatchStreamBuilder::with_row_groups([N])` 串行解码 N 个 row group，掐 prefetch，让单 stream 内存从"当前 RG + 预取下一个"降到"只当前 RG"。约 80-150 行库改动，影响 parquet 全读路径。
4. **paimon 库限 sort-merge 同时活跃 stream 数**：现在 `split.data_files()` 内全部 N 个文件同时实例化为 stream。如果文件间 key range 不重叠可以做滑动窗口，但需要写表方在 `DataFileMeta` 里记录 key range — 是 paimon 写表侧的工作，非短期能做。
5. **切换到 `ParquetPushDecoder` + 应用层限流**：根治路径，能把单 split 内存压到任意硬上限（如 1 GiB）。约 +300 行库改动，要重新接 row filter / row selection / page index / bloom filter，性能可能下降 5-10%（pull mode 有内部 prefetch 流水线）。这是中长期方向，不是短期目标。

## 7. 引用

- [arrow-rs#5523: parquet-rs always loads complete row groups in memory](https://github.com/apache/arrow-rs/issues/5523)
- [arrow-rs#5331: Add option to ParquetRecordBatchStream to limit memory usage](https://github.com/apache/arrow-rs/issues/5331)
- [parquet::arrow::push_decoder::ParquetPushDecoder docs](https://docs.rs/parquet/latest/parquet/arrow/push_decoder/struct.ParquetPushDecoder.html)
- [parquet::file::serialized_reader::SerializedPageReader docs](https://docs.rs/parquet/latest/parquet/file/serialized_reader/struct.SerializedPageReader.html)
- [Paimon CoreOptions documentation — `file.block-size`](https://paimon.apache.org/docs/master/maintenance/configurations/)
