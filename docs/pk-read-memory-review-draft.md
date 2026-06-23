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

# paimon-rust PK 读路径内存调研（Review 稿）

> Doris 集成读 paimon PK + freshness 模式的内存与吞吐问题。这份文档汇总从 jemalloc 接入到内存模型分析的完整链路，**包含一处此前未公开纠正的归因错误**（§4.5），需要外部 review。
>
> 与 `pk-read-memory-investigation.md` / `pk-read-tuning-knobs.md` 的关系：那两份文档是早期产出，本文是合并 + 纠正后的当前判断，主要用于外部 review。两份原文不变。

## 1. 背景

**场景**：Doris 通过 paimon-rust C 绑定读 paimon PK 表，business 数据是 embedding 表 `video_ann_small_v2`，6.6M 行，主要列是 `FixedSizeList<float, N>`（N 在千~万量级）。Doris 业务约束：必须 freshness 模式 + 16 并发。

**观察到的问题**：默认参数下 Rust 进程峰值 RSS ~336 GiB；同业务 Java 在 20 GB JVM 堆上不 OOM（**这个 anchor 数字未对照，详见 §4.5**）。

**目标**：把单进程峰值压到合理范围（最初目标 5 GiB / split，已确认不可达；当前最佳工作点 65 GiB 全局峰值）。

## 2. 工具落地

### 2.1 jemalloc + heap profiling

在 `paimon` crate 加两个 optional feature（仅 Linux 生效，其它平台 no-op）：
- `jemalloc` → 替换全局分配器 + 暴露 `paimon::alloc::print_stats()`
- `jemalloc-profiling` → 在前者基础上 `--enable-prof`，配 `MALLOC_CONF` 出 `.heap` 文件

两个 example（`read_local_demo` / `validate_id_sum_demo`）顶部加 `#[global_allocator]` + main 起止打 stats + 可选 `PAIMON_MEM_STATS_INTERVAL_SECS=N` 周期打。

**踩过的关键坑**：

| 坑 | 现象 | 修法 |
|---|---|---|
| `tikv-jemalloc-ctl` 0.6 默认无 `stats` 模块 | 编译报 `no \`stats\` in the root` | feature gate `tikv-jemalloc-ctl/stats` + `tikv-jemallocator/stats` |
| `tikv-jemallocator` 加 `_rjem_` 前缀 | 用标准 `MALLOC_CONF` 设 prof 时**不报错也不 dump** | 必须用 `_RJEM_MALLOC_CONF` |
| 切换 feature 时 jemalloc-sys 未重编 | `<jemalloc>: Invalid conf pair: prof:true` | `cargo clean -p tikv-jemalloc-sys` 或全 clean |
| Shell 把 `MALLOC_CONF` export 后跑 `cargo` | rustc 自身 abort | export 前 unset，或者命令前缀单进程 |

**判定 prof 是否真编进 binary**：

```bash
_RJEM_MALLOC_CONF="abort_conf:true,prof:true" \
  ./target/release/examples/read_local_demo --help
# 立刻 abort 报 Invalid conf pair → 没编进，需重编
# 走到 demo 自己的 arg 错 → 已编进
```

### 2.2 heap dump 分析脚本

`scripts/diagnose_heap.sh`：自动挑最大的 `.heap` 作 peak、跑 `jeprof --text/--tree/--svg`、过滤出 `paimon::` / `parquet::` 栈帧、addr2line 反查源码行。

CentOS 7 上系统 jemalloc 是 3.6.0，不带 `jeprof`。从上游拷一份 5.3.0 的 perl 脚本即可：

```bash
curl -sSL https://raw.githubusercontent.com/jemalloc/jemalloc/5.3.0/bin/jeprof.in -o ~/bin/jeprof
sed -i 's/@JEMALLOC_VERSION@/5.3.0/g; s/@jemalloc_version@/5.3.0/g' ~/bin/jeprof
chmod +x ~/bin/jeprof
yum install -y graphviz   # dot，--svg 需要
```

### 2.3 三个可调参数

`read_local_demo` 暴露的 CLI（per-run 通过 `Table::copy_with_options` 注入，不动 catalog）：

- `--parallelism N` → 同时驱动 split 并发 fan-out **和** tokio `worker_threads`，默认 16
- `--sort-merge-soft-cap N` → `read.sort-merge-buffer-soft-cap`
- `--batch-size N` → `read.batch-size`

## 3. 实测数据

载体：`ks_hdp.video_ann_small_rowgroup_v2`（`file.block-size=32mb` 写入，row group 实际 ~60 MB / on-disk），p16 + freshness。

| 配置 | 峰值 allocated | rows/s | batches |
|---|---|---|---|
| 270 MB RG, p16 (原表 baseline) | 336 GiB | 30K | - |
| 270 MB RG, p4 | 78 GiB | 33K | - |
| 60 MB RG, p16, sc=∞, batch=8192 | 108 GiB | 21K | 820 |
| 60 MB RG, p16, sc=4, batch=1024 | 62 GiB | 29K | **595 万 ⚠** |
| 60 MB RG, p16, **sc=8, batch=1024** | **65 GiB** | **32K** | **595 万 ⚠** |
| 60 MB RG, p16, sc=8, batch=8192 | 106 GiB | 29K | 595 万 ⚠ |

**sample row group metadata**（任选一个 .parquet 文件）：

```
row groups: 2
  RG[0]: 11689 rows, 244.4 MB  ← 默认配置时
  RG[1]: 1311 rows,  26.8 MB

row groups: 6                  ← file.block-size=32mb 后
  RG[*]: ~2100 rows, ~57-60 MB
```

单行 ~21-28 KB on-disk（embedding 列 `FixedSizeList<float>` 压缩比差，dict 命不中、zstd 也压不动）。

## 4. 内存模型分析

### 4.1 paimon-rust 这边的并发模型

```
process
├─ tokio runtime (worker_threads=N)
└─ N task （每个 task = 一个 split 子集）
   └─ for split in chunk:
        └─ KeyValueFileReader::read(split)            ← sort-merge with LoserTree
           ├─ Vec<ArrowRecordBatchStream>              ← 一个 stream per data file
           │  ↑ 全部立即实例化，全部同时活跃
           │   crates/paimon/src/table/kv_file_reader.rs:328-417
           └─ sort_merge_stream(streams)
              └─ batch_buffer: Vec<BufferedBatch>     ← 老 source batch 在 cursor 跳动时驻留
                 crates/paimon/src/table/sort_merge.rs:914-958
```

**单 split 同时活跃**：`split.data_files().len()` 个 parquet stream + sort-merge `batch_buffer`。每个 parquet stream 持有 1 个 InMemoryRowGroup 的 column chunk 字节。

**单 process 同时活跃** = `parallelism × split.data_files().len()` 个 InMemoryRowGroup。

### 4.2 parquet-rs 单 stream 内存

读 `parquet-58.3.0/src/arrow/async_reader/mod.rs:817-859`，`ParquetRecordBatchStream::poll_next_inner` 是单 slot 状态机：

```rust
loop {
    match request_state {
        RequestState::None { input } => match decoder.try_decode()? {
            DecodeResult::NeedsData(ranges) => begin_request(input, ranges),
            DecodeResult::Data(batch) => return Ok(Poll::Ready(Some(batch))),
            DecodeResult::Finished => return Ok(Poll::Ready(None)),
        },
        RequestState::Outstanding { ranges, future } => {
            let data = future.await?;
            decoder.push_ranges(ranges, data)?;
        }
    }
}
```

**没有 background prefetch**。Decoder 内部每次只问"当前 row group 的所有 projected column ranges"，row group N 全部行 yield 完才发 N+1 的请求。

> **修正**：本文之前曾说有 "prefetch 双 buffer"，今天看代码才发现是错的。详见 §4.5。

单 stream 同时活的内存：
- `InMemoryRowGroup.column_chunks: Vec<Option<Arc<ColumnChunkData>>>`（压缩字节）
- `array_reader` 的 Arrow buffer 中间累积（≈ batch_size 行 × per-row bytes）
- def/rep level buffer（一般小）

### 4.3 parquet-rs InMemoryRowGroup 内部

代码：`parquet/src/arrow/in_memory_row_group.rs:32-200`

```rust
pub(crate) enum ColumnChunkData {
    Sparse { length, data: Vec<(usize, Bytes)> },   // row_selection + offset_index 都有时
    Dense  { offset, data: Bytes },                  // 默认
}

impl ChunkReader for ColumnChunkData {
    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))   // zero-copy slice
    }
}
```

`SerializedPageReader` 通过 `Arc<ColumnChunkData>` 共享访问，每次 `get_bytes` 返回当前 page 的 `Bytes::slice` —— **零拷贝视图，共享底层 alloc**。Dense 模式下整 column chunk 是单个 `Bytes`；Sparse 模式下每个 page 一个独立 `Bytes`，但 ColumnChunkData 本身 immutable，不能 take 出已读 page。

**关键约束**：partial drop column chunk 字节做不到，必须等 InMemoryRowGroup 整体 drop（即 row group 全部行消费完）才释放。

### 4.4 ArrayReader 的 decode 是流式的

代码：`parquet/src/arrow/record_reader/mod.rs:122-137` + `parquet/src/column/reader.rs:202-280`

```rust
// GenericRecordReader::read_records(batch_size) 内部
loop {
    records_read += self.read_one_batch(records_to_read)?;
    if records_read == num_records || !column_reader.has_next()? { break; }
}

// GenericColumnReader::read_records — 真正 batch-by-batch
while total_records_read < max_records && self.has_next()? {
    let remaining_levels = self.num_buffered_values - self.num_decoded_values;
    // 一次只读 min(remaining_records, 当前 page 剩余) 行
    // page 读完调 read_new_page() 切下一 page
}
```

**Rust 解码端也是 batch-by-batch + page-by-page，不是"一次性解整列"**。每次 `read_records(batch_size)` 内部只把 batch_size 行解到 `values: V::Buffer`。`consume_batch()` take 走后 buffer 释放。

### 4.5 ⚠ 此前未公开纠正的归因错误

`pk-read-memory-investigation.md` §2.3 表格写过：

```
| 维度          | Java parquet-mr            | parquet-rs 58            |
| 解码颗粒度    | page 级（~1 page = ~1 MB） | column-chunk 级（整 RG） |
| 单 stream 状态| ~几十 MB                   | ~10 GiB                  |
| async prefetch | 无                         | 默认预取下一个 row group   |
```

**今天对照源码后，这一行的归因不成立**：

1. **Rust 没有 async prefetch**（§4.2），单 slot 状态机
2. **Rust 解码是 page-by-page 流式的**（§4.4），不是 column-chunk 级
3. **Java 也是 column chunk 字节整体 fetch + slice 视图共享**（下面交叉验证）

Java 源码核对（`apache/parquet-java` 主线）：

```java
// ParquetFileReader.ConsecutivePartList.readAll()
ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
// 一次 IO 拉整 column chunk 字节到 List<ByteBuffer>

// Chunk.readAsBytesInput(size)
return BytesInput.from(stream.sliceBuffers(size));
// slice，不 copy

// Chunk.readAllPages()
new DataPageV1(pageBytes, ...);
// pageBytes 是 slice 视图
```

每个 `DataPage` 持有的 `BytesInput` 是 `ByteBuffer.sliceBuffers()` 的视图，**跟 Rust `Bytes::slice` 行为对称**。

`ColumnChunkPageReader` 用 `ArrayDeque<DataPage>` 维护 page queue，`poll()` 出 page 后 DataPage 自身的 BytesInput 仍指向同一份大 ByteBuffer。**整个 column chunk 字节直到 `ColumnChunkPageReadStore.close()` 才通过 `ByteBufferReleaser` 释放**。

**对照表（修正后）**：

| 项 | Java parquet-mr | parquet-rs 58 |
|---|---|---|
| Fetch 颗粒度 | column chunk 整体 | column chunk 整体 |
| 数据持有结构 | `List<ByteBuffer>` + slice 视图 | `Bytes` + slice 视图 |
| 已读 page 是否 partial drop | **否**（slice 共享底层）| **否**（slice 共享底层）|
| Compressed page queue | `ArrayDeque<DataPage>` 出队丢 ref | `VecDeque<PageLocation>` lazy parse header |
| 解码颗粒度 | page-by-page | page-by-page |
| Async prefetch double-buffer | 无 | **无**（原文档说"有"是错的）|

**两边在 column chunk 字节生命周期上是对称的**。

### 4.6 那 Java 20G vs Rust 336G 的差异到底在哪？

**我们没有 Java 端的 heap profile**。"20G JVM 跑 16 并发" 这个 anchor 数字的来源、对照基准都不明确。可能的真实差异：

#### (a) Vectorize 中间 buffer 是 Rust 特有

heap profile 显示 Rust 这边几个 cum% 较高的栈帧：

```
99.7%  paimon::table::data_file_reader::DataFileReader::read_single_file_stream
 72.5%  parquet::arrow::push_decoder::ParquetPushDecoder::try_decode
 38.4% + 33.6%  GenericRecordReader::read_records
 33.1% + 38.3%  GenericColumnReader::read_records
 29.3%  parquet::arrow::array_reader::byte_array::ByteArrayDecoder::read
  4.6%  parquet::arrow::buffer::offset_buffer::OffsetBuffer::try_push
 27.1%  parquet::arrow::async_reader::RequestState::begin_request
```

`OffsetBuffer<I>::try_push` / `ByteArrayDecoderPlain::read` / `RequestState::begin_request` 都是 **cum%**（inclusive），不是 self/exclusive 内存。实际归属难只靠 jeprof text 判断。但能看出：
- byte_array 列（embedding 应该是 FixedSizeList，但这张表可能还有 string 列）解码到 Arrow `OffsetBuffer<I>` 中间累积
- Rust 的 vectorized decode 把整 row group 的 page 累积到一份 Arrow `ArrayBuilder` 里再 emit

Java 解码到 row-by-row 的 `byte[][]` / `int[]` 上下文对象，没有 Arrow 这层 vectorize 中间产物。

#### (b) JVM 堆计量不含 direct buffer

Java parquet-mr 如果走 `useOffHeapDecryptBuffer` 或 direct buffer 路径，column chunk 字节在 off-heap，**JVM `Runtime.totalMemory()` 不算这部分**。20G JVM heap 里**可能不包含** column chunk 字节。Rust jemalloc allocated 是包含一切的。

如果 Java 进程 RSS 实际也是几十~百 GB，只是 heap 数字看着小，那"20G vs 336G"就是**数字层面的不可比**。

#### (c) sort-merge 同时活跃文件数

paimon-java 的 `KeyValueFileReaderFactory` 在 sort-merge 内同时打开多少个文件？是 lazy 打开还是全部立即打开？需要查 paimon-java 源码确认。

paimon-rust 这边明确是"全部立即实例化"（§4.1）。

#### (d) split 切分粒度

`docs/pk-read-issues.md` 问题 1 已知：paimon-rust 把 100M 行 / bucket=1 / 8 commits 切成 8 个 split，paimon-java 切成 1 个 split。

**反过来**：单个 paimon-java split 内的文件数比 Rust 多，但每个文件状态轻所以总占用低。

#### (e) Doris 端的 task 调度

Doris Java 侧 16 并发的并发单位是什么？是 16 个独立 split 同时跑？还是 1 个 split 用 16 个解码线程？这影响 in-flight column chunk 总数。

**结论**：在没有 Java 端 heap profile 对照前，**不能简单归因于"Rust 解码颗粒度比 Java 粗"**。需要更严格的对照实验。

## 5. 已验证的缓解手段

### 5.1 `file.block-size` 调小（最大杠杆）

写表方一次 ALTER + 重写：

```sql
ALTER TABLE ks_hdp.video_ann_small_v2 SET TBLPROPERTIES ('file.block-size' = '32 mb');
INSERT OVERWRITE ks_hdp.video_ann_small_v2 SELECT * FROM ks_hdp.video_ann_small_v2;
```

- **key 是 paimon 的 `file.block-size`，不是 hadoop `parquet.block.size`**（paimon 不读 hadoop key）
- 实际落盘字节 ≠ 设置值。embedding 列压缩比差，设 32 MB 实际看到 ~60 MB（writer 按未压缩内存估算切，列压缩后磁盘字节会膨胀）
- ALTER 不重写已有文件，必须 OVERWRITE 或 `CALL sys.compact(...)`

效果：270 MB RG → 60 MB RG，p16 峰值 336 → 108 GiB。

### 5.2 `read.sort-merge-buffer-soft-cap`

`sort_merge_stream` 的 `batch_buffer: Vec<BufferedBatch>` 长度上限。代码：`crates/paimon/src/table/sort_merge.rs:1077-1119`。

```rust
let cap_hit = matches!(soft_cap, Some(cap) if batch_buffer.len() >= cap);
if output_indices.len() >= batch_size || cap_hit {
    // build_output_interleave + compact_batch_buffer + yield
}
```

cap 命中触发立即 flush + `compact_batch_buffer` GC 老 batch，buffer 长度被钉在 cap 附近。

**已知副作用**：cap 命中时 `output_indices` 大概率只有几行，每次 yield 出来一个 1-行 RecordBatch。p16 freshness 实测下 batches **595 万 / 6.6M rows ≈ 1.13 行/batch**。

### 5.3 `read.batch-size`

两处用同一个值：
- parquet stream 端：`with_batch_size`（控制 yield RecordBatch 行数）—— **不控制 row group decode buffer**（issue [arrow-rs#5523](https://github.com/apache/arrow-rs/issues/5523)）
- sort-merge 端：size flush 触发条件

实测发现：**batches 数与 batch_size 无关**（都是 595 万），因为 cap-hit 几乎完全主导 flush 决策。batch_size 调大反而内存涨（每个 BufferedBatch 持有的行数变多）。

### 5.4 协同矩阵

|  | parallelism | sort-merge soft-cap | batch-size |
|---|---|---|---|
| 控内存 | 线性，Doris 不可调 | 强相关，主旋钮 | 弱（仅与 sc 一起调小有效）|
| 控吞吐 | >8 后边际递减 | 凹曲线，sc=8 局部最优 | sc=∞ 时大 batch 优；sc 小时影响小 |
| 控 batches 数 | 无关 | 强相关（cap-hit 主导）| 无关（cap-hit 主导）|

**Doris 集成首发配置**：

```rust
"file.block-size"                    = "32 mb"
"deletion-vectors.read-mode"         = "freshness"
"read.sort-merge-buffer-soft-cap"    = "8"
"read.batch-size"                    = "1024"
parallelism                          = 16
```

预期：~65 GiB 峰值、~32K rows/s、~595 万 batches。

## 6. 失败的尝试：cap-hit coalescing

### 6.1 思路

让 cap 触发的 flush 不直接 yield，先把 interleave 出来的 RecordBatch 存到 `pending_partial: Vec<RecordBatch>`，行数累积到 batch_size 才 `concat_batches` + yield 一次。`compact_batch_buffer` 提前跑（partial 是 interleave 出的 self-contained 数据，不再持有 source 引用），cap 的内存上限承诺保持不变。

实现 commit：`3e47ade fix(sort-merge): coalesce cap-hit partials back to batch_size`。

### 6.2 实测

| 指标 | 旧 sc=8 batch=1024 | 新 sc=8 batch=1024 | 变化 |
|---|---|---|---|
| 峰值 | 65 GiB | 62 GiB | -5% ✅ |
| **batches** | 595 万 | 6393 | **-99.9%** ✅ |
| 吞吐 | 32K rows/s | **16K rows/s** | **-50%** ❌ |

batches 修对了，但**吞吐砍半**。已 revert（commit `f2bc1b0`）。

### 6.3 吞吐回归原因

每个 1024 行 batch 平均由 595万 / 6393 ≈ **931 个 partial concat 而成**。每个 partial ~1 行 × ~28 KB（embedding 列），一次 1024 行 batch concat：

- 931 次小内存分配 + 释放
- 重新拷贝 ~28 MB 数据（每次 concat 都新建 column buffer）
- 加上 cap-hit 高频时 `build_output_interleave + compact_batch_buffer` 比旧路径多走一次列拷贝

旧实现虽然产 595 万小 batch，但每个 batch 的下游 dispatch 是 1 行 × 28 KB 的 Arc clone，便宜；新实现把这些拷贝集中到 sort-merge 这一层，从单机视角看反而更重。

### 6.4 真正的修法（未实现）

cap-hit 时不 interleave 全列，只把 `output_indices` 涉及的 source 行通过 `arrow_select::take` 摘出来作为新 `BufferedBatch::Source` push 回 buffer，output_indices 重映射到 `(new_idx, 0..N)`。等 output_indices 满 batch_size 时一次性 interleave。

工作量：在 `sort_merge_stream` 里管理"已 take 但未 yield 的 staging buffer"，与 `materialized_slot` / `compact_batch_buffer` / `stream_batch_idx` 三套现有索引共存。约当前实现的 2-3 倍 + 补 take-path 测试覆盖。

**判断依据**：取决于 Doris 集成实测是否发现 595 万 batch 的 dispatch overhead 是 CPU 瓶颈。

## 7. 「能否 partial drop column chunk 字节」专题

### 7.1 当前实现的物理约束

参见 §4.3。Dense 模式下整 column chunk 是单个 `Bytes`，page 是 slice 视图共享底层 alloc。slice 单独 drop **不会**释放底层（Java 同理 §4.5）。

### 7.2 要做 partial drop 必须做到

```
fetch 端：每个 page 落到独立 allocation
  - 要么按 page 独立 IO（对象存储 N 倍延迟，arrow-rs PR #1617 反对）
  - 要么 fetch 完整体后做一次 copy 拆 page（CPU + 一次额外内存）

decode 端：API 改成独占消费
  - ChunkReader: &mut self
  - PageReader 内部持有 ColumnChunkData ownership
  - 每读完一页主动 take 出 drop
```

是 arrow-rs 上游级别的工作。issue [arrow-rs#5331](https://github.com/apache/arrow-rs/issues/5331) 期望方向之一，但**至今无 PR**。

### 7.3 不能这么做的潜在收益

理论上单 row group 解码到一半时已读 page 字节能释放 → 节省"消费过半到 row group 结束"这段时间的峰值，最多 **50% / row group / 列**。

但要付出 fetch 阶段一次额外拷贝或 IO 颗粒度变细的代价。**对对象存储场景成本/收益不划算**，这是上游为什么没人做的原因。

## 8. 未来工作（按 ROI 排）

| 路径 | 估算单 split 峰值 | 工作量 | 风险 |
|---|---|---|---|
| 写表方 `file.block-size=16mb` | ~3 GiB / split | 1 行配置 + OVERWRITE | 低 |
| paimon 库 `with_row_groups([N])` 串行单文件 | ~2 GiB / split（jemalloc 还内存给 OS 时机更紧）| ~80-150 行 | 中（影响所有 parquet 读路径）|
| sort-merge cap-hit coalescing（take 版本）| 不变 | ~200 行 | 中（破坏现有 sort-merge orchestration）|
| Doris 端 task 调度复用线程池而非 16 独立 | 跟现有相同 | Doris 侧改 | 中 |
| paimon 库限 split 内活跃 stream 数 | ~1 GiB / split | 大（破坏 N-way merge 语义）| 高 |
| 切到 `ParquetPushDecoder` + `buffered_bytes()` 限流 | 任意硬上限 | ~300 行 + 重接 filter/selection | 高（性能可能下降 5-10%）|
| arrow-rs 上游 partial drop column chunk | 单 RG 内峰值减半 | 上游 PR | 长期 |
| 拿 Java heap profile 对照 | — | 业务侧配合 | — |

## 9. 仓库改动清单

`jemalloc` 分支领先 `origin/master` 13 commits：

```
8425113 docs(read): PK read memory investigation notes
09cac42 docs(read): tuning-knob reference for PK read parameters
f2bc1b0 Revert "fix(sort-merge): coalesce cap-hit partials back to batch_size"
3e47ade fix(sort-merge): coalesce cap-hit partials back to batch_size
ba9712c feat(examples): expose --sort-merge-soft-cap on read_local_demo
4e7893f chore(scripts): add diagnose_heap.sh for jemalloc heap dumps
4ccd68c feat(examples): make read_local_demo parallelism configurable
7ac00e1 fix(alloc): enable stats/profiling sub-features on jemalloc-ctl
b35945e feat(alloc): add optional jemalloc allocator + heap profiling hooks
```

工具改动可合主线；调研结论文档 §2.3 待按本 review 稿 §4.5 纠正。

## 10. 需要 review 的关键点

请重点审视：

1. **§4.5 的归因纠正**：之前文档说 Rust 比 Java 内存高是因为"column-chunk 级 vs page 级解码"。今天对照源码后这一点不成立，两边架构对称。**这个纠正本身是否正确？我看的源码版本是否有偏差？**

2. **§4.6 的剩余归因猜测**：在没有 Java 端 heap profile 对照前，列了 5 个可能差异点（vectorize buffer / direct buffer 不计入 JVM heap / sort-merge 文件数 / split 粒度 / Doris 调度）。**哪些可以排除？哪些应该优先验证？**

3. **§6 cap-hit coalescing 的失败归因**：把吞吐砍半归因于"931 次小 concat"。**这个数量级估算是否合理？有没有其它可能的原因（cache 局部性、context switch、jemalloc arena 行为）？**

4. **§7 partial drop 的可行性判断**：结论是"上游没人做是因为对象存储场景成本/收益不划算"。**这个判断是否成立？有没有反例（比如 Spark / Trino 这类已经在内存敏感场景跑 parquet 的方案是怎么处理的）？**

5. **§8 ROI 排序**：写表方调小 row group 排第一，paimon 库改 `with_row_groups` 排第二。**这个排序是否合理？是否漏掉了什么 sub-GB 量级的旋钮？**

## 11. 引用

- [arrow-rs PR #1617: Implement async parquet reader](https://github.com/apache/arrow-rs/pull/1617)
- [arrow-rs#5523: parquet-rs always loads complete row groups in memory](https://github.com/apache/arrow-rs/issues/5523)
- [arrow-rs#5331: Add option to ParquetRecordBatchStream to limit memory usage](https://github.com/apache/arrow-rs/issues/5331)
- [parquet-java ParquetFileReader.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java)
- [parquet-java ColumnChunkPageReadStore.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnChunkPageReadStore.java)
- [parquet-rs SerializedPageReader docs](https://docs.rs/parquet/latest/parquet/file/serialized_reader/struct.SerializedPageReader.html)
- [parquet-rs ParquetPushDecoder docs](https://docs.rs/parquet/latest/parquet/arrow/push_decoder/struct.ParquetPushDecoder.html)
- [Paimon CoreOptions documentation — file.block-size](https://paimon.apache.org/docs/master/maintenance/configurations/)
