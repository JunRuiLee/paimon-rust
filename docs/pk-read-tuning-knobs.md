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

# paimon-rust PK 读三大调参

围绕 PK 表 freshness 读路径上三个可调参数：
- `parallelism`（reader 侧 / `--parallelism N`）
- `read.sort-merge-buffer-soft-cap`（表选项 / `--sort-merge-soft-cap N`）
- `read.batch-size`（表选项 / `--batch-size N`）

本文交代每个参数**作用在哪一层、影响哪些指标、为什么会有这种影响**，并给出 Doris 集成场景下的取值建议。背景与全链路排查见 [`pk-read-memory-investigation.md`](pk-read-memory-investigation.md)。

## 1. 数据来源

实测载体：`ks_hdp.video_ann_small_rowgroup_v2`（embedding 表，`file.block-size=32mb` 写入、实际 row group ~60 MB、6.6M 行），读模式 `deletion-vectors.read-mode=freshness`。所有结果出自 `read_local_demo` + `_RJEM_MALLOC_CONF=prof:..., prof_active:true,...` + `PAIMON_MEM_STATS_INTERVAL_SECS=10`。

| 参数组合 | 峰值 allocated | rows/s | batches |
|---|---|---|---|
| p16, sc=∞, batch=8192（baseline）| 108 GiB | 21K | 820 |
| p16, sc=4, batch=1024 | 62 GiB | 29K | 595 万 |
| p16, sc=8, batch=1024 | **65 GiB** | **32K** | 595 万 |
| p16, sc=8, batch=8192 | 106 GiB | 29K | 595 万 |
| p4,  sc=∞, batch=8192 | ~25 GiB（外推）| ~25K（外推）| ~205 |

后文围绕这张表逐个参数解释。

## 2. `parallelism` — split 并发 + tokio worker_threads

### 2.1 作用层级

读 example 用一个常量 N（默认 16）同时驱动两件事：
- **split 并发 fan-out**：`process_one_table` 把 `Plan.splits()` round-robin 切到 N 个 chunk，每个 chunk 用一个独立 `TableRead` + `tokio::spawn` 跑（`crates/paimon/examples/read_local_demo.rs:866`）。
- **tokio runtime worker_threads**：`Runtime::Builder::new_multi_thread().worker_threads(N)`（`read_local_demo.rs:1198` 附近）。两者相等才不至于让 tokio 调度饿死或线程超额。

### 2.2 影响

| 指标 | 方向 | 量级 |
|---|---|---|
| 峰值 RSS | **线性** | 16 路 → 1 路约 16× |
| 吞吐 | 单调递增、有 IO/decode 上限 | 16→4 路吞吐降 ~25-30% |
| batches 总数 | 不变 | 取决于 batch_size，与 parallelism 无关 |

实测两组对照：

| | 270 MB row group | 60 MB row group |
|---|---|---|
| p16 | 336 GiB / 30K rows/s | 108 GiB / 21K rows/s |
| p4 | 78 GiB / 33K rows/s | ~25 GiB（线性外推） |

p4 比 p16 内存少 4×，吞吐反而更高 —— 16 路下 page fault / context switch / jemalloc arena 争抢比 4 路明显。

### 2.3 原因

每条并发跑的是**一个独立 split 的 sort-merge stream**。单 split 内部又有 K 个文件 parquet stream 同时存在（sort-merge 必须从所有输入流同时拿当前最小 key）。所以：

```
峰值 ≈ parallelism × split内文件数 K × 单文件 parquet decode buffer
     ≈ parallelism × K × (~1 row group 解码后的字节)
```

embedding 表里单 row group 60 MB 解码后 ~2 GiB（膨胀 ~35×），K=2-3 时 16 路就是 ~100-200 GiB，跟实测 108 GiB 对得上。

### 2.4 Doris 集成约束

Doris 业务约束 ≥16 路，所以这个旋钮在 Doris 集成里**不能调**，只能在排查 / 离线读时用来确认线性关系或临时止血。

## 3. `read.sort-merge-buffer-soft-cap` — sort-merge buffer 长度上限

### 3.1 作用层级

`crates/paimon/src/table/sort_merge.rs::sort_merge_stream` 的 `batch_buffer: Vec<BufferedBatch>` 长度上限。每条 stream 的"当前 batch"驻留在 `batch_buffer` 直到 cursor 切到下个 batch，且**老 batch 可能在 cursor 来回引用时延迟释放**。

当前实现的 cap 行为（`sort_merge.rs:1077-1119`，cap-hit 路径）：
```rust
let cap_hit = matches!(soft_cap, Some(cap) if batch_buffer.len() >= cap);
if output_indices.len() >= batch_size || cap_hit {
    // ... build_output_interleave + compact_batch_buffer + yield batch
}
```

注意：`cap_hit` 触发的 yield **不等到 output_indices 累积满 batch_size**，立刻产出当前 output_indices 对应的小 batch。

### 3.2 影响

| 指标 | 方向 | 量级 |
|---|---|---|
| 峰值 RSS | **强相关**（线性 + 阈值效应）| sc=4 vs sc=∞ 在 16 路 freshness 上少 40% 内存 |
| 吞吐 | 非单调 | sc=8 比 sc=4、sc=∞ 都更快（局部最优）|
| batches 总数 | **暴增** | 595 万（应该 6.5K 量级）|

sc 取值的实测对比（p16, batch=1024, freshness）：

| sc | 峰值 | rows/s | batches |
|---|---|---|---|
| ∞ (`baseline`)| 108 GiB | 21K | 820 |
| 8 | 65 GiB | 32K | 595 万 |
| 4 | 62 GiB | 29K | 595 万 |

### 3.3 原因

#### 3.3.1 为什么 cap 能压住内存

不设 cap 时，`batch_buffer` 可以远超 `num_streams` 长度：cursor 在新批和旧批间反复跳动时，多个旧批同时驻留。每个 BufferedBatch 持有一个完整 source RecordBatch（embedding 列单批 ~28 MB），buffer 涨到几十个就是 GiB 级。

cap 命中后立刻 `compact_batch_buffer` —— 把没有 cursor 引用的 batch GC 掉，下次循环 buffer 长度就回到接近 cap 的水位。peak buffer length 被钉在 cap 附近。

#### 3.3.2 为什么吞吐曲线呈"凹"

| 区间 | 主导成本 | 现象 |
|---|---|---|
| sc 太大（如 ∞）| memory pressure | jemalloc 频繁向 OS 申请/归还、page fault、L3 miss 多。21K rows/s |
| sc 中等（sc=8）| 平衡 | 内存压力降下，但 cap-hit 频率还没高到 dispatch 拖死。32K rows/s |
| sc 太小（sc=4）| dispatch 高频 | cap 触发更频繁，下游 batch 数量翻倍，单 batch decode/dispatch overhead 占比变大。29K rows/s |

实际从 sc=8 到 sc=4，吞吐反而降 10% —— sc 太小不一定更好。

#### 3.3.3 为什么 batches=595 万 ⚠

cap-hit 触发 yield 时，`output_indices` 大概率只有几行（cap 命中前 merge 循环刚 push 完一行就检测到 buffer 满）。这一行被 interleave 出来一个 1 行 RecordBatch 直接 yield 给下游。后续 cap 触发频繁 → 几乎每行一个 batch。

595 万 batches / 6.6M rows ≈ 1.13 行/batch，与上述模型吻合。

**已知副作用，但尚未修**：

- 尝试过 `cap-hit coalescing`（commit `3e47ade`），把 cap-hit 产出的小 batch 攒到 `pending_partial`，行数够 batch_size 再 concat 一次性 yield。
- 实测：batches 595万→6393（修对），但吞吐 32K→16K（砍半，因每个 1024 行 batch 由 ~931 次 concat 拼出来，列拷贝代价反而高于"小 batch 直接 dispatch"）。
- 已 revert (commit `f2bc1b0`)。真正的修法（cap-hit 时用 `arrow_select::take` 摘行 staging，不做 concat）见 `pk-read-memory-investigation.md` §4.4。

### 3.4 取值建议

- 不设：峰值不可控，仅适用于 batch-size 足够大 + 内存充裕的场景
- sc=8：当前甜区。Doris 集成默认值
- sc=4：进一步压内存，但吞吐回落 10%
- sc<4：意义不大，多数 cursor 排队等切批，sort-merge 频繁让位给 compact

## 4. `read.batch-size` — sort-merge 输出 RecordBatch 行数目标

### 4.1 作用层级

两处用到同一个值，需要分开看：

#### 4.1.1 parquet reader（writer 端）

`crates/paimon/src/arrow/format/parquet.rs:253-255`：
```rust
if let Some(size) = batch_size {
    batch_stream_builder = batch_stream_builder.with_batch_size(size);
}
```

直接传给 `ParquetRecordBatchStreamBuilder::with_batch_size`。**只控制单个 parquet stream yield 出来的 RecordBatch 行数，不影响 parquet 内部 row group decode buffer**（issue [arrow-rs#5523](https://github.com/apache/arrow-rs/issues/5523)：parquet-rs always loads complete row groups in memory）。

#### 4.1.2 sort-merge 输出（合并端）

`sort_merge.rs:1078` 的 size 触发条件：
```rust
if output_indices.len() >= batch_size || cap_hit {
    // ... yield
}
```

是 sort-merge yield 一个 RecordBatch 的"正常"触发条件。问题：cap-hit 大概率比 size 先触发。

### 4.2 影响

p16 + sc=8 + freshness 三组对比：

| batch_size | 峰值 | rows/s | batches |
|---|---|---|---|
| 1024 | 65 GiB | 32K | 595 万 |
| 8192 | 106 GiB | 29K | 595 万 |

**反直觉的结论**：

- batches 数**与 batch_size 无关** —— 都是 595 万。说明 sort-merge 几乎完全走 cap-hit 路径，size 阈值根本碰不到。
- batch_size 调大反而内存翻倍。原因：sort-merge 内每个 stream 当前 batch 行数 ~= batch_size（parquet 那一层吐出来），batch_size=8192 时每个 BufferedBatch 是 8192 × 28 KB = ~220 MB；sc=8 时 buffer 长度 8 × 220 MB = ~1.7 GiB / split × 16 split = ~28 GiB（还没算 parquet decode 自己的 buffer）。

### 4.3 原因

#### 4.3.1 为什么 batches 与 batch_size 无关

只要 cap-hit 频繁触发，sort-merge yield 出去的 batch 几乎都是"output_indices 还没攒满就被强制 flush"的 1-2 行 batch。batch_size=1024 和 batch_size=8192 在 cap-hit 路径上**等价**，都还没机会到达 size 阈值。

#### 4.3.2 为什么 batch_size 影响内存

parquet stream 那边每次 yield 出来的 RecordBatch 大小直接由 batch_size 决定。这个 RecordBatch 被 push 进 sort-merge 的 batch_buffer 作为 `BufferedBatch::Source`，sc=8 时同时驻留 8 个，**单 split sort-merge buffer ≈ 8 × batch_size 行的数据**。

embedding 列单行 28 KB：
- batch_size=1024：~28 MB × 8 = ~224 MB / split × 16 = ~3.6 GiB（占 sc=8 baseline 65 GiB 的小头）
- batch_size=8192：~220 MB × 8 = ~1.8 GiB / split × 16 = ~28 GiB（额外内存大头）

### 4.4 取值建议

- batch_size=1024：Doris 集成推荐值。小到既能控住 sort-merge buffer，又不会让 parquet decoder 自己的 vectorize overhead 变大。
- batch_size=8192（默认）：用于 sc=∞ 场景；与 sc 一起调小没意义（cap 先 fire）。
- batch_size<512：基本观察不到额外收益，但增加 parquet decoder per-batch overhead。

## 5. 参数协同与 Doris 集成推荐

### 5.1 协同矩阵

|  | parallelism | sc | batch_size |
|---|---|---|---|
| 控内存 | 线性，但 Doris 不可调 | 强相关，主旋钮 | 弱相关（仅 sc 一起调小才有效）|
| 控吞吐 | 大于 8 后边际递减 | 凹曲线，sc=8 最优 | sc=∞ 时大 batch 高效；sc 小时 batch_size 影响小 |
| 控 batches 数 | 无关 | 强相关，cap-hit 必小 batch | 无关（cap-hit 主导）|

### 5.2 Doris 集成首发配置

```rust
// 表选项（写表方一次 ALTER + INSERT OVERWRITE）
"file.block-size"                    = "32 mb"
"deletion-vectors.read-mode"         = "freshness"
"read.sort-merge-buffer-soft-cap"    = "8"
"read.batch-size"                    = "1024"

// reader 侧
parallelism = 16
```

预期稳态：~65 GiB 峰值、~32K rows/s、~595 万 batches（已知 Doris 那边的 dispatch overhead 是接下来的优化项）。

### 5.3 旋钮可观察性

- 内存：example 顶部 `paimon::alloc::print_stats` + `PAIMON_MEM_STATS_INTERVAL_SECS=10`
- 吞吐：example 自带 `rows_per_sec` / `drain_ms` 输出
- batches：example 自带 `batches=N` 输出（单 split 累加）
- 单 split sort-merge buffer 长度：测试代码用 `with_peak_observer(Arc<AtomicUsize>)` 可观察（生产路径未暴露）

## 6. 相关文件

| 路径 | 作用 |
|---|---|
| `crates/paimon/examples/read_local_demo.rs` | 三个参数的 CLI 入口 |
| `crates/paimon/src/spec/core_options.rs` | `READ_SORT_MERGE_BUFFER_SOFT_CAP_OPTION` / `read.batch-size` 选项定义 |
| `crates/paimon/src/table/sort_merge.rs` | sort-merge cursor 调度 / cap-hit 触发 |
| `crates/paimon/src/arrow/format/parquet.rs` | parquet `with_batch_size` 注入点 |
| `docs/pk-read-memory-investigation.md` | 排查链路 + 修复路线 |

## 7. 引用

- [arrow-rs#5523](https://github.com/apache/arrow-rs/issues/5523) parquet-rs 总是按 row group 全量缓冲
- [arrow-rs#5331](https://github.com/apache/arrow-rs/issues/5331) parquet stream 限内存 API 提案
- [Paimon CoreOptions](https://paimon.apache.org/docs/master/maintenance/configurations/) `file.block-size` / `read.batch-size`
