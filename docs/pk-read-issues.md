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

# paimon-rust PK 表读取路径已知问题

## 总览

本文记录 paimon-rust 当前在 **primary-key + write-only=true（MoR）** 表读取路径上的偏差与隐患，每条都附带「现象 / 复现 / 代码位置 / 与 Java/C++ 对照 / 修复方向」。

- **实测载体**：本地 `/tmp/paimon_local/bench_db/mor_primitive_<rows>m_<bucket>b`（1M / 10M / 100M × bucket=1 / 16），数据由 `create_mor_table_demo` 写入，每张表 8 commits、`write-only=true` 保留 8 个 L0 sorted run。
- **实验工具**（均在 `crates/paimon/examples/`）：
  - `create_mor_table_demo.rs` — 13 列原语类型 PK MoR 表写入；
  - `read_local_demo.rs` — 多表读，支持 `--count`/默认 consume/`--collect`、`--filter`、16 task 并发 drain、residual filter 兜底；
  - `alter_option_demo.rs` — 包装 `Catalog::alter_table(SchemaChange::set_option)`，写新 schema 版本而不动数据。
- **范围限定**：仅 PK MoR；append-only / data-evolution / 分区表暂不在本文涵盖。

---

## 问题 1 — Split planning 偏离 Java / C++（潜在正确性）

**现象**：100M 行、bucket=1、8 commits 的表，paimon-rust 切出 **8 个 split**（每个 ~200 MB L0 文件独占一个）。同一份元数据下 paimon-java / paimon-cpp 切出 **1 个 split**。

**代码位置**：
- `crates/paimon/src/table/table_scan.rs:725`：PK 表也直接调 `split_for_batch`，等价于 Java 的 `AppendOnlySplitGenerator`。
- `crates/paimon/src/table/bin_pack.rs:66`：实现就是文件级 bin-pack —— 按 `min_sequence_number` 排序，按 `target_split_size`（默认 128 MiB）打包，文件 weight = `max(file_size, open_file_cost)`。

**与 Java 对照**：`paimon-core/src/main/java/org/apache/paimon/table/source/MergeTreeSplitGenerator.java:69-115` 走两步：
1. `IntervalPartition` 按 PK key range 重叠分组成 section。重叠范围的文件**强制**进同一 section；
2. 再对 section（不是文件）做 bin-pack。

**与 C++ 对照**：`paimon-cpp/src/paimon/core/table/source/merge_tree_split_generator.cpp:42-103` 是 Java 的逐字移植。

**正确性影响**：当前 demo 的写入策略让 PK 在 commit 间互斥（commit `c` 只写 `pk = c, c+K, c+2K, …`），所以即使切到不同 split 也不会有同 PK 冲突，bug **不会暴露**。但 UPDATE / 重复写场景下，重叠 PK 落到不同 split → 跨 split 不会 sort-merge → 用户拿到过期版本或重复行。

**修复方向**：将 `MergeTreeSplitGenerator` + `IntervalPartition` + `SortedRun` 移植到 Rust。中等改动量，需要 PK 比较器、区间分组、SortedRun 结构等支撑。

---

## 问题 2 — 非 PK 谓词在 PK MoR 路径被丢弃，无 post-merge 兜底

**现象**：`--filter v_int:INT>=5000000` 跑在 10M 行的 `mor_primitive_10m_16b` 上，paimon-rust 返回**全部 10M 行**；只有 demo 自己加的 residual filter 才把多余的 5M 丢掉。同一谓词如果换成 PK 列 `id`，就只返回 5M（虽然 `is_exact_filter_pushdown` 仍然报 `false`，但实际是精确的）。

**代码位置**：
- `crates/paimon/src/table/kv_file_reader.rs:69-91`：`KeyValueFileReader::new` 用 `project_field_index_inclusive` 把所有非 PK 列谓词剥光，注释明确写：
  > Only keep predicates that reference primary key columns. Non-PK predicates applied before merge can cause incorrect results.
- `crates/paimon/src/table/kv_file_reader.rs:322`：sort-merge 之后**没有再补一道**非 PK filter，merge 完直接 reorder + 输出。

**正确性原理**：旧版本 `pk=42, v_int=10M`、新版本 `pk=42, v_int=100`，filter `v_int >= 5M` 如果直接下推到 parquet：
- 旧版本通过（10M ≥ 5M）；
- 新版本被跳过（100 < 5M）。

Sort-merge 看到的就只有旧版本，最后输出 `pk=42, v_int=10M` —— 但 pk=42 的**真实最新值是 100**，并不满足 filter。剥掉非 PK 谓词避免这种 anomaly，但代价是把过滤完全外推给 caller。

**与 Java 对照**：Java 端通常在 sort-merge 之后再 apply 非 PK 谓词（具体类待实测确认），因此 caller 不需要 residual filter。

**复现**：
```bash
cargo run -p paimon --example read_local_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_10m_16b \
    --count --filter "v_int:INT>=5000000"
```
输出 `rows=5000000 ... exact=false residual_dropped=5000000`。

**修复方向**：在 `kv_file_reader.rs:322` 的 sort-merge 输出之后插入 post-merge filter，复用 `arrow_select::filter::filter_record_batch` + 既有 `Predicate` 树。注意：谓词的 field index 引用的是 `table_fields` 的位置，但 merge 输出 schema 是 `[keys..., values...]`，需要重映射。改动量预估 50–80 行。

---

## 问题 3 — sort-merge 输出 batch_size 写死 1024，疑似在该边界丢行

**现象 A（性能）**：100M_1b 表当前 batch_size=1024 时输出 97,657 个 batch，每 batch 1024 行；与 paimon-java `read.batch-size=8192` 默认值不一致。

**现象 B（正确性，待深挖）**：`mor_primitive_10m_16b` 表在 batch_size=1024 默认下读出 **6,250,000 行**（缺 3,750,000），改成 8160 后读出正确的 10,000,000 行。强烈暗示 sort-merge 在 1024 边界上的尾部 buffer flush 有问题。

**代码位置**：
- `crates/paimon/src/table/sort_merge.rs:489`：默认 `batch_size: 1024`。
- `crates/paimon/src/table/sort_merge.rs:493`：`with_batch_size` 原本带 `#[cfg(test)]`，本会话改成生产可见。
- `crates/paimon/src/table/kv_file_reader.rs:336`：写死 `.with_batch_size(8160)` —— **FIXME**：应来自 `CoreOptions`。
- `crates/paimon/src/table/sort_merge.rs:729`：`output_indices.len() >= batch_size` 边界判定 + 周边 flush 逻辑是疑点，未深挖。

**修复方向**：
1. 把 batch_size 接入正经的 `read.batch-size` option（见问题 5）；
2. 单独追 sort-merge 在 1024 边界的尾部丢行 root cause，加针对性单测。

---

## 问题 4 — parquet 内部 batch_size 默认 1024（Arrow 默认）

**现象**：未设 batch_size 时 parquet reader 用 Arrow 默认值 1024 行/batch；与 paimon-java `read.batch-size=8192` 默认不一致。

**代码位置**：
- `crates/paimon/src/table/data_file_reader.rs:219`：原本传 `None`，本会话写死 `Some(8192)` —— **FIXME**：应来自 `CoreOptions`。
- `crates/paimon/src/arrow/format/parquet.rs:195`：实际消费这个值的位置（`batch_stream_builder.with_batch_size(size)`）。

**修复方向**：与问题 3、5 一并处理。

---

## 问题 5 — `read.batch-size` option 通路缺失

**现象**：问题 3 + 4 的根因 —— paimon-rust 整条 read pipeline 都没有 `read.batch-size` option。Java 端走 `CoreOptions.READ_BATCH_SIZE`（在 `paimon-common`），可表级或 session 级配置。

**应改文件**：
- `crates/paimon/src/spec/core_options.rs`：加 `READ_BATCH_SIZE_OPTION` 常量 + getter（参考已有的 `source_split_target_size()` 实现风格）。
- `crates/paimon/src/table/data_file_reader.rs`：`DataFileReader` 加 `batch_size: Option<usize>` 字段 + builder 方法。
- `crates/paimon/src/table/kv_file_reader.rs`：同样加字段，并把 sort-merge `with_batch_size` 也接到这个值。
- `crates/paimon/src/table/table_read.rs:139, 195`：`read_kv` / `read_raw` 构造 reader 时从 `CoreOptions` 取 `read.batch-size`，往下传。

修复后，问题 3、4 两处硬编码全部回滚为正经 option 读取。

---

## 问题 6 — reader 跨 split 不并行

**现象**：100m_1b（1 split）单核打满 100% CPU 跑 24s；100m_16b（16 split）只有靠 caller 端 `tokio::spawn` 分桶才达到 ~5× 加速。

**代码位置**：
- `crates/paimon/src/table/data_file_reader.rs:84`：`try_stream! { for split in splits { ... } }` —— 严格串行。
- `crates/paimon/src/table/kv_file_reader.rs:275`：同样的串行 `for split in &splits` 模式。

**当前 caller 兜底**：`crates/paimon/examples/read_local_demo.rs` 用 `#[tokio::main(worker_threads = 16)]` + 16 个 `tokio::spawn` round-robin 分桶 split，每个 task 各自调一次 `to_arrow`。caller 侧并发，库内部不动。

**修复方向**：库内把 `for split in splits` 改成 `stream::iter(splits).map(read_one_split).buffered(N)`，N 来自新加的 `read.split-parallelism` option（沿用问题 5 加的 option 通路风格）。改动小，但要确认 split 间无共享可变状态（KV reader 看起来是干净的；DataFileReader 自身是 `#[derive(Clone)]` 的）。

---

## 问题 7 — `source.split.target-size` 不能在读侧动态覆盖

**现象**：split 数太多想临时调大 target-size 验证效果时，只能 `ALTER TABLE` 改持久化的 schema —— 当前 demo 用 `alter_option_demo` 包了一下。理想的做法是 scan 时传一份动态 option，不修改持久状态。

**代码位置**：
- `crates/paimon/src/table/table_scan.rs:560`：`target_split_size = core_options.source_split_target_size()`，`core_options` 来源是 `table.schema().options()` —— 只看持久化 option。
- `crates/paimon/src/table/read_builder.rs`：`ReadBuilder` 没有 `with_dynamic_options()` 或类似动态 override 入口。

**当前绕路**：`crates/paimon/examples/alter_option_demo.rs` 直接走 `catalog.alter_table(ident, vec![SchemaChange::set_option(k, v)], false)`，落新 `schema-N+1` 文件而不动数据。能用，但语义是**持久化修改**。

**与 Java 对照**：Java 提供 `FileStoreTable.copy(dynamicOptions)`，拷一份带覆盖 option 的表对象再 scan，无副作用。

**修复方向**：在 `Table` / `ReadBuilder` 加 `with_dynamic_options(HashMap<String, String>)`，scan 时用 `table.schema().options() ⊕ dynamic` 喂 `CoreOptions::new`。注意覆盖范围只限 read-side option（`source.split.*`、`read.batch-size` 等），写 / commit / schema 路径不受影响。

---

## 当前分支 workaround / FIXME 索引

下列硬编码是本次会话为验证 Java 对齐效果加的临时 patch，问题 5 修好后**全部回滚**：

| 文件 | 行 | 改动 | 等待修复的问题 |
|---|---:|---|---|
| `crates/paimon/src/table/data_file_reader.rs` | 219 | `None` → `Some(8192)` | 问题 4 / 5 |
| `crates/paimon/src/table/kv_file_reader.rs` | 336 | 加 `.with_batch_size(8160)` | 问题 3 / 5 |
| `crates/paimon/src/table/sort_merge.rs` | 493 | 去掉 `#[cfg(test)]` 让 `with_batch_size` 生产可见 | 问题 3 / 5 |

---

## 验证 / 复现入口

```bash
# 写 100M 行 8 commit 1 bucket 的 PK MoR 表
cargo run -p paimon --example create_mor_table_demo --release -- \
    --rows 100000000 --commits 8 --bucket 1 --table mor_primitive_100m_1b

# 读全部（默认 consume 模式 + 16 并发 drain）
cargo run -p paimon --example read_local_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b

# --count 模式（跳过逐 cell 消费）
cargo run -p paimon --example read_local_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b --count

# Filter 实测（PK 列：paimon 内部 row-filter；非 PK 列：demo residual filter 兜底）
cargo run -p paimon --example read_local_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b \
    --count --filter "id:BIGINT>=50000000"

cargo run -p paimon --example read_local_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b \
    --count --filter "v_int:INT>=50000000"

# ALTER 改 split target-size（不动数据，只写 schema-N+1）
cargo run -p paimon --example alter_option_demo --release -- \
    /tmp/paimon_local/bench_db.db/mor_primitive_100m_1b \
    source.split.target-size 2gb
```

---

## 参考源文件

paimon-rust：
- `crates/paimon/src/table/{table_scan,table_read,read_builder,kv_file_reader,data_file_reader,sort_merge,bin_pack}.rs`
- `crates/paimon/src/arrow/format/parquet.rs`
- `crates/paimon/src/arrow/filtering.rs`
- `crates/paimon/src/spec/{core_options,predicate}.rs`

外部对照：
- Java：`paimon-core/src/main/java/org/apache/paimon/table/source/MergeTreeSplitGenerator.java`
- C++：`paimon-cpp/src/paimon/core/table/source/merge_tree_split_generator.cpp`
- C++ 读 demo：`paimon-cpp/examples/read_hdfs_demo.cpp`（filter / count / collect 三态语义来源）
