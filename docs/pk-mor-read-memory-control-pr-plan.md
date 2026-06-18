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

# PK MoR Read Memory Control PR Plan

> Phase 1 — 调研 + 设计文档。**本文档落地后请先停下，等明确确认再进入实现。**

## 1. 背景

[`docs/pk-read-memory-review.md`](./pk-read-memory-review.md) 列出了 PK MoR 读路径上若干内存放大点（manifest 全字节 + `buffered(64)`、sort-merge `MaterializedRow` 单行 batch、`DataFileMeta` 反复深 clone、DV eager 加载、DataFusion `SessionConfig.batch_size` 不被消费等）。

那些是 review 结论，**未经实测验证因果**。本 PR 的目标不是一次性修光，而是先合入**控制面**，让后续每一项优化都能在生产/基准上对比验证：

- 控制面：让 manifest 并发可调、scan/read 可在不动 schema 的前提下覆盖 `read.batch-size` / `source.split.target-size` / 新加的 manifest parallelism。
- **观测留作独立后续 PR**：原计划顺手引入 `tracing`，但项目目前 src 下零 logging 调用，这是一次"全新能力引入"决策，需要单独评估，不与控制面绑定（详见 §5.4 / §10）。本 PR 期间观测靠 `crates/paimon/src/alloc.rs::print_stats` + jemalloc heap profile 兜底。

后续 P0-1 / P0-2 / P0-3 优化都依赖这个 PR 的控制面铺路；observability PR 与之并行推进。

## 2. 调研到的当前代码事实

### 2.1 Manifest planning

- 文件：`crates/paimon/src/table/table_scan.rs`
- 函数：`read_all_manifest_entries`（`:87-213`），由 `plan_manifest_entries`（`:500-594`）调用。
- 当前逻辑（`:131-212`）：
  ```rust
  let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
  let shared_cache = SharedSchemaCache::new();
  let all_entries: Vec<ManifestEntry> = futures::stream::iter(manifest_files)
      .map(|meta| {
          let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
          let cache = shared_cache.clone();
          async move {
              let input_file = file_io.new_input(&path)?;
              let content = input_file.read().await?;          // 全字节
              ...
              let entries = crate::spec::avro::from_manifest_bytes_filtered_shared(
                  &content, &cache, &mut |...| { ... })?;
              ...
              Ok::<_, crate::Error>(filtered)
          }
      })
      .buffered(64)                                              // 硬编码
      .try_collect::<Vec<_>>().await?
      .into_iter().flatten().collect();
  Ok(all_entries)
  ```
- **是否存在硬编码并发**：是。`.buffered(64)` 是字面量，没有走任何 option。
- **manifest 字节是否流式**：否。`input_file.read().await?` 是一次性 `Bytes`（验证：`crates/paimon/src/io/file_io.rs:389` 的 `FileRead` trait 提供 `read(range)`，但当前 caller 用的是更上层 `InputFile::read()` 一次性读全文）。Avro 解析入口 `from_manifest_bytes_filtered`（`crates/paimon/src/spec/avro/mod.rs:132`）和 `from_manifest_bytes_filtered_shared`（`:149`）都接 `&[u8]`，没有 reader-based 版本。
- **相关 option**：无。`source.split.target-size` / `source.split.open-file-cost` 是 split planning 阶段用，跟 manifest 并发无关。
- **`buffered(64)` 内存峰值因果链**：在 review 文档里写的"同时持有 64 个 manifest 字节缓冲"严格说要看 backpressure，但 `.try_collect::<Vec<_>>()` 直接 drain 上游，不存在下游反压；64 路并发 in-flight 是必然的。这条结论站得住。

### 2.2 Read options / CoreOptions

- 文件：`crates/paimon/src/spec/core_options.rs`
- struct：`CoreOptions<'a> { options: &'a HashMap<String, String> }`，构造方式 `CoreOptions::new(options)`（`:265-267`）—— **借用 schema options 的引用**，没有自有所有权。
- 现有 read 相关 option 入口：
  - `read_batch_size()` `:398`（默认 1024）
  - `source_split_target_size()` `:380`（默认 128 MiB）
  - `source_split_open_file_cost()` `:387`（默认 4 MiB）
  - `parquet_page_index_enabled()` `:412`（默认 true）
  - `parquet_bloom_filter_enabled()` `:431`（默认 false）
- **当前 option 来源**：所有调用点都是 `CoreOptions::new(self.table.schema().options())`（grep 确认 14 处，包括 `table_scan.rs:506,604`、`read_builder.rs:72`、`table_read.rs`、几个 writer）。
- **是否已有 dynamic override 入口**：**有**，但不在 `ReadBuilder` 上 —— 在 `Table` 上：
  - `Table::copy_with_options(extra: HashMap<String, String>) -> Self`（`crates/paimon/src/table/mod.rs:201-223`）：merge 进 schema options，**不切 schema 版本**，对应 Java `FileStoreTable.copyWithoutTimeTravel`。
  - `Table::copy_with_time_travel(extra) -> Result<Self>`（`:237-254`）：merge 进 schema options，可能切到时间旅行 snapshot 的 schema，对应 Java `AbstractFileStoreTable.copy(dynamicOptions)`。
  - 实现方式：`schema.copy_with_options(extra)` clone schema 并 merge options；持久化 schema 文件**不动**。
- **review 文档关于 P1-3 的说法需要修正**：原文写"`ReadBuilder` 没有 `with_dynamic_options`"是真，但**整张 Table 已经有等价能力**，调用链 `Table::copy_with_options(extra).new_read_builder()` 已经覆盖大部分需求。本 PR 不需要重造 `ReadBuilder::with_dynamic_options`，但要在调用方 (DataFusion integration) 里**用上** `copy_with_options`。

### 2.3 DataFusion integration

- scan 物理算子：`crates/integrations/datafusion/src/physical_plan/scan.rs`
- 关键函数 `PaimonTableScan::execute`（`:132-174`）：
  - 接到 `_context: Arc<TaskContext>` —— 当前**完全丢弃**（参数名带下划线）。
  - 内部 `let table = self.table.clone();` → `table.new_read_builder()` → `read_builder.new_read()` → `read.to_arrow(&splits)`。
  - batch_size 从 `read.to_arrow` → `KeyValueReadConfig::batch_size` / `DataFileReader::batch_size` 一路下传，最终走 `CoreOptions::new(table.schema().options()).read_batch_size()`。
- table provider：`crates/integrations/datafusion/src/table/mod.rs:163-213` (`PaimonTableProvider::scan`)：
  - **目前已读** `state.config_options().execution.target_partitions`（`:196`），传给 `PaimonScanBuilder.target_partitions`。
  - **目前没读** `state.config_options().execution.batch_size`。
- DataFusion API 是否能区分"用户显式设置 batch size"和"默认值"：**无法可靠区分**。`SessionConfig::batch_size()` 直接返回当前值（默认 8192），DataFusion 53 没有暴露 "user-set?" 的标志位。这意味着默认透传会让所有 paimon 用户的 batch_size 从 1024 变 8192，需要谨慎。
- **review 文档关于 P1-2 的说法成立**：`_context` 确实被丢弃，session batch_size 不生效。但是否默认透传是个 API 决策，本 PR 倾向加开关、默认不透传，详见 §5.3。

### 2.4 Logging / tracing / metrics

- **现状：项目里没有任何 `tracing` / `log` 调用**，也没有 `println!` / `eprintln!` 在 src 下（grep 计数 0）。
- `paimon/Cargo.toml` 和 workspace `Cargo.toml` 里都**没有** `tracing` / `log` 依赖（grep 确认）。
- 唯一近似的观测口子是 `crates/paimon/src/alloc.rs` 的 `print_stats(label)`（jemalloc 计数器，用 `eprintln!`，例子在 `examples/read_local_demo.rs`）—— 这是诊断工具，不是产品级 logging。
- **结论**：引入产品级 logging 是个独立的全新能力决策。**本 PR 不引入**，留作后续独立 PR（详见 §5.4 / §10）。

## 3. 本 PR 范围

1. 新增 read 侧 option `scan.manifest-parallelism`，把 `read_all_manifest_entries` 的 `.buffered(64)` 改成读这个值。
2. 在 DataFusion integration（`PaimonTableProvider::scan` + `PaimonTableScan::execute`）里统一通过 `Table::copy_with_options(...)` 注入 dynamic option，把 dyn options 路径打通（不新增 `ReadBuilder::with_dynamic_options`，复用现成的 `copy_with_options`）。
3. 加可选的 DataFusion `SessionConfig.batch_size` 透传开关，**默认关闭**。
4. 单测覆盖：option 解析、edge values、scan 在 dynamic option 下生效。

## 4. 明确不做

- 不改 `DataFileMeta` 为 `Arc<DataFileMeta>`、不抽 `ReadFileMeta`。
- 不重写 manifest avro streaming reader（仍用 `from_manifest_bytes_filtered_shared`）。
- 不动 sort-merge `MaterializedRow` 路径。
- 不改 `DeletionVectorFactory` eager/lazy 策略。
- 不改 `read.batch-size` 默认值（保持 1024）。
- 不改 `scan.manifest-parallelism` 的"没设置时"行为 —— 默认仍 64，保证零行为差异。
- 不改 `Table::copy_with_options` / schema 持久化语义。
- **不引入 `tracing` / `log` / metrics（`metrics` / `prometheus` / opentelemetry）等任何 observability 依赖**。本 PR 只做控制面（option + dynamic options + batch_size 桥接），observability 留作独立后续 PR。
- 不引入任何新依赖。

## 5. 设计方案

### 5.1 Manifest parallelism option

- **option 名称**：`scan.manifest-parallelism`
  - 命名风格对齐已有 `scan.timestamp-millis`（`crates/paimon/src/spec/core_options.rs:69`）/ `scan.version` / `source.split.target-size`。
  - 不复用 `source.split.*` 因为这是 manifest 平面、不是 split 平面。
  - Java 端目前没对应 option（这点要在 PR 描述里说明）；命名遵循 paimon 的 `<phase>.<knob>` 风格，未来 Java 加同名 option 可对齐。
- **默认值**：**64**，与当前硬编码完全等同 —— 保证零行为差异，不引入 regression 风险。
  - review 里曾建议默认降到 8~16，**本 PR 不动默认**，留给后续基准对比再决定。
- **边界处理**：
  - 解析风格对齐 `read_batch_size()`（`:398-410`）：取 string，`parse::<usize>()` 失败或 `<= 0` 都回退默认。
  - clamp 到 `[1, 1024]`。
  - clamp 时不输出日志（本 PR 不引入 tracing / log，见 §4）；非法值静默回退到默认 / clamp 后的边界值。
- **clamp 上下限影响分析**：
  - **下限 1**：保证至少串行执行 manifest 读取，没有任何"全部跳过"语义。`buffered(0)` 在 futures 里行为是"立刻产出空 stream"，会导致 manifest entries 全空 ⇒ 必须排除。
  - **上限 1024 的影响**：
    - **fd**：每路 in-flight 至少持有 1 个 `InputFile` reader 句柄。1024 已等于多数 Linux 默认 `ulimit -n` 软上限的一半（常见值 1024 / 4096），再高就有 EMFILE 风险。
    - **内存**：当前 manifest 走 `input_file.read().await?` 一次性读全文，单 manifest 平均几百 KB ~ 数 MB。1024 路 in-flight 字节缓冲粗算 1024 × ~1 MiB ≈ 1 GiB 量级，已经是 plan 阶段允许的内存上限。
    - **正常用法不会触上限**：review 文档建议默认 8~16，生产基准上经验值也在 16~64 区间；1024 是"防御性""离谱值"上限，不会卡住任何合理调参。
    - **超过上限的用户意图**：填 `1000000` / `usize::MAX` 几乎肯定是误用或想关掉限流，无论何种情况都不应被尊重 —— clamp 到 1024 比直接报错更宽容（保持向前兼容）。
  - **是否要把上限做成可配**：不做。如果将来真有 user case 需要 >1024（例如 manifest 极小且 IO 路径已大幅压缩），独立 PR 加 escape hatch（如 `scan.manifest-parallelism-max` 或环境变量）；本 PR 不留口子。
- **生效路径**：
  - `CoreOptions::scan_manifest_parallelism() -> usize`（新增 getter）。
  - `read_all_manifest_entries` 增加 `manifest_parallelism: usize` 参数；`plan_manifest_entries` 调用方读 `core_options.scan_manifest_parallelism()` 后下传。
  - `.buffered(manifest_parallelism)` 替换 `.buffered(64)`。
- **必须支持 dynamic options 覆盖**：通过 `Table::copy_with_options` 已自动支持（option merge 进 schema options），不需要额外通路。

### 5.2 Dynamic read options

**关键决策：不新增 `ReadBuilder::with_dynamic_options`**，因为 `Table::copy_with_options(extra)` 已经实现了等价语义（[§2.2](#22-read-options--coreoptions)）。

- **API（已存在，本 PR 不改 paimon-core API）**：
  ```rust
  // 调用方（DataFusion / 集成层）：
  let scoped_table = self.table.copy_with_options(dynamic_options);  // 不写回 schema
  let read_builder = scoped_table.new_read_builder();
  ```
- **合并优先级**（`Table::copy_with_options` 内部由 `TableSchema::copy_with_options` 实现）：
  1. dynamic options 中存在的 key → 覆盖
  2. 未覆盖的 key → 走表 schema options
  3. 都没有 → `CoreOptions::*` 的硬编码默认
- **生效范围保证**：
  - `copy_with_options` 返回**新的** `Table` 实例，所有走它派生出的 `ReadBuilder` / `TableScan` / `TableRead` 都用这份覆盖后的 options。
  - `Table` 是 `Clone` 但 schema 是 owned 的 `TableSchema`，不会污染原 `Table` 实例。
  - 不影响写路径：`copy_with_options` 仅修改内存里的 `schema.options`，**不**触发 schema commit；`Catalog::alter_table` 是另一条独立路径。
  - 验证方式：单测断言 `original_table.schema().options() == before` 在 `copy_with_options` 之后保持不变。
- **dynamic options 注入点**（DataFusion integration）：
  - `PaimonTableProvider::scan`（`crates/integrations/datafusion/src/table/mod.rs`）和 `PaimonTableScan::execute`（`crates/integrations/datafusion/src/physical_plan/scan.rs`）。
  - 当前 plan 阶段已经在 provider 里做（`scan.plan().await`），所以 dynamic options **必须在 provider scan 时就注入**，否则 plan 阶段读到的还是原 options。
  - 实现：`PaimonTableProvider` 暴露一个 `dynamic_options: HashMap<String, String>` 字段（构造函数加可选参数 `with_dynamic_options`），scan 时第一步 `let table = self.table.copy_with_options(self.dynamic_options.clone())`。后续 plan + execute 都用这份 `table`。
  - 复用同一个字段也能容纳 §5.3 的 batch_size 注入。

### 5.3 DataFusion batch size bridge

- **读取方式**：`session_config.options().execution.batch_size`（`SessionConfig::batch_size()`）。
  - 注意：`PaimonTableProvider::scan(state, ...)` 的 `state: &dyn Session` 已能访问；`PaimonTableScan::execute` 的 `_context: Arc<TaskContext>` 也能：`_context.session_config().batch_size()`。
  - 实际取的位置在 **`PaimonTableProvider::scan`**：因为 plan 阶段就要 `read_builder` 拿 `read_batch_size`（split planning 不直接用 batch_size，但 scan 路径下传时用），统一在 provider 入口注入更干净。
- **透传方式**：作为 dynamic option 写入 `read.batch-size`：
  ```rust
  if self.respect_session_batch_size {
      let bs = state.config_options().execution.batch_size;
      dynamic.insert("read.batch-size".to_string(), bs.to_string());
  }
  ```
- **优先级**：和 §5.2 的 dynamic options 合并优先级一致。具体到 batch_size：
  1. 调用方 `PaimonTableProvider::with_dynamic_options(...)` 显式传入的 `read.batch-size` → 最高
  2. DataFusion `SessionConfig.batch_size`（仅当开关开启时）
  3. 表 schema 的 `read.batch-size`
  4. paimon 默认 1024
  顺序通过"先放显式 dynamic、再 entry 不存在时插入 session bs"实现：
  ```rust
  let mut dynamic = self.user_dynamic_options.clone();
  if self.respect_session_batch_size {
      dynamic.entry("read.batch-size".to_string())
             .or_insert_with(|| session_bs.to_string());
  }
  ```
- **是否默认启用**：**默认不启用**。
  - 原因：DataFusion `SessionConfig.batch_size` 默认 8192，paimon `read.batch-size` 默认 1024。默认透传会让所有现有 DataFusion 用户的 paimon batch_size 跳到 8192 —— 这本身可能是**好**事（减少 batch 数、降下游算子重切），但**单 batch 内存峰值会按 8x 放大**，需要先在基准上验证再开。
  - 用 `PaimonTableProvider::with_respect_session_batch_size(bool)` 暴露开关；可由 catalog 集成层在创建 provider 时打开。
  - DataFusion 53 没有公开"user-set vs default"区分，所以无法做"仅当用户显式设置才透传"，开关是唯一可控选项。
- **测试如何断言 batch size 生效**：
  - 单测构造 `SessionContext::with_config(SessionConfig::new().with_batch_size(2048))`；
  - 用一个真实 parquet 文件（`test_utils::write_int_parquet_file`）；
  - 开 `with_respect_session_batch_size(true)`，scan，断言至少有一个 batch `num_rows() == 2048`（或 < 2048 但全文件按 2048 切；考虑文件总行数；安全断言：所有 batch `num_rows() <= 2048`）；
  - 关闭开关时断言 batch 大小 ≤ 1024（默认）。

### 5.4 Observability（暂不引入）

review 文档原本希望本 PR 顺手把 plan / scan 关键节点的 debug 日志也打通。已确认**不在本 PR 引入**：

- 项目目前 src 下零 `tracing` / `log` / `println!` 调用，引入是一次"全新能力"决策，需要单独评估（target 命名约定、subscriber 默认行为、ASF / license 角度等），不应和控制面混在一起合。
- 控制面 PR 的合入与否不应被 observability 决策卡住。
- 本 PR 期间临时观测仍可走现有的 `crates/paimon/src/alloc.rs::print_stats(label)`（jemalloc 计数器，eprintln）+ heap profile 组合，参见 `examples/read_local_demo.rs`。

后续如果决定加 observability，独立 PR 处理：引入 `tracing`、约定 target / level、加入 plan / manifest / read 关键节点的 debug 日志。本 PR 仅保留"option 非法值静默回退"的行为。

> **注**：因此本 PR 不在 `read_all_manifest_entries`、`plan_snapshot`、`KeyValueFileReader::read`、`PaimonTableProvider::scan` 任何位置加日志；这些埋点位置先记在 review 文档里供未来 PR 参考。

## 6. 计划修改文件

需要修改的文件：

- **`crates/paimon/src/spec/core_options.rs`**（风险：低）
  - 加 `SCAN_MANIFEST_PARALLELISM_OPTION` 常量 + `DEFAULT_SCAN_MANIFEST_PARALLELISM = 64`；
  - 加 `scan_manifest_parallelism()` getter，含 clamp，非法值静默回退。
- **`crates/paimon/src/table/table_scan.rs`**（风险：中，参数串改但 callsite 集中）
  - `read_all_manifest_entries` 加 `manifest_parallelism: usize` 参数；
  - `.buffered(64)` 改为 `.buffered(manifest_parallelism)`；
  - `plan_manifest_entries` 调用方读 `core_options.scan_manifest_parallelism()` 下传。
- **`crates/integrations/datafusion/src/table/mod.rs`**（风险：中，API 加字段，但所有直接构造点已确认在 datafusion crate 内）
  - `PaimonTableProvider` 加字段 `dynamic_options: HashMap<String, String>` 和 `respect_session_batch_size: bool`；
  - 构造函数补可选 setter；
  - `scan` 入口先 `copy_with_options(...)`。
- **`crates/integrations/datafusion/src/physical_plan/scan.rs`**（风险：低）
  - 不改逻辑（dynamic options 已在 provider 注入时打进 `self.table`）。

无需修改的文件（确认过）：

- `read_builder.rs` —— 不加 `with_dynamic_options`，复用 `Table::copy_with_options`；
- `table_read.rs` —— batch_size 通路已存在，不动；
- `data_file_reader.rs` / `sort_merge.rs` —— 不动 hot path；
- `spec/avro/*` —— 不改 manifest 解码 API；
- `deletion_vector/*` —— 不在本 PR 改 DV；
- `kv_file_reader.rs` —— 本 PR 不引入 tracing，无改动；
- workspace 根 `Cargo.toml` / 各 crate `Cargo.toml` —— 本 PR 不引入新依赖。

## 7. 测试计划

### 单测（unit）

- `core_options.rs`:
  - `scan_manifest_parallelism()` 默认 64
  - `=8` / `=16` 正常返回
  - `=0` / `=-1` / `=invalid` 返回 64（默认）
  - `=2048` 被 clamp 到 1024
- `table_scan.rs` 测试模块（已有 test pattern，参考 `kv_file_reader.rs` tests）：
  - 启动 mock manifest 集合，断言 `plan_manifest_entries` 在不同 parallelism 下产出的 entry 数一致（仅并发度变，结果不变）。
- `crates/integrations/datafusion/src/table/mod.rs` tests：
  - Provider 在 `with_dynamic_options({"read.batch-size": "256"})` 下，scan 出来的 batch 大小 ≤ 256；
  - `respect_session_batch_size=true` + `SessionConfig.batch_size(2048)` → batch 大小 ≤ 2048；
  - `respect_session_batch_size=false`（默认）→ batch 大小 ≤ 1024（schema 默认）；
  - 显式 dynamic options 优先于 session：`with_dynamic_options({"read.batch-size":"256"}) + respect=true + session=2048` → ≤ 256；
  - 断言 `original_table.schema().options()` 不包含本次 dynamic option（不污染原 Table）。

### 集成测试

- 复用 `crates/integration_tests/`，加一个 manifest 数较多的 PK 表（mock 多 manifest），跑 plan + scan，断言结果集 / 行数与不开 dynamic option 时一致。
- 不需要跨进程或大数据量。

### 手工验证

- 用 `examples/read_local_demo.rs` 跑 `mor_primitive_100m_1b` / `100m_16b` / PartialUpdate 表；
- 通过 jemalloc heap profile + `crates/paimon/src/alloc.rs::print_stats` 对比 `manifest_parallelism=64 vs 16`：是否能看到 plan 阶段 RSS 峰值变化（用于验证 review 文档 P0-1 的因果链是否在生产数据上成立）。

### 不跑的测试

- 大规模长时间稳定性测试（>10M 行）放回归阶段，本 PR 不阻塞。
- 跨平台测试：本 PR 不引入新依赖，无需特殊处理。

## 8. 风险与兼容性

- **`scan.manifest-parallelism` Java 端没有同名 option**
  - 缓解：PR 描述里说明这是 Rust 端先行；命名遵循 paimon style，预留对齐。
- **DataFusion 透传 batch_size 改变行为**
  - 缓解：默认关闭，需调用方显式 `with_respect_session_batch_size(true)`；零行为差异。
- **`PaimonTableProvider` 加字段是 API 变更**
  - 缓解：用 builder pattern / `Default` 兜底，老调用方不用改；已 grep 确认所有直接构造点都在 datafusion crate 内。
- **dynamic options 与 `Table::copy_with_options` 语义不一致风险**
  - 缓解：复用现成方法，不引入新通路；单测断言 schema 持久化文件不变。
- **默认值改变（`scan.manifest-parallelism`）**
  - 缓解：本 PR 不改默认；仅暴露开关。
- **没有 observability 不便定位线上问题**
  - 接受。本 PR 只动控制面；observability 走独立后续 PR，期间用 `print_stats` + heap profile 兜底。

## 9. 建议拆 PR

如果 reviewer 倾向更小粒度，按下面顺序拆 2 个 PR：

- **PR 1：option + manifest_parallelism**（最小可合）
  - `core_options.rs` 加 option（含 clamp + 默认 64）
  - `table_scan.rs` 让 `.buffered(N)` 可配
  - 单测覆盖 option 解析
  - 不动 DataFusion
- **PR 2：DataFusion dynamic options + batch_size bridge**
  - `PaimonTableProvider` 加字段（dynamic options + `respect_session_batch_size`）
  - `scan` 入口走 `copy_with_options`
  - 加 batch_size 透传开关（默认关）
  - 单测 + 集成测试

如果 reviewer OK，可以合一个 PR；本文档默认按合一个 PR 写。

> **不在本拆分内的 observability PR**：将来如果引入 `tracing` + plan / scan 关键节点 debug 日志，作为完全独立的 PR 处理，不与上述 2 个控制面 PR 绑定。

## 10. 已确认决策

> 2026-06-18 用户对 §10 原"待确认问题"逐项确认，结果固化如下：

1. **`scan.manifest-parallelism` 默认值 = 64**：保持零行为差异，"降默认到 16"留给后续基准验证后单独 PR。
2. **不引入 `tracing` / `log` / metrics**：本 PR 仅做控制面，不引入任何 observability 依赖。具体见 §3、§4、§5.4。
3. **`PaimonTableProvider` 加字段不破坏既有调用方**：已 grep 确认所有直接构造点（`try_new` / `try_new_with_blob_reader_registry`）都在 `crates/integrations/datafusion/` 内（`tests/read_tables.rs`、`src/full_text_search.rs`、`src/sql_context.rs`、`src/relation_planner.rs`、`src/catalog.rs`、`src/vector_search.rs`、`src/table/mod.rs`）。`bindings/` 与 `crates/paimon-rest-server/` 无任何引用，新增字段走 setter / `Default` 兜底，老调用方零改动。
4. **`respect_session_batch_size` 留作 `PaimonTableProvider` 字段**，不做成 paimon option。DataFusion-specific 行为不污染 paimon-core option namespace。
5. **clamp 上限维持 1024**：上下限影响分析见 §5.1（下限 1 防 `buffered(0)` 全跳过；上限 1024 防 fd / 内存爆炸；正常用法不会触上限；超过上限的填值几乎肯定是误用，clamp 比报错更宽容）。如果将来真有 >1024 的 user case，独立 PR 加 escape hatch。
6. **option 命名 `scan.manifest-parallelism`**：短，对齐已有 `scan.timestamp-millis` / `scan.version` 风格。
7. **review 文档需加 forward-link 指向本 plan**：在 `pk-read-memory-review.md` 顶部 / P0-1 / P1-2 / P1-3 段加引用，让 reviewer 能从 review 跳到这份控制面 plan。本 PR 编码阶段一并补上。
