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

# paimon-rust PK 读路径能力 —— 自 capabilities 文档以来的进展

<!-- SECTION-OVERVIEW -->

## 总览

[`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) 记录于 commit `a146941`（2026-06-09），列出了 paimon-rust PK 读路径相对 paimon-java 的 30+ 项缺口。本文档**只跟踪自该文档以来已发生的进展**，不重复其完整能力对照。

**时间窗口**：commit `a146941` (2026-06-09) → 当前 HEAD。

**进度分四档**：
1. **已闭合**：代码 + 测试已合入，原文档中标 ✗ 的项可改 ✓
2. **部分闭合**：底层能力已就绪，公开 API / 上层接入未完成
3. **方案已落档**：实施方案有专门 docs，代码未开始
4. **仍待启动**：原文档中未动的剩余项（不在本文展开，参见原文）



<!-- SECTION-CLOSED -->

## 1. 已闭合（✓）

| 原条目 | 简述 | 状态变化 | Commit | 关键改动 |
|---|---|---|---|---|
| **C1** | Split: IntervalPartition + section bin-pack | ✗ → ✓ | `b926af9` | 新增 `crates/paimon/src/table/merge_tree_split_generator.rs`（477 行）：移植 Java `MergeTreeSplitGenerator` 的 `KeyComparator` / `interval_partition` / `pack_sections`。PK 表 plan 时按 key-range 重叠分 section 再 bin-pack，重叠 PK 文件强制进同 split。`table_read.rs` dispatch 同步调整：可能含多版本的 split 走 `KeyValueFileReader`，DV 表保持 raw-read 快路径。|
| **F1.e** | Merge engine: VersionedPartialUpdate | ✗ → ✓ | `8df7047` | 新增 `crates/paimon/src/table/versioned_partial_update.rs`（1189 行）+ `spec` / `kv_file_reader` / `kv_file_writer` / `table_commit` / `schema` 配套改动。完整 6 stages：枚举 + per-file mode option（Stage 1）→ DataFileMeta 字段 `_COMMIT_SNAPSHOT_ID` + `_MERGE_MODE`（Stage 2）→ single-version merge function（Stage 3）→ MV 列 accumulator（Stage 4）→ schema validation 6 条规则（Stage 5）→ E2E + ordering / tombstone 测试（Stage 6）。Lib 测 +47，integration 测 +3。|

> 这两项之外，`a146941` 之后的提交在 PK 读路径上还涉及 `read_local_demo.rs` 的 per-run option override（`6fdc27d`），但那是 demo 端工具改动，不直接闭合原文档条目；它**间接为** F11 / P11 / P2 的部分闭合提供了 demo 端验证（见下节）。



<!-- SECTION-PARTIAL -->

## 2. 部分闭合（❓）

| 原条目 | 简述 | 当前状态 | Commit | 还差什么 |
|---|---|---|---|---|
| **F11** | Scan-time dynamic options override | `Table::copy_with_options` 已存在并被 `examples/read_local_demo.rs` 使用做 per-run override（`--batch-size` / `--target-size` 都从 alter_table 改成内存级覆盖）；但 `ReadBuilder` 公开 API 上**没有** `with_dynamic_options(...)`。 | `6fdc27d` | 在 `crates/paimon/src/table/read_builder.rs` 暴露 `with_dynamic_options(HashMap<String, String>)`，内部转 `Table::copy_with_options` 即可。 |
| **P11** | `source.split.target-size` 读侧动态覆盖 | 同 F11：demo 走通；lib 公开 API 未暴露。 | `6fdc27d` | 同 F11。 |
| **P2** | `read.batch-size` 通路 | demo 走通 per-run override；但库内仍硬编码（`data_file_reader.rs` `Some(8192)` / `kv_file_reader.rs` `.with_batch_size(8160)`）。F11 暴露后**自动联动** —— 因为 `Table::copy_with_options` 已经 plumb 进 `CoreOptions`。 | `6fdc27d` | 1) lib 内部硬编码改成 `CoreOptions::read_batch_size()` 读取；2) 公开 API 同 F11。两步可同 PR。|



<!-- SECTION-PLAN-ONLY -->

## 3. 方案已落档，代码未实施

| 涉及条目 | 方案文档 | Commit | 范围 |
|---|---|---|---|
| C2 / C4 / C5 / C6 / C7 / F9 | [`docs/dv-impl-plan.md`](./dv-impl-plan.md) | `7398d54` | DV 读路径完整方案：Stage 1 KV reader DV wiring（C2）→ Stage 2 Bitmap64 dispatch（C6）→ Stage 3 read-mode + L0 routing（C7 + 顺手修 C4）→ Stage 4a DV-PERFORMANCE raw-read 短路（F9 批读收益 + 顺手修 C5）→ Stage 4b LookupMergeFunction 全套（F9 流式收益，建议 follow-up）。读路径 only，不含 DV 写路径。 |
| F1.e VersionedPartialUpdate（已闭合） | [`docs/versioned-partial-update-impl-plan.md`](./versioned-partial-update-impl-plan.md) | 落档 `ea0b729`；实施 `8df7047` | 列出来作 cross-reference —— 此项已迁到第 1 节（已闭合）。 |



<!-- SECTION-OPEN -->

## 4. 仍待启动

下列条目自 `a146941` 以来**没有动过**，状态与原文档一致。本节不展开分析，仅列出条目；详细参见 [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md)。

**正确性（C）**：
- C3 — KV reader schema-evo mapper 喂错字段
- C8 — sort-merge 同 PK 同 seq tie-break 不稳定
- C9 — bucket-key 字段类型校验缺失
- C10 — `_VALUE_KIND` NULL 静默 fallback
- C11 — type promotion 严格 compatibility check 缺失
- C12 — user sequence-field 类型覆盖有限
- C13 — KV reader schema evolution 单测缺失
- （C2 / C4 / C5 / C6 / C7 落档第 3 节 DV plan，未实施）
- （C1 已闭合于第 1 节）

**功能（F）**：
- F1.c — Merge engine: FirstRow（占位）
- F1.d — Merge engine: Aggregate（20+ FieldAggregator，单独立项）
- F2 — Snapshot startup modes（FROM_SNAPSHOT / FROM_TIMESTAMP / ...）
- F3 — Branch / Tag 读
- F4 — Incremental scan
- F5 — System tables（`$audit_log` / `$files` / 20+）
- F6 — File index pruning（bloom / hash / BSI / range / bitmap）
- F7 — Predicate operators（Like / Between / Transform 系列）
- F8 — Sort engine 选项 MIN_HEAP（不阻塞，可延后）
- F10 — PartialUpdate 进阶子语义（sequence-group / ignore-delete / DELETE）
- （F1.e 已闭合）
- （F9 / F11 落档或部分）

**性能（P）**：
- P1 — Reader 跨 split 并行（library 层）
- P3 — 非 PK 谓词在 single-run section 下推
- P4 — File-index pruning（性能侧表述同 F6）
- P5 — Limit pushdown manifest-entry 级
- P6 — Whole-bucket value filter
- P7 — Parquet dictionary / column-index filter 透传深度审计
- P8 — PlanCache
- P9 — Manifest / file-index / partition cache 审计
- P10 — ORC async read
- （P2 / P11 部分）



<!-- SECTION-FOOTNOTE -->

## 速览

| 进度档位 | 数量 | 占原文档条目比 |
|---|---|---|
| 已闭合 | 2（C1 / F1.e） | 2 / 30+ |
| 部分闭合 | 3（F11 / P11 / P2，共享底层） | 3 / 30+ |
| 方案已落档 | 6（DV 系列：C2 / C4 / C5 / C6 / C7 / F9） | 6 / 30+ |
| 仍待启动 | 22+ | 余下全部 |

> 注：F1.e VersionedPartialUpdate 完整 6 stages 实施 + 测试；C1 移植 Java `MergeTreeSplitGenerator`。两项都属于实质性正确性 / 功能闭合，不是文案修订。

## 维护说明

本文档随每个改动 PK 读路径的合入持续维护：

- 闭合一个原文档条目 → 在第 1 节加一行
- 落档一个新方案文档 → 在第 3 节加一行
- 部分闭合 / 状态变更 → 在第 2 节增量记录
- **不替代** [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md)；该文档是基线快照，本文档是增量进度

