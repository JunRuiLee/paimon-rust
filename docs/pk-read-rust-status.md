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
| **C2** | KV reader DV wiring（含 PU/VPU+DV 闭合） | ✗ → ✓ | Stage 1 `1001b70` + Stage 4 评审修复 | Stage 1 接通 KV reader 的 DV pre-filter（per-split DV factory，跨 row-group 绝对 row-position invariant）。评审修复阶段删除 `kv_file_reader.rs:281-293` 的 PU/VPU+DV reject，对齐 Java `KeyValueFileReaderFactory.java:173-187@e8938f347`（DV 与 merge engine 解耦）。新增 `test_kv_reader_partial_update_with_deletion_vector` 验证 DV 不破坏 column-wise merge；VPU+DV 由 dispatch 层 smoke 覆盖。 |
| **C5** | PK raw 读 drop residual DELETE rows | ✗ → ✓ | Stage 4a `2b3f551` | `data_file_reader.rs` 加 `drop_deletes: bool` + `with_drop_deletes` builder + `read_single_file_stream` 内 RowKind filter；`table_read.rs::read_pk_raw_drop_deletes` 走带 `_VALUE_KIND` 投影的 raw 路径。|
| **C6** | 64-bit Bitmap64DeletionVector 解码 | ✗ → ✓ | Stage 2 `529a4ac` + 评审修复阶段补 RLE/boundary 测试 | Stage 2 把 `DeletionVector` struct 改为 `Bitmap32 / Bitmap64` enum + magic dispatch；评审修复阶段补 RLE-encoded（`runLengthEncode` 等价 `RoaringTreemap::optimize()`）+ 跨 32-bit boundary 测试。Java 字节级 fixture 留作 follow-up（见 [`crates/paimon/tests/fixtures/deletion_vector/README.md`](../crates/paimon/tests/fixtures/deletion_vector/README.md)）。|
| **C7** | DV read-mode option（PERFORMANCE / FRESHNESS）+ L0 routing | ✗ → ✓ | Stage 3 `4ac977c` + 评审修复阶段 P0-1 planner 修复 | Stage 3 加 `DvReadMode` enum + `should_skip_level_zero_for_scan` 矩阵 + `should_apply_value_stats_to_entry` C4 修复。评审修复阶段在 `table_scan.rs::plan_snapshot` 重写 file_groups 分支：DV+FRESHNESS 不再走 raw `split_for_batch`，按 Java `MergeTreeSplitGenerator.java:69-114@e8938f347` rawConvertible + IntervalPartition fallback；`table_read.rs::read_pk` 同样改为 `is_raw_convertible_file_group + has_key_overlap` 统一判定（不再仅 `level == 0`）。 |

> 这两项之外，`a146941` 之后的提交在 PK 读路径上还涉及 `read_local_demo.rs` 的 per-run option override（`6fdc27d`），但那是 demo 端工具改动，不直接闭合原文档条目；它**间接为** F11 / P11 / P2 的部分闭合提供了 demo 端验证（见下节）。



<!-- SECTION-PARTIAL -->

## 2. 部分闭合（❓）

| 原条目 | 简述 | 当前状态 | Commit | 还差什么 |
|---|---|---|---|---|
| **F11** | Scan-time dynamic options override | `Table::copy_with_options` 已存在并被 `examples/read_local_demo.rs` 使用做 per-run override（`--batch-size` / `--target-size` 都从 alter_table 改成内存级覆盖）；但 `ReadBuilder` 公开 API 上**没有** `with_dynamic_options(...)`。 | `6fdc27d` | 在 `crates/paimon/src/table/read_builder.rs` 暴露 `with_dynamic_options(HashMap<String, String>)`，内部转 `Table::copy_with_options` 即可。 |
| **P11** | `source.split.target-size` 读侧动态覆盖 | 同 F11：demo 走通；lib 公开 API 未暴露。 | `6fdc27d` | 同 F11。 |
| **P2** | `read.batch-size` 通路 | demo 走通 per-run override；但库内仍硬编码（`data_file_reader.rs` `Some(8192)` / `kv_file_reader.rs` `.with_batch_size(8160)`）。F11 暴露后**自动联动** —— 因为 `Table::copy_with_options` 已经 plumb 进 `CoreOptions`。 | `6fdc27d` | 1) lib 内部硬编码改成 `CoreOptions::read_batch_size()` 读取；2) 公开 API 同 F11。两步可同 PR。|
| **C4** | L0 + value-stats 不安全（PK + Dedup/PU/VPU） | helper + planner gate 已闭合；端到端 manifest+parquet fixture 回归仍是 follow-up | Stage 3 `4ac977c` + 评审修复阶段 | `should_apply_value_stats_to_entry` helper 矩阵 + 触发场景测试已落（`stats_filter.rs::tests` 含 8 个 case，含 SECTION-RISKS #3 最小复现的 helper-level 断言）；端到端 manifest+parquet fixture 留作 follow-up。 |
| **F9** | Stage 4a 批读 raw 短路（已闭合）+ Stage 4b LookupMerge（未启动） | Stage 4a `2b3f551` 已闭合；Stage 4b 暂无 commit | Stage 4a `2b3f551` | Stage 4b LookupMergeFunction 全套（`changelog-producer=lookup` / `force-lookup` 流式 changelog）仍未实现，列为 follow-up；批读 PK 的 F9 收益已通过 Stage 4a 拿到。 |

> **本次评审修复**（commit pending）落地两项 P0 级行为对齐与若干 P1 防御性测试：
>
> - **P0-1 planner**：`table_scan.rs::plan_snapshot` 不再因 `deletion_vectors_enabled=true` 无条件关闭 PK key-overlap grouping。新增 `is_raw_convertible_file_group`（`crates/paimon/src/table/merge_tree_split_generator.rs`）镜像 Java `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible + `withoutDeleteRow` 双条件；DV+FRESHNESS / 任一 L0 / 任一 `delete_row_count > 0` 都会落到 IntervalPartition section 分组路径。修复了 DV+FRESHNESS 读出旧值 / 重复行的潜在正确性 bug。
> - **P0-2 reader**：`table_read.rs::read_pk` 不再仅以 `level == 0` 判断 needs_merge，改为同一套 rawConvertible + `has_key_overlap` 检查。L1+ 文件带残留 DELETE 时正确路由到 KV sort-merge（不再依赖 Stage 4a `drop_deletes` 兜底）。
> - **P1-1 PU/VPU + DV**：删除 `kv_file_reader.rs:281-293` 的 reject；与 Java `KeyValueFileReaderFactory.java:173-187@e8938f347` engine-agnostic DV 应用对齐。新增 PU+DV e2e 测试（验证 DV 不破坏 column-wise merge）+ VPU+DV dispatch smoke。
> - **P1-2 fail-fast merge-engine**：`plan_manifest_entries` / `plan_snapshot` 用 `?` 而非 `.ok()` 吞错，避免非法 `merge-engine` 静默回落 Deduplicate 语义。
> - **P1-3 Bitmap64 RLE / boundary**：补 RLE-encoded（`optimize()`）+ 跨 32-bit boundary 测试。Java fixture 留作 follow-up。
> - **P1-4 C4 触发场景**：补 `should_apply_value_stats_to_entry` 触发场景断言 + L1+ 性能守护回归。
> - **P1-5 Stage 4a 安全网**：`table_read.rs` 新增 `#[cfg(test)] mod tests` 直接覆盖 release-build dispatch 决策（debug_assert 仅是 belt-and-suspenders）。



<!-- SECTION-PLAN-ONLY -->

## 3. 方案已落档，代码未实施

| 涉及条目 | 方案文档 | Commit | 范围 |
|---|---|---|---|
| F1.e VersionedPartialUpdate（已闭合） | [`docs/versioned-partial-update-impl-plan.md`](./versioned-partial-update-impl-plan.md) | 落档 `ea0b729`；实施 `8df7047` | 列出来作 cross-reference —— 此项已迁到第 1 节（已闭合）。 |
| Stage 4b LookupMergeFunction（F9 流式部分） | [`docs/dv-impl-plan.md`](./dv-impl-plan.md) Stage 4b | `7398d54` | `changelog-producer=lookup` / `force-lookup=true` 流式 changelog 生成的 LookupMergeFunction 全套实现；批读 F9 收益已由 Stage 4a 拿到，此项仅当确有流式 changelog 需求时启动，建议 follow-up。 |



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
- （C1 / C2 / C5 / C6 / C7 已闭合于第 1 节；C4 部分闭合于第 2 节）

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
- （F9 部分闭合：Stage 4a 批读已落，Stage 4b 流式 LookupMerge 待启动；F11 部分）

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
| 已闭合 | 7（C1 / C2 / C5 / C6 / C7 / F1.e + 评审修复阶段 P0 planner 对齐） | 7 / 30+ |
| 部分闭合 | 5（C4、F9 Stage 4a / 4b、F11 / P11 / P2） | 5 / 30+ |
| 方案已落档 | 1（Stage 4b LookupMergeFunction） | 1 / 30+ |
| 仍待启动 | 余下全部 | — |

> 注：F1.e VersionedPartialUpdate 完整 6 stages 实施 + 测试；C1 移植 Java `MergeTreeSplitGenerator`；C2/C5/C6/C7 由 DV 读路径 Stage 1-4a + 评审修复阶段共同闭合；评审修复阶段额外修复了 DV+FRESHNESS planner / reader 与 Java rawConvertible 语义的对齐。

## 维护说明

本文档随每个改动 PK 读路径的合入持续维护：

- 闭合一个原文档条目 → 在第 1 节加一行
- 落档一个新方案文档 → 在第 3 节加一行
- 部分闭合 / 状态变更 → 在第 2 节增量记录
- **不替代** [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md)；该文档是基线快照，本文档是增量进度

