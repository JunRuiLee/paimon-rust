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

# paimon-rust vs paimon-java：PK 读路径能力对照

## 总览

本文盘点 paimon-rust 与 paimon-java 在 **primary-key 表读取路径** 上的能力差异，按 **正确性 / 功能 / 性能** 三个维度分类。

- **范围**：仅 PK + write-only / MoR 的**批读**路径；append-only 表、data-evolution、写入路径、流式读 / CDC / changelog producer 等场景**不在本文**。
- **数据来源**：本会话调研覆盖了 Java 侧 plan / read / filter 三条线（`paimon-core/`、`paimon-common/`、`paimon-format/`）+ 三个针对性 audit（DV / schema evolution / 算法误用）+ Rust 侧 `crates/paimon/src/{table,arrow,spec,file_index,deletion_vector}/`。
- **与 [`pk-read-issues.md`](./pk-read-issues.md) 的关系**：那份文档列了 7 条具体 bug / tech-debt（每条都要修），本文是更宽的能力盘点 —— 既含 issues 重叠项，也含 issues 没覆盖的"未实现 feature"、"算法误用"、"性能差距"。每条尽可能回链 issue 编号。
- **正确性条目分级**：HIGH = 默认配置下可复现 / 实际 PK 表会触发；MED = 边界场景；LOW = 信息级或仅待审计。

> **使用方式**：先看末尾的[总结矩阵](#总结矩阵)做 2 分钟扫描；想了解某条细节再回到对应章节。维度 1（正确性）建议优先关注，HIGH 级共 7 条。

---

## 维度 1 — 正确性 (Correctness)

paimon-rust 在某些合法输入下**会返回错误结果或直接崩溃**。本节按严重度分组：HIGH = 默认配置可复现 / 实际可遇；MEDIUM = 边界场景；LOW = 信息级 / 待审计。

<!-- SECTION-CORRECTNESS -->

### HIGH — 可在默认配置下复现

#### C1. Split planning 用 AppendOnly 算法，重叠 PK 的 L0 文件可能不 sort-merge

- **现象**：PK 表在 plan 阶段也调 `split_for_batch`（按文件 size bin-pack），不做 PK key-range 重叠分组。同 PK 的多个 L0 文件可能被切到不同 split，跨 split 不 sort-merge → 用户拿到旧版本/重复行。
- **触发**：PK 表 + 多个 L0 文件 + PK key range 重叠（UPDATE / 重复写场景）。当前 demo 写入策略让 PK 互斥（`pk = c, c+K, c+2K, …`）所以暴露不出来。
- **代码位置**：
  - Rust：`crates/paimon/src/table/table_scan.rs:725` 调 `split_for_batch`；`crates/paimon/src/table/bin_pack.rs:66` 纯 file-level bin-pack。
  - Java：`paimon-core/.../source/MergeTreeSplitGenerator.java:69-115` 走 `IntervalPartition` 把重叠 key range 文件**强制**进同一 section。
- **关联 issue**：[issues #1](./pk-read-issues.md#问题-1--split-planning-偏离-java--c潜在正确性)

#### C2. KV reader 直接拒绝带 DV 的 split

- **现象**：`KeyValueFileReader::read` 在看到 split 上挂了 `data_deletion_files` 时**直接报错**`"KeyValueFileReader does not support deletion vectors"`。也就是说，DV-enabled 的 PK 表只要 split 包含任何 L0 文件，**整次读取就会失败**。
- **触发**：任何 `deletion-vectors.enabled=true` 的 PK 表 + L0 未空（写后未完成 compaction）→ 读必崩。
- **代码位置**：
  - Rust：`crates/paimon/src/table/kv_file_reader.rs:276-284` 显式 `return Err(Error::Unsupported{...})`；`read_single_file_stream` 调用处（line 306-312）对 `dv` 参数硬传 `None`。
  - Java：`paimon-core/.../io/KeyValueFileReaderFactory.java:173-177` 用 `ApplyDeletionVectorReader` 在 sort-merge **之前** 包住每个文件 reader，每条 row 经过 DV 过滤再进 merge。Rust 的"per-file DV apply before sort-merge"完全没实现。

#### C3. KV reader 给 schema-evolution mapper 喂错字段，PK MoR 在 schema 演化下 panic / 输出损坏

- **现象**：当 PK MoR 表里某些 KV 文件的 `schema_id != table_schema_id` 时，schema evolution 路径走偏 —— `_SEQUENCE_NUMBER` / `_VALUE_KIND` 在 mapper 里查不到（因为传给 mapper 的 `data_fields` 是用户视角 schema，**不带 KV 物理前缀**），被当作 missing column 用 `new_null_array` 填 NULL；但 `_VALUE_KIND` 是 non-nullable → `RecordBatch::try_new` 直接 panic；即便不 panic，sort-merge dedup 依赖 `_SEQUENCE_NUMBER` 排序，全 NULL → 顺序乱套，行内容损坏。
- **触发**：任何 PK MoR 表上做过 ALTER（add/drop/rename/promote 任一列），并且历史文件还没被 compaction 重写到当前 schema_id。
- **代码位置**：
  - Rust：`crates/paimon/src/table/kv_file_reader.rs:290-302` 把 `data_schema.fields()`（用户视角，无 `_SEQ/_VK`）原样传给 inner `DataFileReader::new`；该 reader 的 `read_type` 却是 `internal_read_type = [_SEQ, _VK, user_cols...]`（带前缀）。`arrow/schema_evolution.rs:42-72` `create_index_mapping` 用 field id 在 user-only 集合里查 `_SEQ`(`i32::MAX-1`)、`_VK`(`i32::MAX-2`) → 都返回 `None` → 走 NULL fill 分支（`data_file_reader.rs:230-277`）。
  - Java：`paimon-core/.../utils/FormatReaderMapping.java:208-211` 通过 `KeyValue.createKeyValueFields(...)`（`KeyValueFileReaderFactory.java:322-330`）把 KV 前缀同时注入 `expectedFields` 和 `dataSchema` 视图，两边对齐。
- **修复方向**：在 `kv_file_reader.rs:290` 处给 `data_schema.fields()` 也包一层 `[_SEQ, _VK, ...]` 再下传，跟 `internal_read_type` 对齐。

#### C4. value-stats pruning 错误地应用到 L0 文件

- **现象**：PK + Deduplicate（默认）+ 无 DV 配置下，**plan 阶段对 L0 文件也跑 value-stats min/max 裁剪**。L0 文件的 PK key range 互相重叠，最新版本的 `value` 跟旧版本不同；当查询带非 PK 列谓词（如 `value > X`）时，可能把"含最新版本但 value 不匹配"的 L0 文件裁掉，留下的"含旧版本且 value 凑巧匹配"的文件被读上来，sort-merge 看不到最新版 → **返回过期值**。
- **触发**：PK + Deduplicate（或 PartialUpdate）+ `deletion-vectors.enabled=false` + 查询带非 PK 列范围谓词 + 多个 L0 文件 PK 重叠 + 多版本 value 不同。
- **代码位置**：
  - Rust：`crates/paimon/src/table/table_scan.rs:174-183` 对**所有** entry 调 `data_file_matches_predicates`，L0 短路只在 `skip_level_zero=true`（即 DV 模式或 FirstRow merge engine）才生效（`should_skip_level_zero_for_scan`，line 287-301）。stats 源是 `value_stats`（`stats_filter.rs:91-96`）。
  - Java：`paimon-core/.../KeyValueFileStoreScan.java:139-166` 显式跳过 L0 的 value-stats 裁剪（除非 `dvFreshnessReadEnabled`），代码里有长篇注释解释这个 hazard。
- **修复状态（HEAD `b5cd3a5`）**：✓ 已闭合。
  - 引入 entry 级 gate `should_apply_value_stats_to_entry`（`crates/paimon/src/table/stats_filter.rs:191-226`），矩阵在 PK + 非 DV + L0 + `Deduplicate` / `PartialUpdate` / `VersionedPartialUpdate` 下返回 `false` → 跳过 value-stats 裁剪。
  - 常规 plan 路径在 `crates/paimon/src/table/table_scan.rs:188-194` 接入；cross-schema fallback 在 `crates/paimon/src/table/table_scan.rs:645-651` 对称接入。
  - 单测：`stats_filter.rs::test_should_apply_value_stats_non_dv_pk_l0_dedup_skips`（line 605）+ `test_should_apply_value_stats_overlapping_l0_pk_dedup_skips_both_files`（line 660）。
  - 端到端 manifest + parquet fixture 回归仍是 follow-up（与 [`pk-read-rust-status.md`](./pk-read-rust-status.md) 第 C4 行一致）。

#### C5. raw L1+ 路径不剥 DELETE / UPDATE_BEFORE 行

- **现象**：PK Deduplicate 表的 split 如果**只**含 L1+ 文件（无 L0）会走 `read_raw` 路径（`DataFileReader`），该 reader 完全不识别 `_VALUE_KIND` —— L1+ 文件里残留的 `DELETE` 和 `UPDATE_BEFORE` 行（compaction 不一定每次都剥；只有"覆盖全 key range 的 full compaction + forceKeepDelete=false"才 drop）会作为**幽灵行**返回给用户。
- **触发**：PK 表 compaction 后存在 L1+ 文件残留 DELETE 或 UPDATE_BEFORE 行（CDC 写入特别常见）+ 用户做 batch SELECT。
- **代码位置**：
  - Rust：`crates/paimon/src/table/table_read.rs:111-124,180-182` 走 `read_raw` → `data_file_reader.rs`，文件内**零处** `is_add` / `RowKind` / `_VALUE_KIND` 引用，没有任何 post-filter。
  - Java：`paimon-core/.../MergeFileSplitRead.java:177-180,348` 默认 `forceKeepDelete=false` → 输出包 `DropDeleteReader`（`mergetree/DropDeleteReader.java:33-69`）按 `kv.isAdd()` 剥 DELETE/UPDATE_BEFORE。

#### C6. Bitmap64 DV 格式不被识别，按 invalid magic 报错

- **现象**：表配 `deletion-vectors.bitmap64=true` 写出来的 64-bit DV 文件，paimon-rust 解码时直接当作"invalid magic number"拒绝。
- **代码位置**：
  - Rust：`crates/paimon/src/deletion_vector/core.rs:34, 96-105` 只识别 32-bit `BitmapDeletionVector` magic；全树 `bitmap64`/`Bitmap64` 零结果。
  - Java：`paimon-core/.../deletionvectors/DeletionVector.java:117-144` 按 magic number 分派 `BitmapDeletionVector` / `Bitmap64DeletionVector` 两种实现。

#### C7. `deletion-vectors.read-mode` 选项被静默忽略

- **现象**：`PERFORMANCE` / `FRESHNESS` 二选一在 Rust 里**未识别**。`should_skip_level_zero_for_scan` 硬编码为 PERFORMANCE-equivalent（DV 启用就跳 L0），用户配 `FRESHNESS` 不会读到 L0 + DV 合并的结果，而是直接被 C2（KV 拒 DV）撞死或被 L0 跳过逻辑吞掉。
- **代码位置**：
  - Rust：`crates/paimon/src/table/table_scan.rs:287-301`；全树 grep `dv_read_mode` / `DvReadMode` / `deletion-vectors.read-mode` 零结果。
  - Java：`paimon-api/.../CoreOptions.java:1880-1883`、`paimon-core/.../KeyValueFileStoreScan.java:154-162`。

### MEDIUM — 边界场景

#### C8. sort-merge 在同 PK 同 sequence-number 时 tie-break 不确定

- **现象**：两条 record 的 PK 和 `_SEQUENCE_NUMBER` 都相同时（合法情况，例如同一 checkpoint 内 flush 多次或 writer 故障恢复），Rust 的赢家取决于 HashMap 迭代序与 `Vec<DataFileMeta>` 顺序，跟 Java 不一致。
- **代码位置**：
  - Rust：`sort_merge.rs:158-168` `DeduplicateMergeFunction::merge` 用 `is_ge` 累加（同值时后入者赢）；后入者顺序由 `sort_merge.rs:609,693` 的 LoserTree 决定，最终回到 `kv_file_reader.rs:289-314` 遍历 `split.data_files()` 的顺序，而该 vec 上溯到 `table_scan.rs:607-613` 的 HashMap 分组（无序）。
  - Java：`SortMergeReaderWithMinHeap` 按 `(level, max_sequence_number)` 排序构造 reader，tie-break 稳定。

#### C9. bucket-key 字段验证仅按 name，不校 type

- **现象**：在 schema evolution 改了 bucket-key 列的类型（理论不该发生，但若发生）后，`bucket_predicate` 不会拒绝；hash 出错的 bucket id → 跳过的 bucket 里有数据 → 漏读。
- **代码位置**：
  - Rust：`crates/paimon/src/table/read_builder.rs:88-97` 只查 `has_all_bucket_fields`（name 比对）。
  - Java：`paimon-core/.../KeyValueFileStoreScan.java:120-124` `BucketSelectConverter.convert` 校验类型。

### LOW — 信息级 / 待审计

#### C10. `_VALUE_KIND` 列 NULL / 缺失时静默回退到 INSERT

- `sort_merge.rs:309-315` `value_kind()` 在列为 NULL 或 downcast 失败时返回 `0` (INSERT)。理论上不该发生（schema 强制 non-nullable），但失败模式是 fail-open（错当成 INSERT）而非 fail-loud。

#### C11. type promotion 缺严格 compatibility check

- `data_file_reader.rs:256-271` 直接调 `arrow_cast::cast`，对 narrowing cast（如 BIGINT→INT 越界）默认是 silently truncate。Java 通过 `SchemaEvolutionUtil.checkCompatibility` 在演化时拒绝不兼容变更，Rust 没有等价 precheck。

#### C12. user sequence-field 类型覆盖有限

- `sort_merge.rs:322-365` `user_sequence` 只识别有限类型（详见代码里 hard-coded list），其它类型回退到系统 `_SEQUENCE_NUMBER`。混合情况下（部分行有 user seq，部分行 None）排序会不稳定。

#### C13. KV reader schema evolution 无单测

- `arrow/schema_evolution.rs:74-150` 有 6 个针对 `create_index_mapping` 的单测（identity / added / dropped / reordered / renamed），**但没有任何测试覆盖 KV reader 跨 schema_id 读** —— C3 这种"`_SEQ/_VK` prefix 与 mapper 失配"的 bug 直接被漏掉。补一个 KV evolution 单测能立即触发 C3。

---

## 维度 2 — 功能 (Functionality)

paimon-java 有、paimon-rust **完全没实现**或**仅枚举占位**的能力。用了就报错或得不到预期效果。

<!-- SECTION-FUNCTIONALITY -->

### F1. Merge engines：仅 Deduplicate + 基础 PartialUpdate

- **能力**：用户用 `merge-engine = aggregate / first-row / versioned-partial-update` 配置的表。
- **Java**：`paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyTableUtils.java:59` 分派 5 种引擎。其中 `AggregateMergeFunction` 配套 20+ `FieldAggregator`（在 `mergetree/compact/aggregate/`）：`Sum / Max / Min / First(NonNull) / Last(NonNull) / Listagg / Product / BoolAnd / BoolOr / HllSketch / ThetaSketch / RoaringBitmap32 / RoaringBitmap64 / Collect / NestedUpdate / NestedPartialUpdate / MergeMap / IgnoreRetract / PrimaryKey`。
- **Rust 现状**：`crates/paimon/src/table/sort_merge.rs:150` 只有 `DeduplicateMergeFunction`，`sort_merge.rs:198` 有 `PartialUpdateMergeFunction`。`MergeEngine::FirstRow`（`spec/core_options.rs:80`）**枚举占位但无对应 MergeFunction 实现**；遇到该配置会直接报错或行为退化。Aggregate / VersionedPartialUpdate **完全没有**。
- **影响场景**：所有依赖聚合或 first-row 语义的 PK 表用 paimon-rust 读取会得到错误的合并结果或 panic。

### F2. Snapshot startup modes：仅 latest

- **能力**：从某个 snapshot / tag / timestamp / file-creation-time 启动扫描；compacted-only 视图。
- **Java**：`paimon-core/.../table/source/AbstractDataTableScan.java:214` 的 `createStartingScanner` 调度多种批模式：`LATEST_FULL / FROM_SNAPSHOT(_FULL) / FROM_TIMESTAMP / FROM_FILE_CREATION_TIME / FROM_TAG / COMPACTED_FULL`。
- **Rust 现状**：`crates/paimon/src/table/table_scan.rs:21` 注释明确写源自 pypaimon `FullStartingScanner`，**只有 latest 全量**一种。`scan.startup-mode / scan.snapshot-id / scan.tag-name / scan.timestamp-millis / scan.file-creation-time-millis` 全部 option 无效。
- **影响场景**：time-travel 查询、tag 回滚校验、按时间戳读历史快照全部不能用。

### F3. Branch / Tag 读

- **能力**：`scan.branch / scan.tag-name` 读分支或某 tag 的快照。
- **Java**：`SnapshotManager` 接 `branch` 参数，`TagManager` 提供 tag → snapshot 解析；`StaticFromTagStartingScanner` 把 tag 转 snapshot id 进入 plan。
- **Rust 现状**：`Catalog` 接口里有 tag / branch 相关 API（`paimon::TagManager`、`paimon::table::*` 中可见），但**读路径上没有把 tag/branch 作为 startup mode 接入**。`TableScan` 层只读 latest snapshot。
- **影响场景**：tag 创建可以，但读 tag 跑不起来。

### F4. Incremental scan（两 snapshot 之间的增量批读）

- **能力**：批增量读 —— 给定两个 snapshot 边界，读其间变更（DIFF / DELTA 模式）。
- **Java**：`AbstractDataTableScan.createIncrementalStartingScanner`（line 364）调度 `IncrementalDeltaStartingScanner / IncrementalDiffStartingScanner`。`SnapshotReaderImpl.readChanges()`（line 756）输出 `IncrementalSplit`。
- **Rust 现状**：完全没有。`TableScan` 是一次性 plan，`Plan::splits()` 返回 `Vec<DataSplit>`，无 incremental scan 概念。
- **影响场景**：增量同步 / 数据校验 / batch 级 CDC 不能用 paimon-rust。

### F5. System tables：全部缺失

- **能力**：通过 `<table>$audit_log / $files / $snapshots / $partitions / $options / $schemas / $tags / $manifests / $branches / $consumers / $row_tracking / $compact_buckets / $read_optimized` 等子表读元数据 / 审计 / CDC。
- **Java**：`paimon-core/.../table/system/` 下 20+ 个系统表实现（`AuditLogTable / FilesTable / SnapshotsTable / PartitionsTable / OptionsTable / SchemasTable / TagsTable / ManifestsTable / BranchesTable / ConsumersTable / BucketsTable / CompactBucketsTable / ReadOptimizedTable / AggregationFieldsTable / TableIndexesTable / BinlogTable / StatisticTable / RowTrackingTable / AllTableOptionsTable / AllTablesTable / AllPartitionsTable / CatalogOptionsTable / FileMonitorTable`）。
- **Rust 现状**：`crates/paimon/src/catalog/` 只有 `filesystem.rs`、`mod.rs` 等基础 catalog 实现，**没有任何 system table 实现**。Catalog 不识别 `$audit_log` 等后缀。
- **影响场景**：审计 / 元数据查询 / 调试场景全部走不通；运维必须用 Java/SQL gateway。

### F6. File index pruning：reader 端为零

- **能力**：bloom filter / hash / BSI（bit-slice index）/ range-bitmap / bitmap，作为 manifest entry 的 embedded 索引在 plan 阶段做 per-file 裁剪。
- **Java**：indexer 工厂在 `paimon-common/.../fileindex/{bloomfilter,hash,bsi,rangebitmap,bitmap}/`；reader 入口 `FileIndexPredicate.evaluate(Predicate)`（`paimon-common/.../fileindex/FileIndexPredicate.java:75`）；接入点 `KeyValueFileStoreScan.filterByFileIndex`（line 197），由 `CoreOptions.FILE_INDEX_READ_ENABLED`（`CoreOptions.java:380`）开关。
- **Rust 现状**：`crates/paimon/src/file_index/` 目录**只有 `file_index_format.rs` 一个文件 + `mod.rs`**，仅提供 file-index 文件的格式读写（`FileIndex`、`FileIndexFormatReader`），**没有 bloom / hash / BSI 等任何索引类型的 evaluate 实现**。plan 阶段调不到 file-index 裁剪。
- **影响场景**：写入侧打的索引在 paimon-rust 读侧完全无效，索引文件白存。

### F7. Predicate operators：缺字符串系 + Between + Transform

- **能力**：完整的谓词算子表达力。
- **Java**：25+ leaf functions（`paimon-common/src/main/java/org/apache/paimon/predicate/`）：`Equal / NotEqual / LessThan / LessOrEqual / GreaterThan / GreaterOrEqual / IsNull / IsNotNull / Like / StartsWith / EndsWith / Contains / In / NotIn / Between / NotBetween / AlwaysTrue / AlwaysFalse / VectorSearch / TopN / SortValue`，加 `Transform` 系列（`Lower / Upper / Trim / Substring / Concat / ConcatWs / Cast / Null`）。`LikeOptimization` 自动把 `Like` 重写为 `StartsWith/EndsWith/Contains/Equal`；`Between.optimize` 把 `>=x AND <=y` 折成 `Between`。
- **Rust 现状**：`crates/paimon/src/spec/predicate.rs:215` 的 `PredicateOperator` 只有 **10 种**：`IsNull / IsNotNull / Eq / NotEq / Lt / LtEq / Gt / GtEq / In / NotIn`。**缺**：`Like / StartsWith / EndsWith / Contains / Between / NotBetween`。Transform 系列**完全无概念**。
- **影响场景**：DataFusion / 任何上层 SQL 引擎下推一个 `LIKE 'x%'` 到 paimon-rust 都会 fallback 成全扫描。

---

## 维度 3 — 性能 (Performance)

功能等价但 paimon-rust 跑得慢的场景。

### F8. Sort engine 选项：仅 LoserTree

- **能力**：`sort-engine = min-heap | loser-tree` 二选一。
- **Java**：`SortMergeReader` 接口分派两种实现 —— `SortMergeReaderWithMinHeap`（`paimon-core/.../mergetree/compact/SortMergeReaderWithMinHeap.java:79`）和 `SortMergeReaderWithLoserTree`。MIN_HEAP 在 run 数少时常数更小；LOSER_TREE 在 run 数多时比较次数更少。
- **Rust 现状**：`crates/paimon/src/table/sort_merge.rs:18` 注释明确只实现了 LoserTree。无 option 开关。
- **影响场景**：少 run 场景下少量额外比较开销（通常可忽略）；功能上**不阻塞**任何用例。

### F9. LookupMerge：level-0 + DV 快路径

- **能力**：DV 模式下 level≥1 文件已全部应用 DV，可绕过 sort-merge 走 raw read；少量 L0 文件用 lookup（点查）方式合并到 raw 上，整体省一次完整 sort-merge。批读侧也能受益（DV-PERFORMANCE 模式）。
- **Java**：`LookupMergeFunction` + `LookupChangelogMergeFunctionWrapper` + `LookupLevels` / `LookupFile` 在 `paimon-core/.../mergetree/compact/`、`mergetree/`。读侧 `MergeFileSplitRead` 在 DV-PERFORMANCE 模式下走这条。
- **Rust 现状**：完全没有 lookup-merge 概念。L0 + L1+ 不论 DV 状态都走完整 sort-merge（且如 C2 所述，DV 还会直接 reject）。
- **影响场景**：DV 表读吞吐远不及 Java；叠加 C2 直接读不了。

### F10. PartialUpdate 进阶子语义

- **能力**：`fields.<g>.sequence-group / ignore-delete / partial-update.remove-record-on-delete / …-on-sequence-group`，以及对 DELETE/UPDATE_BEFORE 行的处理。
- **Java**：`PartialUpdateMergeFunction` 配套（`PrimaryKeyTableUtils.java` 内常量；`CoreOptions.java:389-393`）。
- **Rust 现状**：`PartialUpdateMergeFunction`（`sort_merge.rs:198`）有基础合并逻辑，但 `sort_merge.rs:222-228` 对 `DELETE` / `UPDATE_BEFORE` 行**直接 `Error::Unsupported`**；子选项（sequence-group / ignore-delete / remove-on-delete）也未审计实现。
- **影响场景**：任何接 CDC 上游或带删除语义的 partial-update 表都跑不通。

### F11. Scan-time dynamic options override

- **能力**：临时给一次读传一组 option（如 `source.split.target-size`、`read.batch-size`、`deletion-vectors.read-mode`），不修改持久 schema。
- **Java**：`FileStoreTable.copy(dynamicOptions)` 拷一份带覆盖 option 的表对象再 scan。
- **Rust 现状**：`Table` / `ReadBuilder` 没有 `with_dynamic_options(...)`。要改 read-side option 只能 `Catalog.alter_table(SchemaChange::set_option(...))`，本会话用 `examples/alter_option_demo.rs` 包了一下，但语义是**持久化修改**而非临时 override。
- **关联 issue**：[issues #7](./pk-read-issues.md#问题-7--sourcesplittarget-size-不能在读侧动态覆盖)

---

<!-- SECTION-PERFORMANCE -->

### P1. Reader 跨 split 不并行

- **差距**：库内对一组 splits 的处理是 `try_stream! { for split in splits { ... } }`，严格串行 poll；多 split 不会自动并发解码。
- **量化**：`mor_primitive_100m_1b`（1 split 含 8 个 200 MB 文件）单核满载 24s；`mor_primitive_100m_16b`（16 split）若由 caller 起 16 个 `tokio::spawn` 各跑 `to_arrow` 才有 ~5× 加速。
- **代码位置**：
  - Rust：`crates/paimon/src/table/data_file_reader.rs:84`、`kv_file_reader.rs:275` 都是串行 for-loop。
  - 当前 caller 兜底：`examples/read_local_demo.rs` `#[tokio::main(worker_threads = 16)]` + 16 task spawn。
- **Java**：reader 内部也不主动并发，但 Flink/Spark 引擎按 split 分发任务到不同 TaskManager / executor，天然并行。Rust 还没有等价的"按 split 分任务"层。
- **关联 issue**：[issues #6](./pk-read-issues.md#问题-6--reader-跨-split-不并行)

### P2. `read.batch-size` 通路缺失，parquet 默认 1024

- **差距**：parquet 内层 batch_size 与 sort-merge 输出 batch_size 都没有从 `CoreOptions` 读 `read.batch-size`；当前分支为对齐 Java 默认值 8192 写死了硬编码。
- **量化**：100M_1b 改前 97,657 batches × 1024 → 改后 12,255 batches × 8160；drain_ms 26154 → 23324（~10% 性能提升，主要来自减少分配/释放的 sys time）。
- **代码位置**：
  - Rust：`crates/paimon/src/table/data_file_reader.rs:219`（写死 `Some(8192)`）、`kv_file_reader.rs:336`（写死 `.with_batch_size(8160)`）；正确的实现应来自 `CoreOptions::read_batch_size()`，但该 getter 不存在。
  - Java：`CoreOptions.READ_BATCH_SIZE`（`paimon-api/.../CoreOptions.java:1354`，默认 1024 但通常被显式调到 8192）流入 `ParquetReaderFactory`、ORC vectorised batch size。
- **关联 issue**：[issues #3](./pk-read-issues.md#问题-3--sort-merge-输出-batch_size-写死-1024疑似在该边界丢行) / [#4](./pk-read-issues.md#问题-4--parquet-内部-batch_size-默认-1024arrow-默认) / [#5](./pk-read-issues.md#问题-5--readbatch-size-option-通路缺失)

### P3. 非 PK 谓词在 PK MoR 路径**无条件**剥光（vs Java 仅 overlapping section 剥）

- **差距**：paimon-rust 的 `KeyValueFileReader::new`（`kv_file_reader.rs:69-91`）一刀切丢弃所有非 PK 谓词，理由是"防止跨版本 anomaly"。但 paimon-java 在 `MergeFileSplitRead.withFilter:182-219` 走更精细的拆分：把 filter 分成 `filtersForKeys`（PK-only）和 `filtersForAll`（全 filter），**只在 overlapping sections 用 PK-only**，**non-overlapping single-run sections 仍下推全 filter** 到 parquet stats / row-group / file-index 各层。Rust 这种"宁可错杀"的策略在多数实际数据布局下浪费大量裁剪机会。
- **量化**：`--filter v_int:INT>=5000000` 在 10M 表上 paimon-rust 返回全部 10M 行（demo residual filter 丢 5M 才得到正确 5M）；理论上 Java 在 single-run section 上能直接 parquet row-group 裁剪掉一半。
- **代码位置**：
  - Rust：`crates/paimon/src/table/kv_file_reader.rs:69-91` 无差别剥离；sort-merge 之后**也没补 post-merge filter**。
  - Java：`paimon-core/.../operation/MergeFileSplitRead.java:182-219` 拆分；`MergeFileSplitRead.java:313-317` 按 section 类型选择 `filtersForKeys` 或 `filtersForAll`。
- **关联 issue**：[issues #2](./pk-read-issues.md#问题-2--非-pk-谓词在-pk-mor-路径被丢弃无-post-merge-兜底)（issues 那边定位是 "正确性 + 易用性"；本文从性能角度补充）

### P4. File-index pruning 完全缺位

- **差距**：F6 的功能性表述。**性能侧**：bloom/BSI/range 等本来就是为 plan 阶段裁文件用的，paimon-rust 不读不评估，所有打了索引的文件全扫。
- **量化**：未实测，但 bloom/BSI 命中率高的表（点查 / 等值过滤）影响可达数十倍。
- **代码位置**：
  - Rust：`crates/paimon/src/file_index/` 只有 format reader，无 evaluator。
  - Java：`KeyValueFileStoreScan.filterByFileIndex` (line 197) + `FileIndexPredicate.evaluate`（`paimon-common/.../fileindex/FileIndexPredicate.java:75`）。

### P5. Limit pushdown 仅 split 级，缺 manifest-entry 级

- **差距**：Java 把 limit 同时下推到 manifest 扫描（`KeyValueFileStoreScan.applyLimitWhenNoOverlapping`，`KeyValueFileStoreScan.java:278`）和 split 级（`DataTableBatchScan.applyPushDownLimit`，line 134）。第一道在非重叠 + 无 DV + 非 PARTIAL_UPDATE/AGGREGATE 时把 split 内**部分文件**提前裁掉。
- **Rust 现状**：`ReadBuilder::with_limit`（`read_builder.rs:212`）只透传给 split 计数；manifest 阶段没有 limit-aware 文件裁剪。
- **量化**：未实测；小 limit 大表场景影响显著。

### P6. Whole-bucket value filter 缺位

- **差距**：Java `KeyValueFileStoreScan.filterWholeBucketAllFiles/PerFile`（`KeyValueFileStoreScan.java:301, 311`）在 ALL-mode 下，整桶里没有任何文件能命中 value filter 时把整个 bucket 丢掉。
- **Rust 现状**：scan 不区分 ALL-mode；逐文件粗筛过来，bucket 级粗筛缺。
- **量化**：高基数 + bucket 数多的表，value filter 选择性高时可能整桶丢；目前 Rust 全读。

### P7. Parquet 多层下推深度未审计

- **差距**：Java parquet 走 `RowGroupFilter` 多层级（`STATISTICS / DICTIONARY / BLOOMFILTER / COLUMN_INDEX`，见 `ParquetFileReader.java:408-421`）。Rust 走 arrow-rs `ParquetRecordBatchStreamBuilder` 的 `with_row_filter` + `with_row_selection`，**审计确认当前读路径未显式利用 Parquet COLUMN_INDEX、BLOOMFILTER、DICTIONARY 三类内部索引**。
- **代码位置**：
  - Rust：`crates/paimon/src/arrow/format/parquet.rs:149-200`。
  - Java：`paimon-format/.../parquet/ParquetReaderFactory.java:95+` `withRecordFilter / ParquetReadOptions`。
- **量化**：未实测。
- **审计结论（HEAD `029a159`）**：paimon-rust 已具备 manifest stats prune、row-group STATISTICS prune、per-row RowFilter；**未在读路径显式利用 parquet COLUMN_INDEX (page-level)、BLOOMFILTER、DICTIONARY** 三类 parquet 内部索引。`ArrowReaderOptions` 默认 `PageIndexPolicy::Skip`，metadata 不加载 page index；`get_row_group_column_bloom_filter` 在仓库零调用；写端 `ParquetFormatWriter::new`（`parquet.rs:65-73`）只设 compression，未启用 bloom 写入。
- **实施方案**：[`parquet-pushdown-plan.md`](./parquet-pushdown-plan.md) — Stage 1（Page-Index page-level prune，必做、复用 stats-safe 谓词语义，无法安全判断时 fail-open）+ Stage 2（Bloom Filter，对 Eq/In）+ Stage 3（Dictionary filter，follow-up）。完成后本条由 ❓ → ✓。
- **进度**：Stage 1 + Stage 2 已落地。Stage 1：`read.parquet.page-index.enabled` 默认 on，`build_predicate_page_selection` 用 ColumnIndex/OffsetIndex 在 row-group 之外补一层 page-level RowSelection；缺索引 / null page / 保守 op (NotEq/NotIn/EndsWith/Contains/general Like/NotBetween) 一律 fail-open。Stage 2：`read.parquet.bloom-filter.enabled` 默认 off（写端尚未生成 bloom），开启后对 Eq / In leaf 调 `Sbbf::check` 在 row-group 级 prune；其它 op fall-open。Stage 3 (Dictionary filter) 仍待办。

### P8. PlanCache 缺位

- **差距**：Java `SnapshotReaderImpl.buildPlanCache()` 在 plan 阶段预化 DV 索引、accelerate-index 元数据、pkmap sidecar 路径等，多次 plan 复用。
- **Rust 现状**：`TableScan::plan` 每次重新读 manifest / DV index，没有 plan 级缓存。
- **量化**：单次 plan 不影响；高频 plan（streaming / repeated batch）放大问题。

### P9. Manifest / file-index / partition cache 覆盖度未知

- **差距**：Java 用 `SegmentsCache`（Caffeine）缓存 manifest 文件；`FILE_INDEX_IN_MANIFEST_THRESHOLD` 内联小索引；partition cache 缓存 partition 元数据。
- **Rust 现状**：`crates/paimon/src/spec/manifest_file_meta.rs` 等有 manifest 抽象，但**是否有跨调用的内存级缓存层未审计**。
- **量化**：repeated read / 大 manifest 场景影响大。

### P10. ORC async read 缺位

- **差距**：Java 在大 ORC 文件场景下用 `AsyncRecordReader`（`KeyValueFileReaderFactory.java:126`），通过 `file-reader-async-threshold` 触发。Rust 无对应路径。
- **影响**：本会话写入用 parquet，影响不大；ORC 场景下 IO / 解码不能 overlap。

### P11. `source.split.target-size` 读侧不能动态覆盖

- 见 F13 / [issues #7](./pk-read-issues.md#问题-7--sourcesplittarget-size-不能在读侧动态覆盖)。性能侧的影响是：要做 split-size 调优实验必须 alter table（落 schema-N+1）才能看效果，迭代速度慢。

---

## 总结矩阵

<!-- SECTION-MATRIX -->

下表按维度排序。**维度**列：C = 正确性、F = 功能、P = 性能。

| ID | 能力 | paimon-java | paimon-rust | 维度 | 关联 issue |
| --- | --- | --- | --- | --- | --- |
| C1 | Split: IntervalPartition + section bin-pack | ✓ `MergeTreeSplitGenerator` | ✗（用 AppendOnly bin-pack） | C-HIGH | #1 |
| C2 | KV 路径接受 DV split | ✓ `ApplyDeletionVectorReader` 包 reader | ✗ 直接 `Error::Unsupported` 报错 | C-HIGH | — |
| C3 | KV reader 给 schema-evo mapper 喂 `[_SEQ, _VK, ...]` 字段 | ✓ `KeyValue.createKeyValueFields` | ✗ 喂用户视角 schema → mapper 找不到 `_SEQ`/`_VK` → panic | C-HIGH | — |
| C4 | L0 跳过 value-stats 裁剪（避免漏最新版本） | ✓ 显式 skip L0 | ✓ entry 级 gate（`should_apply_value_stats_to_entry`）已闭合；端到端 fixture 回归留作 follow-up | C-HIGH | — |
| C5 | raw L1+ 路径剥 DELETE/UPDATE_BEFORE | ✓ `DropDeleteReader` | ✗ 不剥，幽灵行透出 | C-HIGH | — |
| C6 | Bitmap64 DV 解码 | ✓ | ✗ 只识别 32-bit magic | C-HIGH | — |
| C7 | `deletion-vectors.read-mode` 选项识别 | ✓ PERFORMANCE / FRESHNESS | ✗ 选项被静默忽略 | C-HIGH | — |
| C8 | sort-merge 同 PK 同 seq tie-break 稳定 | ✓ 按 (level, max_seq) 排序 | ✗ HashMap 序 → 不确定 | C-MED | — |
| C9 | bucket-key 字段类型校验 | ✓ | ✗ 仅 name 校验 | C-MED | — |
| C10 | `_VALUE_KIND` NULL/缺失时 fail-loud | ✓ | ✗ silently fallback INSERT | C-LOW | — |
| C11 | type promotion 严格 compatibility check | ✓ `SchemaEvolutionUtil.checkCompatibility` | ✗ 直接 `arrow_cast::cast`，narrow 静默截断 | C-LOW | — |
| C12 | user sequence-field 类型覆盖 | ✓ 完整 | ❓ hard-coded 列表 | C-LOW | — |
| C13 | KV reader schema evolution 单测 | ✓ | ✗ 零覆盖 | C-LOW | — |
| F1 | Merge: Aggregate (20+ FieldAggregator) | ✓ | ✗ | F | — |
| F1 | Merge: FirstRow | ✓ | ✗（仅枚举占位） | F | — |
| F1 | Merge: VersionedPartialUpdate | ✓ | ✗ | F | — |
| F2 | Startup mode: FROM_SNAPSHOT / FROM_TIMESTAMP / FROM_TAG / FROM_FILE_CREATION_TIME / COMPACTED_FULL | ✓ | ✗（仅 LATEST_FULL） | F | — |
| F3 | Branch / Tag 读 | ✓ | ✗（API 有但 scan 不接） | F | — |
| F4 | Incremental scan（两 snapshot 间批增量） | ✓ | ✗ | F | — |
| F5 | System tables（audit_log / files / snapshots / ...） | ✓ 20+ | ✗ | F | — |
| F6 | File index reader: bloom / hash / BSI / range / bitmap | ✓ | ✗（只有 format reader） | F | — |
| F7 | Predicate: Like / StartsWith / EndsWith / Contains | ✓ | ✗ | F | — |
| F7 | Predicate: Between / NotBetween | ✓ | ✗ | F | — |
| F7 | Predicate: Transform 系列（Lower / Upper / Trim / ...） | ✓ | ✗ | F | — |
| F8 | Sort engine 选项：MIN_HEAP | ✓ | ✗（仅 LoserTree） | F | — |
| F9 | LookupMerge（L0 + DV 快路径） | ✓ | ✗ | F | — |
| F10 | PartialUpdate 进阶子语义 + DELETE 处理 | ✓ | ✗ DELETE 直接 `Error::Unsupported` | F | — |
| F11 | Scan-time dynamic options override | ✓ `FileStoreTable.copy(opts)` | ✗（只能 ALTER） | F | #7 |
| P1 | Reader 跨 split 并行 | ✓（外部引擎驱动） | ✗（caller 自己 spawn） | P | #6 |
| P2 | `read.batch-size` 通路 | ✓ option 化 | ✗（两处硬编码） | P | #3/#4/#5 |
| P3 | 非 PK 谓词在 single-run section 下推 | ✓ `filtersForAll` | ✗（无差别剥光） | P | #2 |
| P4 | File-index pruning（plan 阶段） | ✓ | ✗ | P | — |
| P5 | Limit pushdown manifest-entry 级 | ✓ | ✗（仅 split 级） | P | — |
| P6 | Whole-bucket value filter | ✓ | ✗ | P | — |
| P7 | Parquet dictionary / column-index filter 透传 | ✓（多层级） | ❓ Stage 1 (page-index) + Stage 2 (bloom) 已落地；dictionary 待办；方案见 [`parquet-pushdown-plan.md`](./parquet-pushdown-plan.md) | P | — |
| P8 | PlanCache | ✓ | ✗ | P | — |
| P9 | Manifest / file-index / partition cache | ✓ Caffeine + 多层 | ❓ 未审计 | P | — |
| P10 | ORC async read | ✓ | ✗ | P | — |
| P11 | `source.split.target-size` 读侧动态覆盖 | ✓ via dynamicOptions | ✗ | P | #7 |

> 图例：✓ = 完整 / ✗ = 缺失 / ❓ = 部分实现或未审计。维度后缀：HIGH = 默认配置可复现 / MED = 边界场景 / LOW = 信息级或待审计。

---

<!-- SECTION-CROSSREF -->

下表把本文条目映射到 [`pk-read-issues.md`](./pk-read-issues.md) 的具体问题编号：

| 本文条目 | issues 编号 | 关系 |
| --- | --- | --- |
| C1 | #1 | 同一问题，issues 侧重正确性 + 修复方向，本文从能力对照角度看 |
| C2–C13 | — | 本文新增（来自本会话三个 audit subagent 的发现） |
| F1–F11 | — | 本文新增（issues 不覆盖未实现 feature） |
| F11 | #7 | 同源能力，issues 侧重当前 ALTER 绕路，本文加 Java `FileStoreTable.copy` 的对照 |
| P1 | #6 | 同一问题 |
| P2 | #3 / #4 / #5 | 同一组问题 |
| P3 | #2 | 本文从性能角度补充：Java 在 single-run section 仍下推非 PK filter |
| P4–P10 | — | 本文新增 |
| P11 | #7 | 同 F11 |

---

<!-- SECTION-REFERENCES -->

### paimon-rust 关键源文件
- `crates/paimon/src/table/{table_scan,table_read,read_builder,kv_file_reader,data_file_reader,sort_merge,bin_pack}.rs`
- `crates/paimon/src/arrow/format/parquet.rs`
- `crates/paimon/src/arrow/filtering.rs`
- `crates/paimon/src/spec/{core_options,predicate,types}.rs`
- `crates/paimon/src/file_index/{mod,file_index_format}.rs`
- `crates/paimon/src/deletion_vector/{mod,core,factory}.rs`

### paimon-java 关键源文件
- `paimon-core/src/main/java/org/apache/paimon/table/source/{MergeTreeSplitGenerator,AppendOnlySplitGenerator,DataEvolutionSplitGenerator,DataTableBatchScan,SnapshotReaderImpl,AbstractDataTableScan}.java`
- `paimon-core/src/main/java/org/apache/paimon/operation/{MergeFileSplitRead,KeyValueFileStoreScan,AbstractFileStoreScan,BucketSelectConverter}.java`
- `paimon-core/src/main/java/org/apache/paimon/io/KeyValueFileReaderFactory.java`
- `paimon-core/src/main/java/org/apache/paimon/mergetree/compact/{SortMergeReader,SortMergeReaderWithMinHeap,SortMergeReaderWithLoserTree,LookupMergeFunction,DropDeleteReader}.java`
- `paimon-core/src/main/java/org/apache/paimon/mergetree/compact/aggregate/Field*Agg.java`（20+ FieldAggregator）
- `paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyTableUtils.java`
- `paimon-core/src/main/java/org/apache/paimon/table/system/*.java`（20+ system tables）
- `paimon-common/src/main/java/org/apache/paimon/predicate/*.java`（leaf functions + visitors）
- `paimon-common/src/main/java/org/apache/paimon/fileindex/{FileIndexPredicate,bloomfilter,hash,bsi,rangebitmap,bitmap}/...`
- `paimon-format/src/main/java/org/apache/paimon/format/parquet/ParquetReaderFactory.java`
- `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java`

### paimon-cpp 对照（与 Java 对齐时的参考实现）
- `paimon-cpp/src/paimon/core/table/source/merge_tree_split_generator.cpp`
- `paimon-cpp/examples/{create_paimon_mor_table,read_hdfs_demo}.cpp`

---

## 补充：paimon-rust 写出的 manifest 不能被 paimon-java 读取

> 本节是**跨实现互操作问题**，与本文主线（Rust 读 Rust 写的表）无关，**不作为主要修复项**列入维度 1/2/3。但鉴于其严重度（Rust 写的表对 Java 整套生态不可见）值得单独记录。

### 现象

paimon-rust 写出的 `manifest-*` 和 `manifest-list-*` 文件，在 paimon-java 端读取时会立刻失败 —— Java 的 manifest serializer 走的是**位置式 Avro 解码**（`VersionedObjectSerializer`），对 schema 字段顺序、类型、空缺都是硬约束。

### 三处核心 mismatch（按 Java 失败顺序）

#### M1. `ManifestEntry`：`_VERSION` 字段位置颠倒（致命）

| | Rust 实际写出 | Java 期望读到 |
|---|---|---|
| 位置 0 | `_KIND : int32` | `_VERSION : int32` |
| 位置 1 | `_PARTITION : bytes` | `_KIND : tinyint` |
| ... | ... | ... |
| 位置 5 | `_VERSION : int32`（最后） | `_FILE : record` |

- **Rust**：`crates/paimon/src/spec/manifest_entry.rs:175-228` `MANIFEST_ENTRY_SCHEMA`，`_VERSION` 排在最后；`_KIND` 是 `int`。
- **Java**：`paimon-core/.../manifest/ManifestEntry.java:44-52` + `VersionedObjectSerializer.java:40-45`，`_VERSION` 在最前；`_KIND` 是 `tinyint`（byte）。
- **失败模式**：Java `VersionedObjectSerializer.fromRow` 读 `row.getInt(0)` 当 version。Rust 在 0 位写的是 `_KIND` —— `_KIND=0`（Add）→ Java 解释为 `version=0` → 抛 `IllegalArgumentException("Unsupported version: 0")`。

#### M2. `ManifestFileMeta`：缺 4 个 bucket/level 范围字段

Rust schema（`manifest_file_meta.rs:175-198`）9 个字段；Java schema（`ManifestFileMeta.java:44-60`）13 个字段。Rust 缺少：
- 位置 7：`_MIN_BUCKET : int (nullable)`
- 位置 8：`_MAX_BUCKET : int (nullable)`
- 位置 9：`_MIN_LEVEL : int (nullable)`
- 位置 10：`_MAX_LEVEL : int (nullable)`

Rust 把位置 7-8 直接当 `_MIN_ROW_ID / _MAX_ROW_ID`（`long`）。

- **失败模式**：Java 按位置 7 读 `_MIN_BUCKET : int`，遇到 Rust 写的 `_MIN_ROW_ID : long` → `AvroTypeException: Expected int, got long`，manifest list 解析直接挂。

#### M3. `DataFileMeta`：缺 2 个尾部字段 + 类型差

Rust schema（`data_file.rs:30-109` + 嵌入在 `MANIFEST_ENTRY_SCHEMA`）20 个字段；Java schema（`DataFileMeta.java:62-92`）22 个字段。Rust 缺：
- 位置 20：`_COMMIT_SNAPSHOT_ID : bigint (nullable)`
- 位置 21：`_MERGE_MODE : tinyint (nullable)`

另外 `_FILE_SOURCE` 在 Rust 是 `int`，Java 是 `tinyint` —— 即便其它字段对齐，这一项也会让 `row.getByte(15)` 出错。

- **失败模式**：DataFileMetaSerializer 读到位置 20/21 越界 → `ArrayIndexOutOfBoundsException`。

### 其它差异（不致命但仍需修）

- 多处 nullability 差：Rust 把 `_PARTITION_STATS / _KEY_STATS / _VALUE_STATS / _FILE` 写成 `["null", record]` 联合（nullable），Java 是 `notNull`。Java 用 Avro 默认 schema reading 在某些版本下能容忍，某些版本不行。
- 整型类型差：除 `_KIND` 和 `_FILE_SOURCE` 外，其它 `int/long` 现状对得上。

### Top 5 最先失败的字段（按 Java 解析报错顺序）

| 优先级 | Mismatch | Rust 位置 | Java 位置 |
|---|---|---|---|
| 1 | `ManifestEntry._VERSION` 位置 last vs first | `manifest_entry.rs:64-65` | `VersionedObjectSerializer.java:42-43` |
| 2 | `ManifestFileMeta` 缺 4 个 bucket/level 字段 | `manifest_file_meta.rs:194-196` | `ManifestFileMeta.java:55-58` |
| 3 | `_KIND` int vs tinyint | `manifest_entry.rs:180` | `ManifestEntry.java:48` |
| 4 | `DataFileMeta` 缺 `_COMMIT_SNAPSHOT_ID` / `_MERGE_MODE` | `data_file.rs:108` | `DataFileMeta.java:91-92` |
| 5 | `_FILE_SOURCE` int vs tinyint | `manifest_entry.rs:219` | `DataFileMeta.java:82` |

### 影响范围

- ✗ **paimon-rust 写 → paimon-java 读**：完全不可用（任意快照都读不出）。
- ✓ paimon-rust 写 → paimon-rust 读：自洽，本文档主线讨论的就是这条链路。
- ❓ paimon-java 写 → paimon-rust 读：未审计。Rust 的 Avro reader 对未识别字段是否容忍 forward-compat 需要单独验证；如果 Rust 也走"严格位置式"解码，Java 写的多 4 个 bucket/level 字段也会让 Rust 跑偏。

### 修复方向（不在本文主线，记录给后续）

1. **`ManifestEntry`** schema 重排：`_VERSION` 移到位置 0；`_KIND` 类型改为 `int` byte（Avro 没有原生 tinyint，paimon Java 用 `int` 序列化但 `RowType` 标 `tinyint` —— Rust 写的时候要按 byte 范围 0-127 自检）。
2. **`ManifestFileMeta`** schema 补 4 个字段。这 4 个字段语义来自 manifest 内部的 data-file 范围聚合，可以在 manifest write 阶段从 entries 算出。
3. **`DataFileMeta`** schema 补 `_COMMIT_SNAPSHOT_ID / _MERGE_MODE`。语义需要从 commit / merge engine 配置取，可能要扩 `Manifest::write` 的输入。
4. 加跨实现 round-trip 测试：用 Java 端的 `ManifestList.read` / `ManifestEntrySerializer.fromRow` 读 Rust 写出的文件，把 mismatch 兜出来。这是结构性问题最有效的回归保护。

> 这一节追加自本会话末尾的一次专项 audit，未深入到位置式 Avro 解码的所有边界情况；其它字段上可能还有更细的差异未被列出。

