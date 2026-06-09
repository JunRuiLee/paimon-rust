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

# paimon-rust 实现 versioned-partial-update merge engine —— 实施方案

## 总览

paimon-java 的 `merge-engine = versioned-partial-update` 在 paimon-rust 当前**完全没实现**：`MergeEngine` 枚举（`crates/paimon/src/spec/core_options.rs:74`）只有 `Deduplicate / PartialUpdate / FirstRow`，任何配 `versioned-partial-update` 的表用 paimon-rust 加载会在 `MergeEngine::from_str` 解析时直接报错。

本文整理出按 Java 端语义复刻一份 `VersionedPartialUpdateMergeFunction` 到 paimon-rust 的**分阶段实施方案**：六个独立可 PR 的 stage，从最小占位到完整功能渐进推进。

### 目标范围

**第一阶段目标 = UPSERT-only batch read parity**：让 Java/Rust 写出的 **UPSERT mode** versioned-partial-update 表能被 paimon-rust 正确 batch SELECT（行级一致），覆盖 single-version + multi-version 列两种形态。

**明确不在第一阶段范围内**：
- **IGNORE mode / runtime `merge-mode=ignore`**：依赖 lookup capability + DV 读路径，paimon-rust 当前都不具备，第一阶段在 schema 加载与 commit 阶段一律 fail-loud reject（详见 Stage 5）。merge function 内部可保留 IGNORE 分支的逻辑设计，但 validation 不放开。
- **完整 compaction parity**：本文实现的 merge function 只服务 batch SELECT 输出，不能直接复用到 compaction/CDC/changelog producer/lookup changelog 等需要保留 internal DELETE/tombstone 的场景（见风险 #3）。
- **完整 CDC / changelog 流式输出**：Java `getResult` 在 retract-without-followup 时返回 `RowKind.DELETE`；本文 batch read 路径返回 `MergeResult::Omit`，**这条决策只对终端 SELECT 输出成立**。
- **Rust 端写出 versioned-partial-update 表给 Java 读全量验证**：依赖 manifest schema 全量对齐（[`pk-read-issues.md`](./pk-read-issues.md) 补充节 M1-M2 仍 open）；本文只在 schema-level/unit-level 验证新增字段编码正确，**不把"Java 全量读"作为本计划 gate**。
- **Aggregate / FieldAggregator 框架**：见 Stage 5 fail-loud 策略 —— **任何 aggregate 配置一律 reject，不做 silent fallback**。

- **范围**：Java 行为速览 + 6 个 stage 改动清单 + 关键风险 + 验证方式 + 工作量估计。**不写代码，只整理方案**。
- **与已有内部文档关系**：
  - 解决 [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) 的 **F1**（VersionedPartialUpdate 缺失）和 **F10**（PartialUpdate 进阶子语义 + DELETE 处理）；
  - 完整修 [`pk-read-issues.md`](./pk-read-issues.md) **补充节 M3**（`DataFileMeta` 缺 `_COMMIT_SNAPSHOT_ID` 与 `_MERGE_MODE` 两个字段）—— Stage 2 一起补，否则 versioned-partial-update 没有 snapshot ordering 基础设施。

<!-- SECTION-JAVA-SUMMARY -->

## Java 行为速览

来自本会话 `paimon-core/.../mergetree/compact/VersionedPartialUpdateMergeFunction.java` 的 audit 结论：

### Per-file mode（关键架构特征）

每个 KV 数据文件在写入时被打上 `VersionedMergeMode` 字节（`UPSERT=0` / `IGNORE=1`），从 `CoreOptions.versioned-partial-update.merge-mode` 读取，序列化到 `DataFileMeta._MERGE_MODE`（位置 21），读路径上传到 `KeyValue.mergeMode()`。**不是表级而是文件级开关** —— 同一张表里不同文件可以是不同 mode。

### Snapshot ordering 链路（关键正确性基础）

versioned-partial-update **强依赖 same-PK 的多条记录在 merge 时按 `(commit_snapshot_id, sequence_number)` 升序处理**。Java 的链路是：

1. `FileStoreCommitImpl.assignCommitSnapshotId` 在 commit 阶段给 ADD entries 填上当前 snapshot id（`DataFileMeta.commitSnapshotId()`）；
2. manifest 里序列化为 `_COMMIT_SNAPSHOT_ID` 字段（`DataFileMeta` 位置 20，`_MERGE_MODE` 在位置 21 之前）；
3. 读时 `KeyValueDataFileRecordReader` 把 `DataFileMeta.commitSnapshotId()` 透传到每条 `KeyValue.snapshotId()`；
4. `SortMergeReaderWithMinHeap` 在 same-key 下按 `snapshotId` 再按 `sequenceNumber` 排序。

**没有 `_COMMIT_SNAPSHOT_ID` 时 versioned-partial-update 必然出错**：多 writer / 跨 snapshot / sequence 重用场景下 IGNORE/UPSERT 会按错误顺序 apply，结果非确定。所以本计划 Stage 2 必须**两个字段一起补**，且 commit 阶段要按 Java 语义 assign。

### 两类列（结构匹配自动识别）

- **Multi-version (MV)** 列：结构 `RowType<latest_version VARCHAR, latest_value T, all_versioned_values MAP<VARCHAR,T>>`，三字段且 T 严格相等；通过结构匹配自动识别（`VersionedPartialUpdateMergeFunction.java:438-457`，`isMultiVersionType`）。
- **Single-version**：其它列。

### 合并语义（`add(kv)` lines 162-239）

- 输入按 `(snapshot_id, sequence_number)` **严格升序**到达（`advanceSequenceNumber` 强校验）；
- DELETE：清空所有状态 + 标 `currentDeleteRow=true`；UPDATE_BEFORE 静默丢弃；INSERT/UPDATE_AFTER 抹掉之前的 delete 标记；
- 单版本列 + UPSERT 文件：value 非 null 就覆盖；
- 单版本列 + IGNORE 文件：只在当前是 null 时填入；
- MV 列：累加到 `HashMap<String, T>`；UPSERT 模式 put 总覆盖，IGNORE 模式只在 key 不存在时 put；
- 有 `fields.<f>.aggregate-function` 配置的列：调用 aggregator，**完全忽略 mode**（短路在 mode 判断之前）。

### 输出（`getResult` lines 314-344）

- 如有 retract 且没有后续 insert → 整行 `RowKind.DELETE`；
- 否则重建每个 MV 列：按 map key **lex order**（不是数值序，`"v9" > "v10"`）取最大作为 `latest_version`，rebuild 整个 map；
- sequence number 用最后一条 insert 的。

### Validation 规则（`SchemaValidation.java:249-289`）

1. `sequence.snapshot-ordering=true` 必填；
2. `changelog-producer` 必须是 `none` 或 `lookup`；
3. `versioned-partial-update.ignore-mode.enabled=true` 时表必须有 lookup capability（DV / force-lookup / lookup changelog）；
4. PK 必填，`sequence.field` 必须为空；
5. MV 列存在时 `fields.default-aggregate-function` 拒绝；
6. MV 列上禁用 `fields.<mv>.aggregate-function`。


<!-- SECTION-STAGES -->

## 实施分阶段

每阶段独立 PR-able，**强烈建议按顺序推进**。

### Stage 1 — 枚举 + option + per-file mode 占位（小，1-2 小时）

**目标**：让 `merge-engine = versioned-partial-update` 解析成功，运行时返回 `Error::Unsupported` 而不是 panic。让上层 catalog/schema 加载现有表不再炸。

**改动**：
- `crates/paimon/src/spec/core_options.rs:74` `enum MergeEngine` 加 `VersionedPartialUpdate` 变体；`from_str` 加 `"versioned-partial-update"` 映射（line 169-171 旁）。
- 同文件加常量：
  - `VERSIONED_PARTIAL_UPDATE_MERGE_MODE_OPTION = "versioned-partial-update.merge-mode"`，默认 `"upsert"`；
  - `VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED_OPTION = "versioned-partial-update.ignore-mode.enabled"`，默认 `true`；
- 新增 `pub enum VersionedMergeMode { Upsert, Ignore }`，`impl FromStr` + `to_byte()`。
- `CoreOptions` 加 getter：`versioned_partial_update_merge_mode() -> VersionedMergeMode`、`versioned_partial_update_ignore_mode_enabled() -> bool`。
- `crates/paimon/src/table/table_read.rs::to_arrow / read_pk` —— **关键 dispatch 点**：当前 `read_pk` 只 match `MergeEngine::Deduplicate | PartialUpdate`，新增 enum 后必须把 `VersionedPartialUpdate` 也接入这里，**所有 splits 一律走 `read_kv`（sort-merge）**，不允许 L1+ raw path 直接返回未合并的 partial columns / DELETE / MV 中间状态。后续如果想优化 raw path，需要单独证明 compacted 文件已完整物化且不会透出 tombstone。
- `crates/paimon/src/table/kv_file_reader.rs:106` 的 `new_merge_function` switch 加 `MergeEngine::VersionedPartialUpdate => Err(Unsupported{ "Not yet implemented, see plan stage 4" })`。

**Verification**：单测：`MergeEngine::from_str("versioned-partial-update")` 返回 Ok；getter 默认值正确；`new_merge_function` 报错信息友好；`TableRead::to_arrow` 在 `VersionedPartialUpdate` 表上调用走 `read_kv` 路径（不绕开 sort-merge）。

### Stage 2 — `DataFileMeta` 补齐 `_COMMIT_SNAPSHOT_ID` + `_MERGE_MODE`（中-大，6-10 小时）

**目标**：让 manifest schema 的位置 20 / 21 字段一起补齐，把 snapshot ordering 链路 + per-file mode 同时打通。这一阶段也直接修了 [`pk-read-issues.md`](./pk-read-issues.md) 补充节 M3 的两个缺失字段。

**为什么必须一起补**：见上文「Snapshot ordering 链路」—— versioned-partial-update 的 same-PK ordering 同时依赖 `commit_snapshot_id` 和 `sequence_number`。只补 `_MERGE_MODE` 而不补 `_COMMIT_SNAPSHOT_ID`，跨 snapshot / sequence 重用 / 多 writer 场景下顺序就是非确定的。

**改动**：

1. **`DataFileMeta` 加两个字段**：
   - `crates/paimon/src/spec/data_file.rs` 的 `DataFileMeta` 加 `pub commit_snapshot_id: Option<i64>`（位置 20）和 `pub merge_mode: Option<i8>`（位置 21），都默认 `None`。
   - 老 manifest 缺这两个字段时落地为 `None`，跟 Java forward-compat 对齐。
2. **Manifest entry schema**：`crates/paimon/src/spec/manifest_entry.rs` 的 `MANIFEST_ENTRY_SCHEMA`（lines 175-228）embedded `_FILE` record 末尾加：
   ```json
   {"name": "_COMMIT_SNAPSHOT_ID", "type": ["null", "long"], "default": null},
   {"name": "_MERGE_MODE",         "type": ["null", "int"],  "default": null}
   ```
   注意 Java 的 `_MERGE_MODE` RowType 标 `tinyint` 但 Avro 实际写 `int`。Rust 端**严格保持 Avro `int`**，序列化层不要试图用 `int8`。
3. **手写 decoder / default 覆盖**：`crates/paimon/src/spec/avro/manifest_entry_decode.rs` 的手写 decoder 和 default value 路径都要识别这两个字段，老文件 fallback 为 `None`。
4. **commit 阶段 assign commit snapshot id**（**新增步骤，对齐 Java `FileStoreCommitImpl.assignCommitSnapshotId`**）：
   - 写侧 sentinel：当 `sequence.snapshot-ordering=true`（versioned-partial-update 必为 true），`MergeTreeWriter` 等价物在产出新 ADD 文件时**预填 `commit_snapshot_id = Some(i64::MAX)`** 作为"待 assign"标记，对齐 Java `Long.MAX_VALUE`。
   - commit 阶段覆盖规则（**严格对齐 Java**）：在 snapshot commit 真正确定 snapshot id 后，扫所有 ADD entries：
     - `commit_snapshot_id == None` → 填当前 snapshot id；
     - `commit_snapshot_id == Some(i64::MAX)` → 填当前 snapshot id（这是上面写侧塞的 sentinel）；
     - `commit_snapshot_id == Some(other_real_id)` 且 `other_real_id != i64::MAX` → **不覆盖**（写侧已显式给定，比如 compaction reschedule 已知来源 snapshot 的旧文件）。
     - DELETE entries 不被 assign。
   - Rust 入口在 `crates/paimon/src/table/table_commit.rs` 的 commit 路径，在 manifest write 之前 patch 这个字段。
   - **`UNKNOWN_SNAPSHOT_ID` 与 sentinel 不是同一概念**：
     - `i64::MAX`（=Java `Long.MAX_VALUE`）= **写侧"待 assign" sentinel**，commit 阶段必被替换；
     - `UNKNOWN_SNAPSHOT_ID = -1`（或 `i64::MIN`，对齐 Java 习惯）= **读侧老文件 fallback**：老 manifest 缺 `_COMMIT_SNAPSHOT_ID` 字段时，读出 `None`，merge function 把它换成 `UNKNOWN_SNAPSHOT_ID`，要求该值小于所有合法 snapshot id，这样跟新文件的 row 比较时总是被新文件 override，符合"老文件先于新文件 apply"的语义。
     - 两者**不能混用**：sentinel 是写侧合法状态，UNKNOWN 是读侧兜底。
5. **写入侧填 `merge_mode`**：`crates/paimon/src/table/kv_file_writer.rs` 写出 KV 文件时，根据 `CoreOptions::versioned_partial_update_merge_mode()` 把 mode 写到 `DataFileMeta.merge_mode`（仅当 `merge_engine = VersionedPartialUpdate` 时填，否则 `None`）。
6. **读路径透传**：`crates/paimon/src/table/kv_file_reader.rs` 把 `data_file.commit_snapshot_id.unwrap_or(UNKNOWN_SNAPSHOT_ID)` 和 `data_file.merge_mode.unwrap_or(VersionedMergeMode::Upsert as i8)` 一并传给 sort-merge 层（见 Stage 3 的 `MergeRow` 扩展）。`UNKNOWN_SNAPSHOT_ID` 常量建议用 `i64::MIN` 或与 Java 同步的 sentinel。

**完整 DataFileMeta 调用点列表**：所有以下文件存在 `DataFileMeta { ... }` 直接构造，加字段后必须补默认值（`commit_snapshot_id: None, merge_mode: None`）：

- `crates/paimon/src/spec/objects_file.rs`
- `crates/paimon/src/spec/manifest.rs`
- `crates/paimon/src/spec/avro/manifest_entry_decode.rs`
- `crates/paimon/src/table/bin_pack.rs`
- `crates/paimon/src/table/table_scan.rs`
- `crates/paimon/src/table/source.rs`
- `crates/paimon/src/table/postpone_file_writer.rs`
- `crates/paimon/src/table/data_evolution_writer.rs`
- `crates/paimon/src/table/data_file_writer.rs`
- `crates/paimon/src/table/kv_file_writer.rs`
- `crates/paimon/src/table/table_commit.rs`
- `crates/paimon/src/table/data_evolution_reader.rs`
- `crates/paimon/src/table/referenced_files.rs`

实际清单以 `cargo check` + `grep "DataFileMeta {"` 为准。

**Verification**：
- **正向**：用 paimon-java 写一张 versioned-partial-update 表，paimon-rust 能读 manifest（不再 IndexOutOfBounds）；`commit_snapshot_id` / `merge_mode` 两个字段正确解出。
- **schema-level round-trip**：单测序列化 / 反序列化 `DataFileMeta { commit_snapshot_id, merge_mode, ... }` 用本仓库的 manifest entry encoder/decoder 跑两遍，确认字段位置 + Avro 类型 + null 编码字节级一致。
- **反向 Rust 写 → Java 读**：**不作为本计划 gate** —— 还依赖 [`pk-read-issues.md`](./pk-read-issues.md) 补充节 M1（`ManifestEntry._VERSION` 位置颠倒）和 M2（`ManifestFileMeta` 缺 4 个 bucket/level 字段）的全量对齐工作。本 PR 只能保证 `_COMMIT_SNAPSHOT_ID` / `_MERGE_MODE` 两个字段的 wire 格式跟 Java 一致。
- **commit assign 单测矩阵**（对齐 Java `FileStoreCommitImpl.assignCommitSnapshotId`）：
  - `commit_snapshot_id = None` 的 ADD entry，commit 后填上当前 snapshot id；
  - `commit_snapshot_id = Some(i64::MAX)` 的 ADD entry（写侧 sentinel），commit 后**也**填上当前 snapshot id；
  - `commit_snapshot_id = Some(42)` 且 `42 != i64::MAX` 的 ADD entry，commit 后**保持 42** 不被覆盖；
  - DELETE entry 任何状态都不被 assign。
- **老 manifest 兼容**：缺 `_COMMIT_SNAPSHOT_ID` / `_MERGE_MODE` 的老文件读出 `None` 不报错；merge function 在 fallback 阶段把 `None` 换成读侧 `UNKNOWN_SNAPSHOT_ID`（与写侧 `i64::MAX` sentinel 区分）。


### Stage 3 — single-version 列的 merge function（核心，1-2 天）

**目标**：实现 `VersionedPartialUpdateMergeFunction`，**只支持非 MV 列**。MV 列以"未实现 → 报错"占位。能跑通 Java 端 ~70% 的单测（所有不涉及 MV 列的）。

**关键架构问题**：(1) per-file mode + commit_snapshot_id 怎么传到 merge function？(2) same-PK 多条记录到达 merge function 时是否已按顺序排好？

#### MergeRow 扩展

当前 `MergeRow`（`sort_merge.rs:71-79`）有 `batch_idx / row_idx / sequence_number / value_kind / user_sequences`，缺 mode 和 commit_snapshot_id。**采纳：扩 `MergeRow` 直接加字段**（per-row 几字节，可忽略；逻辑最直接）：

```rust
pub struct MergeRow {
    pub batch_idx: usize,
    pub row_idx: usize,
    pub snapshot_id: i64,                  // ← 新增，从 data_file.commit_snapshot_id 来
    pub sequence_number: i64,
    pub value_kind: i8,
    pub user_sequences: Vec<Option<i128>>, // 现有，versioned-partial-update 不消费
    pub merge_mode: VersionedMergeMode,    // ← 新增，从 data_file.merge_mode 来
}
```

`user_sequences` 字段保留服务其它 merge engine（`PartialUpdateMergeFunction` 当前用 user sequence-field）。**versioned-partial-update validation 强制 `sequence.field` 为空（见 Stage 5），因此 versioned-partial-update 本身不消费 `user_sequences`**。

#### Projection / adjustReadType 等价方案（关键）

Java 的 `VersionedPartialUpdateMergeFunction.Factory.adjustReadType` 在读取前会把 merge 阶段必需的字段加回 internal read type：所有 PK 字段 + 所有 MV 字段，**即使用户 projection 没选这些列**也得读上来供 merge accumulator 用；merge 完成后再裁回用户真实请求的 projection。

paimon-rust 必须复刻这套两层 schema：

- **`requested_read_type`**：用户 projection（来自 `ReadBuilder::with_projection` / `read_type()`）。
- **`merge_read_type` = `requested_read_type` + 缺失的 PK 字段 + 缺失的 MV 字段**：去重并保持稳定字段顺序（PK 字段优先，user requested 字段次之，补充的 MV 字段尾部，或维持 table schema 序）。MV 字段识别用 Stage 4 的 `is_multi_version_type`。
- **`internal_read_type` = `[_SEQ, _VK, merge_read_type...]`**：原本就有的 KV 物理 schema 前缀，**基于 `merge_read_type` 而不是 `requested_read_type` 构造**。
- **`merge_output_schema`** 也基于 `merge_read_type` 的 key + value 结构。
- merge function 在内部状态 / accumulator / `MergeResult::MaterializedRow` 都按 `merge_read_type` 走，确保 MV state 完整。
- merge 输出 batch 后，`KeyValueFileReader` 在向上层 yield 之前**按 `requested_read_type` 做最终 projection / reorder**：丢弃用户没请求的内部补充列（PK / MV）保留用户请求的；列顺序对齐用户 projection。

**改动**：
- `crates/paimon/src/table/kv_file_reader.rs` 的 `KeyValueReadConfig` 加 `requested_read_type: Vec<DataField>`，把当前 `read_type` 字段语义改为 `merge_read_type`（或显式拆为两个字段）；`read()` 末尾加最终 projection/reorder 步骤。
- `crates/paimon/src/table/versioned_partial_update.rs` 按 `merge_read_type` 构建 merge function 状态 + 输出 schema。
- `crates/paimon/src/spec/types.rs` 的 `is_multi_version_type` 在 `KeyValueReadConfig` 构造阶段调用，决定 merge_read_type 要补哪些 MV 字段。
- 注意：MV 列的内部状态在 user 不投影 MV 时**仍然必须累积** —— 否则 same-key 跨 batch / 跨 split 的 merge 结果会跟 Java 不一致。

**Verification**（Stage 6 项目对应）：
- 用户只投影 PK：内部仍补 MV 列参与 merge，最终输出只有 PK；
- 用户只投影 single-version：内部仍补 PK + MV，最终只有 single-version；
- 用户不投影 MV 时，MV 内部状态不丢，**后续同 key merge 结果与 Java 一致**；
- 用户投影 MV 时，输出 MV 与 Java 一致。

#### 排序假设修正（关键）

**当前 Rust `SortMergeReader` 的 LoserTree 只按 primary key 排序，不保证 same-PK rows 已按 sequence/snapshot 顺序到达** —— 现有 `PartialUpdateMergeFunction` 也是在 merge 内部自己 sort rows。所以原文"按到达顺序扫描"的表述错了，已修正。

`VersionedPartialUpdateMergeFunction::merge` 必须在内部**显式排序**：

```rust
let mut sorted: Vec<&MergeRow> = rows.iter().collect();
sorted.sort_by(|a, b| {
    a.snapshot_id.cmp(&b.snapshot_id)
        .then_with(|| a.sequence_number.cmp(&b.sequence_number))
        .then_with(|| (a.batch_idx, a.row_idx).cmp(&(b.batch_idx, b.row_idx)))
        // tie-breaker：稳定但实质上不应触发，因 (snapshot, seq) 唯一
});
```

排序键 `(snapshot_id, sequence_number, (batch_idx, row_idx))` —— 前两个对齐 Java 语义，最后一个是 stable 兜底（理论上 (snapshot, seq) 已唯一，留着防数据异常导致非确定输出）。

**改动**：
- `sort_merge.rs:71-79` `MergeRow` 加 `snapshot_id: i64` 和 `merge_mode: VersionedMergeMode`，默认 `Upsert`（兼容非 versioned 表）。
- `kv_file_reader.rs` 构造 file_streams 那一段（约 line 280-310）：把 `data_file.commit_snapshot_id.unwrap_or(UNKNOWN_SNAPSHOT_ID)` + `data_file.merge_mode.unwrap_or(...)` 一并下传。
- 新增 `crates/paimon/src/table/versioned_partial_update.rs`，实现 `VersionedPartialUpdateMergeFunction`：
  - 状态：`current_values: Vec<Option<ScalarValue>>`、`current_delete_row: bool`、`meet_insert: bool`、`latest_seq: i64`、`latest_snapshot: i64`。
  - **入口先按 `(snapshot_id, sequence_number, ...)` 排序**（见上文）；
  - 按排序后顺序扫描 rows：
    - 非 add（DELETE/UPDATE_BEFORE）：`UPDATE_BEFORE` 跳过；`DELETE` 在 `!ignore_delete` 时清空状态 + 标 `current_delete_row=true`；
    - add（INSERT/UPDATE_AFTER）：`meet_insert=true`、`current_delete_row=false`、更新 latest_seq/latest_snapshot；逐列处理：
      - PK 列：value 非 null 就写入；
      - MV 列：Stage 4 之前 `unimplemented!()`；
      - aggregator 列：见 Stage 5，**fail-loud reject 而非退化**；
      - single-version 列：按 `(mode, value, current)` 三态：UPSERT 总覆盖（value 非 null）、IGNORE 仅在 current 是 None 时填入。
  - 输出：`current_delete_row && !meet_insert` → `MergeResult::Omit`（**仅 batch SELECT 语义**，见风险 #3）；否则 `MergeResult::MaterializedRow`。
- `kv_file_reader.rs:106` 的 `new_merge_function` switch 接到这个新实现。

**Verification**：移植 Java `VersionedPartialUpdateMergeFunctionTest.java` 里的 single-version 测试到 Rust：
- `testSingleVersionUpsert` (line 244)
- `testSingleVersionIgnore` (line 253)
- `testDeleteRemovesRecord` (line 388)
- `testRetractIgnoredWhenConfigured` (line 398)

**额外的 Rust-specific 单测**（验证排序假设）：
- 跨 stream 顺序乱序输入：手工构造 same-PK rows 的 `MergeRow` 列表，input 顺序是 `(snap=2, seq=1)` 后 `(snap=1, seq=5)`，断言 merge 后结果跟正序输入一致（即 internal sort 工作）。
- 跨 snapshot 同 sequence：`(snap=1, seq=10)` 和 `(snap=2, seq=10)` 并存，later snapshot 应胜出。

### Stage 4 — Multi-version 列（大，2-3 天）

**目标**：补 MV 列的自动识别 + accumulator + 输出物化。

**改动**：
- `crates/paimon/src/spec/types.rs` 加 free function `is_multi_version_type(dt: &DataType) -> bool`，按 Java factory:438-457 的结构匹配（RowType 三字段 + VarChar / T / Map<VarChar, T> 严格 equals）。注意 Rust 端 `DataType::equal` 默认包含 nullable，要做忽略 nullable 的比对。
- `VersionedPartialUpdateMergeFunction::new` 时扫 read schema，识别 mv 列填入 `mv_field_indices`。
- merge 时 mv 列分支：
  - `mv_states: HashMap<col_idx, HashMap<String, ScalarValue>>`
  - 每条 input：抽出 `(version, val)` pairs（输入有两种 shape：完整 map 或单 `(latest_version, latest_value)` 对）；
  - UPSERT 模式 `insert` 总覆盖；IGNORE 模式仅在 key 不存在时 `insert`。
- `getResult` 阶段重建 mv 列：
  - 取 `map_state` 全部 keys，**lex 序**（用 `&[u8]` cmp 或 `String::cmp`）取最大作 `latest_version`；
  - 对应 value 作 `latest_value`；
  - rebuild 整个 `Map<VarChar, T>` 列。
- 输出端必然走 `MergeResult::MaterializedRow`（不能复用源 batch）。

**Verification**：移植 Java MV 类测试：
- `testMultiVersionExistingKeyUpsert` (line 370)
- `testMultiVersionExistingKeyIgnore` (line 361)
- `testUpsertExistingKeyUpdatesLatestByLexicographicOrder` (line 529)
- `testMapPathLatestIsDerivedFromLexicographicOrder` (line 539)

**MV 列性能 / 复杂度备注**：
- 设 `M = total_version_entries`（单 PK 上累加遇到的 version-value 对总数 = `Σ records_per_input × versions_per_record`），`V = N_distinct_versions`（最终 map 内不同 version key 数）。当前 `mv_states: HashMap<col_idx, HashMap<String, ScalarValue>>` + `getResult` 时 rebuild 整个 map 的复杂度：
  - **累加成本**：`O(M)` —— 每条 input 的每个 (version, value) 一次 HashMap put；
  - **取 latest**：单纯扫 keys 即可，`O(V)`；
  - **若需稳定输出 map key 顺序**（写到 arrow Map 列时为了确定性 / 易测）：额外 `O(V log V)` 排序；
  - **空间**：`O(V × value_size)` 累加缓存，叠加 `getResult` 期间 rebuild Arrow Map 列的 peak 额外 ~`O(V × value_size)`，合计 peak 约 `2 × V × value_size`；
  - 不是 Cartesian product；`O(M)` 跟 input record 数和单条 record 内 versions 数线性相关，跟 distinct version key 数无直接关系。
- 预期典型场景：单 PK `V` 在 1-100 量级，单 value < 1KB。简单实现够用。
- **不在第一阶段优化的项**（标 TODO）：
  - `ScalarValue` 拷贝放大：可换成 Arrow buffer slice + offset 复用，减少 alloc；
  - rebuild whole map：可改增量 patch（只往现有 Map 列上 append/replace 新 entries），但需要 arrow-rs MapArray builder 支持；
  - hash key 的 `String` 拥有：可考虑 `Bytes` / `ArrayRef` 引用，但 lifetime 跨 batch 复杂。
- 性能基线测试矩阵（落在 Stage 6）：单 PK `V` ∈ {1, 10, 100, 1000}，记录 wall + RSS。

### Stage 5 — Validation + fail-loud 边界（中，1 天）

**目标**：把 Java `SchemaValidation.java:249-289` 的 6 条规则在 Rust 端落地；同时把 Rust 当前能力不到位的子场景**显式 reject 而不是 silent fallback**。

#### 5.1 复刻 Java 的 6 条 schema validation 规则

落到 `crates/paimon/src/spec/schema.rs::validate` 或新建 `validate_versioned_partial_update`：
1. `sequence.snapshot-ordering=true` 必填；
2. changelog-producer ∈ {none, lookup}；
3. **ignore-mode 启用时要求 lookup capability**（详见 5.3）；
4. PK 必填 + `sequence.field` 必须为空；
5. MV 列存在时拒绝 `fields.default-aggregate-function`；
6. MV 列禁用 `fields.<mv>.aggregate-function`。

#### 5.2 Aggregate 列 fail-loud（review 第 4 点）

Java 端 aggregate 列的行为是：aggregator 分支**优先于** UPSERT/IGNORE mode、且不受 mode 影响。Rust 端目前**没有** FieldAggregator 框架（[`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) F1 / F10 列过），如果 silently 退化为 single-version UPSERT/IGNORE 处理，**会得到错误结果且用户无感** —— 这是 silent correctness bug。

**短期策略：fail-loud reject**。任何下列配置在加载/合并阶段都直接报错：
- `fields.default-aggregate-function` 设了任何值 —— **不限于 MV 列存在的情形**，即便表里全是 single-version 列，aggregate 没实现就一律 reject；
- 任意 `fields.<field>.aggregate-function` —— 不区分 single-version / MV / PK，**全部 reject**；
- 错误信息明确写：`"versioned-partial-update aggregate columns are not supported yet (Rust port: pending FieldAggregator framework)"`。

**长期**：等 Aggregate merge engine 的 FieldAggregator 框架落地后，再恢复 Java 语义（aggregator 分支优先、不受 mode 影响）。在那之前**不允许 silent fallback**。

#### 5.3 IGNORE mode 的 validation / runtime 边界（review 第 5 点）

Java 语义：
- `ignore-mode.enabled=true` 时表必须有 lookup capability（`needLookup() == true`：DV / force-lookup / changelog-producer=lookup 任一）；
- `ignore-mode.enabled=false` 时允许无 DV/lookup 的 UPSERT-only 表；
- 如果运行时 job 设置 `versioned-partial-update.merge-mode=ignore` 但表级 `ignore-mode.enabled=false`，**写入时 fail-loud**。

Rust 当前现状：
- 没有完整的 lookup / DV 读写能力（[`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) F9 列过）；
- `KeyValueFileReader` 当前会**直接 reject 带 DV 的 split**（[`pk-read-issues.md`](./pk-read-issues.md) C2）；
- 因此 Rust 目前不具备 Java 意义上的 lookup capability。

**短期实施策略**：
- 第一阶段最小路径只支持 **UPSERT-only batch read/write**：要求 `versioned-partial-update.ignore-mode.enabled=false`；
- 若 `ignore-mode.enabled=true` —— 在 lookup/DV 能力补齐前，加载/写入阶段 fail-loud：`"versioned-partial-update ignore-mode requires lookup capability not yet supported in paimon-rust"`；
- 若 runtime `merge-mode=ignore` 且表 `ignore-mode.enabled=false` —— Java 同样 fail，Rust 这条直接对齐；
- **不允许 silent fallback 到 upsert**。

待 Rust 端 lookup-merge / DV 读路径补齐后，重新打开 `ignore-mode.enabled=true`。

**Verification**：6 条 schema 规则各加反例单测；aggregate fail-loud 至少加 3 条（`default-aggregate-function` 配单版本列、`fields.<f>.aggregate-function` 配单版本列、配 MV 列）；ignore-mode 加 2 条（`ignore-mode.enabled=true` reject、runtime ignore + 表禁 ignore reject）。

### Stage 6 — 系统级测试（1 天）

**目标**：端到端验证。

**改动**：
- 基础 E2E：用 paimon-java 写 versioned-partial-update 表，paimon-rust 读出来跟 Java 读相同结果（行级对比）。
- 性能：跑 demo `read_local_demo` 在 versioned-partial-update 表上，确认 merge 不是新瓶颈。

**E2E 测试矩阵**（第一阶段 = UPSERT-only）：

| 场景 | 构造 | 期望 |
|---|---|---|
| **projection: 仅 PK** | 普通表，user 只 select PK 列 | merge 内部仍累积 PK + MV 状态；最终输出只有 PK，行数 / PK 集合与 Java 一致 |
| **projection: 仅 single-version** | 排除 PK 和所有 MV 列 | 内部仍补齐 PK + MV 参与 merge；输出只有 single-version 列；same-PK 多版本经 merge 后产出唯一行，与 Java 一致 |
| **projection: 仅 MV 列** | 排除 PK 和所有 single-version | 输出 MV 列与 Java 一致 |
| **projection: 全列混合** | PK + single-version + MV | 输出全列与 Java 一致 |
| **projection 内部 state 完整性** | user 只 select PK，但 same-PK 跨 split / 跨 batch 多次出现，包含 MV existing key 覆盖 | 由于 internal merge 仍补 MV 列，merge 状态与 Java 一致；最终 PK 不重复（即便 user 没看 MV 列） |
| **跨 snapshot 同 sequence** | snapshot 1 文件写 (pk=1, seq=10, val=A)；snapshot 2 文件写 (pk=1, seq=10, val=B) | 输出 `val=B`（later snapshot 胜） |
| **跨 snapshot 不同 sequence** | snapshot 1: (pk=1, seq=20)，snapshot 2: (pk=1, seq=10) | 仍输出 snapshot 2 的（snapshot id 优先于 seq） |
| **DELETE + lower-level old row（核心 tombstone 测试）** | 直接构造 `MergeRow` 列表喂给 `VersionedPartialUpdateMergeFunction::merge`：L1 old row `(pk=1, snap=1, seq=5, INSERT, val=A)` + L0 new row `(pk=1, snap=2, seq=10, INSERT, val=B)` + L0 DELETE `(pk=1, snap=2, seq=11, DELETE)`，**全部进同一次 `merge(rows)` 调用**。 | `MergeResult::Omit`。**关键验证点**：必须直接构造 MergeRow 调用 merge function 单测，确保 tombstone 行为不会被 raw path 旁路 / 不会因为 planner 把 L1 old row 跟 L0 DELETE 拆到不同 split 导致旧值复活。E2E 层面再加 planner/reader 组合的复现测试。 |
| **Aggregate 配置 reject** | 配 `fields.x.aggregate-function=sum`（不限于 MV 列） | 加载/合并 fail-loud，错误信息含 "aggregate columns are not supported yet" |
| **ignore-mode validation reject** | `ignore-mode.enabled=true` | schema 加载 fail-loud |
| **runtime ignore reject** | 表 `ignore-mode.enabled=false` 但 job 设 `merge-mode=ignore` | 写入 fail-loud（即便表允许，job 强行 ignore 也跟 Java 一致地拒绝当前实现） |
| **`sequence.field` 非空 reject** | versioned-partial-update 表配 `sequence.field=ts` | schema validation reject |

**未来阶段补做的测试（不作为本计划 gate）**：
- 混合 UPSERT / IGNORE 文件串行（依赖 IGNORE mode 解锁）；
- IGNORE existing-key 不覆盖语义；
- `MergeResult` 的 streaming/CDC tombstone 输出；
- Rust 写 → Java 读的全量行级 round-trip（依赖 M1/M2 manifest schema 全量对齐）。

**MV 列性能基线**：
- 1 / 10 / 100 / 1000 versions per PK，单 PK 集 ~10K，记录 read wall + peak RSS；
- 跟 Java 同等设置下做相对对比，确认无数量级差距即可（不追求绝对数值打平）。




<!-- SECTION-RISKS -->

## 关键风险 & 注意点

1. **MV 列 lex 序的"v9 > v10"陷阱**：Rust 用 `String::cmp` / `&[u8]::cmp` 跟 Java `BinaryString.compareTo` UTF-8 字节比对一致 —— 已对齐，但**文档要明确写出来**让用户知道 version 字段需要零填充对齐（用户写 `"v009"` / `"v010"` 而非 `"v9"` / `"v10"`）。
2. **Avro 字段编码**：
   - `_MERGE_MODE`：Java 写盘是 `int` 编码但 RowType 标 `tinyint`。Rust 端**保持 Avro `int`** 以兼容 Java 写出的文件，序列化层不要试图用 `int8` 否则会误读。
   - `_COMMIT_SNAPSHOT_ID`：Java 是 `long` (`bigint`)。Rust 端用 Avro `long`。两者都是 `nullable union`。
3. **DELETE / tombstone 语义边界（强化版）**：
   - 本文设计的 `VersionedPartialUpdateMergeFunction` **是 batch SELECT 专用 merge function**，不是 compaction / CDC / changelog producer / lookup changelog 通用 merge function；
   - `MergeResult::Omit` 仅作为**最终 SELECT 输出**策略 —— batch read 路径上消费者看到 omit，跟 Java `RowKind.DELETE` 在 SELECT 上的语义一致；
   - 同一份 merge 逻辑**不能直接复用**到 compaction / CDC / lookup changelog —— 那些场景需要保留 internal DELETE/tombstone（compaction 要让下游层级看到 retract、CDC 要发出 DELETE event）；
   - 命名上明确（如 `BatchSelectVersionedPartialUpdateMergeFunction` 或在文档注释里标识 `// batch SELECT only`）；
   - 测试矩阵必须包含 "DELETE + lower-level old row" 场景（Stage 6 测试矩阵第 8 条），证明 omit 不会让旧值复活。
4. **per-file mode 默认值**：老文件 `merge_mode = None`（Stage 2 字段是 `Option<i8>`），merge function 应当把 `None` 视作 `Upsert`（最常见、最不破坏现有数据）。
5. **`commit_snapshot_id` 两种 sentinel 必须严格区分**：
   - **写侧 `i64::MAX`（= Java `Long.MAX_VALUE`）= "待 assign"**：`MergeTreeWriter` 等价物预填，commit 阶段必被替换为真实 snapshot id；不允许用 `None`，因为有些场景需要在 commit 前持有合法 `i64` 值。
   - **读侧 `UNKNOWN_SNAPSHOT_ID`（建议 `i64::MIN` 或 `-1`）= "老文件 fallback"**：老 manifest 缺 `_COMMIT_SNAPSHOT_ID` 字段时读出 `None`，merge function 把它换成此值。**必须比所有合法 snapshot id 小**，跟新文件的 row 比较时总 lose（被新文件 override），符合"老文件先于新文件 apply"的语义。
   - **两者不能混用**：commit 阶段处理 `i64::MAX`，read fallback 阶段处理 `None → UNKNOWN_SNAPSHOT_ID`。代码注释里要把这两个 sentinel 的含义、约定值、使用阶段都标清。
6. **alwaysMerge / requireCopy 语义**：Java `requireCopy=false` 因为 GenericRow 每次 reset 都是新对象；Rust `MergeResult::MaterializedRow` 已是 owned RecordBatch，天然满足。
7. **MV 列结构匹配**：Rust 端要慎重处理 nullable 不参与 equals。建议加单测覆盖三类 case：完全相等 / 仅 nullable 不同 / 字段顺序不同。
8. **Sequence ordering 不能靠 debug_assert**（修正）：
   - 原文写"用 `debug_assert!` 保证输入有序"是错的 —— Rust 的 `SortMergeReader` 当前**不保证 same-PK rows 已按 (snapshot, seq) 排好**（LoserTree 仅按 PK 排）；
   - 正确做法：`VersionedPartialUpdateMergeFunction::merge` **内部显式排序** rows，详见 Stage 3 修正条款；
   - 单测必须验证乱序输入 + 跨 stream 顺序场景下输出确定且一致。
9. **silent fallback 红线**：本计划任何场景**不允许 silent fallback**，全部要 fail-loud reject 或显式 unsupported error：
   - Aggregate 列 → reject（无 FieldAggregator 框架）；
   - `ignore-mode.enabled=true` → reject（无 lookup capability）；
   - runtime `merge-mode=ignore` 跟表 `ignore-mode.enabled=false` 冲突 → reject；
   - 含 sequence.field 的 versioned-partial-update 表 → reject（违反 Java validation 规则 4）。

<!-- SECTION-FILES -->

## 文件改动清单（关键 anchor）

| 阶段 | 文件 | 改动要点 |
|---|---|---|
| 1 | `crates/paimon/src/spec/core_options.rs` | `MergeEngine` 加变体；`from_str` 加映射；加 2 个 option 常量 + getter；加 `VersionedMergeMode` enum |
| 1 | `crates/paimon/src/table/table_read.rs` | **`read_pk` 接入 `VersionedPartialUpdate` → 强制 `read_kv` 路径** |
| 1 | `crates/paimon/src/table/kv_file_reader.rs` | line 106 `new_merge_function` switch 加占位 `Unsupported` 分支 |
| 2 | `crates/paimon/src/spec/data_file.rs` | `DataFileMeta` 加 `commit_snapshot_id: Option<i64>` + `merge_mode: Option<i8>` |
| 2 | `crates/paimon/src/spec/manifest_entry.rs` | `MANIFEST_ENTRY_SCHEMA` (lines 175-228) embedded `_FILE` record 末尾加两个字段 |
| 2 | `crates/paimon/src/spec/avro/manifest_entry_decode.rs` | 手写 decoder + default 覆盖两个新字段 |
| 2 | `crates/paimon/src/table/kv_file_writer.rs` | 写出 KV 文件时填 `merge_mode`（仅 versioned-partial-update 表） |
| 2 | `crates/paimon/src/table/table_commit.rs` | **commit 阶段 assign `commit_snapshot_id`（Java `assignCommitSnapshotId` 等价）** |
| 2 | 所有 `DataFileMeta { ... }` 构造点 | 见下表 |
| 3 | `crates/paimon/src/table/sort_merge.rs` | `MergeRow` (lines 71-79) 加 `snapshot_id` + `merge_mode` 字段 |
| 3 | **新文件** `crates/paimon/src/table/versioned_partial_update.rs` | `VersionedPartialUpdateMergeFunction` 主体 + 显式排序逻辑；按 `merge_read_type` 构造状态 / 输出 schema |
| 3 | `crates/paimon/src/table/kv_file_reader.rs` | lines 280-310 file_streams 构造时下传 commit_snapshot_id + merge_mode；新增 `requested_read_type` vs `merge_read_type` 双 schema 逻辑（adjustReadType 等价）；read 末尾按 `requested_read_type` 做最终 projection / reorder |
| 3 | `crates/paimon/src/spec/types.rs` | `is_multi_version_type(&DataType) -> bool` 提前到 Stage 3 可调用（Stage 4 实现细节）—— `KeyValueReadConfig` 构造阶段就要识别 MV 字段 |
| 4 | `crates/paimon/src/spec/types.rs` | 加 `is_multi_version_type(&DataType) -> bool`，nullable-insensitive equals |
| 4 | `crates/paimon/src/table/versioned_partial_update.rs` | 扩展 mv 列分支 + accumulator + map rebuild |
| 5 | `crates/paimon/src/spec/schema.rs` 或新文件 | 6 条 schema validation + aggregate fail-loud + ignore-mode 边界 |

**Stage 2 全量 `DataFileMeta { ... }` 调用点**（加字段后必须补 `commit_snapshot_id: None, merge_mode: None`）：

- `crates/paimon/src/spec/objects_file.rs`
- `crates/paimon/src/spec/manifest.rs`
- `crates/paimon/src/spec/avro/manifest_entry_decode.rs`
- `crates/paimon/src/table/bin_pack.rs`
- `crates/paimon/src/table/table_scan.rs`
- `crates/paimon/src/table/source.rs`
- `crates/paimon/src/table/postpone_file_writer.rs`
- `crates/paimon/src/table/data_evolution_writer.rs`
- `crates/paimon/src/table/data_file_writer.rs`
- `crates/paimon/src/table/kv_file_writer.rs`
- `crates/paimon/src/table/table_commit.rs`
- `crates/paimon/src/table/data_evolution_reader.rs`
- `crates/paimon/src/table/referenced_files.rs`

最终清单以 `cargo check` 报错列表 + `grep "DataFileMeta {"` 为准。


<!-- SECTION-OUT-OF-SCOPE -->

## 不在本计划范围内

- **完整 compaction parity**：本计划实现的 merge function 仅服务 batch SELECT 输出，**不能直接用于 compaction**（compaction 需保留 internal DELETE/tombstone 给下游 level 见到）。Compaction 的 versioned-partial-update 支持是独立工作。
- **完整 streaming / CDC / changelog producer 输出**：同上，需保留 internal DELETE，不能直接复用本 batch read merge function。
- **Aggregate merge engine + FieldAggregator 框架**：Java 有 20+ FieldAggregator，独立工作量大于本计划所有 stage 总和。**第一阶段 fail-loud reject 任何 aggregate 配置**（详见 Stage 5），等框架补齐后再恢复 Java 语义。
- **lookup capability + IGNORE mode runtime**：Rust 端没有 lookup-merge 实现（[`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) F9）+ KV reader 拒绝 DV split（[`pk-read-issues.md`](./pk-read-issues.md) C2）。**第一阶段限定 UPSERT-only**：要求表 `ignore-mode.enabled=false`、runtime `merge-mode=upsert`，否则 fail-loud。等 lookup/DV 能力补齐后再开 IGNORE。
- **paimon-rust 写出的 versioned-partial-update 表给 paimon-java 读**：依赖 manifest schema 的其它 mismatch ([`pk-read-issues.md`](./pk-read-issues.md) 补充节 M1-M2) 一并解决，不是本 merge engine 单独能保证的。
- **silent fallback / 退化策略**：本计划任何 unsupported 配置一律 fail-loud（详见风险 #9）。

<!-- SECTION-VERIFICATION -->

## Verification 矩阵

每阶段独立可验证：

| 阶段 | 验证方式 |
|---|---|
| 1 | `cargo test -p paimon spec::core_options` 单测：枚举解析 / option getter 默认值 / new_merge_function 报错信息；`TableRead::to_arrow` 对 `VersionedPartialUpdate` 表走 `read_kv` 不绕开 sort-merge |
| 2 | paimon-java 写表 → paimon-rust 读 manifest 不 IndexOutOfBounds，两个新字段正确解出；schema-level round-trip 单测（serialize ↔ deserialize 字节级一致）；commit assign 4 项矩阵（`None` / `i64::MAX` / `Some(real)` / DELETE entry）；老 manifest 缺字段 fallback 到 `None` 不报错。**反向 Rust 写 → Java 全量读不作 gate**（依赖 M1/M2 manifest 全量对齐）|
| 3 | 4 个 single-version Java 单测全过；2 条 Rust-specific 排序单测（乱序输入 + 跨 snapshot 同 sequence）；4 条 projection 单测（仅 PK / 仅 single-version / 仅 MV / 全列）确认 `merge_read_type` 正确补齐内部列、最终输出按 `requested_read_type` 裁剪 |
| 4 | 4 个 MV Java 单测全过；MV 列结构匹配 3 个 case（完全相等 / 仅 nullable 不同 / 字段顺序不同）全过；user 不投影 MV 时 same-PK 跨 batch merge 状态完整性单测 |
| 5 | 6 条 schema 规则反例 + 3 条 aggregate fail-loud（不限 MV 列）+ 2 条 ignore-mode 边界 + `sequence.field` 非空 reject，全部 fail-loud 通过 |
| 6 | 12 条 E2E 矩阵（见 Stage 6 第一阶段表）+ MV 列 1/10/100/1000 versions 性能基线 + DELETE-tombstone-merge 直接构造 MergeRow 单测 |

## 工作量估计

- Stage 1: 2-4 小时（多了 `TableRead` dispatch wire）
- Stage 2: 8-12 小时（双字段 + 三态 sentinel commit assign + decoder + 全量 DataFileMeta 调用点 + schema-level round-trip）
- Stage 3: 1.5-2.5 天（merge function 主体 + 显式排序 + Java 单测移植 + Rust 排序单测 + adjustReadType 双 schema 切换 + 最终 projection/reorder + projection 单测）
- Stage 4: 2-3 天（MV 列 type matching + accumulator + 物化 + 性能基线 + 不投影 MV 时状态完整性测试）
- Stage 5: 1 天（schema validation + aggregate fail-loud + ignore-mode 边界 + sequence.field reject）
- Stage 6: 1 天（12 条 E2E + MV 性能基线 + DELETE-tombstone 直接 merge 单测）
- **总计**：5-9 工作日

PR 划分建议：
- **PR1**：Stage 1+2（基础枚举 + dispatch + manifest 双字段补齐 + commit assign，可独立 ship 因为同时修了 M3 互操作 bug）
- **PR2**：Stage 3（single-version 可用 + 显式排序，~70% 单测覆盖）
- **PR3**：Stage 4+5+6（完整功能 + validation + fail-loud + E2E + 性能）


<!-- SECTION-CROSSREF -->

## 与已有内部文档交叉索引

| 已有条目 | 落在本文哪里 |
|---|---|
| [`pk-read-rust-vs-java-capabilities.md`](./pk-read-rust-vs-java-capabilities.md) **F1** — Merge: VersionedPartialUpdate 缺失 | Stage 1-4（枚举 + dispatch + single-version + MV 全套） |
| 同上 **F10** — PartialUpdate 进阶子语义 + DELETE/UPDATE_BEFORE 处理 | Stage 3（DELETE 在 `MergeResult::Omit`；UPDATE_BEFORE 静默跳过）+ Stage 5（aggregate fail-loud） |
| 同上 **F9** — LookupMerge 缺 | Stage 5 IGNORE mode 边界条款（无 lookup capability → reject `ignore-mode.enabled=true`） |
| [`pk-read-issues.md`](./pk-read-issues.md) **补充节 M3** — `DataFileMeta` 缺 `_COMMIT_SNAPSHOT_ID / _MERGE_MODE` | Stage 2 **完整修两个字段**（不再是只补一个） |
| [`pk-read-issues.md`](./pk-read-issues.md) **C2** — KV reader 拒绝 DV split | Stage 5 IGNORE mode 边界（没 DV/lookup → reject ignore-mode）触发原因之一 |
| [`pk-read-issues.md`](./pk-read-issues.md) **C5** — raw L1+ 路径不剥 DELETE/UPDATE_BEFORE | 关键风险 #3：本文 batch SELECT 用 `MergeResult::Omit` 兜住；C5 自身仍需独立修，且本文 Stage 1 强制走 `read_kv` 路径不允许 raw path 绕过 |

<!-- SECTION-REFERENCES -->

## 参考

### paimon-java 关键源文件
- `paimon-core/src/main/java/org/apache/paimon/mergetree/compact/VersionedPartialUpdateMergeFunction.java`
- `paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyTableUtils.java`（dispatch line 75-77）
- `paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java:62-92`（`_MERGE_MODE` 字段在位置 21）
- `paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java:249-289`（6 条 validation 规则）
- `paimon-api/src/main/java/org/apache/paimon/CoreOptions.java:973-1000, 3920-3922`（option key + 枚举值）
- `paimon-core/src/test/java/org/apache/paimon/mergetree/compact/VersionedPartialUpdateMergeFunctionTest.java`（22 个单测）

### paimon-rust 关键 anchor（实施 entry points）
- `crates/paimon/src/spec/core_options.rs:74` — `enum MergeEngine`
- `crates/paimon/src/spec/data_file.rs:30-109` — `DataFileMeta`
- `crates/paimon/src/spec/manifest_entry.rs:175-228` — `MANIFEST_ENTRY_SCHEMA`
- `crates/paimon/src/spec/types.rs` — `DataType` 枚举（待加 `is_multi_version_type`）
- `crates/paimon/src/table/sort_merge.rs:71-79, 121-130, 138-302` — `MergeRow / MergeFunction trait / Deduplicate / PartialUpdate` 现有实现
- `crates/paimon/src/table/kv_file_reader.rs:67-99, 106` — `KeyValueFileReader::new` + `new_merge_function`
- `crates/paimon/src/table/kv_file_writer.rs` — KV 文件写入入口（DataFileMeta 构造）
- `crates/paimon/src/spec/schema.rs` — `Schema` validation 入口

