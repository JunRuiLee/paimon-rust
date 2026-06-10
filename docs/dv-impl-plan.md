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

# paimon-rust 实现 deletion vector 读路径 —— 实施方案

<!-- SECTION-OVERVIEW -->

## 总览

paimon-rust 对 deletion vector (DV) 的支持目前只在 raw 路径（`DataFileReader`）就位 —— `crates/paimon/src/table/data_file_reader.rs` 已经把 `DeletionVectorFactory`（line 96 起）接到 parquet `with_row_selection`（`merge_row_selection`，line 211 / 329 / 344），DV 在 parquet 层就剥掉删除行。但 **PK MoR 路径（`KeyValueFileReader`）在 `crates/paimon/src/table/kv_file_reader.rs:271-275` 直接 `Error::Unsupported` 拒绝带 DV 的 split**，且 `read_single_file_stream` 调用处对 `dv` 参数硬传 `None`。后果：任何 `deletion-vectors.enabled=true` 的 PK 表只要 split 含 L0 文件，整次读取必崩。

叠加：
- `Bitmap64DeletionVector`（64-bit row id，Iceberg 兼容格式）完全没识别（C6）；
- `deletion-vectors.read-mode` 选项（`PERFORMANCE` / `FRESHNESS`）被静默忽略（C7）；
- L0 文件上 value-stats 裁剪不安全（C4，与 read-mode 联动）。

构成一组关联的读路径缺口，**全部都是 capabilities 文档标 HIGH 的项**。

本方案覆盖 `pk-read-rust-vs-java-capabilities.md` 中以下条目：

| 文档条目 | 严重程度 | 本方案对应阶段 |
|---|---|---|
| C2 — KV reader 拒 DV split | C-HIGH | Stage 1（评审修复阶段全 merge engine 闭合） |
| C4 — L0 value-stats pruning 错误 | C-HIGH | Stage 3（同步修） |
| C6 — Bitmap64 DV 不识别 | C-HIGH | Stage 2 |
| C7 — `deletion-vectors.read-mode` 忽略 | C-HIGH | Stage 3 |
| F9 — LookupMerge L0+DV 快路径 | F | Stage 4a / 4b |
| C5 — raw 路径不剥 DELETE / UPDATE_BEFORE | C-HIGH | Stage 4a 顺手修 |

**不在 scope**：DV 写路径（`BucketedDvMaintainer` 等价物 / compaction 时生产 DV / writer 配 DV 启用）。capabilities 文档未列写侧 gap，且 `kv_file_writer.rs` 等处对 DV+PartialUpdate 主动 reject —— 写侧调通后续单立 plan。



<!-- SECTION-BASELINE -->

## 对齐基准

本方案 mirror 的 paimon-java 是 **Kuaishou 内部 fork**（remote `git.corp.kuaishou.com/ks-dataarc/computing/paimon`）的 `20260528` 分支，commit `e8938f347`（full hash `e8938f347e75d660c9c77ba8ca5bfa11a22c9907`）。

所有 Java anchor 形如 `path:line@e8938f347`，行号以该 commit 为准。如分支后续 rebase / 行号漂移，需重新校准 —— 校准命令：

```bash
git -C /path/to/paimon rev-parse HEAD                # 应得 e8938f347...
sed -n '<line>p' <java-file-path>                    # 验证行内容仍匹配
```

> ⚠️ `deletion-vectors.read-mode` / `DvReadMode.{PERFORMANCE,FRESHNESS}`（Stage 3 依赖）是 Kuaishou fork 的私有能力 ——
> `apache/paimon` master 当前**不存在**这套 read-mode 选项，对应 plan 端只有 `batchScanSkipLevel0` + `enableValueFilter()` 单一行为。
> 如未来需要对齐 apache master，需重写 Stage 3：去掉 read-mode option，改为对齐 `batchScanSkipLevel0` 行为，并重新设计 L0 routing。
> 本方案默认混合栈场景：**Kuaishou paimon-java 写、paimon-rust 读**。



<!-- SECTION-JAVA-PIPELINE -->

## Java 等价路径速览（参照实现 anchor）

来自针对 paimon-java Kuaishou fork @`e8938f347` 的 audit（见上节《对齐基准》）。以下 anchor 直接给到本方案要 mirror 的入口。

### 1. Per-file DV apply（在 sort-merge 之前）

- **`paimon-core/.../io/KeyValueFileReaderFactory.java:173-176@e8938f347`**：在构建完 per-file row reader 后，查 `dvFactory.create(file.fileName())`；若 DV 存在且非空，用 `ApplyDeletionVectorReader` 包一层，**再** 喂给 `KeyValueDataFileRecordReader`，最后才进 sort-merge。
- **`paimon-core/.../deletionvectors/ApplyDeletionFileRecordIterator.java:53-74@e8938f347`**：`returnedPosition()`（line 53-55）透传 `iterator.returnedPosition()`；`next()`（line 65-74）行级 `deletionVector.isDeleted(returnedPosition())` 过滤。`returnedPosition()` 来自底层 parquet/orc reader 的 `FileRecordIterator`（**绝对 row position 跨 row-group**），不是内部计数器。
- **DV factory**（reader 侧 / writer 侧两个变体）：Rust 已经有 reader 侧等价的 `crates/paimon/src/deletion_vector/factory.rs::DeletionVectorFactory::new`。

**Rust 走更优的方案**：`crates/paimon/src/table/data_file_reader.rs:344` 用 `dv_to_non_deleted_ranges`（line 353）把 DV 转成 row range list，喂给 parquet `with_row_selection` —— 跳过解码而非解码后过滤。本方案让 KV 路径走同样模式，**不引入新语义**；但 KV + DV 的具体组合（详见 SECTION-RISKS 风险 #1）仍需 Stage 1 单测独立覆盖。

### 2. DV format dispatch（32-bit vs 64-bit bitmap）

- **`paimon-core/.../deletionvectors/DeletionVector.java:97-145@e8938f347`**：先读 `bitmapLength: int32 BE`，再读 `magicNumber: int32 BE`。按 magic 分派：
  - `BitmapDeletionVector.MAGIC_NUMBER = 1581511376`（**BE int32** 比较）→ RoaringBitmap32，最大 2^31-1 行
  - `toLittleEndianInt(magicNumber) == Bitmap64DeletionVector.MAGIC_NUMBER (1681511377)` → `OptimizedRoaringBitmap64`，64-bit 行 id，**LE 字节序** payload

#### Layout（DV blob 本体，**32 / 64 不同构**）

> ⚠️ `DeletionVectorMeta.length`（写端写入 index manifest entry 的字段，在 Rust 端 / Java reader 中通过 `DeletionFile.length` 暴露）**不是统一的物理 blob 总字节数**。它来自 Java `DeletionFileWriter.write` 的 `length = deletionVector.serializeTo(out)`（`DeletionFileWriter.java:56-59@e8938f347`），而两个 `serializeTo` 的返回值语义不同：
>
> - `BitmapDeletionVector.serializeTo:87-99@e8938f347` 返回 `size = data.length = magic(4) + roaring32_bytes`，**不**含外层 length 字段也**不**含 crc
> - `Bitmap64DeletionVector.serializeTo:93-106@e8938f347` 返回 `bytes.length = LENGTH(4) + bitmapDataLength + CRC(4)`，**含**整个外层框
>
> 物理 blob 在文件中的占用：
> - **Bitmap32 物理总字节数 = `metadata.length + 8`**（外层 length 字段 + crc 各 4 字节，**不**计入 metadata.length）
> - **Bitmap64 物理总字节数 = `metadata.length`**（外层框已计入）
>
> Rust 现状 `crates/paimon/src/deletion_vector/factory.rs::DeletionVectorFactory::read`（`:78-94`，Stage 2 已落 `.min(file_size)` clamp）：
> ```rust
> async fn read(file_io: &FileIO, df: &crate::DeletionFile) -> Result<DeletionVector> {
>     let input = file_io.new_input(df.path())?;
>     let file_size = input.metadata().await?.size;
>     let reader = input.reader().await?;
>     let offset = df.offset() as u64;
>     let len = df.length() as u64;
>     // 32-bit: physical blob is len + 8 bytes (outer length + crc frame not counted in len).
>     // 64-bit: physical blob is len bytes (outer length + crc frame already counted).
>     // Clamp to file size so 64-bit reads don't over-read past EOF when the
>     // DV is the last blob in the file.
>     let end = offset
>         .saturating_add(len)
>         .saturating_add(8)
>         .min(file_size);
>     let bytes = reader.read(offset..end).await?;
>     DeletionVector::read_from_bytes(&bytes, Some(len))
> }
> ```
> 这个 `+8` 对 32-bit **正好读到完整物理 blob**；对 64-bit 在 DV 不是文件最后一块时**多读 8 字节**，无害（`read_from_bytes` 按 inner length 字段截取，多余字节被忽略）；当 DV 是文件最后一块时 `.min(file_size)` 把多读截掉，避免 EOF over-read。

**Bitmap32**（`BitmapDeletionVector.serializeTo:87-99@e8938f347` + read 端校验 `DeletionVector.read:103-117@e8938f347`）：

```
[bitmapLength:int32 BE][magic:int32 BE][roaring32 bytes][crc:int32 BE]
```

- `bitmapLength = MAGIC_NUMBER_SIZE_BYTES (4) + roaring32_serialized_size`（含 magic、不含外层 length 字段也不含 crc）
- `metadata.length == bitmapLength`（read 端校验 `bitmapLength == length`）
- 物理 blob 占 `bitmapLength + 8 = metadata.length + 8` 字节
- CRC 跳过不校验

**Bitmap64**（`Bitmap64DeletionVector.serializeTo:93-106@e8938f347` + read 端校验 `DeletionVector.read:117-138@e8938f347`）：

```
[bitmapDataLength:int32 BE][magic:int32 LE][roaring64 bytes LE][crc:int32 BE]
```

- 常量见 `Bitmap64DeletionVector:41-44@e8938f347`：`LENGTH_SIZE_BYTES = CRC_SIZE_BYTES = MAGIC_NUMBER_SIZE_BYTES = 4`
- `bitmapDataLength = MAGIC_NUMBER_SIZE_BYTES + bitmap.serializedSizeInBytes()`（含 magic、不含外层 length 字段也不含 crc）
- `metadata.length == bitmapDataLength + LENGTH_SIZE_BYTES + CRC_SIZE_BYTES = bitmapDataLength + 8`（read 端校验 `bitmapDataLength == length - 8`）
- 物理 blob 占 `metadata.length` 字节（外层 length + crc 已计入）
- magic 在 LE buffer 中写入，所以 read 时用 `toLittleEndianInt` 比较
- CRC 跳过不校验

#### Index file 容器

`DeletionVectorsIndexFile`：`[version:byte=1][ DV blob #0 ][ DV blob #1 ][ ... ]`，每个 DV blob 由 index manifest entry 的 `dvRanges()` 给出 `(offset, length)`：
- `offset` 是物理 blob 起始位置（相对文件头，不去掉 version byte）
- `length` 是 `serializeTo` 返回值，**不等价于**统一的物理 blob 字节数（见上面 Layout 节的 ⚠️）
- 读 blob 时实际拉取的字节数：32-bit 需要 `length + 8`，64-bit 需要 `length`；统一拉 `length + 8` 是安全做法（多余字节由 `read_from_bytes` 按 inner length 字段忽略）

`DeletionVector.read(DataInputStream, length)` 是 blob 解析入口（`DeletionVector.java:97-145@e8938f347`），**按 dataInputStream 顺序读 4+4+(inner_bitmap_length-4)+4 字节**，不依赖 `length` 来截取。

> 历史记录：原文档曾把 `DeletionVectorsIndexFile.java:163-172` 引为 layout anchor —— 该 anchor 实际是 `createWriter()` / `checkVersion()`，与 layout 无关，已删除。layout 主 anchor 改为 `DeletionVector.read` + `Bitmap32/Bitmap64.serializeTo` + `DeletionFileWriter.write`。

### 3. `deletion-vectors.read-mode` (PERFORMANCE / FRESHNESS)

> ⚠️ 本节描述的 read-mode 选项在 apache/paimon master 不存在；这是 Kuaishou fork 的私有能力，详见上节《对齐基准》。

- **`paimon-api/.../CoreOptions.java:1880-1889@e8938f347`**：option 声明，默认 `PERFORMANCE`。`DvReadMode` enum 在同文件 `4144-4155@e8938f347`。
- **`paimon-core/.../KeyValueFileStoreScan.java:154-162@e8938f347`**：plan 时若 L0 entry 跑 value-stats 裁剪，**仅 FRESHNESS 模式允许**；其它模式 throw（`IllegalStateException`）。
- **`paimon-core/.../table/source/DataTableBatchScan.java:67-83@e8938f347`**：PK + `batchScanSkipLevel0` 分支：`options.dvFreshnessReadEnabled()` → 仅 `enableValueFilter()`（保 L0）；否则 → `withLevelFilter(level -> level > 0).enableValueFilter()`（跳 L0）。
- `DataTableStreamScan.java`（同分支）在 streaming bootstrap 路径上做同样的分派。
- `MergeFileSplitRead` **不直接** 读 `read-mode`；它只对 planner 喂进来的文件做 sort-merge + per-file DV apply。所以读端实现的 contract 是：planner 已按 mode 决定 L0 是否在 split 里。

### 4. DV 与 LookupMerge 的关系

⚠️ **澄清 capabilities 文档 F9**：DV 和 LookupMerge 在 Java 是 **两个独立优化**，文档把它们混在一起描述了：

- **DV-PERFORMANCE**：planner 直接不交 L0 给 reader，L1+ 已 DV-applied + 非重叠 → 可走 raw read（绕过 sort-merge）。这是 F9 描述里 "level≥1 文件已全部应用 DV，可绕过 sort-merge 走 raw read" 的含义。
- **LookupMergeFunction**：runtime 用 L0 hash + L1+ 点查取代 sort-merge，由 `mfFactory instanceof LookupMergeFunction.Factory` 选中（`MergeFileSplitRead.java:170-178@e8938f347`），驱动条件是 `changelog-producer=lookup` 或 `force-lookup=true`，**与 `DV_READ_MODE` 无关**。主要服务流式 changelog 生成。

批读用例下，DV-PERFORMANCE 的 raw-read 短路（Stage 4a）就够拿到 F9 的吞吐收益；LookupMergeFunction 全套（Stage 4b）只在确有流式 changelog 需求时才需要。

### 5. 索引文件加载

- 多文件批量：`DeletionVectorsIndexFile.readAllDeletionVectors(IndexFileMeta)` (`paimon-core/.../deletionvectors/DeletionVectorsIndexFile.java:73-96@e8938f347`)，开 index blob 一次，按 `dvRanges()`（`LinkedHashMap<dataFileName, DeletionVectorMeta>`）逐项读。
- 按 split partial：`readDeletionVector(Map<String, DeletionFile>)` (`:105-127@e8938f347`)。
- Rust 已有 `crates/paimon/src/spec/avro/index_manifest_entry_decode.rs` 解码 index manifest entry，`DeletionVectorMeta` 已就绪。

### 6. Empty-file / all-deleted 短路

Java **没有** plan-time "all rows deleted → skip file" 优化（`DataFileMeta.deleteRowCount()` 在 plan 阶段只用于 limit-pushdown 的反向短路：`KeyValueFileStoreScan.java:280-294@e8938f347`，遇到 `deleteRowCount > 0` 时 limit-prune **bail out**）。`KeyValueFileReaderFactory.java:174@e8938f347` 仅在 `dv.isEmpty()` 时跳过 wrap，不跳过文件本身。**Rust 此点跟 Java 一致即可**，不引入新优化。



<!-- SECTION-STAGES -->

## 实施分阶段

每阶段独立 PR-able，建议按顺序推进。

### Stage 1 — KV reader DV wiring (C2)

**目标**：让 `kv_file_reader.rs` 能消费带 DV 的 split。复用 `data_file_reader.rs` 已验证的 DV→parquet-row-selection 模式，**不动** `MergeRow` / `SortMergeReader` —— DV 在 parquet 层就剥掉删除行，sort-merge 看到的是过滤后的行。

> **C2 闭合范围**（评审修复阶段更新）：Stage 1 落地时 `kv_file_reader.rs` 主动 reject `PartialUpdate / VersionedPartialUpdate + DV`，仅 Deduplicate+DV 工作。评审修复阶段验证 Java `KeyValueFileReaderFactory.java:173-187@e8938f347` 的 DV 应用与 merge engine 完全解耦后，删除该 reject，PU/VPU+DV 进入工作集；`crates/paimon/src/table/kv_file_reader.rs::tests` 含 `test_kv_reader_partial_update_with_deletion_vector`（e2e PU+DV）+ VPU+DV dispatch smoke。C2 至此完全闭合，覆盖所有支持的 merge engine。

**改动**（实际改动比初稿描述更小 —— `KeyValueFileReader` 直接复用 `DataFileReader::read_single_file_stream`，不需要新增 KV 自己的 `read_single_file_stream`，也不需要把 `merge_row_selection` / `dv_to_non_deleted_ranges` 抽到公共 module）：

- `crates/paimon/src/table/kv_file_reader.rs`
  - 加 `use crate::deletion_vector::DeletionVectorFactory;`
  - 删除 split 入口拒绝 DV 的 `Error::Unsupported` 分支（原 line 269-277）
  - 在 `for split in &splits` 入口构造 per-split DV factory（mirror `data_file_reader.rs:88-105` 完全相同的模板）：
    ```rust
    let dv_factory = if split.data_deletion_files().is_some_and(|files| files.iter().any(Option::is_some)) {
        Some(DeletionVectorFactory::new(&file_io, split.data_files(), split.data_deletion_files()).await?)
    } else {
        None
    };
    ```
  - 在 `for file_meta in split.data_files().to_vec()` 循环里加 DV 查询 + 透传：
    ```rust
    let dv = dv_factory
        .as_ref()
        .and_then(|factory| factory.get_deletion_vector(&file_meta.file_name))
        .cloned();
    let stream = reader.read_single_file_stream(split, file_meta, data_fields, dv, None)?;
    ```
  - **不**新增 KV 自己的 `read_single_file_stream` —— `DataFileReader::read_single_file_stream` (line 147-154) 已支持 `dv: Option<Arc<DeletionVector>>` 入参；现状 KV 调用处仅硬传 `None`，本改动只是把 `None` 换成 `dv`
- **不抽** `merge_row_selection` / `dv_to_non_deleted_ranges` 到公共 module：KV 路径**不直接**调用这两个 helper —— DV → row selection 转换发生在 `data_file_reader.rs::read_single_file_stream` **内部**，KV 端只透传 `Arc<DeletionVector>`。两个 helper 保持 `data_file_reader.rs` 内部私有。如未来出现 raw / KV 之外的第三个调用方，再抽公共 module。
- `crates/paimon/src/deletion_vector/mod.rs`：把 `MAGIC_NUMBER` 暴露为 `pub(crate)`（测试 helper 需要）；其它 exports 保持不变。

**Verification**：

- 单测（`kv_file_reader.rs` `#[cfg(test)] mod tests`）：构造 1 个含 4 行的 parquet 文件 + DV bitmap{1,3} → KV reader 输出 row 0 / row 2（**smoke test**）
- **多 row-group 单测（必跑）**：构造 ≥2 个 row group 的 parquet 文件（如每组 3 行 / 共 6 行）+ DV bitmap{1,4}；期望输出**绝对** row id `0, 2, 3, 5`。这是 DV row id 必须是文件**绝对** row position（**不是** 单 row-group 内偏移）的强制 invariant 验证 —— 单 row-group 测试无法证明此项。
- **当前实施路线（reader-level fixture）**：用 `RoaringBitmap::serialize_into` 在测试内部构造 DV blob + parquet 文件 + 配套 `DeletionFile` metadata + `DataSplit::with_data_deletion_files(...)`，验证 KV reader DV 通路。这是已合入的实际路线，覆盖了 Stage 1-4a 所有 DV 测试。
- **Java fixture follow-up**：用 paimon-java Kuaishou fork @`e8938f347` 提前生成 PK + DV 表 fixture（写入 + UPDATE 后产生的目录结构），check-in 到 `crates/paimon/tests/fixtures/dv_pk_table/`；paimon-rust 只负责 scan + read，验证旧值不出现。配 `README.md` 记录 (a) Java commit；(b) 建表 DDL 与写入 / UPDATE 语句；(c) 生成命令；(d) 预期结果（行数 / 哪些 row id 被 DV 标删）；(e) 数据规模约束（建议单表 ≤ 1MB）。后续 fixture regenerate / 升级时按此文档复现。**写路径不在本方案 scope，DataFusion SQL 写 DV 不作为必跑项**（避免循环依赖）。Bitmap64 fixture 的 follow-up tracker 见 [`crates/paimon/tests/fixtures/deletion_vector/README.md`](../crates/paimon/tests/fixtures/deletion_vector/README.md)。
- **CI 依赖**：当前实施路线下 reader-level fixture 是 in-test 构造，CI 无新增依赖；Java fixture 路线下 fixture 也是预生成静态测试数据，**CI 不新增 Java 运行依赖**，也**不**要求 paimon-java 在 PATH。
- **不回归现有 lib 测**（716+ 已有单测全过）。

### Stage 2 — Bitmap64 DV (C6)

**目标**：识别 Java `Bitmap64DeletionVector` 写出的 64-bit row-position DV 文件。当前 Rust 只能解 32-bit；Java 写出的 64-bit DV 文件 paimon-rust 当 invalid magic 报错。

**关键事实**（实施前已核实）：

- Rust `roaring 0.11.4::RoaringTreemap::serialize_into` (`treemap/serialization.rs:43-52`) 与 Java `OptimizedRoaringBitmap64.serialize` (`paimon-common/.../OptimizedRoaringBitmap64.java:198-221@e8938f347`) **字节布局完全一致**（都是 portable RoaringTreemap：`[bitmap_count:u64 LE][high_key:u32 LE + RoaringBitmap32 portable bytes] * N`），doc comment 明确"compatible with the official C/C++, Java and Go implementations"
- 因此 Rust 端**直接用 `RoaringTreemap::deserialize_from`** 读 Java 写出的 64-bit DV，**不需要手写 parser**
- `roaring = "0.11"` 默认含 `RoaringTreemap`，**不需要改 Cargo.toml**

**改动**：

- `crates/paimon/src/deletion_vector/core.rs`
  - `DeletionVector` 从 struct 改为 enum，`Arc<...>` 直接放入（不抽 newtype，原文档描述偏差此处选择简洁实现）：
    ```rust
    pub enum DeletionVector {
        Bitmap32(Arc<RoaringBitmap>),
        Bitmap64(Arc<RoaringTreemap>),
    }
    ```
  - 公共 API：`iter() -> DeletionVectorIterator`（**保留现有自定义类型** + `advance_to` 扩展点；内部 `Vec<u64>` 实现已天然兼容两种 variant）、`is_empty() -> bool`、`empty()` 默认返 `Bitmap32` 向后兼容
    - **不要**把签名改成 `Box<dyn Iterator<Item=u64>>` —— 会类型擦除掉 `advance_to` 扩展点
  - 删除 `from_bitmap`（无 production 调用方），加 `from_bitmap32(RoaringBitmap)` + `from_bitmap64(RoaringTreemap)`
  - cfg(test) `bitmap()` 替换为 `as_bitmap32() -> Option<&RoaringBitmap>` + `as_bitmap64() -> Option<&RoaringTreemap>`
  - 常量：保留 `MAGIC_NUMBER`（不改名 —— Stage 1 已暴露 `pub(crate)`），新增 `MAGIC_NUMBER_V2 = 1681511377`、`LENGTH_SIZE_BYTES = 4`、`CRC_SIZE_BYTES = 4`
  - `read_from_bytes` magic dispatch：
    - 读 4 字节 length（BE int32）+ 4 字节 magic
    - `raw_magic == MAGIC_NUMBER` → 32-bit 路径（与现状一致：`bitmap_length == expected_length`）
    - `raw_magic.swap_bytes() == MAGIC_NUMBER_V2` → 64-bit 路径（按 `bitmap_length == expected_length - 8` 校验，再 `RoaringTreemap::deserialize_from`）
    - 都不匹配 → 报 "Invalid magic"
  - 调用方（`factory.rs`、`data_file_reader.rs`、Stage 1 新 KV 路径）按 enum 用，逻辑不变
- `crates/paimon/src/deletion_vector/factory.rs`
  - **关键修复**：`read` 方法的 over-read 范围必须 clamp 到 file size。32-bit 物理 blob = `length + 8` 字节（外层 length 字段 + crc 不在 metadata.length 内）；64-bit 物理 blob = `length` 字节（外层框已计入）。统一用 `read(offset..min(offset+length+8, file_size))` 既覆盖 32-bit 上界，也避免 64-bit over-read 失败。`read_from_bytes` 用 inner length 字段截取，多余字节自动忽略
  - 加 `input.metadata().await?.size` 调用拿 file_size
- `crates/paimon/src/deletion_vector/mod.rs`
  - 加 `#[cfg(test)] pub(crate) use core::MAGIC_NUMBER_V2;`（mirror Stage 1 的 `MAGIC_NUMBER` cfg(test) re-export）
- `crates/paimon/src/spec/core_options.rs`
  - 加 `DELETION_VECTORS_BITMAP64_OPTION = "deletion-vectors.bitmap64"`
  - `pub fn deletion_vectors_bitmap64(&self) -> bool`，默认 false
  - **仅供 schema 信息记录 / 未来写侧选 format**；read 端 magic dispatch 已可识别两种格式，与该 option 无关

**Verification**（沿用 Stage 1 替代 fixture 决策，不强求 Java-generated binary）：

- **核心 round-trip 测试**（`core.rs::tests`）：
  - `test_read_deletion_vector_bitmap64`：构造覆盖空 bitmap / 小 row id（< 2^16）/ 跨 32-bit 边界（> 2^32）/ 多个高位 container 的 64-bit 位置，用 `RoaringTreemap::serialize_into` + 手工拼外层框 → `read_from_bytes` → `as_bitmap64()` 内容一致
  - `test_bitmap64_iter_yields_correct_positions`：含 `1u64 << 33` 等大值 → `iter()` sorted 后断言完整集合
  - `test_bitmap64_length_mismatch`：故意把 `expected_length` 设错 → 报 "Size not match (64-bit)"
  - `test_magic_dispatch_invalid_magic`：随机 4 字节既不是 32-bit BE magic 也不是 64-bit LE magic → 报 "Invalid magic"
  - `test_empty_default_is_bitmap32`：`DeletionVector::default()` / `empty()` 仍是 32-bit variant（向后兼容）
  - `test_read_bitmap32_round_trip`：32-bit 也跑 round-trip（不只依赖 file fixture）
- **KV reader e2e 测试**（`kv_file_reader.rs::tests`）：
  - `test_kv_reader_applies_bitmap64_deletion_vector`：4 行 parquet + 64-bit DV{0,2} → KV reader 输出 k=[1,3]，验证 64-bit DV 完整跑通 KV 通路
  - 测试 helper `write_test_dv64_blob`（sibling of Stage 1 `write_test_dv_blob`）按 Java byte layout 写 64-bit blob
- **Java fixture follow-up**：Java fork @`e8938f347` 提前生成的 binary fixture 留作 follow-up PR（替代 fixture 路线已通过 round-trip 验证字节兼容；Java fixture 提供独立的跨实现校验）

### Stage 3 — read-mode + L0 routing (C7 + C4)

**目标**：识别 `deletion-vectors.read-mode`，让 plan 端按 mode 决定是否保留 L0 + 是否对 L0 跑 value-stats 裁剪。同步修 C4（非 DV 模式下 L0 value-stats 裁剪不安全，会破坏 sort-merge 输入返过期值）。

**改动**：

- `crates/paimon/src/spec/core_options.rs`
  - 新 enum 紧邻 `MergeEngine`（与现有 enum 分组对齐）：
    ```rust
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DvReadMode {
        Performance,
        Freshness,
    }
    ```
  - `DELETION_VECTORS_READ_MODE_OPTION = "deletion-vectors.read-mode"` 紧邻 `DELETION_VECTORS_*` const 分组
  - `pub fn deletion_vectors_read_mode(&self) -> crate::Result<DvReadMode>`，默认 `Performance`（与 Java `CoreOptions.java:1880-1889@e8938f347` 一致）；mirror 现有 `merge_engine()` getter 模板
  - 4 个测试覆盖：默认 / "performance" / "FRESHNESS"（case-insensitive）/ unsupported value 报错
- `crates/paimon/src/table/stats_filter.rs`（**helper 放此处而非 table_scan.rs**：与 `data_file_matches_predicates` 同文件就近）
  - 新增 entry 级 helper（统一 C7 + C4 + DV mode 决策）：
    ```rust
    pub(crate) fn should_apply_value_stats_to_entry(
        level: i32,
        has_primary_keys: bool,
        dv_enabled: bool,
        dv_read_mode: DvReadMode,
        merge_engine: Option<MergeEngine>,
    ) -> bool
    ```
  - 决策矩阵（**订正自原 plan line 324**：FRESHNESS L0 应返 `false` 与 Java 对齐，不是 `true`；mirror Java `KeyValueFileStoreScan.java:154-162@e8938f347` "FRESHNESS: keep L0 unconditionally; stats pruning only applies to L1+"）：

    | level | has_pk | dv_enabled | dv_read_mode | merge_engine | apply |
    |---|---|---|---|---|---|
    | > 0 | * | * | * | * | true |
    | 0 | false | * | * | * | true |
    | 0 | true | true | Performance | * | true（unreachable，L0 已 plan 剥掉）|
    | 0 | true | true | Freshness | * | **false** |
    | 0 | true | false | * | Deduplicate / PartialUpdate / VPU | **false**（C4 修复点）|
    | 0 | true | false | * | FirstRow | true（unreachable，FirstRow 已 skip L0）|
  - `merge_engine: Option<MergeEngine>`（`None` 视作 Deduplicate fail-open）—— 比 `Result<MergeEngine>` 更易在 closure 内 `Copy`，因 `crate::Error` 非 `Copy`
  - 7 个测试覆盖矩阵（L1+ 性能守护 + 非 PK L0 + DV-FRESHNESS-L0 + DV-PERFORMANCE-L0 safe-default + 非 DV PK 三 engine 跳过 + FirstRow safe-default + None 落 Deduplicate）
- `crates/paimon/src/table/table_scan.rs`
  - `should_skip_level_zero_for_scan` 加 `dv_read_mode: DvReadMode` 参数；规则：scan_all_files / 非 PK → false；FirstRow → true；DV+PERFORMANCE → true；DV+FRESHNESS → false；其它 → false
  - `read_all_manifest_entries` 加参数 `deletion_vectors_enabled: bool, dv_read_mode: DvReadMode, merge_engine: Option<MergeEngine>` 透传，filter chain 加 `should_apply_value_stats_to_entry` gate（在 `data_file_matches_predicates` 之前）
  - `plan_manifest_entries` 透传新参数；`plan_snapshot` 内的 cross-schema fallback（`data_file_matches_predicates_for_table` 调用前）也 gate `should_apply_value_stats_to_entry` 保持对称
  - **4 处** `should_skip_level_zero_for_scan` 调用点全部更新（订正自风险 #6 的"5 处"）：`table_scan.rs:543` production + `:881` use import + `:1105`/`:1115` 现有测试
  - 矩阵单测扩到 8 行（含原 2 个 + 新 6 个：`scan_all_files_short_circuits_for_dedup` / `non_pk_table_keeps_level_zero` / `dv_performance_strips_level_zero` / `dv_freshness_keeps_level_zero` / `non_dv_dedup_keeps_level_zero` / `non_dv_partial_update_keeps_level_zero`）

**Verification**（C4 修复与 read-mode 引入**分组验证**，PR 描述要求 reviewer 分别审）：

**read-mode plan 行为组**：

- 单测：`should_skip_level_zero_for_scan` 8 行矩阵覆盖
- 单测：`deletion_vectors_read_mode` getter 4 个 case（默认 / performance / FRESHNESS / unsupported）
- 集成测试（在 Stage 1 的 `dv_pk_table_read.rs` 内，待 Stage 3 PR 加）：FRESHNESS 模式下读出的 L0 行 + DV 一致；PERFORMANCE 模式下 plan 输出不含 L0

**C4 value-stats 修复组**（**改变非 DV PK 表 L0 entry 的 value predicate 结果**，独立验证）：

- 单测：`should_apply_value_stats_to_entry` 矩阵覆盖（7 个测试，含 L1+ 性能回退守护 + C4 修复 + FRESHNESS L0 + 三种 unreachable safe-default + None 落 Deduplicate）
- 注：C4 端到端集成回归（构造 PK 重叠多 L0 + value_stats 不重叠 + 谓词命中旧版本 stats 的真实 entry 流）需要 manifest entry 构造 fixture，留作 follow-up；helper 层面已守护决策矩阵
- **最小复现 fixture**（PR 描述必须引用）：见 SECTION-RISKS 风险 #3

### Stage 4a — DV-PERFORMANCE raw-read 短路 (F9 批读核心收益) + C5 修复

**目标**：所有 PK raw 路径（DV / 非 DV 都覆盖）启用 `drop_deletes`，剥离 raw 读残留的 DELETE / UPDATE_BEFORE 物理行（C5 fix），同时 DV-PERFORMANCE 模式下全 L1+ split 自然走 raw 路径拿到 F9 批读收益。**drop_deletes 范围扩到所有 PK raw 路径**（按用户决策；与 capabilities 文档 C5 范围对齐 —— 非 DV PK Dedup raw 同样有"幽灵 DELETE 行"问题）。

**Planner contract**（Stage 3 的 plan 阶段保证 → Stage 4a reader 端依赖）：

> DV-PERFORMANCE 模式下，planner 交给 reader 的 L1+ split **不含 key-range overlap 的文件组合**。这是 Kuaishou fork @`e8938f347` 的 LSM 写端 + DV 维护端共同保证的 invariant：DV 已把所有过时版本（含跨 level overlap 的旧版本）标 deleted，`pack_sections` 输出的 section 内 key range 单调不重叠。
>
> **Rust 端不加运行时 overlap 检查**（按用户决策方案 B：性能优先）；改为 **debug assertion + 显式负面测试**保护 contract。Rust 端实施了 `debug_assert!(!split_requires_merge(...))`（仅 DV 路径，非 DV 路径已用 `split_requires_merge` 的需求驱动判断守护）。

**改动**（实施明细）：

- `crates/paimon/src/table/data_file_reader.rs`
  - 加 `drop_deletes: bool` 字段，默认 `false`（`DataFileReader::new` 内不变，4 处现有调用点零改动）
  - 加 `with_drop_deletes(self, bool) -> Self` builder method（mirror `with_blob_as_descriptor` 模板）
  - `read_single_file_stream` 入口 caller-contract 校验：`drop_deletes=true` 时 `read_type` 必须含 `_VALUE_KIND` 字段（`VALUE_KIND_FIELD_ID`），否则返 `Error::DataInvalid`
  - stream 外预计算 `(vk_idx, output_schema_without_vk)`（drop `_VALUE_KIND` 列后的 schema）
  - yield 前 RowKind filter：取 Int8 column → `RowKind::from_value(...).is_add()` 构 BooleanArray mask（NULL → INSERT fallback，与 `sort_merge.rs:336-342` 一致）→ `arrow_select::filter::filter_record_batch` → drop `_VALUE_KIND` 列重建 RecordBatch → yield
- `crates/paimon/src/table/table_read.rs`
  - `read_pk` dispatch 不变（DV + L0 → KV merge；全 L1+ → raw；非 DV 走 `split_requires_merge`）
  - 选 raw 路径时加 `debug_assert!(!split_requires_merge(...))`（DV 路径需要 contract 守护，非 DV 路径已由 `split_requires_merge` 的 needs_merge 判断隐式守护，但 debug_assert 在两路径都加保险）
  - **新方法** `read_pk_raw_drop_deletes` + `new_data_file_reader_drop_deletes`：mirror `read_raw` / `new_data_file_reader`，但 `read_type` 前置 `_VALUE_KIND` 字段（new helper `raw_read_type_with_value_kind`）+ 链 `.with_drop_deletes(true)`
  - `read_pk` raw 路径调用从 `read_raw` 换为 `read_pk_raw_drop_deletes`；非 PK / append / 系统表的 `read_raw` 调用（`to_arrow:100`）**保持不变**
  - 现有 `read_raw` / `new_data_file_reader` **保留**（非 PK / append / 系统表用）
- `crates/paimon/src/table/kv_file_reader.rs` 测试 helper
  - `write_kv_parquet_file` 加可选 `vks` 参数支持 mixed RowKind（4 个现有调用点传 `None` 保持原行为）
  - 加测试 `test_pk_raw_drop_deletes_equivalent_to_sort_merge`（核心等价性，KV vs raw + drop_deletes byte-for-byte 一致）
  - 加测试 `test_data_file_reader_default_keeps_delete_rows`（C5 反向：默认 `drop_deletes=false` 时 DELETE / UPDATE_BEFORE 行被原样保留）

**Verification**：

- **等价性测试（必跑，本 stage 核心正确性兜底）**：`kv_file_reader::tests::test_pk_raw_drop_deletes_equivalent_to_sort_merge` 构造 4 行 mixed RowKind parquet（INSERT / DELETE / UPDATE_AFTER / UPDATE_BEFORE）+ DV{0}（删 INSERT 行），分别走 `KeyValueFileReader` 和 `DataFileReader.with_drop_deletes(true)`，断言两路径输出 byte-for-byte 一致（仅剩 k=2，即唯一的 UPDATE_AFTER）。这是 Stage 4a 的核心 invariant，单纯 "DELETE 不出现在结果集" 不够（见 SECTION-RISKS 风险 #9）。
- **C5 反向（必跑）**：`kv_file_reader::tests::test_data_file_reader_default_keeps_delete_rows` 验证 `drop_deletes=false` 默认行为：4 行 mixed RowKind parquet 全部保留（行数 = 4），证明非 PK / append / 系统表 raw 读语义未受影响。
- **dispatch 层 raw / sort-merge 决策**（评审修复阶段对齐 Java rawConvertible 语义）：`table_read.rs::read_pk` 不再仅以 `level == 0` 判断 needs_merge，改用同一套 `is_raw_convertible_file_group` + `has_key_overlap` 检查（mirror `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible + `withoutDeleteRow:151-154`）。release build 下 overlap / DELETE-bearing L1+ split 已被 helper 自然路由到 KV sort-merge；`debug_assert!(!split_requires_merge(...))` 仍保留作为 contract violation 兜底（仅 debug build）。
- **dispatch 层 release-mode 安全网测试**（`table_read.rs::tests`，本评审修复阶段新增）：5 个 case 覆盖 overlap-L1 / `delete_row_count > 0` 的 L1 / 干净 L1 / 任一 L0 / 无 PK comparator 的路由决策，证明 release build 下行为正确，不依赖 debug_assert。
- **集成 dispatch 测试**：DV-PERFORMANCE + 全 L1+ split 走 raw 路径（trace / metric 辅助）—— 不替代等价性测试；留作 Stage 1 fixture 路线后续扩展。

**FILE-LIST 实施 anchor**：
- `data_file_reader.rs:36-48`（struct 加字段）+ `:50-70`（new 默认 false）+ `:72-105`（with_drop_deletes builder）+ `:147-165, 226-300`（read_single_file_stream caller-contract 校验 + filter + drop column）
- `table_read.rs:21-26`（imports）+ `:138-152`（dispatch 加 debug_assert）+ `:158, 162`（raw 路径换 `read_pk_raw_drop_deletes`）+ `:217-302`（read_pk_raw_drop_deletes / new_data_file_reader_drop_deletes / raw_read_type_with_value_kind）
- `kv_file_reader.rs:646-678`（write_kv_parquet_file 加 vks 参数）+ tests 末尾（新增 2 个测试 + 2 个 helper）

**与 dv-impl-plan 风险 #9 / #10 关系**（已落地保护）：
- 风险 #9（reader pipeline 改变）：等价性测试（`test_pk_raw_drop_deletes_equivalent_to_sort_merge`）落地
- 风险 #10（planner contract 依赖）：`debug_assert!(!split_requires_merge(...))` 在 `read_pk` dispatch 的 raw 路径选择处守护；release build 不带，性能不付代价

### Stage 4b — LookupMergeFunction 全套 (F9 流式收益，建议 follow-up)

**前置条件**：仅在确有 `changelog-producer=lookup` / `force-lookup=true` 流式需求时启动；批读 PK 用例下 Stage 4a 已能拿到 F9 核心收益。

**目标**：服务 `changelog-producer=lookup` / `force-lookup=true` 流式 changelog 生成。

**改动梗概**（详化时再细化）：

- 新 `crates/paimon/src/table/lookup/{mod,lookup_levels,lookup_file,lookup_merge_function}.rs`
- `LookupLevels`：L0 文件全量 hash 进 in-memory 索引（PK → row）
- `LookupFile`：L1+ 文件按 PK 点查（基于 paimon 写入时按 PK 排序的事实，二分 / index 查）
- `LookupMergeFunction`：实现 `MergeFunction` trait，runtime 对每条 L1+ row 触发 L0 lookup 合并
- 选择逻辑：`new_merge_function` 在 `changelog-producer=lookup || force-lookup=true` 下返回 LookupMergeFunction

**Verification**（**follow-up 文档负责项，不阻塞 Stage 1-4a 主体 merge**）：

- 移植 Java `LookupMergeFunctionTest` 单测
- E2E 验证 `changelog-producer=lookup` 表读结果与 Java 一致（依赖能跑 Java fixture）—— **此项仅在 Stage 4b 启动时单独立计划，不作为本方案主体的 gate**



<!-- SECTION-RISKS -->

## 关键风险 & 注意点

1. **DV row-id 与 parquet absolute row-position 对齐**：Java `ApplyDeletionVectorReader` 用 `iterator.returnedPosition()`（绝对 row position 跨 row-group）；Rust 走 parquet `with_row_selection`，需 DV row id 与 parquet 文件的绝对 row position 一致（**不是** 单 row-group 内偏移）。`data_file_reader.rs:329 / 344 / 353` 现有 `merge_row_selection` / `dv_to_non_deleted_ranges` 提供了 raw 路径的实现模板，KV 路径同模式即可。但当前这两个 helper 是 `data_file_reader.rs` 内私有函数，Stage 1 抽到 `deletion_vector` module 公共后，**KV 路径仍必须通过基础组合测试独立验证**：empty DV、all-deleted、deleted out-of-range、DV + row_ranges intersection、跨 row-group（绝对 position invariant）、predicate remap 后的 row selection。raw 路径"已验证"仅限其当前覆盖的场景，不能直接外推到 KV + DV 全部组合。

2. **Empty DV 短路**：Java `KeyValueFileReaderFactory.java:174@e8938f347` 在 `dv.isEmpty()` 时跳过 wrap。Rust `dv_to_non_deleted_ranges` 在 empty DV 情况下应返回 full row range（不裁），等价行为。需单测覆盖 empty DV path。

3. **C4 与 C7 的语义边界 + Stage 3 维持合并 PR 的发布要求**：Java 的 "L0 + value-stats 不安全" 逻辑在所有 PK + Deduplicate / PartialUpdate 下都成立，**不止 DV 模式**。本方案 Stage 3 同时引入 read-mode option 和修复 C4 value-stats pruning，按用户决策**维持单 PR**，但 PR 描述必须显式列出：
   - **受影响范围**：所有 PK + Deduplicate / PartialUpdate / VersionedPartialUpdate 表（**不限 DV 模式**）
   - **行为变化**：之前因 L0 文件 value-stats 错误裁剪而过滤掉的行，修复后会返回（语义上是从过期 / 错误结果恢复到正确结果，**不是新引入的回归**）
   - **触发条件**（精确）：必须**多个** L0 文件 + PK 重叠 + 各文件 `value_stats` **不重叠** + 谓词命中**旧版本**所在文件 stats 但不命中**新版本**所在文件 stats。单文件 stats 重叠场景看似 prune 实际只是 wasted IO（sort-merge 仍取最新版），不返过期值；最小复现 fixture 必须显式造多个 stats 不重叠的 L0 文件
   - **最小复现场景**：
     ```
     CREATE TABLE t (k INT PRIMARY KEY, v INT) WITH ('merge-engine'='deduplicate');
     INSERT (1, OLD_V); INSERT (1, NEW_V);  -- 两次都进 L0，PK=1 在两个 L0 文件
     -- compaction 触发前
     SELECT * FROM t WHERE v = NEW_V;
     -- 旧路径（C4 buggy）：file_old 的 value_stats min=max=OLD_V 不命中谓词 → file_old 被 prune
     --                    sort-merge 输入只剩 file_new，正常返回 (1, NEW_V) — 看似没问题
     -- 但反过来 SELECT WHERE v = OLD_V：file_new 的 stats=NEW_V 不命中 → file_new 被 prune
     --                                  sort-merge 只剩 file_old，返回 (1, OLD_V) — 过期值！
     -- 新路径：L0 entry 跳过 value-stats，两个文件都进 sort-merge，按 sequence 取胜出版本
     ```
   - **修复前后 monitoring**：保留观察日志至少 1 个 release cycle，文档化"由错变对"的预期场景，避免被误判为回归

4. **Bitmap64 endianness**：32-bit BE magic vs 64-bit LE magic 是 Java 历史包袱（Iceberg 兼容）。Rust magic dispatch 必须按 byte-order-aware 比较，**不能直接** `as i32`。Stage 2 单测要显式覆盖两种字节序。

5. **LookupMerge 真实需求确认**：F9 描述模糊。建议 Stage 4a 先 ship（小、覆盖批读），Stage 4b 等明确流式 changelog 需求再启动，避免大量 dead code。决策点放在 Stage 4a merge 之后。

6. **`should_skip_level_zero_for_scan` 调用方更新**：现有 `table_scan.rs:543` production + `:881` use import + `:1105`/`:1115` 测试调用点需要同步加 `dv_read_mode` 参数。Rust 编译器会强制所有调用点更新，但 Stage 3 PR 要确保新签名覆盖**所有 4 处调用点**（实施时 grep 全树确认；Stage 3 实施已订正自最初描述的"5 处"）。

7. **`DeletionVector` enum 改造的兼容性**：当前 `pub struct DeletionVector` 是公开类型，外部 crate 可能引用了 `.bitmap()` 等方法。改 enum 后这些 API 会破坏。Stage 2 PR 要 grep workspace 内所有引用，提供过渡 API（如 `as_bitmap32()` 转 Option），或在 enum 上保留同名 method 派发到内部。

8. **`DeletionVectorMeta.length` 的 32 / 64-bit 不同构（实现强制要求）**：核实自 Java `DeletionFileWriter.java:56-59@e8938f347`（写端 `length = serializeTo(...)`） + `BitmapDeletionVector.serializeTo:87-99@e8938f347` 返回 `size`（不含外层框）vs `Bitmap64DeletionVector.serializeTo:93-106@e8938f347` 返回 `bytes.length`（含外层框）。
   - **Bitmap32**：`metadata.length == bitmapLength`（不含外层 8 字节）；物理 blob 占 `length + 8` 字节
   - **Bitmap64**：`metadata.length == bitmapDataLength + 8`（含外层 8 字节）；物理 blob 占 `length` 字节
   - **read 端拉取**：统一 `read(offset..offset + length + 8)` 是安全做法（32-bit 正好读完整 blob，64-bit 多读 8 字节由 `read_from_bytes` 忽略）；Rust 现状 `factory.rs:70-75` 已按此做。Stage 2 magic dispatch 重构必须**保持** `+8` 拉取，**不能**改成 `read(offset..offset + length)`。
   - **read 端校验**：32-bit `inner_bitmapLength == length`；64-bit `inner_bitmapDataLength == length - 8`。两者偏移公式**不通用**，dispatch 后必须按各自公式处理。
   - Stage 2 单测必须包含：(a) 32-bit + 正常 length；(b) 64-bit + 正常 length；(c) `metadata.length` 故意设错的负面 case。

9. **Stage 4a raw-read 短路改变 reader pipeline，不只是性能优化**：DV apply 顺序、RowKind 处理点、sequence 字段保留点都可能与 sort-merge 路径不同。仅断言"DELETE 不出现在结果集"或"trace 走 raw 路径"**不够**；必须用 ADD / DELETE / UPDATE_BEFORE 混合行 + DV bitmap 的等价性测试（sort-merge vs raw-read 输出 byte-for-byte 一致）兜底。见 Stage 4a Verification 第一项。

10. **Stage 4a 的 planner contract（key-range non-overlap）依赖 Java 写端 invariant**：DV-PERFORMANCE 模式下，paimon Java（Kuaishou fork @`e8938f347`）的写端 + DV 维护端共同保证交给 reader 的 L1+ split 不含 key-range overlap 的文件组合。Rust 端按用户决策选 contract + 校验路线（**不**加运行时 overlap 检查），用 debug-only `debug_assert!(!split_requires_merge(...))` + 显式负面测试守护此 contract。如果未来发现 Java 写端有 contract 违约的场景（DV 写时序 bug、跨 commit 时 DV 未刷新等），方案应改为 contract 失败回退 sort-merge（不影响正确性，仅性能下降）—— 此时再讨论是否引入运行时检查。



<!-- SECTION-FILE-LIST -->

## 文件改动清单（关键 anchor）

| 阶段 | 文件 | 关键行号 / 改动点 |
|---|---|---|
| 1 | `crates/paimon/src/table/kv_file_reader.rs` | 删 split 入口拒绝 DV 的 `Error::Unsupported`；加 per-split `DeletionVectorFactory`；file 循环里查 `dv` 并替换 `read_single_file_stream` 硬传的 `None` |
| 1 | `crates/paimon/src/deletion_vector/factory.rs` | 复用现状；`DeletionVectorFactory::{new, get_deletion_vector}` 接 KV 路径 |
| 1 | `crates/paimon/src/deletion_vector/mod.rs` | 把 `MAGIC_NUMBER` 暴露 `pub(crate)`（仅供测试 helper 引用），其它不变 |
| 1 | 新 `crates/paimon/tests/fixtures/dv_pk_table/` | Java-generated PK + DV 表 fixture（含 README，记录 commit / DDL / 生成命令）—— 替代 fixture 路线下可推迟到 follow-up PR |
| 1 | 新 `crates/paimon/tests/dv_pk_table_read.rs`（或现有 integration 模块） | 读 fixture 验证 PK + DV 通路；多 row-group absolute position invariant 测试 |
| 2 | `crates/paimon/src/deletion_vector/core.rs` | struct → enum (`Bitmap32(Arc<RoaringBitmap>) / Bitmap64(Arc<RoaringTreemap>)`)；加 `MAGIC_NUMBER_V2`；`read_from_bytes` magic dispatch；`from_bitmap32 / from_bitmap64`；cfg(test) `as_bitmap32 / as_bitmap64`；新增 6 个 64-bit 测试 |
| 2 | `crates/paimon/src/deletion_vector/factory.rs` | `read` 方法 over-read 范围 clamp 到 file size（避免 64-bit 物理 blob 在文件末尾时超读） |
| 2 | `crates/paimon/src/deletion_vector/mod.rs` | 加 `#[cfg(test)] pub(crate) use core::MAGIC_NUMBER_V2;` |
| 2 | `crates/paimon/src/spec/core_options.rs` | 加 `deletion-vectors.bitmap64` option + `deletion_vectors_bitmap64()` getter |
| 2 | `crates/paimon/src/table/kv_file_reader.rs` | tests 加 `write_test_dv64_blob` helper + `test_kv_reader_applies_bitmap64_deletion_vector` e2e 测试 |
| 3 | `crates/paimon/src/spec/core_options.rs` | `DvReadMode` enum + `DELETION_VECTORS_READ_MODE_OPTION` const + `deletion_vectors_read_mode()` getter + 4 个测试 |
| 3 | `crates/paimon/src/table/stats_filter.rs` | `should_apply_value_stats_to_entry` entry 级 helper（订正 FRESHNESS L0 = `false` 与 Java 对齐）+ 7 个矩阵单测 |
| 3 | `crates/paimon/src/table/table_scan.rs` | `should_skip_level_zero_for_scan` 加 `dv_read_mode` 参数 + 矩阵扩到 8 行；`read_all_manifest_entries` 加参数透传 + filter chain helper gate；`plan_manifest_entries` 透传；`plan_snapshot` 内 cross-schema fallback 对称 gate；4 处现有调用点全部更新（订正自原"5 处"） |
| 4a | `crates/paimon/src/table/table_read.rs` | dispatch 选 raw 时加 `debug_assert!(!split_requires_merge(...))`；新增 `read_pk_raw_drop_deletes` / `new_data_file_reader_drop_deletes` / `raw_read_type_with_value_kind` helper；`read_pk` raw 路径换为 `read_pk_raw_drop_deletes`（`read_raw` / `new_data_file_reader` 保留给非 PK / append / 系统表）。锚点：`read_pk` / `read_pk_raw_drop_deletes` / `raw_read_type_with_value_kind` 三个函数 |
| 4a | `crates/paimon/src/table/data_file_reader.rs` | 加 `drop_deletes: bool` 字段 + `with_drop_deletes` builder（不改 `new` 签名）；`read_single_file_stream` 加 caller-contract 校验 + RowKind filter + drop `_VALUE_KIND` 列。**所有 PK raw 路径启用**（drop_deletes 是 RowKind 正确性修复，与 DV mode 解耦），非 PK / append / 系统表保持默认 `drop_deletes=false` |
| 4a | `crates/paimon/src/table/kv_file_reader.rs` | `write_kv_parquet_file` 加 `vks` 参数；新增 `test_pk_raw_drop_deletes_equivalent_to_sort_merge`（核心等价性）+ `test_data_file_reader_default_keeps_delete_rows`（C5 反向） |
| 4b | 新 `crates/paimon/src/table/lookup/...` | 全部（建议 follow-up） |
| 评审修复 | `crates/paimon/src/table/merge_tree_split_generator.rs` | 新增 `is_raw_convertible_file_group` 函数（`pub(crate)`）+ 4 个矩阵测试。Mirror Java `MergeTreeSplitGenerator.java:69-81@e8938f347` rawConvertible + `withoutDeleteRow:151-154`。被 `table_scan.rs::plan_snapshot` 与 `table_read.rs::read_pk` 共同消费 |
| 评审修复 | `crates/paimon/src/table/table_scan.rs` | **P0-1 planner**：`plan_snapshot` 不再因 `deletion_vectors_enabled=true` 无条件关闭 PK key-overlap grouping；按 Java rawConvertible + `(deletion_vectors_enabled || is_first_row || one_level)` 三选一才走 `split_for_batch`，否则走 `interval_partition + pack_sections`。**P1-2 fail-fast**：`plan_manifest_entries` / `plan_snapshot` 用 `?` 而非 `.ok()` 处理 `merge_engine()`。新增 `test_plan_manifest_entries_invalid_merge_engine_errors` |
| 评审修复 | `crates/paimon/src/table/table_read.rs` | **P0-2 reader**：`read_pk` 不再仅以 `level == 0` 判断 needs_merge，改为 `is_raw_convertible_file_group + has_key_overlap` 统一判定。新增 `#[cfg(test)] mod tests`（5 个 case：overlap-L1 / `delete_row_count > 0` / 干净 L1 / 任一 L0 / 无 PK comparator）作 release-mode 安全网 |
| 评审修复 | `crates/paimon/src/table/kv_file_reader.rs` | **P1-1 PU/VPU+DV**：删除 `:281-293` 的 `Error::Unsupported` reject；新增 `test_kv_reader_partial_update_with_deletion_vector`（PU+DV e2e）+ `test_kv_reader_partial_update_dispatch_no_longer_rejects_dv`（PU+DV smoke）+ `test_kv_reader_versioned_partial_update_dispatch_no_longer_rejects_dv`（VPU+DV smoke） |
| 评审修复 | `crates/paimon/src/table/read_builder.rs` | 反向更新 `test_direct_table_read_rejects_partial_update_with_deletion_vectors` → `test_direct_table_read_partial_update_with_dv_no_longer_rejected`，断言 read 路径不再以 `Unsupported` 短路 |
| 评审修复 | `crates/paimon/src/table/stats_filter.rs` | **P1-4 C4 触发场景**：新增 `test_should_apply_value_stats_overlapping_l0_pk_dedup_skips_both_files`，含 L1+ 性能守护回归断言 |
| 评审修复 | `crates/paimon/src/deletion_vector/core.rs` | **P1-3 RLE / boundary**：新增 `test_read_deletion_vector_bitmap64_run_length_encoded`（`RoaringTreemap::optimize()` 等价 Java `runLengthEncode`）+ `test_read_deletion_vector_bitmap64_cross_32bit_boundary` |
| 评审修复 | 新 `crates/paimon/tests/fixtures/deletion_vector/README.md` | 记录 Java fork commit `e8938f347`、目标 fixture 列表、当前 Rust 等价测试覆盖范围、Java fixture follow-up 生成步骤 |



<!-- SECTION-OUT-OF-SCOPE -->

## 不在本计划范围内

- **DV 写路径**：`BucketedDvMaintainer` 等价物 / compaction 时生产 DV / writer 配 DV 启用。capabilities 文档未列写侧 gap，且 `crates/paimon/src/table/kv_file_writer.rs` 等处对 DV+PartialUpdate 主动 reject —— 写路径调通后续单立 plan。本方案默认 **paimon-java 写、paimon-rust 读** 的混合栈场景。
- **`changelog-producer=lookup` 全链路**：Stage 4b 占位但建议 follow-up，仅当确有流式 changelog 需求时启动。
- **paimon-rust 写出的 DV 给 paimon-java 读**：DV 写路径不在 scope，依赖前者完成。
- **DV index file 的 row-tracking 元数据**：当前 `paimon-rust` 用 `DeletionVectorMeta` 已能消费 Java 写出的 index manifest entry，本方案不再扩展 index manifest schema。

<!-- SECTION-VERIFICATION -->

## Verification（每阶段独立可验证）

| 阶段 | 验证 |
|---|---|
| 1 | 单测：4 行 parquet + DV bitmap{1,3} → 输出 row 0 / row 2（smoke）；多 row-group parquet（≥2 个 row group / 共 6 行）+ DV bitmap{1,4} → 输出绝对 row id `0,2,3,5`（**absolute position invariant，必跑**）；集成：Java fixture（Kuaishou fork @`e8938f347` 提前生成）通路验证 PK + DV 表 read |
| 2 | 代码生成 fixture round-trip（替代 fixture 路线）：32-bit / 64-bit DV 字节用 `RoaringBitmap::serialize_into` / `RoaringTreemap::serialize_into` 拼外层框 → `read_from_bytes` → 还原 bitmap 集合一致；fixture 覆盖空 bitmap / 小 row id / 跨 32-bit 边界（> 2^32）/ 多高位 container；magic dispatch 单测覆盖 BE 32-bit / LE 64-bit / 无效 magic 三条 case；`DeletionFile.length` 故意设错的负面 case；KV reader e2e 单测验证 64-bit DV 完整通路（Java fixture 留作 follow-up） |
| 3 | **read-mode 组**：`should_skip_level_zero_for_scan` 8 行矩阵（含 scan_all_files 短路 / 非 PK / DV-PERFORMANCE / DV-FRESHNESS / 非 DV Dedup / 非 DV PartialUpdate / FirstRow）；`deletion_vectors_read_mode` getter 4 case；FRESHNESS / PERFORMANCE plan 输出对比 / **C4 组**（独立验证，PR 描述要求 reviewer 分别审）：`should_apply_value_stats_to_entry` 7 行矩阵（含 L1+ 性能守护 + C4 修复 + FRESHNESS L0 = false + safe-default）；端到端 entry-stream 回归留作 follow-up；最小复现 fixture 见风险 #3 |
| 4a | **等价性测试（必跑）**：mixed RowKind L1+ 文件 + DV bitmap，sort-merge (`KeyValueFileReader`) vs raw-read 短路 (`DataFileReader.with_drop_deletes(true)`) 输出 byte-for-byte 一致（`test_pk_raw_drop_deletes_equivalent_to_sort_merge`）；C5 反向：默认 `drop_deletes=false` 时 DELETE / UPDATE_BEFORE 行原样保留（`test_data_file_reader_default_keeps_delete_rows`）；现有 4 个 Stage 1/2 DV 测试更新 `vks` 参数（None 等价于全 INSERT）后仍全过；dispatch 层 overlap contract 由 `debug_assert!` + Stage 1 已有 `split_requires_merge` 测试间接守护，端到端 dispatch + benchmark 留作 follow-up |
| 4b | （**follow-up，不阻塞 Stage 1-4a 主体 merge**）移植 Java `LookupMergeFunctionTest` 单测；E2E 与 Java 行级一致（依赖能跑 Java fixture，单独立计划） |

每阶段 PR merge 前要确保：
- `cargo test -p paimon --release --lib` 全过
- `cargo test -p paimon-datafusion --release` 全过
- 涉及 schema / option 改动时，`cargo test -p paimon --release --lib spec::core_options` 单独跑过

<!-- SECTION-EFFORT -->

## 工作量估计

| 阶段 | 估时 | 备注 |
|---|---|---|
| Stage 1 | 1 天 | KV reader DV wiring + 单测 + E2E 集成测 |
| Stage 2 | 半天 | Bitmap64 dispatch + 黄金字节测试 |
| Stage 3 | 半天 | read-mode + L0 stats fix + C4 回归测试 |
| Stage 4a | 1-2 天 | raw-read 短路 + C5 RowKind 后过滤 + benchmark |
| **本方案主体（Stage 1-3 + 4a）** | **3-4 工作日** | |
| Stage 4b | 1-2 周 | 单独 follow-up，建议视流式需求启动 |

<!-- SECTION-CROSS-REFS -->

## 与已有内部文档的交叉索引

| capabilities 文档条目 | 严重程度 | 本方案对应 | 关系 |
|---|---|---|---|
| C2 (KV reader 拒 DV split) | C-HIGH | Stage 1 + 评审修复阶段 | **完全闭合**：Stage 1 接通 KV reader DV pre-filter（Deduplicate）；评审修复阶段删除 PU/VPU+DV reject，对齐 Java engine-agnostic DV 应用 |
| C4 (L0 value-stats pruning 错误) | C-HIGH | Stage 3 | **同步修复**（Java 默认行为） |
| C5 (raw 路径不剥 DELETE) | C-HIGH | Stage 4a | **顺手修复**（Stage 4a 让 raw 路径开始服务更多 PK 用例，不修 C5 会暴露幽灵 DELETE 行） |
| C6 (Bitmap64 不识别) | C-HIGH | Stage 2 | **直接修复** |
| C7 (`deletion-vectors.read-mode` 忽略) | C-HIGH | Stage 3 | **直接修复** |
| F9 (LookupMerge L0+DV 快路径) | F | Stage 4a / 4b | **拆分** —— 4a 拿批读收益，4b 是流式 changelog（建议 follow-up） |

**与 `versioned-partial-update-impl-plan.md` 的关系**：VPU validation 已检查 ignore-mode → needLookup（`Schema::validate_versioned_partial_update`），DV 是 lookup capability 之一。**本方案 Stage 1 落地后**，VPU 表 `versioned-partial-update.ignore-mode.enabled=true` 配合 `deletion-vectors.enabled=true` 才能真正读取（之前会被 KV reader 直接 reject）。本方案 Stage 3 引入的 `deletion-vectors.read-mode` 也会让 VPU IGNORE-mode 文件在 PERFORMANCE / FRESHNESS 下行为可控。

**与 `pk-read-issues.md` 的关系**：本方案 Stage 1 关闭 issue 中 "DV 表无法读" 项；Stage 3 关闭 "L0 ghost 旧值" 项。

<!-- SECTION-REFERENCES -->

## 参考

### Java 关键源文件（all anchors @`e8938f347`）

- `paimon-core/.../io/KeyValueFileReaderFactory.java:173-176@e8938f347` —— `ApplyDeletionVectorReader` wrap 入口
- `paimon-core/.../deletionvectors/ApplyDeletionFileRecordIterator.java:53-74@e8938f347` —— `returnedPosition()` 透传 + `next()` 行级 `isDeleted` 循环
- `paimon-core/.../deletionvectors/DeletionVector.java:97-145@e8938f347` —— magic dispatch + 32 / 64 layout 主 anchor（read 端校验 `DeletionFile.length` 的两种公式）
- `paimon-core/.../deletionvectors/BitmapDeletionVector.java:36@e8938f347`（MAGIC）+ `:87-99@e8938f347`（serializeTo）—— 32-bit impl
- `paimon-core/.../deletionvectors/Bitmap64DeletionVector.java:40@e8938f347`（MAGIC）+ `:41-44@e8938f347`（length / crc / magic 常量）+ `:93-106@e8938f347`（serializeTo）—— 64-bit impl
- `paimon-core/.../deletionvectors/DeletionVectorsIndexFile.java:73-127@e8938f347` —— index file 读取（`readAllDeletionVectors` / `readDeletionVector`）
- `paimon-api/.../CoreOptions.java:1880-1889@e8938f347` —— `DV_READ_MODE` option 声明（**Kuaishou fork 私有**）；`DvReadMode` enum 见同文件 `:4144-4155@e8938f347`
- `paimon-core/.../KeyValueFileStoreScan.java:154-162@e8938f347` —— L0 + value-stats 安全性（FRESHNESS-only，**Kuaishou fork 私有**）
- `paimon-core/.../table/source/DataTableBatchScan.java:67-83@e8938f347` —— PERFORMANCE / FRESHNESS plan-time 分派（**Kuaishou fork 私有**）
- `paimon-core/.../operation/MergeFileSplitRead.java:170-178@e8938f347` —— LookupMerge 选择 + DV apply 入口；`forceKeepDelete()` setter 在 `:177-178@e8938f347`，默认值见字段初始化 `:94@e8938f347`
- `paimon-core/.../mergetree/DropDeleteReader.java`（按需查阅）—— PK merge reader 输出之后的 DELETE 行剥离 contract

### Rust 现状关键 anchor

- `crates/paimon/src/deletion_vector/core.rs:18-147` —— 现有 32-bit DV 实现 + magic
- `crates/paimon/src/deletion_vector/factory.rs:39-76` —— DeletionVectorFactory 构造 + 加载
- `crates/paimon/src/table/data_file_reader.rs:96 / 211 / 329 / 344 / 353` —— raw 路径 DV wiring 模板（KV 路径要 mirror）
- `crates/paimon/src/table/kv_file_reader.rs:271-275` —— **本方案 Stage 1 删除点**
- `crates/paimon/src/table/table_scan.rs:175 / 287` —— **本方案 Stage 3 改动点**
- `crates/paimon/src/table/table_read.rs:82-97` —— **本方案 Stage 4a 改动点**
- `crates/paimon/src/spec/core_options.rs:20 / 242` —— `deletion-vectors.enabled` 已就绪
- `crates/paimon/src/spec/core_options.rs:59 / 381` —— `force-lookup` 已就绪（Stage 4b 用）
