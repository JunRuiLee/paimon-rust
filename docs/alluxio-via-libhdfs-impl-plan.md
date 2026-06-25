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

# paimon-rust 通过 opendal JNI HDFS 接入 Alluxio —— 实施方案

## 总览

paimon-rust 当前的 HDFS 访问走 **`opendal/services-hdfs-native`**（纯 Rust 实现 HDFS RPC 协议）：`crates/paimon/Cargo.toml:83` 启用 `storage-hdfs = ["opendal/services-hdfs-native"]`，`crates/paimon/src/io/storage_hdfs.rs:20` 使用 `HdfsNativeConfig`，工作区根 `Cargo.toml:62` 还用 `[patch.crates-io]` 把 `hdfs-native` 替换成 Kuaishou 内部 fork。这条路径不经 JVM、不经 Hadoop `FileSystem` 抽象，因此**无法插入 alluxio-client.jar 来路由到 Alluxio 集群** —— Alluxio master 用自有 gRPC 协议，不是 HDFS NameNode。

厂内 Alluxio 没有 Rust SDK，而 bleem BE（参见 `bleem/fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java:431,470,478` 与 `HiveKwaiUtils.convertToAlluxioPath`）通过 Hadoop `FileSystem.get(uri, conf)` + alluxio-client.jar 走通，依赖的是 alluxio-client 实现了 Hadoop FS SPI（`fs.alluxio.impl=alluxio.hadoop.FileSystem`）。本方案让 paimon-rust 复用同样的机制：**新增 `opendal/services-hdfs`（libhdfs / JNI）后端**，通过 `alluxio-client.jar` 把 `alluxio://` 路径下沉到 Alluxio 集群。

两层 gating，与 bleem 的策略对齐但粒度更细：

```text
alluxio_effective = session_use_alluxio  AND  table_options[alluxio.cache-enabled]
```

- **`session_use_alluxio`**（调用级，默认 `false`）：caller 构造 `FileIO` 时显式传入。即使表声明缓存可用，session 没开就走原 scheme —— 保留"读最新源数据"的逃生通道。
- **`alluxio.cache-enabled`**（表级，默认 `false`）：写在表 options 里，标识当前表所在路径已被 Alluxio 集群缓存覆盖。
- 两者都为 `true` 时，`hdfs://` / `viewfs://` 路径替换为 `alluxio://` 走 JNI 后端；否则保持原 scheme，走 `services-hdfs-native`。

**Catalog 元数据**（`schema/`、snapshot、manifest 读写）**永远走 native**，alluxio 切换只影响 Table 数据读取 —— 与 bleem `PaimonScanNode` 只在 RawFile/DataSplit/DeletionFile 的 path 上替换 scheme 的行为一致。

### 范围

- **In**：opendal JNI HDFS 后端接入、`alluxio://` scheme 改写、Rust 层 `FileIOBuilder::with_alluxio` + `FileIO::with_alluxio` + `Table::with_alluxio` API、C 绑定 `paimon_catalog_get_table` / `paimon_table_open_path` 加 `use_alluxio` 参数、`CoreOptions::alluxio_cache_enabled()` getter、单元测试。
- **Out**：
  - JVM/libhdfs/alluxio-client.jar 的部署交付（部署侧自带 `JAVA_HOME` + classpath + `HADOOP_CONF_DIR`，本方案在模块注释里给出要求清单）。
  - 非 HDFS-族存储（S3/OSS/...）的 Alluxio 接入。
  - **写路径 Alluxio 行为**：`Table::with_alluxio(true)` 后的 Table 仅用于读路径。当前 C 绑定（`bindings/c/src/catalog.rs`）只暴露 `paimon_read_builder_*` / `paimon_table_read_*`，未暴露写 API；Rust 层 `new_write_builder` 等 API 不在本方案保证范围。调用方若在 alluxio Table 上触发写，结果未定义。
  - **`paimon_plan_from_split_bytes` 路径**：plan 不持有 FileIO，alluxio 状态随 Table 走。**调用方约定**：worker 拿到 split bytes 后必须用 `paimon_catalog_get_table` 或 `paimon_table_open_path`（带正确 `use_alluxio` 参数）独立构造 Table，再用该 Table 配合 plan 调 `paimon_table_read_to_arrow`。本方案不在 split bytes 中携带 alluxio 状态。

### ABI 变更声明

`paimon_catalog_get_table` / `paimon_table_open_path` 新增 `use_alluxio: bool` 末位参数，是**破坏性 ABI 变更**。当前阶段（项目早期、bindings/c 主要服务于 bleem/paimon-cpp 内部调用方）选择直接改签名而非引入 `_ext` / `_with_options` 变体；PR 描述中明确提示调用方同步更新。

---

## 设计

### 两类 FileIO 的语义分工

| FileIO | 用途 | `use_alluxio` |
|---|---|---|
| **catalog 级**（`FileSystemCatalog.file_io`、`RESTCatalog` 内的 catalog FileIO） | 读写 schema 文件、snapshot、manifest、其他 catalog 元数据 | **永远 false**（始终 native） |
| **table 数据级**（`Table.file_io`，用于 raw file / DataSplit / deletion vector 读取） | 数据文件 IO | `session_use_alluxio AND core_options.alluxio_cache_enabled()` |
| **table 元数据级**（`Table.schema_manager` 内持有的 FileIO） | 读取历史 schema / data evolution schema | **永远 native**（详见 Stage 4） |

Catalog 不感知 alluxio 开关。"session_use_alluxio"是 caller（C++ 端，下到 C 绑定）显式传入的运行时 flag，C 绑定层在 `paimon_catalog_get_table` / `paimon_table_open_path` 拿到 Table 后立刻调 `Table::with_alluxio(session_use_alluxio)` 完成 effective 计算与**仅 `Table.file_io`** 的替换（`Table.schema_manager` 保持原 native FileIO 不动）。

### Storage 层后端选择

- 现 `Storage::Hdfs { config: HdfsNativeConfig, op }` 变体拆为：
  - `#[cfg(feature = "storage-hdfs")] HdfsNative { config: HdfsNativeConfig, op }` —— 现有行为。
  - `#[cfg(feature = "storage-hdfs-jni")] HdfsJni { config: opendal::services::HdfsConfig, op }` —— 新增 JNI 路径。
- `Storage::build` 根据 `FileIOBuilder::use_alluxio` 选后端：

  | `use_alluxio` | scheme | 路由 | 备注 |
  |---|---|---|---|
  | `false` | `hdfs` / `viewfs` | `HdfsNative` | 保持现状 |
  | `false` | `alluxio` | **报错** | `ConfigInvalid("alluxio scheme requires use_alluxio=true")` |
  | `true` | `hdfs` / `viewfs` / `alluxio` | `HdfsJni` | path 在 `create()` 时被改写为 `alluxio://` |
  | `true` | 其他 scheme | **报错** | `ConfigInvalid("alluxio mode only supports hdfs/viewfs/alluxio scheme, got: <scheme>")` |
  | `true` | 任何 scheme + 未编译 `storage-hdfs-jni` | **报错** | `ConfigInvalid("binary not built with storage-hdfs-jni feature")` |

- `Storage::create(path)` 的 `HdfsJni` 分支：调 `hdfs_to_alluxio_path(path)` 把 `hdfs://nn:8020/p` / `viewfs://cluster/p` 改写为 `alluxio://nn:8020/p` / `alluxio://cluster/p`（`alluxio://` 原样返回），再交给 libhdfs operator。改写逻辑与 bleem `HiveKwaiUtils.convertToAlluxioPath`（`bleem/fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HiveKwaiUtils.java:872`）对齐。

### `FileIO::with_alluxio` 重建语义

`FileIO` 当前只持有 `Arc<Storage>`，原始 scheme 与 props 在 `FileIOBuilder::into_parts()` 时被 move 进 `Storage::build`，无法从 `FileIO` 反推。`with_alluxio` 要做"切换 flag 重建 Storage"必须先扩展 `FileIO` 的状态：

```rust
#[derive(Clone, Debug)]
pub struct FileIO {
    storage: Arc<Storage>,
    // 新增：保留构造时的原始配置，供 with_alluxio 重建用。
    // Arc 共享避免 clone 大 HashMap，scheme/use_alluxio 是小字段直接持有。
    scheme: String,
    props: Arc<HashMap<String, String>>,
    use_alluxio: bool,
}

impl FileIO {
    pub fn with_alluxio(&self, enabled: bool) -> Result<Self> {
        if enabled == self.use_alluxio {
            return Ok(self.clone());
        }
        // 用保存的 scheme + props 重新走 builder。所有调用方传入的 with_props
        // 都原样保留，不依赖从 Storage 反推。
        FileIOBuilder {
            scheme_str: Some(self.scheme.clone()),
            props: (*self.props).clone(),
            use_alluxio: enabled,
        }
        .build()
    }
}
```

`FileIOBuilder::build()` 把 `scheme_str` / `props` / `use_alluxio` 在构造 `Storage` 之前 clone 一份保存进 `FileIO`。`Storage` 内 operator 都是 lazy（参见 `crates/paimon/src/io/storage.rs:243-258` 的 HDFS 分支已有 `Mutex<Option<Operator>>` 模式），重建本身只是配置切换，无 IO 成本。

测试覆盖：带 `hdfs.name-node`、`hdfs.enable-append`、对象存储无关 props（混入但不影响 hdfs 后端）三种场景，`with_alluxio(true)` 后再 `with_alluxio(false)` 不丢任何 props。

### 可观测性

为线上排障，`FileIO::with_alluxio` 与 `Table::with_alluxio` 在切换路径上打 `tracing::debug!`，输出：原始 scheme、最终 scheme、`session_use_alluxio`、`alluxio.cache-enabled`、`effective`。debug 级别默认不开，需要时通过 `RUST_LOG=paimon::io=debug` 打开。同时 `FileIO` 暴露 `pub fn use_alluxio(&self) -> bool` 供测试与 caller 自查。

### JNI 部署要求

启用 `storage-hdfs-jni` feature 的二进制需要：

- **编译期**：`JAVA_HOME` 指向 JDK，opendal `services-hdfs` → `hdfs-sys` 在 build 时通过 `libjvm.so/dylib` 链接。
- **运行时**：
  - 启动进程能找到 `libjvm`（`LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` 或 `java.library.path`）。
  - `CLASSPATH` 包含 `alluxio-client-*.jar` 与 hadoop-common 全家桶。
  - `HADOOP_CONF_DIR` 或 `HADOOP_HOME/etc/hadoop` 下的 `core-site.xml` 注册 alluxio FS：
    - `fs.alluxio.impl=alluxio.hadoop.FileSystem`
    - `fs.AbstractFileSystem.alluxio.impl=alluxio.hadoop.AlluxioFileSystem`
  - 或依赖 alluxio-client 自带的 `ServiceLoader` SPI 自动注册（>= alluxio 2.x 默认带）。
- **JVM 内存**：libhdfs 进程内只能有一个 JVM 实例，多个 `HdfsJni` Storage 共享同一个 JVM。Heap / GC 通过环境变量 `LIBHDFS_OPTS` 或 hadoop 的 `HADOOP_OPTS` 调。

部署要求在 `crates/paimon/src/io/storage_hdfs_jni.rs` 模块顶部注释里完整给出。

---

## 改动清单

### Stage 1 — Cargo features

**`crates/paimon/Cargo.toml`**
- 保留 `storage-hdfs = ["opendal/services-hdfs-native"]`（native，默认开）。
- 新增 `storage-hdfs-jni = ["opendal/services-hdfs"]`（JNI / libhdfs）。
- **`storage-all` 不包含 `storage-hdfs-jni`**：避免给常规"打开所有存储后端"的 CI/开发环境加上 `JAVA_HOME` + libhdfs 的强系统依赖。需要 JNI 的部署显式 `--features storage-hdfs-jni` 启用。
- 两者可同时启用：`use_alluxio=false` 走 native，`use_alluxio=true` 走 JNI。

**`bindings/c/Cargo.toml`**
- 透传 `storage-hdfs-jni` feature。

### Stage 2 — 表参数

**`crates/paimon/src/spec/core_options.rs`**（与 `MERGE_ENGINE_OPTION` 等同文件）
- 新增 `pub const ALLUXIO_CACHE_ENABLED: &str = "alluxio.cache-enabled"`。
- `impl<'a> CoreOptions<'a>` 加 `pub fn alluxio_cache_enabled(&self) -> bool`，缺省 `false`，识别 `"true"` (case-insensitive)。
- 单元测试覆盖 true / false / 缺失 / 大小写。

### Stage 3 — FileIO + Storage 改造

**`crates/paimon/src/io/file_io.rs`**
- `FileIOBuilder` 新增字段 `use_alluxio: bool`（默认 `false`），公共方法 `with_alluxio(bool)`。
- `FileIOBuilder::into_parts` 一起返回 `use_alluxio`（签名变更，调用方仅 `Storage::build` 一处）。
- `FileIO` 结构扩展：新增 `scheme: String`、`props: Arc<HashMap<String,String>>`、`use_alluxio: bool` 三个字段（见 Design § FileIO::with_alluxio 重建语义）。
- `FileIOBuilder::build()` 在构造 `Storage` 前 clone 一份 scheme/props/use_alluxio 保存进 `FileIO`。
- `FileIO` 新增方法：
  ```rust
  pub fn with_alluxio(&self, enabled: bool) -> Result<Self>
  pub fn use_alluxio(&self) -> bool
  ```
  前者用保存的原始 scheme/props 重新走 `FileIOBuilder::build()` 切换 alluxio flag；后者供 caller / 测试自查。切换路径打 `tracing::debug!`。

**`crates/paimon/src/io/storage.rs`**
- `Storage` enum 拆分 `Hdfs` 变体：
  - `#[cfg(feature = "storage-hdfs")] HdfsNative { config: Box<HdfsNativeConfig>, op: Mutex<Option<Operator>> }`
  - `#[cfg(feature = "storage-hdfs-jni")] HdfsJni { config: Box<opendal::services::HdfsConfig>, op: Mutex<Option<Operator>> }`
- `Storage::build`：按 design 表实现 4 种报错 + 2 种正常路由。`use_alluxio=true` 时如果 scheme=hdfs/viewfs 也接受（在 `create()` 时改写）。
- `Storage::create(path)`：`HdfsJni` 分支调 `hdfs_to_alluxio_path(path)` 改写后再交 libhdfs operator；相对路径由 **扩展后的** `hdfs_family_relative_path(path, allowed_schemes)` 提取，接受 `hdfs/viewfs/alluxio` 三种 scheme（详见下）。
- `parse_scheme` 加 `"alluxio" => Ok(Scheme::Hdfs)`（opendal 的 `Scheme::Hdfs` 对应 JNI）。

**`crates/paimon/src/io/storage_hdfs.rs`**（保留文件名，不重命名）
- 现有 native 实现保持不动，避免大块 diff / 影响 blame。`storage_hdfs_jni.rs` 作为对称的新文件加入。
- `hdfs_relative_path` 改名为 `hdfs_family_relative_path(path, allowed_schemes: &[&str])`，参数化支持的 scheme 集合；HDFS_SCHEME_PREFIXES 常量变成函数入参。
- 错误信息从 `"should start with hdfs:// or viewfs://"` 改为 `"should start with one of: <allowed_schemes>"`，调用方传入实际允许列表。
- native 后端调用方传 `["hdfs://", "viewfs://"]`；JNI 后端调用方传 `["hdfs://", "viewfs://", "alluxio://"]`（虽然进 `create()` 时已改写为 alluxio://，但保留 hdfs/viewfs 兼容以防有调用直接传 native 路径）。

**`crates/paimon/src/io/storage_hdfs_jni.rs`（新文件）**
- 模块顶部注释完整列出部署要求（见 Design § JNI 部署要求）。
- `hdfs_jni_config_parse(props: HashMap<String,String>) -> Result<HdfsConfig>`：从 paimon options 抽取 hadoop conf 相关 key，与 native 共享 `hdfs.name-node` 等约定。
- `hdfs_jni_config_build(cfg: &HdfsConfig, path: &str) -> Result<Operator>`：构造 libhdfs operator，root=`/`。
- `hdfs_to_alluxio_path(path: &str) -> Result<String>`：scheme 改写：
  - `hdfs://nn:8020/p` → `alluxio://nn:8020/p`
  - `viewfs://cluster/p` → `alluxio://cluster/p`
  - `alluxio://x/p` → `alluxio://x/p`（原样）
  - 其他 → `ConfigInvalid`
- 单元测试：3 种 scheme 的改写、相对路径提取、配置构建。

### Stage 4 — Table 切换入口

**`crates/paimon/src/table/mod.rs`**
- 新增方法：
  ```rust
  impl Table {
      pub fn with_alluxio(self, session_use_alluxio: bool) -> Result<Self>;
  }
  ```
  - 内部：`let effective = session_use_alluxio && CoreOptions::new(self.schema.options()).alluxio_cache_enabled();`
  - `effective=true` 时**只**替换 `self.file_io = self.file_io.with_alluxio(true)?`，**`self.schema_manager` 保持不动**。
  - `effective=false` 原样返回。
  - 切换路径打 `tracing::debug!`，输出 session/table/effective/scheme 四个字段。
- **设计要求**：`Table.schema_manager` 在 `Table::new` 时由 native catalog FileIO clone 得到（`crates/paimon/src/table/mod.rs:132`），`with_alluxio` 不替换它 —— 这是 "catalog/schema 元数据永远 native"语义的一部分。后续 `copy_with_options` / `copy_with_time_travel` 已经把 `schema_manager` 一起 clone（`crates/paimon/src/table/mod.rs:220`），不会丢失这个不变量。
- 单元测试：
  - `(session × table)` 4 种组合下 `Table.file_io.use_alluxio()` 是否切换。
  - `Table::with_alluxio(true)` 后 `schema_manager` 持有的 FileIO `use_alluxio() == false`（通过给 SchemaManager 暴露内部 file_io 引用或 test-only getter 实现）。
  - `copy_with_options` 后 `file_io` 仍是 alluxio、`schema_manager` 仍是 native。

### Stage 5 — C 绑定

**`bindings/c/src/catalog.rs`**
- `paimon_catalog_get_table(catalog, identifier, use_alluxio: bool)` —— 加 bool 参数；拿到 table 后调 `table.with_alluxio(use_alluxio)?` 替换。
- `paimon_table_open_path(table_path, options, options_len, use_alluxio: bool)` —— 同样在构造完 Table 后切换。
- `paimon_catalog_create` **不动**（catalog 元数据始终走 native）。
- `paimon_plan_from_split_bytes` **不动**（worker 拿 Plan 时 Table 已经由上面两个入口构造，alluxio 状态随 Table 走）。

**`bindings/c/include/paimon.h`**
- 同步两个函数签名（`bool` 通过 `<stdbool.h>`）。

**`bindings/c/build.rs`**
- 如果 header 由 cbindgen 生成，确认 bool 参数导出正确；否则手改 header。

### Stage 6 — Catalog 内部

不动。Catalog trait、`FileSystemCatalog`、`RESTCatalog` 都保持现状，catalog FileIO 永远 `use_alluxio=false`。

---

## 测试

| 范围 | 文件 | 内容 |
|---|---|---|
| 表参数 getter | `crates/paimon/src/spec/core_options.rs` | true / false / 缺失 / 大小写 |
| Storage build 校验 | `crates/paimon/src/io/storage.rs` | 4 种报错路径 + 2 种正常路由 |
| scheme 改写 | `crates/paimon/src/io/storage_hdfs_jni.rs` | hdfs/viewfs/alluxio 三种 scheme |
| `hdfs_family_relative_path` | `crates/paimon/src/io/storage_hdfs.rs` | 三种 scheme + 错误消息含 allowed_schemes |
| `FileIO::with_alluxio` 配置不丢 | `crates/paimon/src/io/file_io.rs` | 带 `hdfs.name-node` / `hdfs.enable-append` / 无关 props 后 `with_alluxio(true).with_alluxio(false)` 配置无损 |
| Table 切换 | `crates/paimon/src/table/mod.rs` | `(session × table)` 4 种组合；`schema_manager` 保持 native；`copy_with_options` 保持 alluxio 状态 |
| C 绑定 round-trip | `bindings/c/src/catalog.rs` 测试模块 | `use_alluxio=true` + 表声明 `alluxio.cache-enabled` 后 Table 内 file_io 与 schema_manager 的 alluxio 状态 |
| Alluxio smoke test（gated） | 新增 `tests/alluxio_smoke.rs` 或现有 integration_tests | 环境变量 `PAIMON_TEST_ALLUXIO_URI=alluxio://...` 启用；`FileIO.new_input(uri).exists()` 跑通；对 classpath / `fs.alluxio.impl` 缺失场景断言错误消息包含关键提示词 |

JNI feature 在没有 libhdfs 的开发机上跳过编译；CI 启用 `storage-hdfs-jni` 的 job 需要单独配 `JAVA_HOME` + libhdfs。alluxio smoke test 只在配了 `PAIMON_TEST_ALLUXIO_URI` 的 job 跑。

---

## 改动顺序

1. Stage 1 + Stage 2（Cargo features + 表参数 key/getter）—— 独立可合。
2. Stage 3（FileIO/Storage 改造，含单元测试）—— 不依赖 C 绑定。
3. Stage 4（`Table::with_alluxio`）—— 依赖 Stage 2/3。
4. Stage 5（C 绑定 + header）—— 依赖 Stage 3/4。

每步本地 `cargo build` + `cargo build --features storage-hdfs-jni`（如本机有 JDK）+ `cargo test` 通过即可合。

---

## 风险

1. **JVM 启动开销**：进程内首次 `HdfsJni` 触发 libhdfs `JNI_CreateJavaVM`，秒级延迟。`Mutex<Option<Operator>>` 已做 lazy init，但首次 query 的 P99 会被拉长。**缓解**：部署侧可考虑 warm-up，在进程启动后空跑一次 alluxio path 触发 JVM 初始化。
2. **classpath / hadoop conf 缺失**：alluxio-client.jar 不在 classpath、`core-site.xml` 没注册 `fs.alluxio.impl` 时，libhdfs `hdfsConnect` 会返回 `IOException`，opendal 层翻译成不带上下文的 IO 错误。**缓解**：`storage_hdfs_jni.rs` 模块注释明确给出最小部署 checklist；运行时 IO 错误时在错误消息里附带"check CLASSPATH / HADOOP_CONF_DIR / fs.alluxio.impl"提示。**测试**：smoke test 中对缺 classpath / 缺 `fs.alluxio.impl` 的报错断言这些关键词。
3. **JNI 与纯 Rust 协程**：libhdfs 调用阻塞 JNI 线程，opendal 在内部用 `tokio::task::spawn_blocking` 隔离，但密集 IO 时 blocking pool 可能跑满。**缓解**：监控 tokio blocking pool 占用；必要时通过 opendal 配置调 pool 大小。
4. **`hdfs-sys` 工具链耦合**：编译期依赖系统 libhdfs 头文件。**缓解**：feature `storage-hdfs-jni` 不进默认 features、也不进 `storage-all`，只在显式打开时才链接 libhdfs；本地开发不开此 feature 无影响。
5. **Kuaishou hdfs-native fork patch 与 services-hdfs 共存**：workspace `Cargo.toml:62` 的 `[patch.crates-io] hdfs-native = ...` 只覆盖 hdfs-native crate，不影响 `services-hdfs` 的 `hdfs-sys`。**确认**：两者依赖树独立。
6. **viewfs authority ≠ alluxio master 名称**：方案按 bleem 同款做同名 scheme 替换（`viewfs://cluster/p → alluxio://cluster/p`）。如果某些环境 alluxio master 名称与 viewfs mount table 名不同，会读错集群或连不上。**当前选择**：跟 bleem 行为对齐（厂内环境一致），不做 authority 映射。若后续遇到不一致环境，再扩展 `alluxio.authority` / `alluxio.authority-map` 配置。
7. **ABI 变更影响调用方**：`paimon_catalog_get_table` / `paimon_table_open_path` 加 `use_alluxio` 末位参数是破坏性变更。**缓解**：bindings/c 当前主要服务厂内 bleem / paimon-cpp，PR 描述里同步提示调用方更新；不引入 `_ext` 兼容入口。后续如需对外稳定 ABI，再考虑 options struct 模式。
8. **写路径未定义行为**：`Table::with_alluxio(true)` 后理论上仍可调 `new_write_builder` / commit 等 API。**当前选择**：C 绑定层未暴露写 API，Rust 层调用方自负其责。文档明确"Out of scope"，必要时后续在 `Table::with_alluxio` 内部把写 builder 标记为不可用。