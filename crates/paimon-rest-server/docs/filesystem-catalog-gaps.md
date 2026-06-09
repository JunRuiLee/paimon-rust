# FileSystemCatalog 能力分析与改进建议

> 适用版本：`crates/paimon/src/catalog/filesystem.rs`（本文档撰写时）。
> 结论：`FileSystemCatalog` 的 database / table CRUD、schema 版本管理、路径安全校验、级联删除均已完整，**足以支撑 `paimon-rest-server` 跑通元数据 CRUD 与 write+commit+read 全链路**，本次不改动其代码。以下短板作为后续独立 issue 跟进。

## 现状（已完整实现）

- **Database**：`create` / `list` / `get` / `drop`（含 `cascade` 级联删除）。
- **Table**：`create` / `list` / `get` / `drop` / `rename`。
- **Schema 版本管理**：`schema/schema-{id}` 文件布局，`get_table` 加载最新版本。
- **路径安全**：拒绝 `..` 等路径穿越（`Identifier::validate`）。

布局：
```
warehouse/
  {db}.db/
    {table}/
      schema/schema-{id}      # TableSchema JSON
      snapshot/snapshot-{id}  # 由 commit 写入（SnapshotManager）
```

## 短板与改进建议

### 1. `alter_table` 列级变更 —— ✅ 已解决

- **原现状**：`TableSchema::apply_changes` 只处理 `SetOption` / `RemoveOption`，列级变更返回 `Error::Unsupported`；`RESTCatalog::alter_table` 整体 `Unsupported`。
- **现状（已实现）**：`apply_changes`（`spec/schema.rs`）已支持全部现有 `SchemaChange` 变体（AddColumn/RenameColumn/DropColumn/UpdateColumnType/UpdateColumnPosition/UpdateColumnNullability/UpdateColumnComment/UpdateComment），对照 Java `SchemaManager.generateTableSchema` 做字段 id 分配（`highest_field_id + 1`）与 move 语义，并用 `ColumnAlreadyExist`/`ColumnNotExist` 报错；REST 端打通 `AlterTableRequest`/`RESTApi::alter_table`/`RESTCatalog::alter_table` 与 server 的 `POST .../tables/{table}` 路由。`SchemaChange` 线格式已对齐 Java（internally-tagged `action`）。
- **遗留**：仅支持**顶层列**（`field_names` 路径长度 1）；嵌套 struct 字段、`UpdateColumnType` 的 cast 兼容校验、Java 的 `dropPrimaryKey`/`updateColumnDefaultValue` 变体暂未实现。

### 2. database 属性未持久化

- **现状**：`create_database` 接受 `properties` 但不落盘（无 database 元数据文件），`get_database` 返回的 options 为空。
- **影响**：通过 REST `get_database` 拿不回创建时设置的属性；server 侧 `GetDatabaseResponse.options` 只能是空。
- **建议**：在 `{db}.db/` 下写一个 database 元数据文件（如 Java 的 `database properties`），`create` 时写、`get` 时读、`alter_database` 时改。
- **优先级**：中低。多数测试不依赖 database 属性回读。

### 3. `create_table` 无并发锁

- **现状**：源码注释 `todo: consider with lock`；建表是「检查存在 → 写 schema」两步，非原子。
- **影响**：并发建同名表可能都通过存在性检查后各自写入，产生竞态。
- **建议**：引入基于文件系统的原子标记（如先 `mkdir` 表目录失败即视为已存在），或显式 catalog 锁。
- **优先级**：低（本地文件系统测试场景并发度低）。

### 4. `list_partitions` 默认实现忽略分页

- **现状**：默认实现扫描 manifest 聚合分区，`list_partitions_paged` 忽略 `max_results` / `page_token`，一次性返回全量。
- **影响**：大分区表无法分页；REST 分页语义无法端到端验证。
- **建议**：在默认实现中对聚合结果按 `page_token` 切片并返回 `next_page_token`。
- **优先级**：低。

## 与本 server 的关系

`paimon-rest-server` 当前直接复用 `FileSystemCatalog`，对上述短板的处理方式：

- `alter_database` 端点为 no-op（仅校验 database 存在），与短板 2 一致。
- 未暴露 `alter_table` 端点（客户端 `Catalog` trait 走 REST 时本就 `Unsupported`），与短板 1 一致。
- 分区端点未实现；如需验证分区分页，应先补齐短板 4。
