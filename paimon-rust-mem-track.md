# paimon-rust Reader 内存跟踪（query 级 MemTracker）

> **阅读指引**：本节「最新进展与最终方案」是当前实现的权威描述。其后的「背景与动机／方案选型／
> …」保留了完整的探索与演进历史（global allocator hook 方案及其失败原因），供追溯，但**已被本节取代**。

## 最新进展与最终方案（2026-07，权威）

### 结论一句话
放弃 global allocator hook（跨线程失配、GB 级漂移无法解决），改为在**持有大块 arrow 数据 / 触发解压的读路径代码里显式计数** `mem_tag::account(±bytes)`，运行在有 tag 的 scanner 线程。覆盖三处：MOR sort-merge 缓冲、append-only 在途 batch、**parquet 解压 row-group 驻留（按 metadata 估算）**。结果：query 级统计**非负、有界、稳定、能区分 query、反映 MOR ≫ 普通表**。因为统计不再依赖分配线程，runtime 最终**用回 multi-thread**（拿回 IO 并行）。

### 演进链（每一步为何被推翻）
1. **global allocator（`CountingAlloc`）+ thread-local tag**：Rust 每次 alloc/dealloc 归属当前线程 tag，C++ 在 attach 的 scanner 线程读 counter 差值 consume。
   - **失败**：诊断分桶（tagged/untagged）实测证明——大量 IO 内存在 **tokio blocking-pool 线程**（opendal `tokio::fs`、hyper）alloc/free，那些线程**没有 tag**。读 alluxio 时 `untagged_free=173GB`（free 逃逸→counter 偏大 +173G）；读 local 时 `untagged_alloc=33GB`（alloc 逃逸→counter 偏小 −33G）。铁律 `tagged_net + untagged_net ≈ 0` 坐实是纯跨线程失配。
2. **改 current-thread runtime**（`Builder::new_current_thread`，per-scanner-thread）：消除 async worker + work-stealing。
   - **仍失败**：current-thread runtime **仍带 blocking 线程池**（tokio `max_blocking_threads` 默认 512），而 opendal fs 用 `tokio::fs`（内部 `spawn_blocking`）、hyper 也用 blocking 池。IO 分配/释放照样落在无 tag 线程，诊断数值几乎不变。→ 证明「thread-local tag」前提（alloc/free 同线程）被第三方 IO 库从根本上破坏，非配置可救。
3. **停用 global allocator，改读路径显式统计**：只统计我们持有的 arrow 数据（sort_merge buffer + append-only 在途 batch）。MOR 实测到 ~11G（预期 ~30G），缺口 ~19G。
   - **缺口定位**：查证 parquet crate `poll_next`/`try_decode` 是**在调用线程同步解压**（无 spawn/rayon），解压后的 row-group 驻留在 crate 内部 buffer，`get_array_memory_size` 够不到——不是线程问题，是「只统计持有的 RecordBatch」的口径盲区。
4. **补 parquet 解压驻留估算 + runtime 换回 multi-thread（最终）**：见下。既然统计全在 scanner 线程 poll 逻辑里执行、与分配线程解耦，current-thread runtime 不再必要，换回 multi-thread 恢复 IO 并行。

### 最终实现
- **停用** `#[global_allocator]`（`bindings/c/src/lib.rs` 注释掉，`CountingAlloc` 保留备用）。
- `mem_tag::account(delta)` 保留，改由**业务代码显式调用**；C++ 侧管线（`MemTagScope` + `_sync_mem` 读 counter net_bytes + `paimon_mem_counter_*` FFI）**不变**，counter 数据来源从 allocator 变成显式 account。
- **runtime 用回 multi-thread**：`block_on` 走全局 `RUNTIME`（`Runtime::new()`），恢复 IO 并行；current-thread 版保留为 `block_on_current_thread`（dead_code 备用）。统计与 runtime 类型解耦——`account` 在 scanner 线程 poll 读路径时执行，与「IO 在哪个线程分配」无关。
- **通用 RAII** `mem_tag::ScopedBytes`（new 计入 / drop 释放），三处插桩共用。
- **插桩点**：
  | 路径 | 位置 | 结构 | 口径 |
  |---|---|---|---|
  | MOR sort-merge 缓冲 | `crates/paimon/src/table/sort_merge.rs` | `MergeMemAccount`（RAII）：每次 `batch_buffer` 变更后重算 `Σ get_array_memory_size + Σ cursor.rows.size()`，account 差值；`Drop` 归还 | 精确（持有的 arrow batch + key 编码） |
  | append-only 在途 batch | `crates/paimon/src/table/data_file_reader.rs::read` | `ScopedBytes`：在途 batch yield 窗口 account | 精确 |
  | **parquet 解压 row-group 驻留** | `crates/paimon/src/arrow/format/parquet.rs::read_batch_stream` | `ScopedBytes` 绑定到 stream 生命周期（build 后计入、stream drop 释放） | 估算：各 row-group「投影 leaf 列 `uncompressed_size()` 之和」的**最大值**（reader 一次解压一个 row-group，峰值≈最大 row-group 投影列未压缩量） |
- **不双算**：`DataFileReader::read` 是 append-only 专用入口；MOR 走 `kv_file_reader` 直接调 `read_single_file_stream`（绕过 `read`），只在 sort_merge 计。cursor 的 Source batch 与 batch_buffer 同一 Arc，只在 buffer 计一次，额外只加 cursor `rows`。parquet 解压估算与 batch/buffer 是**不同层的量**（crate 内部解压态 vs 我们持有的解码结果），不重叠。
- **MOR 的 N 路并发自然叠加**：每个 split 同时打开 N 个 parquet 流，各带一个 parquet `ScopedBytes` guard，N × per-file peak 相加，对应 N 路并发解压驻留。

### 已知口径与近似
- **parquet 估算用「投影列未压缩字节」**（方案 2，非全列 `total_byte_size`）：宽表只读少数列时不高估——这是选投影列口径的关键，否则会把没解压的列也算进去导致又一次偏大失真。
- 仍是**量级近似**：`uncompressed_size` 是解压前的未压缩原始字节，不含 decoder 的 dictionary 展开/额外拷贝；若 `row_selection`/filter 跳过大量行，会偏高（未按命中行折减）。若实测过冲，下一步可按 row_selection 命中比例折减。
- 覆盖不到的残余（decoder 临时拷贝、opendal 预取 `Bytes`）落入 Doris `UntrackedMemory` 兜底桶，进程级仍可见。

### 实测（重启 BE 加载新 dylib 后）
| 场景 | Query tracker | 评价 |
|---|---|---|
| 普通表（append-only） | ~4G | 符合预期 |
| MOR 表（仅 sort_merge + cursor rows，补 parquet 前） | ~11G（预期 ~30G） | 缺 ~19G ≈ `UntrackedMemory`，定位为 parquet 解压驻留盲区 |
| MOR 表（补 parquet 解压估算后） | 待验证，预期上升接近 ~30G | 关注是否过冲 / 超物理内存 |
| 修复前（allocator 方案） | +131G / −10G | 超物理内存 / 负值，已消除 |

> 补 parquet 解压估算后需重新跑 MOR + 普通表验证：预期 MOR 上升接近 30G、仍非负有界；普通表可能因单文件 parquet 解压估算略升，正常。若 MOR 过冲/超物理内存，多半是未按 `row_selection` 命中行折减，按需折减。

### 关键文件与提交
- paimon-rust-kwai（分支 `add_pred_dump`）：停用 allocator + 显式 BufferedBatch 统计、track cursor rows、current-thread runtime（后又换回 multi-thread）、parquet 解压驻留估算、`mem_tag::ScopedBytes` 通用 RAII。
- bleem4（分支 `paimon_rust`）：`73f3b46`（C++ mem track 管线）、`55cbe27`（counter guard 移到 scanner 线程）。C++ 侧在最终方案下无需改动。

---

## 背景与动机

Doris BE 的内存统计是「显式计数」：只有走 Doris 自家 `Allocator` 模板的分配，才会经
`CONSUME_THREAD_MEM_TRACKER` 计入当前线程 attach 的 query 的 `MemTrackerLimiter`
（详见 `be/src/runtime/memory/`、`be/src/runtime/thread_context.h`）。

`PaimonRustReader`（`be/src/format/table/paimon_rust_reader.cpp`）通过 FFI 调用动态库
`libpaimon_c`（源码仓库 `paimon-rust-kwai`）读取 paimon 数据。paimon-rust 在 Rust 侧分配的内存
（parquet IO buffer、解压/解码缓冲、arrow array builder、PK sort-merge cursor、Vortex 整文件
读入等）**完全不经过 Doris Allocator**，因此过去对 query 级 MemTracker 不可见——只能在进程级
jemalloc 统计里看到，无法归因到具体 query，也无法被 query 内存限额约束。

**需求（强需求）**：`PaimonRustReader` 执行 `paimon_record_batch_reader_next` 期间在 Rust 侧
消耗的内存，必须精确统计到「当前正在执行的那个 query」的 MemTracker；**多 query 并行时必须能区分**。

## 方案选型

考虑过三种方案，最终选 **档位2：per-reader 计数器 + thread-local tag**。

| 方案 | 多 query 区分 | Vortex/子线程 | 风险 |
|---|---|---|---|
| Rust 分配点直接调 Doris consume（malloc 重定向） | parquet/ORC ✅ | ❌ 子线程无 attach，记进 Orphan，开 `enable_memory_orphan_check` 会 DCHECK crash | 重蹈 Doris 废弃全局 malloc hook（#53794）的老坑 |
| 单个全局 AtomicI64 净值 + C++ 读边界差（档位1） | ❌ 全局共享，并发互相污染 | ✅ 天然覆盖 | 只能作量级指标，不满足强需求 |
| **per-reader 计数器 + thread-local tag（档位2，选用）** | ✅ 精确，计数器物理隔离 | ✅ 做了 tag 传播 | 对线程模型免疫，最坏只漏记不串错 |

### 为什么 malloc 重定向方案不行（关键认知）

- **Doris 的 malloc 本身不做统计**：`be/src/runtime/memory/jemalloc_hook.cpp` 里的 `doris_malloc`
  只是把符号别名到 jemalloc，不调用任何 tracker。重定向到它不会产生任何 track。
- 要 track 必须重定向到「带 consume 的分配函数」，而 consume 走 thread-local，依赖
  「分配恰好发生在 attach 了 task 的线程」。
- **Vortex 反例**：Vortex 用 `std::thread::Builder` spawn 独立 OS 线程解码，该线程没有
  `SCOPED_ATTACH_TASK`，其 thread-local tracker 是兜底的 `Orphan`；在该线程 consume 会触发
  `memory_orphan_check()` 的 DCHECK 直接 crash（`be/src/runtime/memory/thread_mem_tracker_mgr.h`）。

### 档位2 的核心思想

把两件事解耦：
- **「分配统计」**：线程无关。Rust 全局 allocator 每次 alloc/dealloc 只做一次无副作用的原子加，
  累加到「当前线程 tag 指向的 per-query 计数器」。任意线程（scanner、tokio worker、Vortex 子线程）
  都能正确累加。
- **「归属 query」**：线程相关。真正调 `CONSUME_THREAD_MEM_TRACKER` 只发生在 C++ scanner 线程，
  那里一定 attach 了当前 query（`be/src/exec/scan/scanner_scheduler.cpp` 入口
  `SCOPED_ATTACH_TASK(ctx->state())`），所以既不会 crash 也不会串错。

> **注**：tag 的安装范围经历过一次重要修正。最初由 Rust 的 `..._next` 函数内部用 `CounterGuard`
> 安装，只覆盖单次 `next`；上线观测发现 query tracker 偏大（超物理内存）甚至负值。根因与最终的
> **方案 A**（tag 由 C++ 全程控制 + 独立 counter handle）见下文「## 修订：tag 作用域从 next 扩到全程」。
> 本节以下的「实现机制」「文件清单」均以方案 A 为准。

## 修订：tag 作用域从 next 扩到全程（修复偏大/负值）

### 问题现象

初版（tag 仅在 Rust `..._next` 函数内）上线后，BE 内存快照里 `Query` tracker 出现严重失真：
偏大到超过物理内存（如 `Query 84GB` vs `Physical 40GB`），有时又偏小甚至负值。

### 根因：alloc 与 dealloc 跨越 tag 边界，不配平

一块内存只有当它的 alloc 和 dealloc **落在同一个 tag 作用域内**才会配平。初版 tag 只覆盖
`next`，导致两类失配：

- **偏大（主因）**：arrow batch buffer 在 `next` 内由 Rust 分配（tag 在 → counter += X），但它经
  Arrow C Data Interface 零拷贝交给 C++，真正释放发生在 C++ `ArrowBatch` 析构时回调 Rust 的
  release（此时已离开 `next`，tag = null → `account(-X)` 是 no-op）。→ counter 只加不减，
  随读的 batch 数单调膨胀。
- **偏小/负值**：`init_reader`/open 期分配的结构（table/schema/plan handle）在 `next` 外（tag=null，
  不计），若在 `next` 期间释放（tag 在，counter -= Y）→ counter 变负 → C++ 据此 RELEASE 一个
  本没 CONSUME 的量。

注：进程级快照里 `UntrackedMemory` 为负、`Query` 短暂为负也可能是 Doris 原生现象（批量 flush
阈值、reserved、跨线程 consume/release），与本缺陷叠加。本修订只解决我们引入的那部分。

### 方案 A：C++ 全程控制 tag + 独立 counter handle

让 tag 覆盖**所有调用 Rust 的 C++ 方法的完整区间**（`init_reader` / `get_next_block`（含
`ArrowBatch` 析构）/ `close`），与 Doris 原生「全程 `SCOPED_ATTACH_TASK`」对齐：

- arrow buffer：`next` 内 alloc、`get_next_block` 内 batch 析构时 dealloc，**同一 tag** → 配平。
- open 元数据：`init_reader` 内 alloc、`close` 内 reader free 时 dealloc，**同一 tag** → 配平。
- counter 提为**独立 handle**，在 `PaimonRustReader` 构造时创建——因为 open 期 reader 尚不存在，
  counter 必须比 reader 先生、比 reader 后死。

tag 的安装/恢复由 C++ 经新增 FFI 驱动，Rust `next` 内**不再**自带 `CounterGuard`。Vortex 逐跳
传播不变（`next` 仍在 scanner 线程、tag 由 C++ 已设好，子线程照常读 `mem_tag::current()`）。

## 实现机制

```
Rust cdylib libpaimon_c:
  #[global_allocator] CountingAlloc(System)
     alloc/dealloc/realloc/alloc_zeroed → mem_tag::account(delta)
        → CURRENT_COUNTER (thread_local 裸指针) 非空 → 该 query 的 AtomicI64.fetch_add(delta)
  FFI（独立 counter handle，由 C++ 持有）:
     paimon_mem_counter_create() / _destroy(c)
     paimon_mem_counter_enter(c) -> old_token   // 装 tag 指向 c，返回旧 tag
     paimon_mem_counter_restore(old_token)       // 恢复
     paimon_mem_counter_net_bytes(c) -> i64      // 读净占用(alloc-dealloc)
  paimon_record_batch_reader_next:
     不再自带 guard；block_on(stream.next()) 在 scanner 线程同步解码
     Vortex: 逐跳传播 tag 到子线程(见下)

C++ PaimonRustReader (scanner 线程, 已 SCOPED_ATTACH_TASK 当前 query):
  构造: _mem_counter = paimon_mem_counter_create();
  init_reader:     MemTagScope tag(_mem_counter); Defer{_sync_mem()};  → open 期分配计入
  get_next_block:  MemTagScope tag(_mem_counter); Defer{_sync_mem()};  → next+batch析构 全在 tag 内
  close:           MemTagScope tag(_mem_counter); _release_all_mem(); _handles.reset();
  析构:            DCHECK(_paimon_mem_reserved == 0); paimon_mem_counter_destroy(_mem_counter);
     _sync_mem(): net = paimon_mem_counter_net_bytes(_mem_counter);
                  delta = net - _paimon_mem_reserved;
                  delta>0 → CONSUME_THREAD_MEM_TRACKER(delta)
                  delta<0 → RELEASE_THREAD_MEM_TRACKER(-delta)
                  _paimon_mem_reserved = net;
```

口径：counter = 当前净占用（alloc − dealloc），任意时刻反映 Rust reader 真实驻留内存。C++ 读差值
delta，正负都处理；close 时 `_release_all_mem` 按 `_paimon_mem_reserved` 把 query tracker 归零。

### MemTagScope + Defer 的 LIFO 顺序

`get_next_block` 内栈对象构造顺序：`MemTagScope tag` → `Defer sync_mem` → … → `ArrowBatch batch`。
LIFO 析构（逆序）：

1. `batch` 析构 —— 调 Arrow release 回调把 buffer 归还 Rust（此时 tag 仍在 → dealloc 计回 counter）
2. `sync_mem`（`Defer`）—— 读 counter 净值并同步（batch 已释放，反映真实驻留；tag 仍在）
3. `tag`（`MemTagScope`）—— restore，恢复上一层 tag

所有返回路径（成功/EOF/error）都经此序。`init_reader`/`close` 同理以 `MemTagScope` 包裹全程。

### Vortex 多层线程的 tag 传播（实现期重要修正）

实现中发现 Vortex 读路径是**两层线程跳转**（计划里曾假设一层）：

```
scanner 线程 (next 装 tag)
  → tokio::task::spawn_blocking (vortex.rs read_batch_stream)  ← tokio blocking 线程, 不继承 tag
     → run_vortex_on_thread std::thread::spawn                 ← decode 线程, 不继承 tag
```

每一跳都不继承 thread-local，因此**逐跳重装**：
- `spawn_blocking` 闭包入口：在 scanner 线程（tag 有效）捕获 `mem_tag::current()`，包成 `usize`
  跨线程移动，闭包入口 `CounterGuard::enter` 重装。
- `run_vortex_on_thread`：再 `mem_tag::current()`（此时在 blocking 线程，上一跳已重装）→ 传给
  decode 线程入口重装。

裸指针跨线程的安全性：父线程 `block_on` / `join()` 全程阻塞，owning reader（持有 counter）存活，
指针有效期覆盖子线程整个生命周期。

## 改动文件清单

### Rust（paimon-rust-kwai）
- `crates/paimon/src/mem_tag.rs`（新增）— thread-local tag：`account` / `set_current` /
  `restore_current` / `current` / `CounterGuard`，含单测
- `crates/paimon/src/lib.rs` — 注册 `pub mod mem_tag`
- `bindings/c/src/alloc.rs`（新增）— `CountingAlloc` 包 `System`，四路径调 `mem_tag::account`
- `bindings/c/src/mem.rs`（新增）— 独立 counter handle `paimon_mem_counter`，FFI
  `paimon_mem_counter_create/destroy/enter/restore/net_bytes`（不再内聚于 reader）
- `bindings/c/src/lib.rs` — `#[global_allocator] CountingAlloc` + `mod alloc; mod mem;`
- `bindings/c/src/table.rs` — reader wrapper 用 `ArrowRecordBatchStream`（counter 已解耦出去），
  `paimon_record_batch_reader_next` 不再自带 `CounterGuard`（tag 改由 C++ 驱动）
- `crates/paimon/src/arrow/format/vortex.rs` — `read_batch_stream` 的 `spawn_blocking` 与
  `run_vortex_on_thread` 两跳传播 tag

### C++（bleem4）
- `be/src/format/table/paimon_rust_reader.h` — 成员 `void* _mem_counter`（持有
  `paimon_mem_counter*`，用 `void*` 避免与 cbindgen 匿名 typedef 冲突）、`_paimon_mem_reserved`、
  方法 `_sync_mem` / `_release_all_mem`
- `be/src/format/table/paimon_rust_reader.cpp` — `#include "runtime/thread_context.h"`、匿名
  namespace 的 `MemTagScope`（RAII enter/restore）、构造创建 / 析构销毁 counter handle、
  `init_reader`/`get_next_block`/`close` 三处以 `MemTagScope` 包裹全程、`_sync_mem` 读
  `paimon_mem_counter_net_bytes`、析构 `DCHECK`

### 产物同步
- `thirdparty/installed/lib/paimon_rust/libpaimon_c.dylib` 与
  `thirdparty/installed/include/paimon_rust/paimon.h` 均为**符号链接**，分别指向
  `paimon-rust-kwai/target/{debug}/` 与 `bindings/c/include/`。重新 `cargo build -p paimon-c`
  即自动刷新（当前项目用 debug 版）。
- cbindgen 由 `bindings/c/build.rs` 在构建时自动重生成 `paimon.h`（**失败只 warning**，需校验
  新符号是否出现）。

## 构建与验证状态

已完成：
- `cargo check -p paimon-c` / `cargo build -p paimon-c [--release]` 通过
- `cargo test -p paimon mem_tag` 3 个单测通过（account 空转、guard 设置/恢复、嵌套）
- `paimon.h` 含 `paimon_mem_counter_create/destroy/enter/restore/net_bytes`，dylib 导出；旧的
  `paimon_record_batch_reader_mem_net_bytes` 已随重构移除
- `./build.sh --be` 退出码 0、`BUILD SUCCESS`（中途 `paimon_mem_counter` typedef 与具名前向声明
  冲突，已改用 `void*` 成员修掉）

待验证（需运行中的集群）：
1. `enable_paimon_rust_reader=true` 跑 paimon query，在 query profile / `/mem_tracker` web 页
   看该 query tracker 是否反映 paimon-rust 量级（与走 paimon-cpp 对比）
2. parquet 表 与 Vortex 表 分别验证；Vortex 验证 tag 传播生效（计入对应 reader，非漏记/Orphan）
3. 多 query 并发：2–3 条并发，各 query tracker 数值彼此独立、不串
4. 配对/泄漏：query 结束后 tracker 回到接近 0；反复跑无累积增长
5. orphan 安全：开启 `enable_memory_orphan_check` 跑上述用例，确认不 crash

## 已知约束与风险

- **CountingAlloc 无条件全局拦截**：统计 cdylib 内**所有** Rust 分配。无 tag 时 `account` 是空转
  （一次 thread-local 读 + null 判断，开销极轻），不影响其它链接该库的二进制（REST server、
  Python bindings、测试）。
- **realloc delta**：用 `new_size - layout.size()`，依赖传入 layout 是旧布局（标准保证），正确。
- **System vs jemalloc**：CountingAlloc 包 `System`；BE 进程级 jemalloc 与之独立，不冲突。
- **依赖「读路径无新 spawn」**：parquet/ORC 同步在 scanner 线程成立；Vortex 已显式逐跳传播。
  未来读路径若再引入 `tokio::spawn`/新线程池，需同样传播 tag，否则该部分漏记（但**不会串错 query**）。
- **ImportRecordBatch 零拷贝**：arrow buffer 在 `ArrowBatch` 析构调 release 回调时才归还 Rust。
  方案 A 用 `MemTagScope` 把整个 `get_next_block`（含 batch 析构）纳入 tag，且 `Defer sync_mem`
  在 batch 析构之后、tag restore 之前执行，保证该释放被 counter 计回、且 `_sync_mem` 读到的是
  释放后的净值。
- **tag 作用域必须覆盖所有 Rust alloc/dealloc 配对**：这是方案 A 的核心约束（init/next/close 全程
  +独立 handle）。任何新增的、会触发 Rust 分配或释放的 C++→Rust 调用路径，都必须用 `MemTagScope`
  包裹，否则会重新出现初版的偏大/负值失配。

## 参考

- 详细方案文档：`~/.claude/plans/precious-gathering-engelbart.md`
- Doris 移除 malloc hook 的上游提交：`726c8d13831`（#53794）
