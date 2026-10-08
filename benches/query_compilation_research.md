# Roc 查询编译落地评估：TUM / HyPer / Umbra / InkFuse / LingoDB

调研日期：2026-10-08。对照分支：`codex/cranelift-query-compilation`。

本文覆盖与执行引擎落地直接相关的主线：数据中心式编译、并行调度、向量化、低延迟编译、自适应执行、中间表示、跨架构、调试及有状态算子。它不是 TUM 全部数据库论文的穷举书目。下文区分论文结论、当前代码事实，以及据此提出的 Roc 工程建议；论文中的历史后端版本和性能数字不能直接套到 Cranelift 0.135.5。

## 判断

继续用 Cranelift 验证是合理的，但下一步应优先建立可切换、可验证、有资源预算的编译片段，而不是扩大手写 CLIF 的算子范围。建议形态：**现有 Arrow 执行路径 + 可选的编译片段 + 共用语义和状态契约**。

现有结果证明：在 M4 Pro 上，经过针对性的 lowering，非空 Int64 的固定 Filter→Project 查询可以达到相同 bitmap 算法的 LLVM AOT 水平。它还没有证明复杂表达式、NULL、字符串、哈希访问、多核、冷缓存或完整查询延迟同样受益。完整测量见 [基准说明](README.md#optimized-simd-lowering-2026-10-08)。

## 论文地图与阅读结论

以下引用均为作者、所在机构或正式论文入口。不同研究路线不是 Umbra 的顺序版本；尤其 InkFuse 和 LingoDB 应分别看待。

| 工作 | 直接相关的结论 | Roc 应吸收的部分 |
| --- | --- | --- |
| [Efficiently Compiling Efficient Query Plans for Modern Hardware，VLDB 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) | 以数据流组织代码，用 produce/consume 跨越算子边界，减少中间结果与迭代器开销。 | 编译单位应是有明确入口、出口的片段；聚合 build/finalize、exchange 等边界不能忽略。 |
| [Morsel-Driven Parallelism，SIGMOD 2014](https://www-db.in.tum.de/~leis/papers/morsels.pdf) | 细粒度工作调度与数据、算子状态的 NUMA 局部性共同决定并行效率。 | 机器码与 worker 状态分离；单核内核结果不能预测多核扩展性。 |
| [Data Blocks，SIGMOD 2016](https://www.db.in.tum.de/downloads/publications/datablocks.pdf) | 向量化的压缩扫描可以给编译流水线供数；二者可以组合。 | 先保留 Arrow/扫描解码边界，不把所有存储编码都变成 JIT 变体。 |
| [Everything You Always Wanted to Know About Compiled and Vectorized Queries，VLDB 2018](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) | 在相同算法、数据结构和并行框架下比较：编译减少指令；向量化在隐藏 cache miss 延迟上有优势。 | 保持公平控制；专门测大哈希表、缓存失效、选择率分布，不能只看热缓存算术。 |
| [Adaptive Execution of Compiled Queries，ICDE 2018](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf)；[Making Compiling Query Engines Practical，2019 在线 / TKDE 2021 卷期](https://portal.fis.tum.de/en/publications/making-compiling-query-engines-practical/) | 以 morsel 为切换单位，利用相同状态与工作范围在不同执行模式之间转换；扩展工作也关注调试。 | 先定义状态契约，再做后台编译和执行模式切换。 |
| [Umbra: A Disk-Based System with In-Memory Performance，CIDR 2020](https://cidrdb.org/cidr2020/papers/p29-neumann-cidr20.pdf) | 超内存处理要求存储与执行协作；模块化 step 也为暂停、调度提供边界。 | 保留背压、取消和可恢复执行位置；Arrow buffer 借用不能越过失效点。 |
| [On Another Level: How to Debug Compiling Query Engines，DBTest 2020](https://www.db.in.tum.de/~kersten/codegen_debugger.pdf) | 生成器和生成代码是两个调试层次，只看运行时指令会丢失来源。 | 能从错误或机器码定位回表达式、片段和生成位置。 |
| [Tidy Tuples and Flying Start，VLDB Journal 2021](https://link.springer.com/article/10.1007/s00778-020-00643-4) | 将算子、数据结构、tuple、SQL value、codegen 分层；紧凑 Umbra IR 和低延迟后端一起降低准备成本。 | SQL 语义不要散落在 CLIF emitter；预编译 helper 与生成代码分工。 |
| [Profiling Dataflow Systems on Multiple Abstraction Levels，EuroSys 2021](https://www.db.in.tum.de/~beischl/papers/Profiling_Dataflow_Systems_on_Multiple_Abstraction_Levels.pdf) | 性能归因需要连接算子、流水线与机器指令等抽象层。 | 从现在开始保留片段 ID、源表达式 ID、阶段耗时和代码范围。 |
| [Designing an Open Framework for Query Optimization and Compilation，VLDB 2022](https://www.db.in.tum.de/~jungmair/papers/p2485-jungmair.pdf) | LingoDB 借助 MLIR 把优化和 lowering 表达为分层变换。 | 学习分层和可检查性，不必立即引入完整 MLIR 栈。 |
| [Efficiently Compiling Dynamic Code for Adaptive Query Processing，ADMS 2022](https://db.in.tum.de/people/sites/schmidt/papers/dynamic-blocks.pdf) | Dynamic Blocks 表达可选、替代、重排片段，降低反复重编译的需要。 | 选择率和谓词代价可能在运行时变化；初期只保留少数候选，避免变体组合爆炸。 |
| [Practical Planning and Execution of Groupjoin and Nested Aggregates，VLDB Journal 2022](https://link.springer.com/content/pdf/10.1007/s00778-022-00765-x.pdf) | 聚合与 join 的算法、估计和并行争用需要联合处理。 | 先定义 local update、combine、finalize；不能只把现有调用循环改写成机器码。 |
| [Bringing Compiling Databases to RISC Architectures，VLDB 2023](https://www.db.in.tum.de/~gruber/p791-gruber.pdf) | FireARM 展示了 ISA、ABI 和专用代码生成的移植成本；低延迟与代码质量需要同时评估。 | 明确 target lowering，x86-64/ARM 分别验证指令、语义和性能。 |
| [Declarative Sub-Operators for Universal Data Processing，VLDB 2023](https://db.in.tum.de/~jungmair/papers/p2799-jungmair.pdf) | 状态及状态访问显式化，细粒度算子可组合，硬件变换不必复制整套关系算子。 | 让读取、选择、计算、状态更新和输出可独立表示。 |
| [Incremental Fusion，ICDE 2024](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/inkfuse.pdf) | InkFuse 由有限的 sub-operator 集合产生向量化解释原语和融合代码，减少两套执行逻辑的维护分叉。 | Arrow fallback 和 JIT 最终应受同一语义描述与测试约束；不是再手写一套不相关执行器。 |
| [Compile-Time Analysis of Compiler Frameworks for Query Compilation，CGO 2024](https://home.cit.tum.de/~engelke/pubs/2403-cgo.pdf) | 在 Umbra 中直接比较 LLVM、Cranelift 和 DirectEmit 等；Cranelift 的低延迟优势并非数量级保证，运行时 helper 与指令选择也影响表现。 | 后端可替换；阶段计时和真实查询回归比“fast JIT”标签更可靠。论文中的旧 API 缺陷需按当前版本重新核实。 |
| [Simple, Efficient, and Robust Hash Tables for Join Processing，DaMoN 2024](https://db.in.tum.de/~birler/papers/hashtable.pdf) | Unchained hash table 综合处理分区、布局、探测和倾斜，而不是仅优化几条指令。 | 加入 join 时单独评估数据结构；别让 JIT 吞掉算法问题。 |
| [LingoDB-CT，SIGMOD Companion 2025](https://db.in.tum.de/~jungmair/papers/lingodb-ct.pdf) | 将优化、编译与运行信息关联起来，有助于理解复杂查询的行为。 | EXPLAIN、IR dump、性能事件要有共同标识。 |
| [TPDE: A Fast Adaptable Compiler Back-End Framework，CGO 2026](https://aengelke.net/pubs/2602-cgo1.pdf) | 通过 SSA IR adapter 直接消费现有表示，组合快速分析和代码生成；避免再翻译一份 IR 的成本。 | 可以保留未来快速后端接口；其基线代码质量定位不等于替代所有优化后端。 |
| [Maintainable, Low-Latency and High-Quality Query Compilation with MLIR and TPDE，ADMS 2026](https://db.in.tum.de/~jungmair/papers/adms-2026-fast-compilation.pdf)；[作者解读](https://www.lingo-db.com/blog/mlir-tpde/) | 不只优化机器码后端，也减少 MLIR 前端开销；小数据与大数据的编译/执行取舍不同。 | 分层本身也有成本；不要因“架构漂亮”引入未测量的多轮 lowering。 |

特别注意：2024 年 Cranelift 论文的 C++/Rust 集成负担不等于本项目的负担，Roc 本身是 Rust。2026 年 TPDE 的结果也不构成立刻替换 Cranelift 的理由。当前应保持后端边界清楚，等完整片段与工作负载具备后再比较。

## 当前代码中的落地缺口

本节是对仓库的工程判断，不是论文对 Roc 的结论。

### P0：先约束语义，防止两个执行引擎算出不同结果

当前 JIT 的私有 `Expr` 仅覆盖非空 Int64 及一部分算术/比较，见 [jit/mod.rs](../src/jit/mod.rs)。新增功能必须明确：

- `WHERE NULL` 不通过，但 NULL 也不等于 false；布尔值与 validity 都要保留。不要拿 NULL 槽位的任意 payload 去做会报错的运算。
- [CASE](../src/expr/scalar/case.rs) 只对所需行计算分支；当前 [AND/OR](../src/expr/scalar/conjunction.rs) 则会依次求值各个参数。不能笼统以“SQL 应该短路”为由改变现有行为。谓词重排和 SIMD eager evaluation 都需要显式的错误/副作用规则。
- 保留 checked arithmetic；后续 Decimal128 的精度、scale、宽中间结果、舍入和溢出，浮点 NaN/±0、hash/equality 一致性，字符串和时间语义都需要单独定义。
- [聚合](../src/expr/agg/accumulator.rs) 已有 NULL 与整数前缀溢出行为。比如 `[MAX, 1, -1]` 的最终数学和可表示，但逐项 checked SUM 会在中途溢出。随意用多个 SIMD accumulator 或重排 combine 不能只比较最终数学和。浮点归约重排也需要明确接受什么误差。
- 差分测试比较 value、validity、行数、顺序和成功/失败；只比较非空输出值不够。现有 Arrow 路径是回归基线，复杂 SQL 还需手算/独立参考结果，避免两边共用错误逻辑。

建议先提取小型、带类型/nullable/错误属性的片段描述和语义契约。不要在每个 Cranelift 分支里重新发明 SQL 规则，也不必先造一个完整通用编译器。

### P0：编译一次、共享代码、隔离 worker 状态

[Pipeline::execute](../src/pipeline/execution.rs) 先初始化 global context，再为每个 worker 调用 `new_executor`。把 `compile()` 塞进 `new_executor`，会按并行度重复编译同一片段。

当前 `CompiledFilterProject` 把 code owner、函数指针和查询元信息放在一起；`CodeMemory::drop` 会释放机器码。它可以移动到 worker，但没有可直接复用的共享代码发布/回收协议。仅包一层 `Arc` 也不自动证明内部 `JITModule` 可安全并发访问。

应建立三个独立生命周期：

1. 不可变代码及其常量：共享、计费、可缓存；审查 finalized code 所有权和 `Send/Sync`。
2. 每次查询的 runtime context：参数、schema 契约、取消信息、helper 环境。
3. 每个 worker 的 mutable state：聚合器、输出缓冲、游标、临时 arena。

必须保证所有在途调用结束后才释放代码。缓存淘汰不能等价于立即 `free_memory`；编译任务也不能借用将随查询取消而释放的输入 buffer。

### P0：编译延迟、后台任务和收益门槛

当前 `compile()` 是同步的，且 [emit.rs](../src/jit/emit.rs) 在普通编译路径也执行 `ctx.func.display().to_string()` 保存 CLIF。生产路径可把文本诊断改为可选，并分别测 lowering、codegen、finalize、诊断格式化。

现有 compile/drop 基准约 543.5 µs；以下用它作为启动成本近似计算。对于约 50% 选择率：

| 批大小 | Arrow−JIT 的每批节省 | 简单同步编译盈亏平衡 |
| ---: | ---: | ---: |
| 2048 行 | 2.819−1.239 ≈ 1.580 µs | 约 344 批 |
| 16384 行 | 21.016−9.654 ≈ 11.362 µs | 约 48 批 |

这是固定查询的热缓存估算，不是通用阈值。后台编译时还要估计排队时间、编译期间已消耗的输入和剩余收益；编译会争用 CPU/内存带宽，不能视为免费。

建议使用有界编译队列、同 key 合并编译、时间/IR/机器码大小预算和 query 取消处理。第一版先提供明确的 Off/Force/Auto 模式；Auto 用已测成本保守触发。不要在异步执行线程里同步编译大型片段，也不要每批启动一个编译任务。

### P0：模式切换必须在可恢复边界发生

先让无状态 Filter→Project 在完整输入批处理完毕后切换。不要在行处理一半、或 `MoreResult` 尚未耗尽时替换 executor。

现有 [ProcessResult](../src/exec/mod.rs) 包含 `NeedMoreInput`、`MoreResult`、`Finished`，并有 `finish()`；这些不是普通数组函数签名可以替代的行为。未来 join 一批输入可能产生多批输出，必须保留 probe row、match cursor、输出位置和已提交状态。

聚合的 Arrow/JIT 路径需要共用逻辑状态或明确转换协议：只换函数指针，旧聚合器不会自动变成新布局。尤其当前聚合更新可能先修改成功前缀再报错；发生运行时错误后直接回退 Arrow 并重放输入，会重复更新。

区分三种情况：进入执行前不支持 → Arrow；编译失败 → 按策略 Arrow；执行中算术错误/状态错误 → 传播错误，不能把它当编译失败重试。

### P0：保留调度、背压与取消契约

现有 executor 在批次调用、sink 和 `finish` 等边界管理 yield 与 shutdown。把整个 query 变成一次长时间不返回的 native call，会破坏这些性质。

代码生成应显式表达预算耗尽、输出缓冲满、完成、执行错误等返回状态。初期保留现有 batch 边界；一旦加入 n:m join，输入批较小也不保证输出或单次执行时间有界，需要额外工作预算。

构建片段时，不跨越 Aggregate build/finalize、exchange、会等待的 source/sink。存储解码先保留现有实现。内核持有的 Arrow buffer 必须覆盖整个 native call；字符串指针若进入跨批状态，要拥有其 backing storage 或复制进受管理 arena。

### P1：融合程度和 SIMD 都要有成本边界

我们刚解决的是 ARM 直接 Int64 比较。增加表达式深度、列数、NULL、helper call 后，寄存器压力、spill、代码尺寸和编译成本都会变化。

“少物化”不能作为唯一目标：hash probe 存在依赖链，分段向量化和预取可能制造更多独立内存访问。批量向量化执行与使用 SIMD 指令也不是同一件事。

建议同时保留标量、小批 SIMD 和 helper 路径；先按静态规则选择，再考虑运行时测量。为 IR 指令数、基本块数、展开倍数和生成代码字节数设预算。不要把目前 64 行展开、4 行投影展开复制到任意大表达式。

### P1：缓存的 key、参数与失效

不能只按 SQL 文本或表达式打印字符串缓存。建议 key 至少包含：规范化片段与引用映射、输入/输出类型与 nullability、内嵌常量、所选参数化策略、错误语义、状态布局版本、helper ABI 版本、后端版本/flags、目标架构与 CPU features。

当前阈值等常量嵌入机器码，利于特化，却会降低相似查询间复用。应明确哪些是 runtime parameter，哪些值得生成专用版本；同时约束变体数和代码缓存容量。第一版做进程内缓存，避免先承担跨进程机器码地址、重定位和失效问题。

### P1：复杂算子和 runtime helper 的边界

建议复用预编译的分配器、字符串操作、哈希表、排序、spill 和错误设施。对热点再逐步内联或生成专用片段；逐行调用重 helper 会抹掉融合收益，批量 ABI 需要一起评估。

helper 边界要有稳定的 C ABI、显式布局/opaque handle、长度/容量和错误状态。不能把 Rust `Vec`、trait object 等内部布局当跨生成代码的稳定协议。错误和 panic 不能随意 unwind 穿过 JIT 栈。

当前仓库已经有 worker-local aggregate 及 combine/finalize；先复用这套生命周期。之后才决定哪些状态布局需要为 JIT 优化。Join 暂无现成执行实现，更不能把编译后端当成 hash join、倾斜处理、spill 和输出续传的替代品。

### P1：可观测性和 native-code 验证

现有 CLIF 和汇编导出是好起点，还需要关联：plan/fragment/expression ID → IR → function/code range → runtime profile。

记录编译阶段耗时、缓存命中、回退原因、执行模式、处理行数/选择率、机器码大小、compile queue 等待及取消。基准要能回答“慢在算法、生成器、编译器、分配还是调度”。Linux 上可接 perf 的 JIT 符号协议；macOS 需要对应验证路径，不能假定相同工具链。

当前 Cranelift 0.135.5 的 `finalize_definitions` 负责重定位后将内存准备为可执行状态。不要自行跳过 finalize 或热修改正在执行的代码。未来 helper、栈回溯和代码缓存扩展要验证目标平台上的 relocation、unwind、可执行内存发布和回收。2024 年论文提及的旧版 JIT wrapper 问题是回归线索，不是断言当前版本仍有相同 bug。

## 建议的实现顺序与验收条件

| 阶段 | 范围 | 验收条件 |
| --- | --- | --- |
| 1. 片段接入 | 给 `Operator`/pipeline 构建增加明确的可编译描述或 lowering hook；先融合相邻 Filter/Project。引入共享代码 owner 与独立 worker context。 | 多 worker 一次编译；unsupported 自动保留 Arrow；完整 pipeline 输出、finish、取消行为等价。 |
| 2. 语义补齐 | nullable Int64、布尔三值逻辑、CASE；统一类型/错误/active-mask 契约。 | 对所有位偏移、尾部、无效 payload、未选中分支错误、溢出做差分测试；不支持的类型能清楚拒绝。 |
| 3. 第一个有状态片段 | `Filter → Project → SUM/COUNT`，先无 GROUP BY；保持原有聚合状态及溢出规则。 | JIT 直接更新 local state，减少输出物化；空输入/全 NULL/跨批合并正确；与相同算法 AOT 同台比较。 |
| 4. 启动与缓存 | 可选 IR dump、有界后台编译、进程内代码缓存、同 key 合并、成本门槛、批次边界切换。 | 短查询不系统性退化；取消无悬空代码；并发 miss 不形成编译风暴；端到端 P95/P99 可解释。 |
| 5. 更广算子 | GROUP BY、字符串/decimal helper、join 的 build/probe 与可恢复输出。 | 倾斜、扩张输出、OOM/spill、状态合并及错误退出测试；不破坏背压与取消。 |
| 6. 后端和策略扩展 | x86-64 SIMD、有限变体自适应；有需要再比较 LLVM 或 TPDE。 | 同一个片段 IR/ABI/算法测 codegen 与 execution Pareto 曲线；不拿另一系统的整机结果代替后端比较。 |

阶段 1–3 优先。为后续模式切换预先约束状态和安全边界，但不要求第一版同时实现所有自适应机制。阶段 4 的最小队列/资源预算可提前，以免试验接入后阻塞执行线程。

## 基准与正确性清单

保留三类不同问题的基线：同算法 AOT/JIT 用于分析 lowering/backend；原有 Arrow 用于评估项目实际收益；完整 pipeline 用于评估编译、调度和状态成本。算法变化时，为 AOT 对照实现同一算法，再报告新的比较。

- 尺度：空输入、极小批、不同批大小；热/冷数据；数据和状态超过 LLC；表达式深度与投影列数扩展。
- 数据：选择率 0/稀疏/约 50/全通过，随机与成片分布；NULL 密度与位偏移；字符串长短分布；group 基数和倾斜。
- 并行：1/多 worker、并发短长查询、缓存命中/未命中、编译排队；x86-64 与 AArch64 分开测，不只做反向测量顺序。
- 正确性：随机合法 typed IR、边界值、active lane、CASE 错误分支；切换前后恰好处理一次；输出续传、取消、错误后的清理。
- 指标：首次响应/完整延迟、P50/P95/P99、吞吐、编译 CPU 时间与等待时间、peak RSS、代码缓存和机器码体积；热点采样配合指令/cache miss/branch/spill 证据。
- 查询：先 TPC-H 形状的片段；具备相应计划支持后再做 TPC-H/TPC-DS/JOB。当前仓库不是完整 SQL 前端，不把手工构造几个片段称为跑通完整基准。

当前最有价值的下一项实验是阶段 1–3 的最小闭环：**在真实 pipeline 中编译一次，多个 worker 复用代码，用 nullable Filter→Project→SUM/COUNT 验证语义和状态，再测包含编译成本的总延迟。** 这比继续优化同一个非空整数过滤内核，更能判断路线是否值得扩展。
