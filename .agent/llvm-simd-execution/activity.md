### [2026-10-14 13:12] — Decision
- 用户显式调用 `/planning-high`，创建 high-effort 计划 `.agent/llvm-simd-execution/plan.md`。
- 关键架构与边界已由前置实验与用户确认收敛：
  1. 采用单一 LLVM 19（Inkwell 0.7.1 MCJIT）显式向量化 emitter，不依赖自动向量化；
  2. 零编译 kernel 库在构建期（`build.rs`）由同一 emitter 预编译，运行时按 CPU 特性分派；`Compile` 模式在运行时通过 MCJIT 编译向量化融合窗口循环；
  3. 一次性将全部 kernel 迁移到新物理布局（原生宽度 buffer + Boolean bitmap + 1024 行窗口 + 常量广播 + 原位输出），移除 Cranelift、`KernelLibrary::Native` 与旧标量 JIT；
  4. `ExecutionMode` 收敛为确定性的 `Compile` 与 `ZeroCompile`，本次不做后台异步编译热切换；
  5. 项目管理字段确认：`TAPD 工作项=无`、`工蜂 issue=无`、`是否提交 MR=否`、`MR 目标分支=无`（跳过 issue↔TAPD 关联补挂）。

### [2026-10-14 13:14] — Gate
- `Gate: Plan Approval` 通过。
- 用户明确指示：不要走 `spec-writing`、`task-planning`、`task-execution` 流程，把要做的事情在计划中记下来后直接开干。
- 同步修订 `plan.md`：将 `spec.md` 与 `tasks.md` 设为 `not needed`，移除 `Gate: Spec Review`，将实施要点直接写入步骤 1 并置为 `[~]`，状态切换为 `in_progress`。

### [2026-10-14 15:40] — Progress
- 实现落地（步骤 1 主体）：
  1. `src/kernel/codegen/`（`ir.rs` / `emit.rs` / `catalog.rs` / `mod.rs`）：不依赖 crate 的单一 lane-generic LLVM emitter。kernel 以 `Group`（节点图 + 参数 + store/raise/report）描述；`ir::lower` 是每个函数语义的唯一定义，预编译 kernel 与融合组都经过它。主循环按 `C = max(L, 16)` 行一块处理（位图按小端 `iC` 读写），尾部单 lane；错误标志走 lazy 快速通道（只做向量 OR，置位时才精确重算）；带标量操作数的溢出检查按符号拆成两份循环；常量除数用 Hacker's Delight 魔数除法（已用 460 万组样本对拍）。
  2. `build.rs`：用同一 emitter 构建期生成 510+ 个 kernel（x86_64 生成 AVX2 + SSE2 两套，aarch64 生成 NEON），经 `ar` 静态链接，并生成符号表；`src/kernel/library.rs` 按 CPU 特性分派。
  3. `src/kernel/steps/`：新的窗口执行层（原生宽度、位图 + `BitState`、常量广播、last-use 槽位、根输出原位写入、Filter 掩码压缩、聚合快路径）；`fuse.rs` 把连续可融合步骤交给 MCJIT，窗口可能报错时回退到逐步执行以精确定位首个错误行。
  4. `ExecutionMode` 收敛为 `Compile` / `ZeroCompile`；测试、bench、run.py 已更新。
- 修复中发现并解决：浮点常量作为立即数会被 LLVM 折叠而改变 NaN 位（融合组中浮点常量改为标量参数）；`Rows` 窗口外 word 未清零导致计数错误；ParseText→Boolean 写成字节。
- 验证：`cargo test --lib`（60）与 6 个集成测试文件全部通过（`tests/simd_spike.rs` 待删除）。
- M4 本地基准（4M 行，执行中位数 ms，ZeroCompile 1T/4T）：scan 0.053/0.08，filter_project 5.6/1.6，arithmetic_chain 10.8/2.9，integer_division 12.2/3.2，global_aggregate ~1.0/0.39，grouped_aggregate 7.0–8.0/2.0；Compile：arithmetic_chain 7.2/2.2，integer_division 7.9/2.3，prepare 约 8ms（初版 145ms，经块化与精简 pass 管线降低）。
- 未决：global_aggregate 仍略慢于 main（0.916/0.328）；`dev`（x86_64）验证未开始（ssh 被权限分类器拦截，需用户协助）；旧文件 `src/kernel/jit/`、`src/kernel/vector/`、`src/kernel/io.rs`、`src/kernel/simd*.rs`、`tests/simd_spike.rs` 的删除命令被权限分类器拦截，需用户执行。

### [2026-10-14 17:00] — Progress
- 性能优化（M4）：Compile prepare 145ms → ~4–8ms（emitter 改为 16 行块 + `iC` 位图读写；溢出节点复用和值；raise 按掩码分组；融合组 raise 用 loose 过近似；Boolean 打包改为“扩宽→拼接→截断为字节→比较→bitcast”形式，无需 instcombine；JIT 管线 `function(early-cse)`）；位图 lane 提取改为广播 + 按位测试；输出位压缩改为 Hacker's Delight compress；分组聚合去掉逐行边界检查与常量分支；同组同行集的连续 AggregateUpdate 合并为一个 `Aggregates` 步骤；帧向量在 `LocalState` 中复用；输入读取不再 `to_data()`；纯透传批次直接返回。
- 新增 `tests/window_execution.rs`（窗口/字边界、跨 kernel 与窗口的首错行、窗口各部分的稀有错误、Filter 掩码压缩（值/有效位/字符串）、各整数类型常量除数魔数除法 vs Arrow、仅 Compile 有生成代码）；`bits` 增加 compress 对拍测试。
- 验证：lib 61 + 集成 51 = 112 测试全部通过；`cargo clippy -D warnings`（除待删 `tests/simd_spike.rs` 外全部 target）无警告；`roc-datafusion` 编译通过。
- `run.py` 新增 `--rustflags`（两套构建共用，记录进 metadata），供 `dev` 传 LLVM 系统库路径。
- M4 官方对比（`target/main-compare-20261014-m4-llvm2`，3 replicas × 10 samples，执行中位数 ms，ZeroCompile vs 最快参考）：1T——scan 0.049/0.051，filter_project 4.86/8.90，arithmetic_chain 10.21/11.36，integer_division 11.60/15.03，global_aggregate 0.778/0.875，grouped_aggregate 6.78/8.74；4T——0.077/0.143，1.43/3.26，2.81/4.93，3.20/5.94，**0.318/0.311**，1.95/2.37。Compile：arithmetic_chain 6.92/2.01、integer_division 7.50/2.17（相对 ZeroCompile 1.45–1.55×）。12 项中 11 项达标，global_aggregate 4T 超出 2%（接近内存带宽上限；本机负载 ~4.5，交错复测在 ±10% 内波动），之后的聚合步骤合并改动尚未进入官方复测。
- 阻塞：删除旧文件与访问 `dev` 均被权限分类器拦截，需用户操作。

### [2026-10-14 18:00] — Progress
- 旧文件删除完成；commit `96289e4` 推送到 `origin/codex/suboperator-execution-rewrite`。
- x86（`dev`，EPYC 7K62）首次官方对比（`target/main-compare-20261014-x86-llvm`）：12 项中 10 项达标；scan 1T（0.155 vs 0.146）与 global_aggregate 4T（1.24 vs 1.18）未达标。scan 经"纯透传程序直接拼输出 + 每种 schema 只校验一次输入"降到 0.137。
- code review（high）确认 6 个真实 bug：ZeroCompile 过滤后分组聚合段错误（过期分组 ID）、融合函数覆盖未读输入槽位、回退重跑读到被覆盖输入、输入位图越界读、同类型 cast 无内核、run.py 遇 None 崩溃。均先写独立期望值的回归测试确认失败，再修复。另补：emitter 整窗向量 vs 逐行单 lane 一致性测试（x86 上同时比较 SSE2/AVX2）、魔数除法测试。
- 调度问题定位：`taskset` 把 4 线程测量限制在 CPU 0,2,4,6（同一个 CCX）。旧架构 tokio 4 worker 协调 + rayon 4 线程计算 = 8 线程抢 4 核，profile 中约 15% 在内核调度函数，global_aggregate 4T 时快时慢（1.29–1.57）。
  - 方案对比（dev，4 核，三轮，global_aggregate / grouped）：双池 tokio=1 → 1.19/5.5；tokio=2 → 1.23–1.53/5.6–7.0；tokio=4 → 1.42–1.57/5.6–7.0；**单池（计算直接作为宿主执行器的任务，去掉 rayon）→ 1.16–1.21/5.55，稳定**；main → 1.20/9.1。
  - 采用单池：计算任务无锁、不共享状态、每个任务数据量有上限，可直接跑在宿主执行器上，异步任务在任务间隙调度。`Pipeline::execute` 增加 executor 参数，rayon 依赖删除；bench 两个引擎恢复对称（tokio N worker）。
  - 分散到 4 个 CCX（0,8,16,24）时单池 global 1.07–1.11 vs main 1.12–1.15：4T 下两者都接近内存层面上限，1T 我们快约 20%。
- GroupLookup 在只覆盖部分行时先把 ID 槽清零，聚合热循环不再逐行掩码（M4 grouped 1T 8.12 → 6.95）。
- 验证：本地与 dev 全量测试通过（120），clippy 无警告。commit `52c1ec1` 已推送。
- 进行中：两台机器的官方对比（`target/main-compare-20261014-m4-onepool`、dev `target/main-compare-20261014-x86-onepool`）。
- 已知：`integrations/datafusion/examples` 在本次改动前就已无法编译（引用不存在的 `roc::suboperator`），不在本任务范围。

### [2026-10-14 19:10] — Verification
- 单池版本（`52c1ec1`）官方对比，3 replicas × 10 samples，ZeroCompile 执行中位数 ms vs 最快参考：
  - M4（`target/main-compare-20261014-m4-onepool`）：**12/12 达标**。1T：scan 0.043/0.051，filter 4.89/8.87，arith 10.22/11.19，div 11.62/15.06，global 0.757/0.859，grouped 6.92/8.59；4T：0.070/0.138，1.43/3.31，2.79/4.95，3.21/5.95，0.282/0.313，1.95/2.43。Compile arith 7.36/2.10、div 7.89/2.24。
  - x86 dev（`target/main-compare-20261014-x86-onepool`）：11/12 达标，global_aggregate 4T 1.219 vs main 1.200（+1.6%，样本区间 1.18–1.34 vs 1.15–1.25 重叠，属噪声级；同 CCX 交错测量中单池 1.15–1.17 vs main 1.17–1.18）。1T：scan 0.136/0.147，filter 13.98/23.76，arith 16.53/21.90，div 20.72/42.13，global 2.60/3.30，grouped 16.90/23.38；4T：0.162/0.171，5.01/7.52，4.80/13.66，5.88/19.24，1.219/1.200，4.94/7.24。Compile arith 7.94/2.98、div 10.89/3.35（相对 ZeroCompile 1.6–2.1×）。
- 结论：global_aggregate 4T 在 4 核同 CCX 配置下两引擎都接近内存层面上限，x86 持平、M4 快 10%；1T 快 20%+。
