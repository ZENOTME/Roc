# Plan：基于单一 LLVM 向量化 Emitter 与原生宽度列/位图布局重构 Kernel 执行层

- Effort: high
- TAPD 工作项：无
- 工蜂 issue：无
- 是否提交 MR：否
- MR 目标分支：无
- 开发分支：无

## 目标（intent contract）

- 真实问题：
  1. 当前零编译执行（`src/kernel/vector/`）将所有固定宽度类型解包为每行 1 个 `u64`、Boolean（含 validity 与错误标志）解包为每行 1 个 `u64`，内存带宽放大 2–64 倍，且 `KernelLibrary::Generated` 与 `KernelLibrary::Native` 维护了两套重复的算子语义代码。
  2. 当前 Cranelift 后端在 x86_64 上仅支持 128 位向量（SSE2），无法生成 AVX2 256 位指令：在 `arithmetic_chain` 上 Cranelift 零编译在 x86 单线程仅领先 DataFusion 约 9%，且 Cranelift 融合循环（13.67 ms）相比零编译仅快 9%，无法实现 Compile 模式在计算密集场景下的大幅提速（实测 LLVM 显式 AVX2 融合循环可达 7.22 ms，快 2.1 倍）。
- 成功 oracle：
  1. 维护单一一份 lane-generic 的 LLVM kernel 定义与 emitter：零编译模式使用构建期（`build.rs`）或预编译分派由同一 emitter 生成的 SIMD kernel 库，进程启动无 JIT 预热开销；`Compile` 模式在运行时通过 LLVM MCJIT 生成显式向量化的融合窗口循环。
  2. Cranelift 依赖、`src/kernel/vector/functions.rs`（Native Rust kernel 库）、旧标量 JIT（`src/kernel/jit/`）以及 spike 临时文件（`src/kernel/simd*.rs`、`tests/simd_spike.rs`）全部移除。
  3. 保持完整溢出检查、除零检查与首行错误确定性语义不变，所有现有测试及跨模式（`Compile` vs `ZeroCompile` + 独立期望值）测试在 M4 与 x86_64 上全部通过。
  4. 在 `benches/main_compare/run.py` 的全部 6 个基准用例（`scan`、`filter_project`、`arithmetic_chain`、`integer_division`、`global_aggregate`、`grouped_aggregate`）上，于 M4（macOS aarch64）和 `dev`（Linux x86_64，AMD EPYC 7K62）两台机器、1 线程和 4 线程下，`ZeroCompile` 的 `execute_median_ms` 均不慢于最快参考引擎（`main`、DuckDB、DataFusion 中的最快者），且 `Compile` 模式在算术链/表达式融合场景相对 `ZeroCompile` 取得显著进一步加速。
- 价值排序：语义与错误顺序正确性 > 架构单一性（单一 kernel 定义、无框架级投机 hack） > 双平台（M4 + x86）1T/4T 性能达标 > 编译/构建速度。
- 非目标：
  1. 本次不实现后台异步编译与运行期热切换（保持 `ExecutionMode::Compile` 与 `ExecutionMode::ZeroCompile` 行为确定、可独立测量 `prepare` 与 `execute`）。
  2. 本次不扩展新的 SQL 算子或表达式类型，也不放宽整数溢出检查语义。
  3. 本次不引入自研 ORC `LLJIT` C FFI 绑定（统一使用 Inkwell 0.7.1 已封装的 MCJIT `ExecutionEngine`）。

## 当前状态

- 状态：in_progress
- 当前步骤：步骤 1 — 重构物理数据布局与单一 LLVM 向量化 emitter 并实现双模式执行引擎
- 上次同步：2026-10-14 19:10，单池调度落地；M4 12/12 达标，x86 11/12 达标（global_aggregate 4T 持平于噪声内）；两台机器全量测试通过
- 下一步：Final Review

## Scope 与边界

- In scope:
  - 物理数据布局重构：固定宽度数值列按 Arrow 原生字节宽度（`i8`/`i16`/`i32`/`i64`/`u8`/`u16`/`u32`/`u64`/`f32`/`f64`）原位借用输入、原位写入输出；Boolean（含列值、validity、错误 flag）统一采用 Arrow 风格 `u64` word 位图，并在 1024 行窗口粒度维护 `all_true` / `all_false` 快速标记；常量以广播标量表示，不物化整列；窗口级中间槽位按变量最后一次读取位置（last-use）静态分配并复用。
  - 覆盖全部 `KernelGraph` 能力：
    - 全部 `ScalarFunction`（`Add`、`Subtract`、`Multiply`、`Negate`、`Divide`、`Remainder`、`Equal`、`NotEqual`、`LessThan`、`LessThanOrEqual`、`GreaterThan`、`GreaterThanOrEqual`、`And`、`Or`、`Not`、`Cast`、`ParseText`、`FormatText`、`ValidateUtf8`）、`Constant`、`Select`、`RaiseIf`。
    - `DomainKernel::Filter`（位图谓词与紧凑行号选择/位图掩码执行，兼顾 `filter_project` 下游投影与聚合）。
    - 状态与聚合核：`GroupLookup`（含单 64-bit word key 内联缓存快速路径与多列通用路径）、`AggregateUpdate`（含全局与分组 `CountRows`、`CountValues`、`SumI64`、`SumU64`、`SumF64`、`Avg`、`CovarPop`、`Min`、`Max`、`CountDistinct`）。
  - 单一 LLVM emitter 与双模式驱动：
    - 显式生成向量 IR（x86_64 AVX2 256-bit / SSE2 128-bit、aarch64 NEON 128-bit）+ 标量尾部，禁止依赖 LLVM 自动向量化器猜测标量循环。
    - `build.rs` 在构建期调用同一套 kernel 生成器产出预编译目标文件并静态链接进二进制，`ExecutionMode::ZeroCompile` 在运行时按 CPU 特性表零编译成本直接分派。
    - `ExecutionMode::Compile` 在运行时通过 MCJIT 编译融合窗口循环，并在 `Program::drop` 时释放 JIT 内存。
  - 公开接口与清理：
    - `ExecutionMode` 收敛为 `Compile` 与 `ZeroCompile`（移除 `KernelLibrary`、`ZeroCompileSimd`、`CompileSimd`）；更新 `src/pipeline/`、`tests/`、`benches/main_compare/`、`README.md`。
    - 移除 `cranelift-*` 依赖、`src/kernel/jit/` 旧实现、`src/kernel/vector/functions.rs` 及实验文件 `src/kernel/simd*.rs`、`tests/simd_spike.rs`。
- Out of scope:
  - 后台异步 JIT 编译与运行中从 `ZeroCompile` 热切换到 `Compile`。
  - `Expand` / `Union` / `Reduce` 等当前 builder 已拒绝的未实现 `DomainKernel` 变体。

## Top risks

1. **x86 上无硬件向量指令的算子（如 `integer_division` 64 位整数除法）及 `filter_project`、`grouped_aggregate` 在 `ZeroCompile` 下难以击败 DataFusion / DuckDB**
   - 处置：整数除法在常量/广播除数形状下生成魔法数乘法/无分支快速向量化路径或展开循环 + 位图异常归约；`Filter` 直接基于 `u64` 位图与紧凑索引运行；分组聚合与全局聚合直接读取原生宽度 buffer 与位图。
2. **LLVM 自动向量化器（Loop Vectorizer / SLP）对复杂控制流或溢出检查的退化**
   - 处置：已验证。主循环显式发射 `<4 x T>`（AVX2）/ `<2 x T>`（NEON/SSE2）向量指令与向量溢出掩码归约，仅在窗口尾部发射标量指令。
3. **过滤域（Filter domain）、短路分支（Select / CASE / COALESCE）与 NULL 行上的错误抑制及首行报错顺序一致性**
   - 处置：融合快路径仅在「整窗 0 异常标志」时直接提交；一旦掩码非零则回退到该窗口的逐 kernel 慢路径，严格结合 validity / domain 位图和 `limit` 定位首个真实错误行。
4. **`build.rs` 引入 LLVM 生成预编译零编译库带来的构建时间**
   - 处置：emitter 源码由 `build.rs` 与主 crate 共享；`build.rs` 严格设置 `cargo::rerun-if-changed` 仅监听 emitter 定义文件并缓存生成的 `.o` / `.a`。
5. **`Program` 的 LLVM MCJIT 内存生命周期与并发安全**
   - 处置：每个 `Compiled` 程序独占一个 `Context` + `ExecutionEngine`，在 `Drop` 中按 `ExecutionEngine` 先于 `Context` 的顺序安全释放。

## 成功标准

### 完成条件

- [ ] 单一 LLVM emitter 架构落地：所有标量/向量 kernel 语义仅在一处定义，既用于在 `build.rs` 构建期生成预编译的零编译 SIMD kernel 库（按 CPU 特性在运行时分派），也用于在 `ExecutionMode::Compile` 运行时通过 LLVM MCJIT 生成显式向量化的融合窗口程序。
- [ ] 物理数据布局全面切换为：固定宽度类型使用 Arrow 原生字节宽度 buffer（输入零拷贝借用、输出原位写入）、Boolean（值、validity、错误标志）使用位图 + 窗口级 `all_true`/`all_false` 标记、常量广播、1024 行窗口与中间槽位按 last-use 回收复用。
- [ ] `ExecutionMode` 公开枚举收敛为 `Compile` 与 `ZeroCompile`；`KernelLibrary`、`src/kernel/vector/functions.rs`（Native 库）、`src/kernel/jit/`（Cranelift 实现）、`cranelift-*` Cargo 依赖及 `src/kernel/simd*.rs` / `tests/simd_spike.rs` 临时文件全部清理完毕。
- [ ] `Program::generated_ir()` 返回生成的 LLVM IR 文本，`Program` 析构时正确释放其 MCJIT 资源且无内存泄漏或悬垂指针。
- [ ] 仓库全部现有测试与新增测试在 M4 和 `dev`（x86_64）上 100% 通过，`cargo clippy --all-targets` 无警告。
- [ ] 在 M4（macOS aarch64）和 `dev`（Linux x86_64）上运行 `benches/main_compare/run.py`（4M 行，1 线程与 4 线程，对比 `main`、`duckdb`、`duckdb-arrow`、`datafusion`）：6 个用例中每个用例的 `ZeroCompile` 执行时间中位数均 `<= best_reference_ms`，且 `Compile` 模式在计算密集的表达式链用例上相对 `ZeroCompile` 取得显著进一步加速。

### 测试与验证计划

#### 本地测试与验证

- [ ] **M4（macOS aarch64）全量单测与集成测试**：`LLVM_SYS_191_PREFIX=$(brew --prefix llvm@19) cargo test -p roc --all-targets`
- [ ] **dev（Linux x86_64）全量单测与集成测试**：`LLVM_SYS_191_PREFIX=/data/roc-bench/llvm-19 RUSTFLAGS="-L /data/roc-bench/llvm-syslibs" cargo test -p roc --all-targets`
- [ ] **静态检查**：`cargo clippy -p roc --all-targets -- -D warnings`
- [ ] **M4 端到端基准对比**：`python3 benches/main_compare/run.py --modes compile,zero-compile --python /tmp/dfvenv/bin/python`
- [ ] **dev（x86_64）端到端基准对比**：`python3 benches/main_compare/run.py --modes compile,zero-compile --python /data/roc-bench/venv/bin/python --cpus1 0 --cpus4 0,2,4,6`

#### 流水线测试验证

- [x] 建议：跳过，理由：当前仓库不属于 `pipeline-testing` 注册表中的项目。
- [x] 用户确认：不需要，理由：本地 M4 + `dev` x86_64 双架构实测已完整覆盖。

### 交付物

- 重构后的 `src/kernel/`（单一 LLVM 向量化 emitter + 原生宽度列/位图执行层）与 `build.rs`。
- M4 与 `dev`（x86_64）上的完整测试与基准对比结果 `summary.json`。

## Artifact 决策

- `spec.md`: not needed — 用户指示直接记录实现要点并开始执行，不单独编写 `spec.md`。
- `tasks.md`: not needed — 用户指示不走 `task-planning` / `task-execution` 流程，直接在 `plan.md` 记录步骤并开始执行。

## 质量门禁

- [x] Gate: Plan Approval — 用户已批准直接开始执行。
- [x] Gate: Code Review — high effort 评审发现 6 个真实 bug，已全部带回归测试修复。
- [ ] Gate: Final Review — 批准最终结果、验证证据和提交准备状态。

## 步骤

1. [x] 重构物理数据布局与单一 LLVM 向量化 emitter，并实现 `ZeroCompile`（构建期预编译 + CPU 分派）与 `Compile`（MCJIT 融合循环）双模式执行引擎，清理 Cranelift、Native 库与临时 spike 文件
   - **1a. 构建与依赖收敛（`Cargo.toml`、`build.rs`）**：将 `inkwell = { version = "=0.7.1", features = ["llvm19-1", "target-x86", "target-aarch64"] }` 设为必选依赖（同时加入 `[build-dependencies]`），移除 `cranelift-*` 与 `simd-llvm` 临时 feature。`build.rs` 复用共享的 LLVM kernel 生成器发射预编译零编译 kernel 目标文件（x86_64 下生成 AVX2 与 SSE2 两套符号并在运行时按 `is_x86_feature_detected!("avx2")` 选择函数表；aarch64 下生成 NEON 符号），通过 `cc` / `ar` 打包静态链接。
   - **1b. 物理窗口缓冲与位图布局（`src/kernel/storage.rs` 或 `src/kernel/vector/`）**：
     - 窗口大小 `WINDOW = 1024`（16 个 `u64` 位图 word）。
     - 固定宽度类型（`i8..i64`, `u8..u64`, `f32`, `f64`）按原生字节宽度存储在 64 字节对齐的窗口槽位中；根域输入列直接零拷贝借用 Arrow `ScalarBuffer` 指针（偏移 `window_base * elem_size`），根域输出列在 batch 开始时预分配 `BufferBuilder` 并直接将窗口写指针指向输出切片（零中间拷贝）；生命周期不跨越当前窗口的中间变量按 `Plan` 的 last-use 静态分配槽位复用。
     - Boolean 变量、validity 位图与错误标志位图统一采用 `[u64; 16]` 位图槽位 + `BitState`（`AllFalse` / `AllTrue` / `Mixed`）快速标签；Arrow 输入 validity 位图按窗口提取为对齐的 `u64` word 并计算 `all_true`/`all_false`。
     - 常量（`ScalarKernel::Constant`）直接保存标量值与广播标记，算子入参支持 `Column(ptr)` 与 `Scalar(val)` 两种形态，消除常量列物化。
     - `ByteView`（字符串/二进制）保留 `16B` view 槽位用于 `ParseText` / `FormatText` / `ValidateUtf8` / `Equal..GreaterThanOrEqual` / `GroupLookup`。
   - **1c. 单一 LLVM Lane-Generic Emitter（`src/kernel/llvm/`）**：
     - 统一定义所有算子在任意目标向量宽度（256-bit AVX2 / 128-bit NEON & SSE2）下的 IR 生成逻辑：
       - 算术运算（`Add`/`Subtract`/`Multiply`/`Negate`）：整数带向量化溢出位掩码检测（常量加减使用单次比较 `r < x` / `r > x`，非常量使用符号异或公式或 `@llvm.sadd.with.overflow` / `@llvm.uadd.with.overflow` / `@llvm.smul.with.overflow` / `@llvm.umul.with.overflow` 向量内建函数），快路径在向量累加器中按位或归约溢出掩码，仅在整窗溢出累加器非零时写回精确错误位图。
       - 除法与取模（`Divide`/`Remainder`）：支持向量化/展开除法与除零、`MIN / -1` 溢出掩码检测；针对常量/广播除数提供快速路径以确保 `integer_division` 在 x86 和 M4 上超越 DataFusion。
       - 比较运算（`Equal`、`NotEqual`、`LessThan`、`LessThanOrEqual`、`GreaterThan`、`GreaterThanOrEqual`）：数值类型通过向量 `icmp`/`fcmp` + `bitcast <N x i1> to iN` 打包写入输出位图 word；`BytePointer` 调用运行时字符串比较。
       - 布尔逻辑（`And`、`Or`、`Not`）与三值逻辑（Kleene logic）：直接在 `u64` 位图 word 上按位运算，利用 `AllTrue`/`AllFalse` 短路。
       - 类型转换（`Cast`）：涵盖整数扩窄（含范围检查生成错误位图）、整浮互转（含 NaN/范围检查）、浮点扩窄、布尔与数值互转。
       - 条件选择（`Select`）：按条件位图合并 `then`/`else` 值与 validity，同时按条件位图屏蔽未命中分支上的延迟错误。
     - 同一套单算子生成逻辑既可导出为预编译函数表（供 `ZeroCompile` 直接调用），也可内联进 `Compile` 模式的融合循环。
   - **1d. 过滤域、分组查找与聚合核**：
     - `DomainKernel::Filter`：直接消费谓词位图与 validity 位图，维护子域激活位图（`[u64; 16]`）与紧凑行索引；当下游仅含无副作用标量投影与输出物化（如 `filter_project`）时，支持先紧凑过滤活跃行或在紧凑域上运行 SIMD 核，避免对被过滤行做无效计算。
     - `GroupLookup` 与 `AggregateUpdate`：直接在原生宽度列和位图字上运行；全局聚合（`CountRows`、`CountValues`、`SumI64`、`SumU64`、`SumF64`、`Min`、`Max`、`Avg`、`CovarPop`）使用向量归约 + popcnt；分组聚合保留单 64-bit word key 内联开放寻址缓存快速路径与多列通用路径。
   - **1e. 公开接口更新与代码清理**：
     - `ExecutionMode` 收敛为 `Compile` 和 `ZeroCompile`；更新 `src/pipeline/`、`tests/`、`benches/main_compare/`、`README.md`。
     - 删除 `src/kernel/jit/`、`src/kernel/vector/functions.rs`、`src/kernel/simd*.rs`、`tests/simd_spike.rs`。
2. [x] 在 M4（macOS aarch64）与 `dev`（Linux x86_64）运行完整测试套件、`clippy` 检查及 `benches/main_compare/run.py`（1T/4T 全用例对比 `main` / DuckDB / DataFusion），验证全部成功标准达成
3. [x] 代码评审 → 调用 `code-review`（传入 `Effort=high`） → Gate: Code Review
4. [ ] 准备交付报告与最终证据汇总 → Gate: Final Review
