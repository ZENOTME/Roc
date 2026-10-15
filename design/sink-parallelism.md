# 设计决策：Row compute 与 Sink 收尾的 StateKernel

状态：设计已确定，待实现。基于 `feat/tpcds` 的 `c98b13a`，更新于 2026-10-15。
相关：[`tpcds-progress.md`](tpcds-progress.md)。

## 1. 决策

**StateKernel 替代 `finish_on`。迁移完成后删除 `SinkExec::finish_on` 和 `exec::Cpu`；
sink 不再提交 CPU job。最终 combine 和 finish 只负责状态交接与结果发布。**

- 一个 pipeline 仍然只有一个 Program，实例仍然持有这个 Program 的 LocalState。
- `ScalarKernel` 改名为 **RowKernel**，`DomainKernel` 改名为 **RowDomainKernel**。
  两者共同构成 row compute 运算体系：变量、row domain、行映射及状态副作用的契约，
  是执行引擎进行向量化和融合的基础。RowDomainKernel 属于这套 row 计算体系。
- **StateKernel** 扩展现有 kernel 模型，直接操作同一个 builder 注册的 state。
  它没有 row domain，独立存放在 `KernelGraph.state_kernels`，由同一个 Program
  在实例末尾执行。它与 row Steps 分离，因此天然形成 row compute 的融合边界。
- sink operator 在 `build_pipeline` 中注册行计算、StateKernel 和 sink 所需的 StateId。
  所有状态共用现有 KernelBuilder 和 LocalState，无需额外的 Program 类型或两组 Program。

第一步迁移接受跨实例计算串行执行，暂时保留 sink 内的同步归并、建表和窗口计算。
随后由 operator 拆分 pipeline，将这些计算放入独占分区的 StateKernel，达到 sink
只交接、发布的最终职责。两步都由 pipeline 执行层调度，sink 不持有 CPU 提交接口。

## 2. 四个 sink 的迁移

| Sink | 实例末尾的 StateKernel | 第一步 combine / finish 中保留的串行工作 | 预期影响 |
| --- | --- | --- | --- |
| Sort | 将 RowsState 局部排序为 SortedRun；有 fetch 时做局部 TopK | 收集有序段、全局归并及发布 | 基本不变 |
| Table | 将 RowsState 中的局部批次拼接 | 汇集局部列、构建全局索引、重排列并发布表 | q23 类查询变慢 |
| Aggregate | 导出 partial，并按 group key hash 分区 | 按分区合并 partial，组装并发布结果 | 高基数 group by 变慢 |
| Window | 无，第一步沿用输出 batch 收集路径 | 对全部输入串行排序、计算窗口并发布 | q51、q47、q57、q67 退回串行 |

这些是迁移前的性能预期，需在同一环境实测。Sort 的最终归并、Table 的全局索引和
Aggregate 的跨实例合并都有实质 CPU 开销；第一步明确接受这些串行工作。

## 3. 数据结构与 row compute 边界

现有 `KernelGraph.kernels: Vec<(DomainId, Kernel)>` 保留为 row compute 的表示。
类型改名后，现有 Kernel 的结构为：

```rust
enum Kernel {
    Scalar(RowKernel),
    Domain(RowDomainKernel),
}

struct KernelGraph {
    // 原有的 row compute 图。
    kernels: Vec<(DomainId, Kernel)>,
    // 按注册顺序，在实例末尾执行。
    state_kernels: Vec<StateKernel>,
    // 其余字段沿用现有定义。
}

struct Program {
    steps: Steps,
    state_kernels: Vec<StateKernel>,
    // 状态工厂、输入输出等字段沿用现有定义。
}
```

StateKernel 是 kernel 模型中的独立类型，不作为绑定 DomainId 的 Kernel 枚举变体。
row lowering 和 codegen 只处理 row compute；StateKernel 不进入 Steps 或 row plan
的执行项。Plan 仍读取它的 state 访问集合，用于状态存活分析。

Program 的两个执行入口分别处理行和末尾状态，以下为目标接口：

```rust
fn run(
    &self,
    batch: &RecordBatch,
    local: &mut LocalState,
    guard: &ShutdownGuard,
) -> Result<Option<RecordBatch>>;

fn finish(
    &self,
    local: &mut LocalState,
    guard: &ShutdownGuard,
) -> Result<()>;
```

`run` 沿用现有逻辑；`finish` 同步执行注册的 StateKernel。每个 StateKernel 可以内部
遍历数据、调用原生排序或其他算法，但不参与 row compute 的向量化与融合。

## 4. StateKernel 的契约

与 RowKernel 的逐行契约对应，StateKernel 的契约是：

1. **输入**：同一个 KernelBuilder 注册的 StateId，以及排序键等静态参数；不读取、
   也不产生 var，不属于任何 row domain。每种 StateKernel 声明读写的 state 集合。
2. **所有权**：执行时独占当前实例的可变状态；共享只读数据可以被引用。共享可变目标的
   获取与交接由执行层或 sink 协调，StateKernel 自身是同步 CPU 运算，不提交任务。
3. **效果**：原地变换 state，或者在同一个 StateId 槽位替换为新类型，例如
   `RowsState → SortedRun`。后续 StateKernel 和 sink 按变换后的类型读取该槽位。
4. **时机**：正常结束时，每个执行实例在最后一次输入之后、combine 之前执行一次
   `Program::finish`；其中的 StateKernel 按注册顺序各执行一次。提前停止和失败的
   行为见第 7 节。
5. **空状态**：必须正确处理空输入对应的初始状态。空输入和 `program.idle()` 路径
   也执行 finish 和 combine，例如生成空 SortedRun、保留全局 COUNT 的零值语义。
6. **取消**：开始执行及长时间循环中检查 ShutdownGuard，及时返回取消错误。排序或
   归并等长计算需要在其工作阶段中提供检查点。
7. **错误**：直接返回 `Result`，由 Program / 执行层传播；它没有 row var，因此不采用
   RowKernel 的 error flag / RaiseIf 表达方式。失败的局部状态不交给 combine。
8. **模式一致**：Compile 和 ZeroCompile 使用同一份原生 StateKernel 实现。
   row compute 两种模式的等价结果交给相同收尾逻辑，末尾阶段无需独立 codegen。

## 5. Append 与 operator 注册

`Append { state, columns }` 是带副作用的 **RowKernel**，与 AggregateUpdate 同类。
它把当前 domain 中的行追加到 RowsState；即使没有输出 var，也必须执行。RowsState
拥有或保留后续使用所需的数据，不保存会随着当前 batch 或 scratch 复用而失效的借用。

Sort、Table 通过 Append 积累局部行；Aggregate 继续更新分组和 accumulator state。
Append 的当前迁移范围是无序生产 pipeline。ordered pipeline 和需要保序的 Collect
保留现有输出 batch 路径，避免跨实例汇集丢失输入顺序。

同一个 KernelBuilder 内注册即可共享 StateId。示例 API 表示目标注册方式：

```rust
// SortOperator::build_pipeline 里
let rows = builder.state(StateBuilder::new(|| Ok(RowsState::default())));
builder.append(rows, columns)?; // RowKernel::Append
builder.state_kernel(StateKernel::Sort { state: rows, keys })?;
let sink = SortSinkExec::new(rows); // 接收同一槽位中的 SortedRun
```

同一个 Program 创建 LocalState、运行 Append、执行 Sort，并把完成的 LocalState
交给 sink。sink 直接按 rows 取出 SortedRun。

## 6. Builder 校验与状态存活分析

“StateKernel 只在末尾执行”需要以下约束：

- StateKernel 引用的 StateId 必须属于当前 builder 的已注册状态。
- **同一 Program 中，被 StateKernel 写过的 state，row kernel 不能作为数据源读取**，
  例如 TableGather 或 Expand::Lookup 不能读取末尾才构建好的表。需要这种读写依赖时，
  operator 必须拆成具有依赖关系的 pipeline。
- Append、GroupLookup、AggregateUpdate 对局部状态的构建和累积属于生产侧副作用，
  允许随后由 StateKernel 整理该状态。访问分析要区分这种状态更新与 TableGather、
  Lookup 读取状态内容作为行计算输入，不能用“引用了同一个 StateId”一概拒绝。
- 校验覆盖整个图，与注册先后顺序无关；后来增加的 row kernel 也必须满足约束。
- StateKernel 之间按注册顺序检查状态变换的输入输出类型；例如 Sort 后该槽位是
  SortedRun，后续消费者应按 SortedRun 契约访问。

Plan 的死状态分析必须把 StateKernel 读写的 state 算作活跃。否则 `Program.kinds`
中的槽位会是 None，`new_local` 就不会调用其 StateBuilder。仅被 StateKernel 使用的
state 也必须创建，并保持原 StateId 槽位。存活标记和新增状态类别应支持 RowsState
等普通状态，不应为通过分析而冒用 Table / Accumulator 的运行时布局。

Append 的 `has_effect`、输入 var、state 访问和 domain placement 要进入现有分析，
保证它及其输入计算不会因没有输出列而被删除。

## 7. 执行层改动

以下位置以 `c98b13a` 的 [`pipeline/execution.rs`](../src/pipeline/execution.rs) 为基准。
保持“所有 CPU job 成功后才 combine”的语义，末尾阶段不与尚未结束的输入 job 重叠。

**多 worker 路径（当前第 378 行的 combine 循环之前）：**

1. 沿用现有循环，等待所有 row CPU job 成功，取得 idle 中的全部 LocalState。
2. 若 sink 未返回 Finished，为每个 local 通过现有 `on_cpu` 提交一个 finish job：
   调用 `program.finish(&mut local, &guard)`，成功后返回 local。
3. 所有 finish job 成功后，按提交时的 local 顺序依次调用 sink.combine。
4. 沿用 source.finish，再调用 sink.finish，发布结果并放行下游。

**单 worker 和 ordered 路径（当前第 201 行 combine 之前）：**

在当前执行上下文中直接同步调用 `program.finish(&mut local, &guard)`，成功后继续
原有 combine 和 source/sink 收尾。沿用现有同步执行的 panic/error 边界。

其他分支的行为明确如下：

| 情况 | Program::finish | 后续行为 |
| --- | --- | --- |
| 输入正常结束，包括空输入 | 每个 local 执行一次 | 全部成功后 combine，最后发布 |
| program.idle() 为真 | 对新建的 local 仍执行一次 | 不读取输入，继续正常 combine 和发布 |
| sink 返回 Finished | 跳过 | 保持原有提前停止流程，包括 combine、source.finish 和 sink.finish |
| row job 失败 | 不执行 | 不 combine、不发布末尾结果 |
| 任一 finish job 失败或取消 | 传播错误，局部结果作废 | 不 combine、不发布末尾结果 |

上表的 Finished 只跳过 **Program::finish**。原有 source/sink 生命周期仍须完成；
提前停止的 sink 应能接收未经该末尾变换的 local。本次通过 Append 迁移的阻塞 sink
在正常输入阶段不返回 Finished。已经流式交付的输出沿用现有错误语义。

## 8. 跨实例工作与后续并行化

第一步接受第 2 节列出的跨实例部分串行执行，保持现有串行 combine 接口；各 sink
通过同步函数完成归并和发布，全程不再依赖 Cpu。

之后由 operator 拆成两条 pipeline 恢复并行：前一条 pipeline 末尾的 StateKernel
做 hash 分区；后一条 pipeline 将每个分区完整交给一个实例，在实例末尾做排序和
窗口计算。计算仍由 Program 执行，调度仍在 pipeline 图中，不需要旁路调度。

这一步需要明确的“完整分区归一个实例”分发能力，普通 batch 切片分发不能代替该
契约。同一窗口 partition 的行必须汇集齐备；局部排序得到的 runs 仍需全局归并。
状态 kernel 的完成本身是局部事件，跨实例完成依赖由 pipeline 图表达。

## 9. 迁移步骤与验证

1. 将 ScalarKernel / DomainKernel 改名为 RowKernel / RowDomainKernel；实现 Append、
   StateKernel、builder 校验和状态存活分析，增加 Program::finish 及执行层的并行 finish。
2. 迁移 Sort：Append → 局部 Sort StateKernel → 串行归并，验证结果与排序并测量性能。
3. 迁移 Table 和 Aggregate：局部拼接、partial 导出及分区放入 StateKernel，跨实例
   combine / finish 使用同步实现。
4. Window 切回同步排序和窗口计算，移除所有剩余 Cpu 调用，然后删除 SinkExec::finish_on、
   exec::Cpu、ExecutorCpu 及相关适配代码。pipeline 统一调用 sink.finish。
5. 重新运行 q51、q47、q57、q67、q23；记录两种模式、1 / 多 worker 的正确性和耗时，
   对比迁移前相同配置，报告串行退化。

测试至少覆盖：

- Compile / ZeroCompile 结果一致，排序与窗口保持要求的顺序和 peer/frame 语义。
- 空输入、program.idle()、全局 COUNT = 0，以及 StateKernel-only state 的创建。
- 多次 run 后只调用一次 finish，StateKernel 注册顺序、槽位类型变换及 sink 取出结果。
- builder 拒绝 row kernel 读取末尾变换的 state；允许 Append / aggregate 的生产侧更新。
- 长任务响应取消；单 worker 和多 worker 的 finish 出错均不调用 combine、不发布末尾结果。
- sink 返回 Finished 跳过 Program::finish，并保留 source/sink 正常收尾；ordered 和保序
  Collect 保持原路径。

本文仅记录设计；本次文档更新未修改执行代码或运行性能测试。
