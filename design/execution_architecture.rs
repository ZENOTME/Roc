//! Roc 执行抽象：本文件是自上而下的 review 入口，也是可运行示例。
//!
//! cargo run --example execution_architecture --features jit
//!
//! 以 Scan(x) -> Project(x+1) -> Filter(x<10) -> SUM(x) 为例：
//!
//! 1. 上层提交确定类型/schema 的物理树与表达式描述（src/operator、src/expr）。
//!    ReferenceExpression(0) 表示当前输入第 0 列。用户不必知道 Variable，
//!    也不需要提供求值方法；因此反序列化生成物理计划仍然可行。
//!
//! 2. build_pipeline_graph(tree, mode) / PipelineGraphBuilder::finish(mode)。
//!    Operator::build_pipeline 按阻塞依赖拆 pipeline，先递归构建 child，
//!    再直接向 PipelineBuilder.suboperators 追加当前算子的 suboperator。
//!    因此 Reference 可以引用上游已经确定的 Variable，无需暂存 operations。
//!    mode 在这里决定；返回的图已经可执行，运行时没有再次 prepare 的入口。
//!
//! 3. operator / expression 各自的 build_suboperator。
//!    AggregateOperator 创建分组状态与 GroupLookup；AggregateExpression 创建
//!    accumulator 与 AggregateUpdate。标量表达式各自递归构建参数与计算节点。
//!    SuboperatorBuilder 只注册 state/变量/单个 suboperator，维护列绑定和校验。
//!    不保存物理算子，不遍历表达式树，也没有统一的 aggregation::lower。
//!    x+1 产生 Variable；后面的 Reference(0) 直接引用它，不强制物化投影。
//!    Filter 加入 Materialize suboperator，选出行并更新后续 source 数组。
//!    这条边界保证被过滤掉的行不会在后续表达式中触发除零等错误。
//!    聚合下降为 GroupLookup 与各个 AggregateUpdate；分组表和 accumulator
//!    都通过普通 StateId 声明，没有聚合专用的执行上下文字段。
//!
//! 4. SuboperatorContext::compile(mode) -> ProgramBuilder::build。
//!    builder 按依赖绑定原生 kernel；JIT 将支持的计算组合为 Cranelift 循环。
//!    编译期可以拆分行域，但输出始终是一个扁平的 Program 列表。过滤、投影
//!    物化与 sink local update 本身也是 Program，不存在外层 region 解释器。
//!    编译完成即丢弃 SuboperatorContext、producer 表和编译模式。
//!
//! 5. ProgramContext（src/suboperator/program.rs）只有两个字段：
//!    programs: Vec<Program>、state_builders: Vec<StateBuilder>。
//!    Program 声明 inputs/outputs/states，绑定 call、kernel 参数或机器码 owner，
//!    并记录最后使用后可释放的变量。机器码 owner 防止函数指针悬空。
//!    原生/JIT 使用同一执行契约；CASE/COALESCE 的分支只运行被选中的行。
//!
//! 6. ProgramExecutionContext 保存独占的 Vec 状态槽和数组槽。
//!    ID 带作用域，用于拒绝跨计划误绑定；槽位按 index 访问，不逐行查 HashMap。
//!    StateBuilder 是普通类型工厂；常量、哈希表、累加器和 sink local state
//!    一视同仁。当前 sink 的状态都由工厂创建，无需额外外部 executor 绑定。
//!    ColumnBuffer 保留 Arrow owner/offset/validity/children，无需复制导入。
//!
//! 7. Pipeline（src/pipeline/mod.rs）持有 source、ProgramContext 和 sink。
//!    sink 直接是 Box<dyn SinkExec>。聚合算子先 new_execute_state，再
//!    build_suboperator 注册 GroupLookup/AggregateUpdate 与普通 StateId，
//!    最后 build_exec 将合并状态和 StateId 交给具体 sink。
//!    source/sink 仅共享 OnceLock 中的最终结果；合并状态由 sink 独占，
//!    combine(&mut self, ProgramExecutionContext, guard) 用 StateId 取出
//!    每份 local state 并合并。这样无需给合并状态加锁或保留通用全局引用。
//!    Exchange 把 pending batches 与输出连接放入普通 ExchangeLocalState；
//!    BatchUpdate 程序同步入队，ExchangeSinkExec::flush 异步发送。
//!    自定义结果收集器同样注册 Vec<RecordBatch> 状态，combine 时取出结果。
//!
//! 8. Pipeline::execute（src/pipeline/execution.rs）是唯一的异步协调循环。
//!    SourceExec 自己持有游标与资源，start 才启动 I/O，构图不会启动扫描。
//!    创建至多 parallelism 份独占 local；异步读取 batch，
//!    将 batch/local 移入 Rayon，按顺序执行 Program（含 sink local update）。
//!    返回的 local 先 async flush，再回到池中复用，不绑定线程，不另建 worker。
//!    同时等待 source 和 CPU 完成，避免读下一批时阻塞上一批的异步输出。
//!    pending read 保持到完成，避免要求每种 source 都支持取消后重试。
//!    提前停止只丢弃借用 source 的读取 future，source 本身仍可 finish。
//!    批次大小和 local 数量限定在途工作；每个 Program 之间检查取消。
//!    EOF 且所有工作成功后逐份传入 local 做 combine，再调用 source/sink.finish。
//!    空输入仍初始化并 combine，保证无分组 COUNT 返回 0 而不是丢失结果。
//!
//! 9. 验证入口。
//!    PipelineGraph::explain() 打印实际 Program 列表；准备时间就是构建时间。
//!    src/suboperator/program_tests.rs 验证连接、过滤、普通状态和融合。
//!    src/pipeline/execution.rs 验证单 source、状态复用、背压、失败与取消。
//!    tests/physical_expressions.rs 覆盖完整计划；全部现有算子/类型保留语义。
//!    标准数值/布尔可 JIT；整数除余、通用 cast、复杂类型和聚合使用原生 kernel。
//!    原项目没有的 join/sort 不在现有算子范围，微基准不代表整查询性能。

use arrow::{
    array::{ArrayRef, AsArray, Int64Array},
    datatypes::{DataType, Int64Type},
};
use roc::{
    error::Result,
    expr::{
        ExpressionResultType,
        scalar::{ConstantExpression, FunctionExpression, FunctionKind, ReferenceExpression},
    },
    suboperator::{ExecutionMode, ProgramContext},
};
use std::sync::Arc;

fn build_program(mode: ExecutionMode) -> Result<ProgramContext> {
    // 用户仍然描述表达式，不需要知道 VariableId。
    let x =
        ReferenceExpression::new(0, ExpressionResultType::new(DataType::Int64, true)).into_ref();
    let multiply = FunctionExpression::binary(
        FunctionKind::Multiply,
        x,
        ConstantExpression::int64(Some(2)).into_ref(),
        DataType::Int64,
        true,
    )
    .into_ref();
    let output = FunctionExpression::binary(
        FunctionKind::Add,
        multiply,
        ConstantExpression::int64(Some(1)).into_ref(),
        DataType::Int64,
        true,
    )
    .into_ref();
    let program = ProgramContext::from_expressions(&[output], mode)?;
    Ok(program)
}

fn run(mode: ExecutionMode) -> Result<()> {
    let program = build_program(mode)?;
    // 每个并行实例独占 local；ProgramContext 可以被多个实例只读使用。
    let mut local = program.new_execution_context();
    let input: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))];
    let output = program.execute(&input, 3, &mut local)?;
    assert_eq!(
        output[0]
            .as_primitive::<Int64Type>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(3), None, Some(7)],
    );
    // 保留输出的 Arrow owner，后续调用不会覆盖已经交出的结果。
    let empty: Vec<ArrayRef> = vec![input[0].slice(0, 0)];
    assert!(program.execute(&empty, 0, &mut local)?[0].is_empty());
    println!("{mode:?}: [3, NULL, 7], compiled={}", program.is_compiled());
    Ok(())
}

fn main() -> Result<()> {
    run(ExecutionMode::Native)?;
    #[cfg(feature = "jit")]
    {
        run(ExecutionMode::Compiled)?;
        run(ExecutionMode::Staged)?;
    }
    Ok(())
}
