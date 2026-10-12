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
//!    SuboperatorScopeBuilder 只注册 state/变量/单个 suboperator，维护列绑定和校验。
//!    不保存物理算子，不遍历表达式树，也没有统一的 aggregation::lower。
//!    x+1 产生 Variable；后面的 Reference(0) 直接引用它，不强制物化投影。
//!    Filter 加入 ScopeSuboperator::Filter，封住当前普通节点段并打开嵌套 scope。
//!    列绑定与 SourceId 不变；祖先变量可以在子 scope 使用，不提前物化整个 batch。
//!    scope 内按数据依赖生成，ScopeBreaker 最后生成，再进入下一段。
//!    ScopeSuboperator 可以提供 body 输出变量，只在进入 body 后可见。
//!    DependencyToken 表达无数据的先后依赖，不占运行时数组槽；
//!    with_dependencies 为新建节点绑定前置 token，状态更新也可以返回 token。
//!    scope token 表示 body 已进入，不表示循环结束。
//!    聚合下降为 GroupLookup 与各个 AggregateUpdate；分组表和 accumulator
//!    都通过普通 StateId 声明，没有聚合专用的执行上下文字段。
//!
//! 4. SuboperatorScope::compile(mode) -> ProgramBuilder::build。
//!    按 scope 链生成一个行循环。普通节点使用 LocalEmitContext；只有
//!    ScopeEmitContext 可以留下跨节点的 if，完成子 scope 后逆序关闭。
//!    CASE/COALESCE 使用局部控制块与合并参数，未选中分支不会执行。
//!    数字中间值保留原生 SSA 类型；类型判断在编译时完成。
//!    算术、比较、除余、数字 cast 直接生成指令；字符串转换和状态操作
//!    生成具体标量 helper 调用，不存在批量 kernel 回退。
//!
//! 5. ProgramContext 保存机器码 owner、常量、已绑定的 state 与批次布局。
//!    编译完成丢弃构建图。Program 仅为可读的编译期调度元数据，不执行。
//!    generated_ir() 可查看类型已经确定的 Cranelift IR。
//!    执行时直接调用一个生成入口，没有 call_native/BoundOperation 分发。
//!
//! 6. ProgramExecutionContext 保存独占状态槽。
//!    批次边界负责 Arrow 数组和标量 ABI 存储的转换，局部运算不创建数组。
//!    Filter 对后续代码打开 if；中间变量只存在于正确的 SSA 作用域。
//!    输出端口记录存活行；直接转发且未删行的输入列保留原 Arrow owner。
//!    每 1024 行检查取消；helper 错误通过调用状态返回，不跨 JIT 栈展开。
//!    数组、schema 是 Arrow 的生产依赖；compute/row 仅可用于测试参照。
//!
//! 7. Pipeline（src/pipeline/mod.rs）持有 source、ProgramContext 和 sink。
//!    sink 直接是 Box<dyn SinkExec>。聚合算子先 new_execute_state，再
//!    build_suboperator 注册 GroupLookup/AggregateUpdate 与普通 StateId，
//!    最后 build_exec 将合并状态和 StateId 交给具体 sink。
//!    source/sink 仅共享 OnceLock 中的最终结果；合并状态由 sink 独占，
//!    combine(&mut self, ProgramExecutionContext, guard) 用 StateId 取出
//!    每份 local state 并合并。这样无需给合并状态加锁或保留通用全局引用。
//!    Exchange 把 pending batches 与输出连接放入普通 ExchangeLocalState；
//!    生成代码收集输出，批次成功后 BatchUpdate 同步入队，flush 异步发送。
//!    自定义结果收集器同样注册 Vec<RecordBatch> 状态，combine 时取出结果。
//!
//! 8. Pipeline::execute（src/pipeline/execution.rs）是唯一的异步协调循环。
//!    SourceExec 自己持有游标与资源，start 才启动 I/O，构图不会启动扫描。
//!    创建至多 parallelism 份独占 local；异步读取 batch，
//!    将 batch/local 移入 Rayon，调用生成入口，成功后发布 sink 批次。
//!    返回的 local 先 async flush，再回到池中复用，不绑定线程，不另建 worker。
//!    同时等待 source 和 CPU 完成，避免读下一批时阻塞上一批的异步输出。
//!    pending read 保持到完成，避免要求每种 source 都支持取消后重试。
//!    提前停止只丢弃借用 source 的读取 future，source 本身仍可 finish。
//!    批次大小和 local 数量限定在途工作；生成循环定期检查取消。
//!    EOF 且所有工作成功后逐份传入 local 做 combine，再调用 source/sink.finish。
//!    空输入仍初始化并 combine，保证无分组 COUNT 返回 0 而不是丢失结果。
//!
//! 9. 验证入口。
//!    PipelineGraph::explain() 打印实际 Program 列表；准备时间就是构建时间。
//!    src/suboperator/program_tests.rs 验证连接、过滤、普通状态和融合。
//!    src/pipeline/execution.rs 验证单 source、状态复用、背压、失败与取消。
//!    tests/physical_expressions.rs 覆盖完整计划；全部现有算子/类型保留语义。
//!    所有支持的运算走生成代码；不支持的类型签名明确报错，没有解释回退。
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
    run(ExecutionMode::Compiled)?;
    Ok(())
}
