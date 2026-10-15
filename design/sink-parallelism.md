# 设计决策：LocalSink 与 sink 的分阶段交接

状态：LocalSink 协议及第 12 节的并行 finish 已实现并验证，多 worker 回退已消除。第 1–11 节保留第一版设计与验证记录，冲突时以第 12 节为准。设计基于 `feat/tpcds` 的 `c98b13a`，更新于 2026-10-15。
相关：[`tpcds-progress.md`](tpcds-progress.md)。

本文面向实现者，写明接口、协议、执行层改动、每个 sink 的实现和验证要求。
文中的行号以 `c98b13a` 为准，实现时以符号名为准。

## 1. 决策

**删除 `SinkExec::finish_on`、`exec::Cpu` 及其全部实现和调用方。sink 不再提交
CPU job。每个执行实例配一个 LocalSink：可并行的局部工作在 CPU job 里由
LocalSink 完成，必须跨实例的工作在 coordinator 上由 sink 单线程完成。**

分工原则：

- **可并行的局部工作放在 LocalSink。** 它跟着执行实例走，在 CPU job 里同步执行，
  多个实例由执行层并行调度。sink 不持有任何 CPU 提交接口。
- **必须跨实例的工作留在 sink，单线程完成。** sink 独占自身，不需要锁或其他并发
  控制。前提是这部分工作尽可能少；每个 sink 的剩余工作及其量级见第 7 节。
- **Program 不变。** Program 本身无状态；kernel 写入的 state 都归 sink 所有，
  由 LocalSink 负责容量判断和交接（第 2 节不变式）。

放弃的方案（不要实现）：在 KernelGraph 中加入 StateKernel / `Program::finish`、
新增 `Append` RowKernel、让 GraphOutput 写入 state 槽位、在 Program 内部提供
中断或让出机制。理由：实例末尾的工作本来就在所有 `run` 之后、与 Program 的
循环分离，放进 kernel 模型只会增加存活分析、builder 校验和槽位类型检查等配套，
没有收益。

## 2. 术语与不变式

**执行实例**：池中的一对 `(LocalState, Box<dyn LocalSink>)`。多 worker 路径共有
`parallelism` 对，ordered / 单 worker 路径只有 1 对。CPU job 从池中取出一对、独占
使用、用完还回；一个实例处理的是一组不连续的 batch。

**batch**：一次 `Program::run` 的输入。执行层保证每个 batch 不超过
`PipelineExecutionConfig::batch_rows` 行（[`execution.rs`](../src/pipeline/execution.rs)
中大 chunk 会先切开）。Program 内部再按 `WINDOW = 1024` 行分窗口，对外不可见。

**chunk**：一次 `run` 产生的输出 `RecordBatch`。它是 variable 物化后的结果：
alias 列直接复用输入数组，root domain 上的定长输出由 kernel 原地写入输出数组，
其余列（被过滤的行、非 root domain、bool、bytes）每个窗口拷贝一次
（[`steps/output.rs`](../src/kernel/steps/output.rs)）。chunk 拥有自己的数据，
可以跨 batch 保存。

不变式（实现和以后新增 kernel 都必须遵守）：

1. **写 state 的 kernel 必须标记 `has_effect`。** 目前是 `GroupLookup`、
   `AggregateUpdate`。因此被写入的 state 一定存活、一定被 `new_local` 创建。
2. **kernel 写入的 state 都归 pipeline 的 sink 所有。** LocalSink 通过 StateId
   访问它们，负责容量判断；sink 负责取走和重置。只读的 TableState
   （`TableGather`、`Expand` 读取）不受 LocalSink 管理。
3. **sink 自己使用、kernel 不使用的状态放在 LocalSink 的字段里**，不注册 StateId。
   因此存活分析（`kernel/plan.rs`）不需要任何改动。
4. **每次 `run` 都必须能完整执行。** 容量不足要在执行前由 LocalSink 预判并交接，
   不允许在 `run` 中途发现不够再报错退出。

## 3. 接口

放在 `src/exec/mod.rs`，替换现有的 `SinkStatus`、`SinkExec`、`Cpu`、`CpuJob`、
`Inline`、`run_all`。

```rust
/// LocalSink::consume 的结果，也是交接的原因。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeStatus {
    /// consume 返回：这个 batch 正常消费完，无事发生。
    /// 传给 finish / SinkExec::consume：这个实例的输入正常结束，最后一次交接。
    End,
    /// 本地满了：交接之后这个实例继续接收输入。
    Flush,
    /// 全局已经够了：整个 pipeline 停止读取输入。
    Stop,
}

/// kernel 写入的 state，按 StateId 访问。
pub struct StateContext<'a> {
    local: &'a mut LocalState,
}

impl StateContext<'_> {
    pub fn get_mut<T: 'static>(&mut self, id: StateId) -> Result<&mut T>;
    /// 移出槽位。只允许在 SinkExec::consume(End) 中使用，之后该实例不再执行。
    pub fn take<T: 'static>(&mut self, id: StateId) -> Result<T>;
}

/// 创建 LocalSink 时的执行参数。
pub struct LocalSinkContext {
    /// 每次 run 输入行数的上界，用于容量预判。
    pub batch_rows: usize,
}

/// 一个执行实例的 sink 部分。在 CPU job 中同步执行。
pub trait LocalSink: Send + 'static {
    /// 每次 Program::run 之后调用一次。chunk 为 None 表示这次 run 没有非空输出
    /// （没有输出的 Program，例如聚合，每次都是 None）。
    fn consume(&mut self, chunk: Option<RecordBatch>, states: &mut StateContext<'_>)
        -> Result<ConsumeStatus>;

    /// 交接前整理本地数据（排序、导出 partial 等可并行的 CPU 工作）。
    /// status 是 Flush、Stop 或 End，说明为什么交接。
    fn finish(
        &mut self,
        status: ConsumeStatus,
        states: &mut StateContext<'_>,
        guard: &ShutdownGuard,
    ) -> Result<()>;

    /// SinkExec::consume 用它 downcast 回自己的具体类型。
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

pub trait SinkExec: Send + 'static {
    fn start(&mut self, _guard: &ShutdownGuard) -> Result<()> {
        Ok(())
    }

    /// 每个执行实例调用一次，在第一个 batch 之前。
    fn local_sink(&self, ctx: &LocalSinkContext) -> Result<Box<dyn LocalSink>>;

    /// 在 coordinator 上接收一个实例整理好的数据。
    /// Flush / Stop：取走数据，并把 LocalSink 和 kernel 槽位恢复为可继续运行的
    ///   状态（这个实例之后还会收到 End）。
    /// End：取走全部数据，不需要恢复，这个实例之后被丢弃。
    fn consume<'a>(
        &'a mut self,
        status: ConsumeStatus,
        local: &'a mut dyn LocalSink,
        states: StateContext<'a>,
        guard: &'a ShutdownGuard,
    ) -> BoxFuture<'a, Result<()>>;

    /// 所有实例都交接 End 之后调用一次：跨实例工作与发布结果。
    fn finish<'a>(&'a mut self, guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>>;
}
```

`downcast` 失败（类型不是该 sink 自己的 LocalSink）返回 `Error::internal`。

`StateContext` 只暴露 state，不暴露 `LocalState` 的 scratch、buffers 等字段。
`get_mut` 在槽位为 None 或类型不符时返回 `Error::internal`。

## 4. 交接协议

### 4.1 一个实例的生命周期

```
          ┌──────────────── 每个 batch ────────────────┐
创建 ──→  run(batch) ──→ LocalSink::consume(chunk?) ──┤
          │                    │                       │
          │                End │ 继续下一个 batch       │
          │          Flush/Stop│                       │
          │                    ▼                       │
          │   LocalSink::finish(Flush|Stop)   （CPU job 内）
          │   SinkExec::consume(Flush|Stop)   （coordinator）
          │   Flush：继续；Stop：全局停止派发
          └────────────────────────────────────────────┘
输入结束 ──→ LocalSink::finish(End)   （阶段 2，并行）
         ──→ SinkExec::consume(End)  （阶段 3，coordinator，按池顺序）
所有实例 ──→ SinkExec::finish         （阶段 3）
```

### 4.2 各方法的契约

**`LocalSink::consume`**

- 每次 `run` 之后恰好调用一次，包括没有输出的 run（传 None）。输出 chunk 行数为 0
  时传 None。
- 返回 End：继续。返回 Flush：本地满了。返回 Stop：全局已经够了。
- **容量预判**：返回 End 意味着承诺"再来一个最多 `batch_rows` 行的 batch 也能
  完整处理"。做不到就返回 Flush。预判只用 `LocalSinkContext::batch_rows`，
  不需要知道下一个 batch 的实际大小。
- 可以修改 kernel 槽位中的 state，但**不能改变槽位类型**：下一次 `run` 会按原类型
  downcast。

**`LocalSink::finish(status)`**

- 只整理本地数据，把结果放进 LocalSink 自己的字段，例如排好序的 run、导出的
  partial。**不清空、不移出 kernel 槽位**：清理是 `SinkExec::consume` 的职责。
- 在 CPU job 中执行（ordered / 单 worker 路径见 5.3），可以做 O(N) 的工作。
  长时间循环中检查 `guard.is_shutdown_requested()`，返回 `Error::cancelled()`。
- 在一次交接中只调用一次；同一个实例可能先后收到多次 `finish(Flush)`，最后
  恰好一次 `finish(End)`。

**`SinkExec::consume(status)`**

- 在 coordinator 上执行，应当只做 O(1) 或 O(实例数) 的移动（`mem::take`、
  `mem::replace`），重的工作放在 `LocalSink::finish` 或 `SinkExec::finish`。
- 可以 await（例如 Exchange 发送，见 7.2）。
- Flush / Stop 之后，LocalSink 和 kernel 槽位必须处于"可以继续运行"的状态：类型
  不变、内容合法（通常是清空后的初始状态）。End 之后不需要恢复。
- 每个实例最后恰好收到一次 End，无论之前是否有过 Stop。

**`SinkExec::finish`**

- 所有实例的 End 交接之后调用一次。跨实例工作（归并、建索引、合并 partial）
  和结果发布都在这里，单线程。

### 4.3 Stop 的语义（本次只做最简单的版本）

- 任意一个 LocalSink 返回 Stop，执行层把 pipeline 标记为 stopped：不再派发新的
  job，丢弃待派发的 batch 和正在等待的 source 读取，和现在 `SinkStatus::Finished`
  之后的行为一致。
- **已经在跑的 job 让它跑完**，它们中途的 Flush / Stop 交接照常进行。sink 在
  stopped 之后收到的数据由 sink 自己决定是否忽略。
- 之后所有实例照常走阶段 2 和阶段 3。
- 不实现共享停止标记、不中断正在跑的 job（见第 9 节遗留问题）。

## 5. 执行层改动（`src/pipeline/execution.rs`）

### 5.1 多 worker 路径

**池**：`idle: Vec<(LocalState, Box<dyn LocalSink>)>`，大小为 `parallelism`。每一对由
`program.new_local()` 和 `sink.local_sink(&ctx)` 创建，`ctx.batch_rows = config.batch_rows`。

**CPU job**（替换现有 `on_cpu` 闭包和 `Completed` 类型）：

```rust
struct Completed {
    pair: (LocalState, Box<dyn LocalSink>),
    /// None：这批 batch 正常跑完；Some(Flush | Stop)：中途交接。
    handoff: Option<ConsumeStatus>,
    /// 中途交接时还没有执行的 batch，按原顺序。
    leftover: Vec<RecordBatch>,
}

// job 闭包：只处理中途交接（Flush / Stop）
for (i, batch) in batches.into_iter().enumerate() {
    let chunk = program.run(&batch, &mut state, &guard)?
        .filter(|c| c.num_rows() != 0);
    let status = local.consume(chunk, &mut StateContext::new(&mut state))?;
    if status != ConsumeStatus::End {
        local.finish(status, &mut StateContext::new(&mut state), &guard)?;
        return Ok(Completed { pair, handoff: Some(status), leftover: rest_of(batches, i + 1) });
    }
}
// 这批 batch 正常跑完：不调用 finish。实例还会被别的 job 取走继续接收输入，
// 输入是否结束只有 coordinator 知道。
Ok(Completed { pair, handoff: None, leftover: vec![] })
```

```rust
// coordinator，输入结束且所有 job 都已返回之后
// 阶段 2：每个实例恰好一次 finish(End)，并行
let pairs = try_join_all(idle.into_iter().map(|(mut state, mut local)| {
    on_cpu(executor, move || {
        local.finish(ConsumeStatus::End, &mut StateContext::new(&mut state), &guard)?;
        Ok((state, local))
    })
})).await?;
// 阶段 3：按池顺序交接 End，然后跨实例收尾
for (mut state, mut local) in pairs {
    sink.consume(ConsumeStatus::End, &mut *local, StateContext::new(&mut state), &guard).await?;
}
source.finish(&guard).await?;
sink.finish(&guard).await?;
```

**coordinator 收到 Completed**（替换现有 `return_local` 和 `consume` 两个函数）：

1. 若 `handoff` 为 Some(s)：`sink.consume(s, &mut *local, StateContext::new(&mut state), guard).await?`。
2. 若 s 为 Stop：置 `stop = true`，清空 `pending` 和 `morsel`。
3. 若 s 为 Flush 且未 stop：把 `leftover` 按原顺序放到 `pending` 队列**前端**。
4. 把这一对放回 `idle`。

**派发**：新增 `pending: VecDeque<RecordBatch>`。派发 job 时先从 `pending` 取 batch
（最多 128 个，与现有上限一致），`pending` 为空才读 `morsel` / source。stopped 之后
不再派发。其余逻辑（`now_or_never` 合批、`yield_batches` 让出、shutdown 检查）不变。

**输入结束后**（`(eof || stop) && jobs.is_empty() && pending.is_empty()`）：

- **阶段 2**：对 `idle` 中每一对通过 `on_cpu` 提交一个 job，执行
  `local.finish(End, ..)` 并返回这一对；`try_join_all` 等待，同时 select
  `shutdown_guard.shutdown_requested()`。任何一个失败或取消：返回错误，不进入阶段 3。
- **阶段 3**：按池中顺序对每一对 `sink.consume(End, ..).await?`；检查 shutdown；
  `drop(source)`；`self.source.finish(..)`；`self.sink.finish(..)`。

### 5.2 空输入与 `program.idle()`

- `program.idle()` 为真时不读取输入，池中的每一对没有任何 `run`，
  `LocalSink::consume` 一次也不会被调用。
- 阶段 2 和阶段 3 照常对每一对执行。这保证全局聚合在空输入下仍然输出
  COUNT = 0 等结果（与现在 combine 新建 local 的语义一致）。

### 5.3 ordered 与单 worker 路径

`parallelism == 1 || self.ordered` 时只有一对，沿用现有 inline 循环：

- 每个 batch 切片 `run_inline` 之后，inline 调用 `local.consume(..)`。
- 返回 Flush / Stop：inline 调用 `local.finish(status, ..)`，然后
  `sink.consume(status, ..).await?`。Flush 继续处理剩余输入；Stop 跳出输入循环。
  不需要 `pending` 队列，顺序自然保持。
- 输入结束：`parallelism > 1` 时通过 `on_cpu` 执行 `local.finish(End, ..)`（避免在
  coordinator 线程上做排序等长计算），`parallelism == 1` 时 inline 执行；然后
  `sink.consume(End, ..)`、`source.finish`、`sink.finish`。
- inline 调用的 `LocalSink::consume` / `finish` 与 `run_inline` 一样用
  `catch_unwind` 包住，panic 转为 `ErrorKind::TaskFailed`。

### 5.4 错误、取消、panic

- `run`、`LocalSink::consume`、`LocalSink::finish` 任一出错，所在 job 失败，整个
  pipeline 返回错误；之后不再调用任何 `SinkExec::consume` / `finish`，不发布结果。
- CPU job 中的 panic 由现有 `on_cpu` 的 `catch_unwind` 处理。
- 取消检查点保持现状，并在阶段 2 等待期间、阶段 3 每次 consume 之前检查。

## 6. Program 与 pipeline 构建

- `Program`、`KernelGraph`、`Plan`、kernel 模型均**不改**。
- `PipelineBuilder::set_sink(sink, outputs)` 保持不变：有 `outputs` 时 Program 产生
  chunk，经 `LocalSink::consume(Some(chunk))` 交给 LocalSink；没有时每次传 None。
  更新 `pipeline/mod.rs` 中提到 `SinkExec::consume` 的注释。
- 不再需要 `Pipeline.ordered` 以外的新标记。ordered 只决定走 5.3 的路径。

## 7. 各 sink 的实现

### 7.1 总览

| Sink | LocalSink::consume（job 内） | LocalSink::finish（并行） | SinkExec::consume | SinkExec::finish（单线程） | 剩余串行量级 |
| --- | --- | --- | --- | --- | --- |
| Collect | 直接 push 到共享输出 | 无 | 无 | 无 | 无 |
| Exchange | 缓存 chunk，达到阈值 Flush | 无 | 发送缓存的 chunk | 无 | 发送本身 |
| Buffer（Union、Spool） | 缓存 chunk | 无 | 移走 chunk | 写入共享 Buffer | O(chunk 数) |
| Limit、SingleRow | 缓存 chunk，全局计数够了 Stop | 无 | 移走 chunk | 截取并写入 Buffer | O(chunk 数) |
| Sort | 缓存 chunk，逐 chunk 编码排序键 | 局部排序（有 fetch 时 TopK） | 移走有序 run | P 路归并，一次 interleave | O(N log P) |
| Window | 缓存 chunk，逐 chunk 编码各规格的键 | 每种规格局部排序 | 移走 | 每种规格 P 路归并，线性计算窗口 | O(N log P) + O(N) |
| Table | 缓存 chunk，逐 chunk 算 key 编码与 hash | 无 | 移走 | 计数排序，每列 gather 一次，建索引 | O(N) |
| Aggregate | 预判容量（初版恒为 End） | 导出 partial | 移走 partial；Flush/Stop 时清空槽位 | 串行合并 partial | O(各实例 group 数之和) |

P 为实例数，N 为行数。

### 7.2 Collect（`operator/collect.rs`）

- LocalSink 持有 `Arc<Mutex<Vec<RecordBatch>>>`（即 `CollectOperator.output`），
  `consume(Some(chunk))` 直接 push，返回 End。
- unordered 时顺序为各 job 的完成顺序（与现在一致，本来就不确定）；ordered 时只有
  一个实例，为 source 顺序。

### 7.3 Exchange（`exec/exchange.rs`）

本次保持 `ExchangeSink::send` 接口不变（单机负载，`try_send` / 多生产者留待以后）。

- LocalSink 缓存 chunk 并累计行数；累计行数达到 `EXCHANGE_FLUSH_ROWS`（初值
  `8 * batch_rows`，作为常量放在 exchange.rs，可调）时返回 Flush，否则 End。
- `SinkExec::consume(任意 status)`：`mem::take` 取出缓存的 chunk，按顺序对每个
  exchange `send(..).await`，与现有 `consume` 的发送逻辑相同。
- `start` 中创建 outputs 的逻辑不变。

### 7.4 Buffer、Limit、SingleRow（`exec/buffer.rs`、`operator/materialize.rs`）

`BufferSinkExec` 只服务于 Union、Spool、Limit、SingleRow（Sort 和 Window 改用各自
的 sink，见 7.5、7.6）。

- `Finish` 类型去掉 `Cpu` 参数，改为同步：
  `Box<dyn FnOnce(Vec<RecordBatch>) -> Result<Vec<RecordBatch>> + Send>`；
  `exec::finish` 辅助函数随之删除或改为直接构造。
- LocalSink 缓存 chunk。有 `limit` 时，与 sink 共享一个 `Arc<AtomicUsize>` 行计数：
  每个 chunk `fetch_add(rows)`，加上之后 ≥ limit 就返回 Stop，否则 End。
- `SinkExec::consume(任意 status)`：把 chunk 按交接顺序追加到 sink 的列表。
- `SinkExec::finish`：对列表执行 `finish` 闭包（Limit 的 skip/fetch 截取、SingleRow
  的行数检查），写入共享 `Buffer`。
- Limit 在 ordered 输入上只有一个实例，chunk 按 source 顺序交接，保留"前 N 行"语义；
  unordered 时可能多收几行，由 finish 闭包截取。

### 7.5 Sort（`exec/sort.rs`，新增 `SortSinkExec`）

`SortOperator::build_pipeline` 改用 `SortSinkExec`，结果仍写入 `Buffer`，读取它的
pipeline 仍然 `set_ordered()`。

- **LocalSink::consume**：缓存 chunk；对排序键列编码，追加到本实例的 `RowKeys`
  （把 `RowKeys::new` 改造成可以按 chunk 追加：`push(columns, keys, rows)`，
  offsets 连续）。只编码键，不拼接 payload 列。始终返回 End。
- **LocalSink::finish**：对本实例的行号（跨 chunk 连续编号）按
  `(键字节, 行号)` 排序；有 fetch 时先 `select_nth_unstable_by` 再截断、排序
  （与现有 `sort_chunks` 的 TopK 逻辑相同）。结果为 `SortedRun { chunks, keys, order }`。
- **SinkExec::consume**：移走 SortedRun。Flush 时把 LocalSink 恢复为空（Sort 初版
  不会返回 Flush，但必须正确处理）。
- **SinkExec::finish**：P 路归并，使用 `BinaryHeap` 或败者树（不要用现有 `merge`
  的逐行线性扫描），比较 `(键字节, run 序号, run 内位置)`，取够 fetch 即停。
  归并结果为 `(run, 行号)` 序列，映射到 `(chunk 全局序号, chunk 内行号)` 后，对每一列
  调用 `arrow_select::interleave::interleave` 一次，得到一个 RecordBatch 写入 Buffer。
  payload 列只拷贝一次。无行或 `fetch == Some(0)` 时不写入。
- 删除 `sort_chunks`、`PARALLEL_ROWS`、旧 `merge`；`concat_chunks`、`take` 若无其他
  调用方也删除。

### 7.6 Window（`exec/window.rs`，新增 `WindowSinkExec`）

`WindowOperator::build_pipeline` 改用 `WindowSinkExec`，结果写入 `Buffer`。规格
（partition keys、order keys 相同的窗口共用一次排序）的分组逻辑沿用 `compute`。

- **LocalSink::consume**：缓存 chunk；对每种规格，把该 chunk 的 partition 键
  （升序、nulls first，与现有 `spec` 一致）和 order 键编码追加到本实例该规格的
  两个 `RowKeys`。始终返回 End。
- **LocalSink::finish**：对每种规格，按 `(partition 字节, order 字节, 行号)` 对本实例
  行号排序。
- **SinkExec::consume**：移走 chunks、各规格的 RowKeys 和局部顺序。
- **SinkExec::finish**：
  1. 按实例、chunk 的顺序给所有行编全局行号，把所有 chunk 拼接成一个输入 batch
     （输出 = 输入列 + 窗口列，拼接是唯一一次 payload 拷贝）。
  2. 对每种规格，P 路归并各实例的局部顺序，得到全局有序行号。比较时通过
     `(实例, 局部行号)` 访问各实例已编码的键，**不在 finish 中重新编码**。
  3. 用全局有序行号调用 `values`（把 `parts` / `orders` 参数改为能按全局行号取键的
     访问器），计算该规格下每个窗口的值，按行号写回。现有帧（Partition、
     RowsToCurrent、RangeToCurrent）在有序输入上都是线性的。
  4. 组装输出 batch 写入 Buffer。无行时不写入。
- 删除 `compute` 中的 hash 分桶、`PARALLEL_ROWS`、`run_all` 调用。

### 7.7 Table（`exec/table.rs`、`kernel/table.rs`）

建表结果和 probe 侧的读取方式（桶布局、`hash_word` / `hash_bytes`、`starts`、
`shift`、`Keys`）**完全不变**，只改变构建过程。

- 把 `kernel/table.rs` 的 `Index::new` 拆成两部分：
  - **局部准备**（每个 chunk 一次）：word 键时得到 `key_words` 和 `hash_word`；
    编码键时 decode + `bind_key` 编码得到 `data` / `offsets` 和 `hash_bytes`；以及
    每行是否参与索引（NULL key 且 `!nulls_equal` 时不参与）。是否为 word 键只取决于
    键类型和 `nulls_equal`，在 sink 构造时确定。
  - **全局组装**：总行数检查（< `END`），`buckets = rows.max(2).next_power_of_two()`，
    `shift`，桶号 = 参与索引 ? `hash >> shift` : `buckets`；对所有
    `(实例, chunk, 行)` 做稳定计数排序；按该顺序生成 `Keys` 和每一列（对每一列调用
    一次 `interleave`）；`Table::from_parts` 发布。
- **LocalSink::consume**：缓存 chunk 并做局部准备。始终返回 End。
- **SinkExec::consume**：移走 chunks 与准备结果。
- **SinkExec::finish**：全局组装并 `self.shared.publish(..)`。无键时只拼接列。
- 现有实现是"拼接一次 + 重排一次"；新实现每列只拷贝一次，键的编码和 hash 在
  job 内并行完成。

### 7.8 Aggregate（`exec/aggregate/mod.rs`、`kernel/aggregation.rs`）

kernel（`GroupLookup`、`AggregateUpdate`）不变。`AggregateSinkExec.states:
Vec<Vec<StateId>>`（每个 grouping set：`[group, accumulators...]`）同时交给 LocalSink。

- **LocalSink::consume(None)**：容量预判。初版不设阈值，恒返回 End。实现一个可选
  阈值 `flush_groups: Option<usize>`（默认 None）：当某个 set 的
  `group 数 + batch_rows > flush_groups` 时返回 Flush。该阈值只假设 kernel 位于
  root 空间（每行输入最多新增一个 group）；聚合位于 join 之后时增长没有静态上界，
  这是已知限制（第 9 节）。
- **LocalSink::finish(任意 status)**：对每个 set，从 `&mut GroupState` 和
  `&mut AccumulatorState` **不移出槽位地**导出 partial：`accumulator.resize(group 数)`，
  `GroupIndex::columns(&self)`、`Accumulator::state(&self)` 都是借用，可以直接使用。
  新增一个借用版本的导出函数替代 `kernel::aggregation::partial` 中的 `take_state`。
  partial 存入 LocalSink 的字段。
- **SinkExec::consume**：移走 partial。Flush / Stop 时还要通过 `StateContext::get_mut`
  **同时**清空该实例所有 set 的 GroupState 和所有 AccumulatorState，使下一次 `run`
  可以继续：为 `GroupIndex` 和 `Accumulator` 新增 `clear()`（保留容量）。group 和
  accumulator 必须一起清空，否则 accumulator 的大小与 group 数不一致。End 时不清空。
- **SinkExec::finish**：对每个 set，用现有 `merges` 中的 `AggregateMergeState`
  依次 `merge` 所有 partial（按交接顺序），`finish_columns`，`Layout::assemble`，
  写入 `result.finalized`。
- 删除 `finish_on` 中的分区路径：`PARTITION_GROUPS`、`PartialAggregate::partitions`、
  `group_count`、`run_all` 调用，以及只为分区存在的代码。

## 8. 删除清单

- `exec/mod.rs`：`SinkStatus`、旧 `SinkExec`（`consume(chunk)`、`combine`、`finish_on`）、
  `Cpu`、`CpuJob`、`Inline`、`run_all`。
- `pipeline/execution.rs`：`ExecutorCpu`、`return_local`、`consume`、旧 `Completed`；
  测试中的 `RecordingSink`、`FeedbackSink`、`StopSink` 按新接口重写。
- `pipeline/graph.rs` 测试中的 `UnusedExec` 按新接口重写。
- 各 sink 中与 `Cpu` 相关的代码（见第 7 节）。

## 9. 遗留问题（本次不做）

- **Stop 的优化**：共享停止标记，让正在跑的 job 在 batch 之间提前返回；Stop 之后
  剩余实例是否跳过阶段 2。
- **Exchange 同步发送**：`try_send` 或一个 exchange 多个生产者，需要和 exchange
  service 的实现一起确定。届时 Exchange 的 LocalSink 改为在 job 内同步发送，只在
  反压时缓存并 Flush。
- **聚合的容量阈值**：何时启用 `flush_groups`，以及聚合位于 join（Expand）之后时
  增长没有静态上界的处理。
- **输出 chunk 的大小**：Expand 放大时单次 `run` 的输出没有上界，目前不控制。
- **剩余串行工作的并行化**：若实测高基数聚合的合并（O(group 数)）或大表建表
  （O(N)）成为瓶颈，再由 operator 拆成两条 pipeline：前一条按 key hash 分成 P 份，
  后一条每个实例处理一个完整分区。需要"完整分区归一个实例"的分发能力。

## 10. 实施步骤与验证

建议按以下顺序实现，每一步都保持全部测试通过：

1. **接口与执行层**：新增第 3 节的接口；重写 `execution.rs` 的两条路径（第 5 节）；
   先把 Collect、Exchange、Buffer（含 Limit、SingleRow）迁移过来，并重写执行层测试。
2. **Sort**：`SortSinkExec`、可追加的 `RowKeys`、堆归并、interleave 输出。
3. **Window**：`WindowSinkExec`，`values` 改为按全局行号取键。
4. **Table**：拆分 `Index::new`，`TableSinkExec` 改为局部准备 + 全局组装。
5. **Aggregate**：借用导出 partial、`clear()`、串行合并，删除分区路径。
6. **删除**第 8 节的全部内容，确认没有残留的 `Cpu` 引用。
7. **性能**：重新运行 q51、q47、q57、q67、q23 和高基数 group by 的查询，记录
   Compile / ZeroCompile、1 / 多 worker 的耗时，与迁移前同一环境对比。单独记录
   `SinkExec::finish` 的耗时，用来判断第 9 节的并行化是否需要。

测试至少覆盖：

- 执行层：
  - Flush 后剩余 batch 按原顺序重新派发，结果不丢不重（用小阈值的测试 LocalSink）。
  - Stop 后不再派发，已在跑的 job 跑完，所有实例仍各收到一次 End。
  - ordered 路径在 Flush / Stop 下保持顺序。
  - 空输入和 `program.idle()`：每个实例仍执行 `finish(End)` 和 `consume(End)`。
  - `LocalSink::consume` / `finish` 出错或 panic 时 pipeline 失败、不调用
    `SinkExec::finish`；阶段 2 中取消能及时返回。
- Sort：多实例、相等键、NULL 顺序、升降序、fetch（含 0 和大于行数）、空输入；
  与迁移前结果一致。
- Window：多种规格、多实例下同一 partition 跨实例的行、peer 与三种帧、空输入。
- Table：word 键与编码键、`nulls_equal` 两种设置、NULL key、多实例；probe 结果与
  迁移前一致。
- Aggregate：多 grouping set、全局聚合空输入（COUNT = 0）、高基数、多实例；
  `flush_groups` 设小值时多次 Flush 的结果与不 Flush 一致。
- Limit / SingleRow：ordered 输入保留前 N 行；SingleRow 多于一行报错、零行输出 NULL。
- Compile 与 ZeroCompile 结果一致。

第 1–10 节保留设计约定；实际实现与验证结果见下节。


## 11. 实现与验证（2026-10-15）

已实现全部八类 sink 的 LocalSink 协议；移除 `SinkStatus`、`combine`、
`finish_on`、`Cpu`、`CpuJob`、`Inline`、`ExecutorCpu` 与旧分区/排序收尾路径。
Program/kernel 的执行模型、存活分析、StateId 类型绑定保持不变。

- 执行层：Flush 剩余 batch 放回 pending 队首；Stop 丢弃待派发输入和 pending
  source read，等待在途 job 完成；阶段 2 全部成功后才按池顺序交接 End。
- Sort/Window：逐 chunk 编码、本地排序、BinaryHeap 归并。Sort 直接 interleave
  原 chunk；Window 仅拼接一次输入，使用已编码键计算三种帧。
- Table：KeyPlan 在构造时绑定键，局部准备 hash/编码，稳定计数排序后每列一次
  interleave；probe 的 bucket、hash 与 NULL 语义不变。
- Aggregate：借用导出 partial，按 set 串行合并；`with_flush_groups` 默认关闭，
  Flush 同时 clear GroupIndex 和 Accumulator。保留第 9 节的 root-domain 限制。
- TPC-DS runner 的 `sink finish ms` 为各 pipeline 的 `SinkExec::finish` 墙钟耗时之和，
  不包含 LocalSink::finish 或 consume 交接；pipeline 可重叠，因此此和可能大于
  查询 execute 墙钟耗时。计时包装仅在 DataFusion 验证工具中，核心执行器不计时。

### 正确性

- `cargo test --locked --workspace`：187 项测试通过（含 doctest）。
- 新增协议测试覆盖 Flush 的 batch 顺序及不丢不重、Stop 后在途 Flush 和每实例
  唯一 End、空输入、真正 idle 的空表 join、无输出的 run、ordered 收尾线程、
  consume/finish 错误和 panic，以及阶段 2 取消。
- sink 测试覆盖多 run 相等键/NULL/TopK（含 fetch=0）、跨实例窗口 partition、
  三种帧、多规格、word/编码建表键与 NULL，以及整数/字符串键、多 grouping set、
  DISTINCT/MIN/SUM/COUNT 的多次 Flush 与不 Flush 等价。
- SF1 全量 103 条语句，4 worker：Compile **103/103**、ZeroCompile **103/103**，
  均与 DataFusion 对照；1/4 worker 的性能矩阵共 **56/56** 配置通过对照。
  全量逐语句结果见 [`status.md`](../integrations/datafusion/tpcds/status.md) 和
  [`验证 CSV`](../benches/sink_parallelism_validation.csv)。

### 同机性能

Apple M4 Pro，Rust 1.95.0，LLVM 19.1.7，默认 release 构建；DuckDB 1.4.5 生成
SF1 Parquet。基线为 `37e69782efb87a6b0e8d0b415170b0b8d25e0695`，与本变更
使用同一数据和依赖锁文件。每种配置重复 3 次，记录最快一次 Roc 总耗时；该次的
translate/build/execute/finish 分项一并保留。1/4 worker 分别使用 1/4 个 runtime
线程；测量时未并行执行本任务的构建或测试。单轮结果不代表稳定的跨环境收益。

下表单位 ms，**迁移前 → 迁移后**：

| 查询 | ZeroCompile / 1 | ZeroCompile / 4 | Compile / 1 | Compile / 4 |
| --- | ---: | ---: | ---: | ---: |
| q23a | 3495.7 → 3131.3 | 1007.9 → 1278.0 | 3570.6 → 3350.8 | 1195.2 → 1434.4 |
| q23b | 3517.2 → 3209.4 | 999.9 → 1269.3 | 3653.9 → 3495.2 | 1176.2 → 1439.0 |
| q47 | 911.6 → 867.5 | 303.3 → 316.6 | 1063.1 → 1008.0 | 447.2 → 459.8 |
| q51 | 555.0 → 597.3 | 199.0 → 393.3 | 585.2 → 656.9 | 219.2 → 403.3 |
| q57 | 339.6 → 313.1 | 114.8 → 112.1 | 496.0 → 465.4 | 259.4 → 260.9 |
| q67 | 1275.0 → 1273.1 | 487.0 → 823.0 | 1348.5 → 1429.5 | 523.1 → 862.4 |
| 高基数 GROUP BY | 71.0 → 65.2 | 20.9 → 40.7 | 68.1 → 66.0 | 22.6 → 40.5 |

高基数查询返回 240,000 个 group：

```sql
SELECT ss_ticket_number, COUNT(*), SUM(ss_net_paid)
FROM store_sales
GROUP BY ss_ticket_number;
```

原始分项数据见 [`sink_parallelism.csv`](../benches/sink_parallelism.csv)，环境与
口径见 [`metadata`](../benches/sink_parallelism_metadata.json)。复跑指定查询：

```sh
cargo build --locked --release -p roc-datafusion --example tpcds
target/release/examples/tpcds <sf1-dir> --mode zero --workers 4 --repeat 3 \
  q51 q47 q57 q67 q23a q23b
```

将 mode 换为 `compile`、workers 换为 `1` 补齐矩阵；高基数查询通过 `--sql` 传入。

**这次迁移不能宣称为整体性能提升。** 单 worker 的 q23、q47、q57 和高基数聚合
有所改善；4 worker 的 q51、q67、q23 和高基数聚合明显回退。ZeroCompile / 4 的
q51 总耗时增加约 98%，q67 增加约 69%，高基数聚合增加约 95%。对应的串行 sink
收尾总耗时分别为 276.4、445.8、24.9 ms；高基数聚合的 execute 为 40.2 ms。
Compile / 4 呈现同样的主要回退方向。这些数据支持继续评估第 9 节的跨实例工作
分区方案；本次按既定设计保留串行合并，没有重新引入 sink 提交 CPU job。

后续逐阶段计时与 CPU 采样见 [q51/q67 回退定位](sink-parallelism-hotspots.md)。
已定位 Window 归并/计算、ROLLUP 的键入表/键列输出，以及 `sink_finish` 未覆盖的
End 交接时 LocalState 串行析构；另以临时实验验证了派发粒度造成的本地排序
并行度不足。生产实现未随诊断实验变更。


## 12. 修订：`SinkExec::finish` 通过 `Cpu` 分区并行（2026-10-15，已实现）

### 12.1 背景与决策

第 11 节和 [回退定位](sink-parallelism-hotspots.md) 表明，"跨实例工作单线程
即可"只在剩余工作为 O(实例数) 的交接时成立。Window、Aggregate、Table 的跨实例
工作是 O(N)，单 worker 下已占查询时间的 26%–36%，4 worker 的加速上限因此只有
约 2 倍。具体来说：

- Window：全局 heap 归并和 `values` 全部串行。实际上窗口只需要同一个 partition
  的行聚在一起并在 partition 内有序，不需要全局有序；旧实现正是按 partition key
  分桶并行的。
- Aggregate：键入表（`GroupIndex::intern`）和键列输出占 finish 的约 80%，只并行
  accumulator 合并没有用。
- End 交接时 LocalState 在 coordinator 上析构，q67 约 50 ms。
- job 合批上限（128 个 batch）使本地排序只用上 1–2 个 worker。

**决策：保留 LocalSink 协议，只给 `SinkExec::finish` 增加一个 `cpu: &dyn Cpu`
参数。** LocalSink 在本地按 key 分成 K 份；`SinkExec::finish` 把各实例的同一个
分区打包成一个任务，用 `cpu.spawn` 并行执行，最后串行发布。

本节修订以下前文约定，冲突时以本节为准：

- 第 1 节"sink 不再提交 CPU job"：改为**只有 `SinkExec::finish` 可以通过 `Cpu`
  提交 CPU 工作**。`LocalSink` 的方法和 `SinkExec::consume` 仍然不能提交。
- 第 3 节：恢复 `Cpu`（新接口见 12.2），`SinkExec::finish` 增加参数，
  `LocalSinkContext` 增加 `partitions`。
- 第 4.2 节"finish 不移出 kernel 槽位"：`finish(End)` 例外，见 12.3。
- 第 7 节：Window、Aggregate、Table 的 finish 改为分区并行，见 12.5–12.7。
- 第 8 节删除清单：不再删除 `Cpu` 和 `ExecutorCpu`。

不采用的替代：sink 在 finish 内自己开线程（不遵守 worker 数，并且会阻塞
coordinator）；把 `Executor` 直接传给 sink（`Executor` 有泛型方法和 GAT，不是
object safe，`SinkExec` 无法以 trait object 使用）；执行层通用的 `next_round`
（可行，但这次选择更直接的 `Cpu`）。

### 12.2 接口（`src/exec/mod.rs`）

```rust
pub type CpuJob = Box<dyn FnOnce() -> Result<()> + Send>;

/// 执行器上的 CPU 工作，与 Program 的 job 共用同一组 worker。
pub trait Cpu: Send + Sync {
    /// 调用时立即提交；返回的 future 只用于等待结果。
    /// 不能推迟到第一次 poll 才提交，否则逐个 await 会串行执行。
    fn spawn(&self, job: CpuJob) -> BoxFuture<'static, Result<()>>;
    /// 执行器的 worker 数。
    fn parallelism(&self) -> usize;
}

/// 先全部 spawn，再 try_join_all；结果按输入顺序返回。
/// 每个 job 把结果写入自己的槽位（同 37e6978 中的 run_all）。
pub async fn run_all<T: Send + 'static>(
    cpu: &dyn Cpu,
    work: Vec<Box<dyn FnOnce() -> Result<T> + Send>>,
) -> Result<Vec<T>>;

pub struct LocalSinkContext {
    pub batch_rows: usize,
    /// 跨实例分区数：同一个 pipeline 的所有实例相同。
    pub partitions: usize,
}

pub fn partition_count(parallelism: usize) -> usize {
    if parallelism <= 1 { 1 } else { (2 * parallelism).next_power_of_two() }
}

trait SinkExec {
    fn finish<'a>(
        &'a mut self,
        guard: &'a ShutdownGuard,
        cpu: &'a dyn Cpu,
    ) -> BoxFuture<'a, Result<()>>;
}
```

- `partitions` 由执行层用 `partition_count(parallelism)` 计算，`parallelism`
  是配置的 worker 数，**不是实例数**。ordered 路径只有 1 个实例，但第二轮仍然
  可以用满 worker。分区数必须是 2 的幂（Table 按 hash 高位分区，见 12.7）。
- `SinkExec::finish` 按数据里实际的分区数来组织任务，不要再用
  `cpu.parallelism()` 推算分区数，避免两边不一致。
- 测试用的 `Inline` 实现：`spawn` 当场执行 job，返回已完成的 future，
  `parallelism()` 返回 1。

### 12.3 协议修订：`finish(End)` 在 worker 上释放 kernel 状态

`LocalSink::finish(End)` 执行之后，这个实例不会再运行 Program。因此
`finish(End)` 可以用 `StateContext::take` 移出 kernel 槽位，在 CPU job 里析构，
避免由 coordinator 在 End 交接后串行析构。

- 只允许在 `status == End` 时这样做。`finish(Flush)` 和 `finish(Stop)` 仍然
  不能移出或清空槽位（清空由 `SinkExec::consume` 负责），否则 Flush 之后的下一次
  run 会失败。
- 这一条对 Aggregate 必须实现（12.6）。其他 sink 的大块数据在 LocalSink 字段里，
  会被移交给 sink 继续使用，不需要处理。
- 执行层在 End 交接之后析构 `LocalState` 的位置保持不变；里面已经没有大对象。

### 12.4 执行层（`src/pipeline/execution.rs`）

- 恢复 `ExecutorCpu { executor: Arc<E>, parallelism }`：`spawn` 内部调用现有的
  `on_cpu`（它在函数体内同步调用 `executor.spawn`，返回 `'static` 的句柄；panic
  由 `catch_unwind` 转为 `TaskFailed`），再 `Box::pin` 包装。
- 多 worker 路径和 ordered / 单 worker 路径都构造同一个 `ExecutorCpu`，在阶段 3
  传给 `sink.finish(&guard, &cpu)`。
- 两条路径创建 LocalSink 时都传
  `LocalSinkContext { batch_rows, partitions: partition_count(parallelism) }`。
- **job 合批上限**：在现有"最多 128 个 batch"之外，再加一个按行数的上限
  `JOB_ROWS`（初值 `16 * batch_rows`，作为常量放在 execution.rs，可调）。累计
  行数达到上限就停止合批，让输入更均匀地分到各个实例上。实验中 128 → 16 使
  q51 的本地 Window 排序墙钟从约 92 ms 降到 45 ms。

### 12.5 Window（`src/exec/window.rs`）

每种规格（partition keys 和 order keys 都相同）各自分区，规格相同的窗口继续共用
一次排序。

**`LocalSink::consume`**：不变，逐 chunk 编码各规格的 partition 键和 order 键。

**`LocalSink::finish(任意 status)`**：对每种规格，按 partition 键的编码字节计算
hash（用 `kernel/table.rs` 的 `hash_bytes` 或同等的 64 位 hash），取低位分到
`partitions` 个桶；每个桶内按 `(partition 字节, order 字节, 本地行号)` 排序。
结果为 `spec → bucket → 有序的本地行号`。规格没有 partition key 时，所有行都
进入桶 0。

**`SinkExec::consume(End)`**：移走 chunks、键和各桶的局部顺序（不变）。

**`SinkExec::finish(cpu)`**：

1. 给所有行编全局行号（按实例、chunk 的顺序），拼接一次输入 batch。拼接可以
   按列通过 `run_all` 并行。
2. 对每个有参数的窗口成员，`scalar::decode` 其参数列，按列通过 `run_all` 并行。
3. 对每种规格，提交 K 个任务（`run_all`），任务 k：
   - 对 P 个实例的桶 k 做 P 路归并（堆只有 P 个元素），得到该桶的全局有序行号；
   - 对该规格的每个窗口成员调用 `values`，返回 `(有序行号, 各成员的值)`。
   各个桶之间没有共同的 partition，任务之间完全独立。
4. 串行地把各桶的值按行号散回每个成员的 `cells`，然后对每个成员
   `scalar::encode`（按成员通过 `run_all` 并行）。
5. 组装输出 batch，写入 Buffer（不变）。

### 12.6 Aggregate（`src/exec/aggregate/mod.rs`、`kernel/aggregation.rs`）

**`LocalSink::finish(status)`**：

1. 照现在的做法，从槽位借用导出每个 grouping set 的 partial。
2. **分区**：对**有 group key** 的 set，按 group key 的 hash **低位**分成
   `partitions` 份（同 `37e6978` 中的 `PartialAggregate::partitions`，用
   `GroupIndex::hash_rows`。高位留给合并时的表槽位，所以必须用低位）。**没有
   group key 的 set（全局聚合，以及 ROLLUP 中的空 set）不分区**，只放在分区 0。
   否则每个分区都会输出一个 group，旧实现的 q67 就因此多出了 7 行中间结果。
3. 如果 `status == End`：用 `StateContext::take` 移出所有 set 的 GroupState 和
   AccumulatorState，在这里析构（12.3）。

**`SinkExec::consume`**：不变（移走 partial；Flush / Stop 时清空槽位）。

**`SinkExec::finish(cpu)`**：

- 统计所有 partial 的总行数。少于 `PARTITION_GROUPS`（恢复该常量，初值 16384）
  时，在 coordinator 上串行合并所有分区：结果正确，只是不拆任务。
- 否则对每个 set 的每个非空分区提交一个任务（`run_all`），任务 (set, k)：
  - 新建 `AggregateMergeState`（对第一个任务沿用 `merges` 中的那个，其他通过
    `factory.merge(set)` 创建，同 `37e6978`）；
  - `merge` 所有实例的分区 k；
  - `finish_columns`（包括 group key 列的输出编码）；
  - **在任务内析构** merge state（包括 `GroupIndex`），只把列返回。
  回退定位显示，键入表和键列输出占 finish 的约 80%，最终 `GroupIndex` 的析构约
  22 ms，三者都必须在任务内完成。
- 串行地用 `concat_parts`（同 `37e6978`）把每个 set 的各分区结果拼起来，
  `Layout::assemble`，发布。

### 12.7 Table（`src/exec/table.rs`、`kernel/table.rs`）

桶号是 `hash >> shift`，也就是 hash 的高位。按 hash 的**高 b 位**分区
（`K = 2^b`），分区 k 正好对应一段连续的桶 `[k·B/K, (k+1)·B/K)`，K 段按顺序
拼起来就是原来那张表。probe 侧完全不变。

**`LocalSink::finish(任意 status)`**：把本地的 `(chunk, 行)` 按
`hash >> (64 - b)` 分到 K 个分区；不参与索引的行（`hashes[r] == None`）单独放
一组。

**`SinkExec::finish(cpu)`**：

1. 算出总行数、`buckets`、`shift`（与现在相同）。如果 `buckets < K`，或者
   `K == 1`，就走现有的串行 `assemble`。
2. 否则提交 K 个任务，加上一个处理不参与索引的行的任务。任务 k：
   - 对分区 k 的所有行（来自所有实例）按桶号做稳定计数排序，只覆盖本分区的
     桶区间；
   - 按得到的顺序生成本段的键（words，或 data/offsets）和每一列的数据
     （对所有 chunk 的数组做 `interleave`，数组列表用 `Arc` 共享）；
   - 返回本段的局部 `starts`、键和列。
   不参与索引的行那个任务不排序，直接按原顺序 gather。
3. 串行拼接：`starts` 加上前面各段的行数偏移后依次拼接；键依次拼接（encoded
   键的 offsets 要加上偏移）；每一列把 K+1 段 `concat` 起来（按列通过 `run_all`
   并行）。不参与索引的行放在最后一个桶之后，与现在的布局相同。
4. `Table::from_parts`，发布。

结果必须与串行 `assemble` 逐字节一致：桶布局、桶内的行顺序（稳定排序，按实例、
chunk、行的顺序）、键、列都相同。

### 12.8 Sort

本次不改变归并方式（读取排序结果的 pipeline 是单实例消费，回退的查询里 Sort
不是热点）。只做一处小改动：最终的 `interleave` 按列通过 `run_all` 并行。
按键范围切分的并行归并留到以后。

### 12.9 实施顺序

1. `Cpu`、`run_all`、`Inline`、`ExecutorCpu`、`partition_count`、
   `LocalSinkContext::partitions`、`SinkExec::finish` 的新签名；更新所有 sink 和
   测试中的 `SinkExec` 实现（不需要并行的 sink 忽略 `cpu`）。
2. Aggregate（12.6），包括 `finish(End)` 中释放 kernel 状态（12.3）。
3. Window（12.5）。
4. job 合批的行数上限（12.4）。
5. Table（12.7）。实现之前先给 `SinkExec::finish` 加临时分段计时，确认 q23 的
   finish 时间主要花在哪里，并与实现之后对比。
6. Sort 的按列 interleave（12.8）。

### 12.10 验证

正确性（在第 10 节的测试之外新增）：

- 分区一致性：同一个 key 在不同实例中一定进入同一个分区。用 `partitions` 为
  1、2、8，以及实例数与分区数不同的组合来测。
- Aggregate：没有 group key 的 set 不分区；q67 的 ROLLUP 中间结果为 501,976 行，
  不多出空分区 group；`PARTITION_GROUPS` 上下两种路径结果一致；Flush 后状态仍可
  继续使用（`finish(Flush)` 不移出槽位）。
- Window：同一个 partition 跨多个实例的行；没有 PARTITION BY 的规格只有一个桶；
  两个窗口共用一种规格（q51 的两个 MAX）。
- Table：分区并行和串行 `assemble` 的结果逐字节一致（`starts`、键、列），覆盖
  `buckets < K` 的回退、NULL key、`nulls_equal` 的两种设置、word 键和编码键。
- `cpu` 中的任务出错或 panic 时，pipeline 返回错误、不发布结果；等待期间取消能
  及时返回。
- Compile 与 ZeroCompile 一致；SF1 全量 103/103 两种模式。

性能：使用第 11 节相同的矩阵和口径重新测量，同时重跑回退定位中 q51、q67 的分段
计时。目标是：4 worker 的 q51、q67、q23 和高基数聚合回到迁移前的水平（±10%）；
1 worker 不比第 11 节迁移后的结果差。结果追加到本文和 `sink_parallelism.csv`。

### 12.11 实现与复测

已实现上述 Cpu 接口、三阶段交接、Aggregate/Window/Table 的分区任务、Sort 的按列
interleave 和按行数限制合批。只有 final finish 能提交工作，任务仍使用 Program 的
执行器。`run_all` 先提交全部任务，按输入顺序取结果；等待期间取消会结束 finish，
所有结果发布前再次检查 guard。Window 的字节 Cell 借用输入列，计算和编码任务都
持有输入 batch 的 Arc，确保取消等待后仍在执行的任务不会访问已释放数据。

验证：

- `cargo test --locked --workspace`：**192 passed**，包括 doctest。
- 新增及扩展测试覆盖 K=1/2/8、实例数与 K 不同、空全局聚合与 ROLLUP、
  16,384 阈值两侧、Flush 后继续运行、End 状态移出、跨实例 Window partition、
  两个 MAX 共用规格、无 PARTITION BY、字符串输出、Table 的逐字节布局一致性、
  NULL 两种语义及小表回退。
- Cpu 测试覆盖立即提交、结果顺序、ordered 单实例仍使用配置的分区数、错误、
  panic 和取消时不发布。
- SF1 **ZeroCompile 103/103、Compile 103/103**（4 worker），完整记录见
  [`finish_parallel_validation.csv`](../benches/finish_parallel_validation.csv)。
- 性能矩阵新增 28 个配置全部与 DataFusion 对照通过。

#### 未插桩性能矩阵

沿用第 11 节机器、数据与口径：Apple M4 Pro、Rust 1.95.0、LLVM 19.1.7、release，
runtime 线程数等于 worker 数，每项取 3 次中最小总耗时，单位 ms。完整记录已追加到
[`sink_parallelism.csv`](../benches/sink_parallelism.csv) 的 `finish_parallel` 行；
迁移前和串行迁移后的原记录保留。

4 worker：

| 查询 | Zero：迁移前 | 串行 LocalSink | 并行 finish | Compile：迁移前 | 串行 LocalSink | 并行 finish |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| q23a | 1007.9 | 1278.0 | 999.1 | 1195.2 | 1434.4 | 1197.0 |
| q23b | 999.9 | 1269.3 | 989.9 | 1176.2 | 1439.0 | 1199.6 |
| q47 | 303.3 | 316.6 | 295.8 | 447.2 | 459.8 | 449.2 |
| q51 | 199.0 | 393.3 | 194.3 | 219.2 | 403.3 | 226.4 |
| q57 | 114.8 | 112.1 | 108.8 | 259.4 | 260.9 | 259.3 |
| q67 | 487.0 | 823.0 | 471.2 | 523.1 | 862.4 | 517.4 |
| 24 万组聚合 | 20.9 | 40.7 | 21.8 | 22.6 | 40.5 | 21.7 |

**4 worker 的 14 个配置均回到迁移前 ±10% 内。** 相比串行 LocalSink，
ZeroCompile q51 减少 50.6%，q67 减少 42.7%；Compile 分别减少 43.9%、40.0%。

1 worker：

| 查询 | Zero：串行 LocalSink | 并行 finish | 变化 | Compile：串行 LocalSink | 并行 finish | 变化 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| q23a | 3131.3 | 3448.7 | +10.1% | 3350.8 | 3425.8 | +2.2% |
| q23b | 3209.4 | 3295.8 | +2.7% | 3495.2 | 3753.9 | +7.4% |
| q47 | 867.5 | 821.9 | -5.3% | 1008.0 | 1002.5 | -0.5% |
| q51 | 597.3 | 554.6 | -7.1% | 656.9 | 591.1 | -10.0% |
| q57 | 313.1 | 311.0 | -0.7% | 465.4 | 464.7 | -0.2% |
| q67 | 1273.1 | 1231.2 | -3.3% | 1429.5 | 1277.5 | -10.6% |
| 24 万组聚合 | 65.2 | 69.4 | +6.4% | 66.0 | 66.2 | +0.3% |

**“1 worker 不退化”的目标尚未完全达到。** 为排除历史测量的波动，额外对 q23
做了两轮交替顺序复测，每个进程仍取 3 次最小值：ZeroCompile q23a 的串行版本为
3095.7 / 3068.1 ms，当前版本为 3313.8 / 3218.9 ms，仍慢 7.0% / 4.9%；
Compile 为 3359.5 / 3279.2 → 3366.5 / 3344.5 ms，慢 0.2% / 2.0%。
q23b 的交替复测差异为 ZeroCompile 0%–3.0%、Compile -1.4%–-0.1%。
尝试过单分区直接串行 merge，没有稳定收益，未保留该改动。当前证据不能把单 worker
差异归因于排队，仍需单独定位；原矩阵中的不利数据没有用复测结果覆盖。
原始复测及实验记录见 [`finish_parallel_profiles`](../benches/finish_parallel_profiles/README.md)。

#### q51 / q67 热点复测

临时源码副本插桩，每条查询 6 次、丢弃首轮、报告剩余 5 次中位数；比较列沿用
[前次诊断](sink-parallelism-hotspots.md) 的串行 LocalSink 数据。两组均为 ZeroCompile、
4 worker / 4 runtime 线程。下表为墙钟或 coordinator 上的串行耗时，单位 ms。

| 查询 / 阶段 | 串行 LocalSink | 并行 finish |
| --- | ---: | ---: |
| q51 P7 store Aggregate final finish | 55.8 | 17.5 |
| q51 P8 store Window local finish 墙钟 | 35.7 | 16.0 |
| q51 P8 store Window final finish | 51.5 | 24.3 |
| q51 P12 两个 MAX local finish 墙钟 | 35.5 | 25.0 |
| q51 P12 两个 MAX final finish | 124.3 | 56.7 |
| q67 P3 Aggregate local finish 墙钟 | 51.4 | 90.1 |
| q67 P3 End 交接后的 LocalState 串行析构 | 50.5 | 0.009 |
| q67 P3 Aggregate final finish | 365.8 | 118.1 |
| q67 P4 RANK local finish 墙钟 | 44.4 | 17.5 |
| q67 P4 RANK final finish | 82.5 | 49.5 |

- q51 P12：分区归并和两个 MAX 的任务等待墙钟为 **40.2 ms**，串行 scatter
  约 **9.2 ms**，并行 concat/decode/encode 分别约 2.2/1.5/2.6 ms。三个 Window
  的 local finish 墙钟合计从约 91.9 降至 47.3 ms，合批行数上限改善了实例分布。
- q67 P3：merge/evaluate 的任务等待墙钟 **103.4 ms**，任务耗时之和约 367.3 ms；
  键入表、键列编码和最终 GroupIndex 析构确实都在这些任务中。剩余串行
  `concat_parts` **4.8 ms**、`Layout::assemble` **10.0 ms**。
- q67 的 ROLLUP 输出在全部 6 轮均为 **501,976 行**。空 grouping set 只用一个
  分区，没有恢复旧实现多出 7 个空 group 的问题。
- local Aggregate finish 变长包含了新增分区和移入 worker 的原状态析构，不能只看
  单项判断回退。coordinator 原有的约 50 ms 析构已经消失。

原始 interval、线程 ID、分组行数、统计 CSV 和可重放插桩 patch 均在
[`finish_parallel_profiles`](../benches/finish_parallel_profiles/README.md)。任务耗时之和
不是墙钟，pipeline 之间会重叠。插桩查询的热运行中位数为 q51 219.9、q67 524.9 ms，
不要与未插桩 best-of-three 的 194.3/471.2 ms 拼成耗时等式。

#### Table 的前后测量

实现前已单独计时 q23a/b：所有 Table finish 合计中位数分别只有 **5.9/10.4 ms**。
其中 bucket order 为 2.2/3.0 ms，key gather 为 0.9/1.3 ms，payload interleave
为 0.7/2.3 ms。Table 不是 q23 的主要回退来源。

按设计实现分区后，worker 上 bucket/key/interleave 的耗时之和分别为 q23a
3.1/1.0/1.1 ms、q23b 5.0/1.6/3.7 ms；跨段 concat 的等待墙钟为 1.2/5.1 ms。
所有 Table final finish 的等待时间之和反而为 55.8/79.3 ms，其中 partition 任务
等待合计 49.2/63.8 ms，明显包含与其他 pipeline 竞争同一执行器的排队。
它不是净增的查询耗时，多个 pipeline 可重叠；目前不能宣称 Table 本身加速。
这也说明 `sink_finish` 累计值在引入 CPU 任务之后，必须连同 worker 实际工作量和
查询墙钟一起解读。q23 的整体恢复主要来自 Aggregate 分区 merge/evaluate。

### 12.12 后续

第 12 节完成之后，按 [`source-parallelism.md`](source-parallelism.md) 把执行层
改为每个实例一个 async 任务、新增 LocalSource，并把 Aggregate、Window、Sort 的
按分区收尾下推到下游 source。第 12 节中的按分区处理函数会被直接复用。
