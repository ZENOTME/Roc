# 设计决策：LocalSink 与 sink 的分阶段交接

状态：设计已确定，待实现。基于 `feat/tpcds` 的 `c98b13a`，更新于 2026-10-15。
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

本文仅记录设计；本次文档更新未修改执行代码或运行性能测试。
