# 设计决策：并行 source 与 sink 收尾的下推

状态：设计已确定，待实现。在 [`sink-parallelism.md`](sink-parallelism.md) 第 12 节
之后实施。写于 2026-10-15。
相关：[`sink-parallelism-hotspots.md`](sink-parallelism-hotspots.md)、
[`tpcds-progress.md`](tpcds-progress.md)。

本文面向实现者。文中的代码位置以写作时的工作区为准，实现时以符号名为准。

## 1. 为什么要做

### 1.1 现状

sink 侧在 sink-parallelism 第 1–12 节之后的流程是：

```
上游 pipeline： 实例并行 LocalSink::consume / finish
              → coordinator 串行 SinkExec::consume（交接）
              → SinkExec::finish：按分区并行（第 12 节，通过 Cpu）
              → 把完整结果物化（拼成一个 batch / 一张表）
下游 pipeline： coordinator 逐个读取 source.next_batch（async）
              → 按 batch 派发 CPU job
```

剩下的问题有三类：

1. **收尾和读取之间是两段，各有屏障和物化。** 第 12 节的分区任务全部完成后，
   还要串行地拼接、组装，才能发布结果（Aggregate 的 `concat_parts` 和
   `Layout::assemble`、Window 的散回和整表拼接）。下游 source 再把这个大结果切成
   morsel。最慢的分区任务决定了下游何时开始，拼接也是额外的 O(N) 串行拷贝。
2. **收尾工作无法按需执行。** finish 时必须把所有分区都处理完，即使下游只需要
   一部分。例如 `ORDER BY … LIMIT 100` 也要把全部数据归并完，下游的 Stop 也
   省不掉任何收尾工作。
3. **下游读取由 coordinator 驱动。** source 只有一个游标，由 coordinator 串行
   读取，再按 batch 合批派发给 job。这带来了 `pending` 队列、合批上限、Flush 之后
   剩余 batch 放回队列等复杂度；回退定位里"本地排序只用上 1–2 个 worker"也来自
   这里。

### 1.2 参考 DuckDB：sink 收尾与 source 配合

DuckDB 的 Sink 接口（`GetLocalSinkState / Sink / Combine / Finalize`）与我们的
LocalSink 协议基本同构。在这个问题上它的做法是：**Finalize 只把结果整理成分区，
真正的按分区收尾放到读取它的 source 里做**：

- 哈希聚合：Finalize 准备好 radix 分区；读取方的 source 任务每领取一个分区，就
  合并这个分区的 partial，然后直接扫描输出。
- 窗口：按 partition key 的 hash 分组；读取时按分组领取，排序并计算窗口值，然后
  输出。

需要说明的是：**DuckDB 和我们一样，下游 pipeline 要等上游 pipeline（包括
Finalize）完成之后才会被调度。** 我们的 `PipelineGraphExecutor` 也是在上游
`execute`（包括 `sink.finish`）返回之后，才把依赖它的 pipeline 放入就绪队列
（`src/pipeline/execution.rs` 的调度循环）。所以这样做**不能让上游收尾和下游
执行跨 pipeline 重叠**。

它真正带来的收益是：

1. **去掉收尾内部的屏障和最后的物化。** 分区 k 处理完就立刻交给下游的 Program
   和 LocalSink，不必等所有分区完成，也不必拼成一个大结果。慢分区只拖慢领到它的
   那个实例，其他实例继续处理别的分区，并接着执行下游的计算。
2. **局部性。** 同一个实例刚合并好、还在缓存里的数据，紧接着就被下游的 Program
   消费掉。
3. **按需执行。** 只处理被领取到的分区。下游 Stop 后，剩下的分区不再处理；Sort
   按键范围切分后，`LIMIT` 只需要归并前几个范围。
4. **动态负载均衡。** 实例按需领取分区，倾斜时自动平衡，不再依赖 coordinator
   的合批策略。

### 1.3 为什么要连带改造执行层

把收尾放到 source 里，前提是 **source 能被多个实例同时读取**，并且读取发生在
实例自己的执行流里，而不是由 coordinator 统一读取再派发。我们本来就运行在 tokio
上，source 的读取本身是 async 的（扫描、Exchange 要 await IO），所以最自然的模型
是：**每个实例是一个 async 任务，自己读取 source、执行 Program、交给 LocalSink**。
CPU 工作在任务的 poll 里同步执行，IO 时 await。这是 DataFusion 的 partition stream
和 DuckDB 每线程执行 pipeline 的共同做法。

## 2. 决策

1. **新增 `LocalSource`**，与 LocalSink 对称：每个实例一个，按实例读取输入。
   实例变成 `(LocalSource, LocalState, LocalSink)` 三元组。
2. **执行层改为"每个实例一个 async 任务"。** 去掉 coordinator 按 batch 派发的
   循环、`pending` 队列、合批上限和单独的阶段 2。全局 `SinkExec::consume` 在 async
   Mutex 下调用，相当于 DuckDB 加锁执行 Combine。单 worker 和 ordered 只是实例数
   为 1 的情况，不再需要单独的路径。
3. **sink 收尾下推到下游 source。** Aggregate、Window、Sort 的 `SinkExec::finish`
   只把各实例交来的分区整理好，发布一个分区化的结果；按分区的合并或计算，由下游
   LocalSource 在领取分区时执行。
4. **Table 不变。** 它通过 state 被 probe 读取，没有 source，仍然在
   `SinkExec::finish` 中按 sink-parallelism 第 12 节分段并行构建。

## 3. 接口（`src/exec/mod.rs`）

```rust
pub struct LocalSourceContext {
    pub batch_rows: usize,
    /// 本 pipeline 的实例数。
    pub instances: usize,
}

pub trait SourceExec: Send + 'static {
    fn start(&mut self, guard: &ShutdownGuard) -> Result<()>;   // 不变

    /// 每个实例调用一次，在 start 之后。
    fn local_source(&self, ctx: &LocalSourceContext) -> Result<Box<dyn LocalSource>>;

    /// 所有实例任务结束之后调用一次（不变）。
    fn finish<'a>(&'a mut self, guard: &'a ShutdownGuard) -> BoxFuture<'a, Result<()>>;
}

pub trait LocalSource: Send + 'static {
    /// 本实例的下一个 batch，不超过 ctx.batch_rows 行；None 表示本实例已无输入。
    /// IO 型 source 在这里 await；CPU 型 source 同步计算后直接返回。
    fn next<'a>(&'a mut self, guard: &'a ShutdownGuard)
        -> BoxFuture<'a, Result<Option<RecordBatch>>>;
}
```

删除 `SourceExec::next_batch`。所有 source 都通过 `local_source` 读取。

**分区化的结果**（`src/exec/partitioned.rs`，新增）：上游 sink 发布、下游 source
读取的共享对象。

```rust
/// 上游 SinkExec::finish 发布；下游的多个 LocalSource 共同领取。
pub struct Partitioned<P> {
    parts: Vec<P>,                                // 待处理的分区（各实例交来的局部数据）
    done: Vec<OnceLock<Arc<Vec<RecordBatch>>>>,   // 每个分区处理后的输出，只算一次
}

/// 一次读取（一个下游 pipeline）的领取状态；每个读取的 pipeline 各有一个。
pub struct Claims {
    next: AtomicUsize,
}
```

- 分区 k 的输出由第一个领到它的实例计算，存入 `done[k]`，计算完成后释放
  `parts[k]` 中的局部数据。
- 同一份结果被多个 pipeline 读取时（Spool），每个读取方有自己的 `Claims`，
  计算结果通过 `OnceLock` 共享，只算一次。
- 只有一个读取方时，输出不需要缓存：领到分区的实例边算边产出，可以不经过
  `done`。实现可以先统一走 `done`，再按需优化。
- ordered 读取（只有一个实例）按 0、1、2… 顺序领取，保证顺序。

## 4. 执行层（`src/pipeline/execution.rs`）

### 4.1 实例任务

`Pipeline::execute` 改为：

```rust
self.sink.start(guard)?;
self.source.start(guard)?;
let instances = if self.ordered { 1 } else { parallelism };
let shared = Arc::new(Shared {
    sink: asyncband::mutex::Mutex::new(sink),   // 全局 SinkExec，在锁内调用 consume
    stopped: AtomicBool::new(false),
    ..
});
let tasks = (0..instances).map(|_| executor.spawn(instance(
    source.local_source(&src_ctx)?, program.new_local()?, sink.local_sink(&sink_ctx)?, ..)));
try_join_all(tasks).await?;               // 任一失败：取消其余（shutdown），返回错误
self.source.finish(guard).await?;
shared.sink.into_inner().finish(guard, &cpu).await
```

实例任务：

```rust
async fn instance(..) -> Result<()> {
    if !program.idle(&state) {
        while !shared.stopped.load(Acquire) {
            let next = select(guard.shutdown_requested(), source.next(guard)).await;  // 取消时返回 cancelled
            let Some(batch) = next? else { break };
            let chunk = catch_unwind(|| program.run(&batch, &mut state, guard))?;
            let status = catch_unwind(|| local.consume(chunk, ctx(&mut state)))?;
            if status != End {
                catch_unwind(|| local.finish(status, ctx(&mut state), guard))?;
                shared.sink.lock().await.consume(status, &mut *local, ctx(&mut state), guard).await?;
                if status == Stop { shared.stopped.store(true, Release); break; }
            }
            yield_now().await;
        }
    }
    catch_unwind(|| local.finish(End, ctx(&mut state), guard))?;
    shared.sink.lock().await.consume(End, &mut *local, ctx(&mut state), guard).await
}
```

要点：

- **async Mutex** 使用 asyncband 的 `mutex`（在 `Cargo.toml` 中给 asyncband
  加上 `mutex` feature）。核心库不引入 tokio 依赖，`yield_now` 沿用
  execution.rs 中现有的实现。
- **`program.idle()` 为真时不读输入**，但仍然执行 `finish(End)` 和交接
  `End`（保持全局 COUNT = 0 的语义）。
- **超过 `batch_rows` 的 batch** 由 LocalSource 负责切分，执行层不再切分。
- **每个 batch 之后 `yield_now`**，保证同一线程上的其他任务（包括 IO）有机会运行。
- **panic**：`run`、`LocalSink::consume` / `finish` 用 `catch_unwind` 包住，与
  现在的 `run_inline` 相同，转为 `ErrorKind::TaskFailed`。
- **Stop**：设置共享标记，其他实例在下一个 batch 之前看到标记后停止读取，然后照常
  执行 `finish(End)` 和交接 End。不再有"在途 job 跑完"的特殊情况。
- **End 交接不再要求"所有实例都准备好"**：每个实例准备好就加锁交接。任何实例
  出错，整个 pipeline 失败，`sink.finish` 不会被调用，结果不会发布。
- **sink-parallelism 第 12 节的 `Cpu`** 仍然只用于 `SinkExec::finish`，实现不变
  （`ExecutorCpu` 包装 `on_cpu`）。

### 4.2 删除

- coordinator 按 batch 派发的循环、`morsel` 槽位、`pending` 队列、128 个 batch
  合批上限、`JOB_ROWS`、`Completed`、阶段 2 的单独实现。
- 单 worker / ordered 的 inline 路径（并入实例数为 1 的情况）。
- `SourceExec::next_batch` 及其所有实现，改为 `local_source`。

## 5. 各个 source 和 sink

### 5.1 IO 型 source

| Source | LocalSource 的实现 |
| --- | --- |
| 扫描（`exec/scan.rs`） | `ScanHandle` 已经是"同一 pipeline 的所有 source 共享"的设计，`consumer()` 可以多次调用，底层是 mpmc 通道（`operator/scan/channel.rs`）。每个 LocalSource 持有自己的 `consumer()`，直接 `next().await`；超过 `batch_rows` 的 batch 在本地切片。 |
| Exchange（`exec/exchange.rs`） | `ExchangeHandle::consumer()` 形式上也支持多次调用，但多个 consumer 是否共享同一条流，取决于 service 的实现，**需要确认**。确认之前，用一个 async Mutex 包住单个 consumer，各实例轮流取。 |

### 5.2 Buffer（Union、Spool、Limit、SingleRow 的结果）

`BufferSourceExec`：`start` 时取得 Buffer 中的 chunk 列表（不再 clone 成本地
队列），LocalSource 通过共享的原子计数器领取 chunk，按 `batch_rows` 切片产出。
ordered 读取（Limit、Sort 结果）只有一个实例，按顺序领取。

### 5.3 Aggregate

- **上游 `SinkExec::finish`**：不再合并。把各实例按分区交来的 partial 整理为
  `Partitioned<AggregateParts>`（每个分区：各实例的 partial 列表），连同 `Layout`
  一起发布到 `AggregateSharedState`。没有 group key 的 set 只有分区 0（同
  sink-parallelism 12.6）。总 group 数少于 `PARTITION_GROUPS` 时，可以只发布一个
  分区。
- **下游 `AggregateSourceExec` 的 LocalSource**：领取分区 k →
  `AggregateMergeState` 合并该分区所有实例的 partial → `finish_columns` → 按
  `Layout` 生成本分区的输出（每个 grouping set 的输出列，非本 set 的 group 列填
  NULL，grouping id）→ 析构 merge state → 按 `batch_rows` 切片产出。
- `Layout::assemble` 改为按分区生成输出，不再拼接所有分区。
- 输出顺序在分区之间不确定，与现在的语义相同（聚合结果本来就无序）。

### 5.4 Window

- **上游 `SinkExec::finish`**：不再计算。把各实例按桶交来的局部数据（chunks、
  已编码的键、各桶的局部顺序）整理为每种规格的 `Partitioned<WindowBucket>`，
  发布到 Buffer 的替代物 `WindowResult`。
- 多种规格的处理：不同规格的分桶方式不同，一行在不同规格下落在不同的桶里，
  而输出要求同一行的所有窗口列在同一个 batch 里。因此：
  - **只有一种规格时**（最常见，包括 q51 的两个 MAX）：下游 LocalSource 领取桶 k
    → P 路归并 → 计算该规格所有窗口成员的值 → 用 `interleave` 从各实例的 chunk
    中取出本桶的行，加上窗口列 → 切片产出。不需要全局拼接和散回。
  - **有多种规格时**：沿用 sink-parallelism 第 12 节的做法，在
    `SinkExec::finish` 中按规格、按桶通过 `Cpu` 并行计算，散回后发布到 Buffer，
    下游按 5.2 读取。
- 没有 PARTITION BY 的规格只有一个桶，只能由一个实例处理，与现在相同。

### 5.5 Sort

- **上游 `SinkExec::finish`**：从各实例的有序 run 中按固定步长采样排序键，选出
  K−1 个分界键，把每个 run 按分界切成 K 段（二分查找），发布
  `Partitioned<SortRange>`。
- **下游 LocalSource**：读取排序结果的 pipeline 是 ordered，只有一个实例，按
  0、1、2… 顺序领取范围；领到范围 k 时，对 P 段做归并，`interleave` 出 payload，
  切片产出。fetch 和下游 Stop 时，后面的范围不会被处理。
- 单实例消费时，范围的归并本身是串行的。可以预先用 `Cpu` 并行计算后面的若干
  范围（预取），这是后续的优化，本次不做。

### 5.6 Table

不变：`SinkExec::finish` 按 sink-parallelism 第 12.7 节分段并行建表，probe 通过
TableState 读取。

## 6. 与 sink-parallelism 第 12 节的关系和实施顺序

第 12 节的按分区处理函数（合并 partial 的分区 k、计算窗口的桶 k、建表的段 k），
在本文中会被下游 LocalSource 直接调用。因此实施顺序为：

1. **先完成 sink-parallelism 第 12 节**，修复回退，把按分区处理的函数写好、测好。
   以此作为本文的性能基线。
2. **执行层**：`LocalSource`、实例任务、删除 coordinator 派发循环（第 3、4 节）。
   扫描、Exchange、Buffer、Aggregate 的 source 先用最简单的方式实现 `local_source`
   （Aggregate 先读取 finish 中已经合并好的结果），保证行为不变，单独测量执行层
   改造本身的影响。
3. **Aggregate 收尾下推**（5.3）。
4. **Window 收尾下推**（5.4，单一规格的情况）。
5. **Sort 按范围切分**（5.5）。

## 7. 风险与待定

- **CPU 工作会占住 tokio worker。** 一次 `run` 最多 `batch_rows` 行，时间很短；
  但 CPU 型 LocalSource 领取一个分区时，合并或计算整个分区可能需要几十毫秒，这段
  时间会占住一个 worker。这和现在的 `on_cpu` job 情况相同，并不更差。如果影响到
  IO 延迟，再让 LocalSource 把一个分区的处理拆成几步。
- **分区数和倾斜。** 动态领取比静态分配更能容忍倾斜，分区数可以取
  `4 × parallelism`（仍为 2 的幂）。单个巨大分区（例如没有 PARTITION BY 的窗口）
  仍然只能由一个实例处理，分区内并行留待以后。
- **内存。** 分区的局部数据在被领取之前一直保留；被多个 pipeline 读取的结果
  （`done` 缓存）在所有读取方结束之前一直保留。与现在物化整个结果相比，峰值不会
  更高。
- **Exchange 多消费者的语义**需要和 service 的实现方确认（5.1）。
- **sink 的全局锁。** `SinkExec::consume` 只做 O(1) 的移动（sink-parallelism 4.2
  的约定），锁的竞争可以忽略。Exchange 在锁内 await 发送，与现在由 coordinator
  串行发送等价。

## 8. 验证

正确性：

- 执行层：多实例下结果不丢不重；Stop 后其他实例停止读取并各自交接一次 End；
  `program.idle()`；空输入；运行中出错、panic、取消；单实例（单 worker、ordered）
  与多实例结果一致。
- 扫描：多个 consumer 并发读取，行数和内容与单 consumer 一致。
- Aggregate：分区领取下的结果与第 12 节一致；空 grouping set 不重复输出；
  Spool 多读取方时每个分区只合并一次。
- Window：单一规格下推与多规格回退两条路径；同一 partition 跨实例；没有
  PARTITION BY。
- Sort：范围切分边界上的相等键、NULL 顺序、升降序；fetch 和下游 Stop 时不处理
  后续范围（可以用计数验证）。
- Compile 与 ZeroCompile 一致；SF1 全量 103/103。

性能：与 sink-parallelism 第 11 节相同的矩阵和口径，以第 12 节完成后的结果为
基线，分别记录第 6 节每一步的效果。重点观察 q51、q67、q23、高基数聚合，以及
带 `ORDER BY … LIMIT` 的查询。
