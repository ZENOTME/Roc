# 设计决策：并行 source 与 sink 收尾的下推

状态：已实现并验证（2026-10-15），基于 sink-parallelism 第 12 节提交
`cc7d80382f80e7e25fbcb1b095bc04b7c46b0e50`。实现与测量见第 9 节。
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


## 9. 实现与效果（2026-10-15）

实现位于 `codex/source-parallelism` 分支，独立 worktree；原工作区未提交的方案修改未搬入。

### 9.1 落地范围与实现选择

- 删除 `SourceExec::next_batch`，引入 `LocalSourceContext` / `LocalSource`；扫描、
  Exchange、Buffer 和物化算子的读取都迁移完成。
- 每个实例持有自己的 source、Program 状态和 local sink。Flush/Stop/End 在
  asyncband Mutex 下交接；单 worker 和 ordered 均使用同一路径。删除 coordinator
  合批队列、morsel 槽、JOB_ROWS 和独立 End 准备阶段。配置中的 `yield_batches`
  为兼容调用方保留，但所有实例都在每个 batch 后 yield。
- Stop 使用可唤醒的共享信号，既检查是否还应读取，也中断已经 pending 的 IO；每个
  成功实例仍交接一次 End。实例错误或 panic 会取消并等待同 pipeline 的其他实例，
  不调用全局 finish；不代替调用方取消整个 graph。
- `Partitioned<P>` / `Claims` 由 `PartitionedResult`、`PartitionedSourceExec` 封装。
  每个读取 pipeline 有自己的 claims；输出与错误均只计算、缓存一次。计算时取走
  输入，释放该分区 work closure。Sort/Window 跨桶共享的 Arrow payload 和编码键
  在最后一个引用释放时回收。单读取方暂时也缓存输出，符合第 3 节的初版选择。
- Aggregate 按 grouping set 和 hash 分区发布 partial，读取时 merge 并直接组装本
  分区列，删除全局 `concat_parts`。小输入每个 set 一个分区；空 global set 仍
  只输出一行。沿用原来的 `partition_count`（多 worker 为 2×parallelism 向上取
  2 的幂），没有把分区数调优混入本轮。
- 单规格 Window 按桶归并、interleave payload 并计算成员窗口；删除该路径的全局
  concat/scatter。多规格保持原 Cpu 并行路径，结果再经统一 source 读取。
- Sort 以全局固定步长采样，选择最多 4×parallelism 个键范围；二分切各 run，
  相等编码键落入同一范围。ordered source 按范围读取，fetch 在发布时裁掉不需要的
  后缀，Stop 保留尚未计算的范围。没有加入预取。
- Table 沿用原来的 Cpu 并行建表。Exchange 仍共享单个 consumer，未假定 service
  的多 consumer 语义。
- DataFusion 适配器同步改为各 consumer 直接领取并读取 partition。原实现为每个
  consumer 启动一组 decoder task，领取后发往该 consumer 的私有队列；新实例模型
  下会造成输入被少数实例占住。移除这一层任务和队列后，扫描按需求推进。

### 9.2 正确性

`cargo test --locked --workspace`：**198 passed**，无编译警告，包括 doctest。
SF1 全量 103 条语句、4 worker / 4 runtime threads：**ZeroCompile 103/103，
Compile 103/103**，逐条与 DataFusion 比对。

新增或改造覆盖：多实例输入不丢不重及 chunk 大小；每个实例只交接一次 End；
Flush 重用、ordered 前缀、Stop 唤醒 pending read；idle/空输入；source 和 local
sink 的错误、panic、取消；共享分区被两个读取方同时读取只计算一次并释放输入；
空 grouping set 与 rollup；单规格字符串窗口、无 PARTITION BY 和多规格回退；
排序相等键、NULL、升降序、fetch 及未读取范围的计算计数；DataFusion runtime
中直接拉取和释放 stream。

并行领取改变浮点聚合的合并顺序，标准差可能出现约 1e-12 的末位差异。
对应测试现在对每种模式与 worker 数分别验证数学参考值；Decimal SUM/AVG 仍严格
相等，标准差沿用原先参考值检查的 1e-9 相对容差。

### 9.3 分阶段性能

Apple M4 Pro，Rust 1.95.0，内核 LLVM 19.1.7，锁定依赖、默认 release profile、
同一 SF1 Parquet 数据。每种配置重复 3 次，选最快一次的 Roc 总耗时，并保留该次
translate/build/execute 分项。1/4 worker 分别对应 1/4 runtime threads；本任务的
构建、测试和查询计时不重叠。环境、原始记录、复跑脚本和可应用到基线的中间阶段
patch 见 [`benches/source_parallelism`](../benches/source_parallelism/README.md)。

以下为首轮完整矩阵，单位 ms，**基线 → 最终版（总耗时变化）**：

| 查询 | ZeroCompile / 1 | ZeroCompile / 4 | Compile / 1 | Compile / 4 |
| --- | ---: | ---: | ---: | ---: |
| q23a | 3423.6 → 3516.5 (+2.7%) | 1078.6 → 1016.2 (-5.8%) | 3486.3 → 3599.6 (+3.2%) | 1269.2 → 1227.0 (-3.3%) |
| q23b | 3400.1 → 3374.3 (-0.8%) | 1031.2 → 1027.2 (-0.4%) | 3646.6 → 3581.4 (-1.8%) | 1218.9 → 1242.1 (+1.9%) |
| q47 | 1017.0 → 846.2 (-16.8%) | 310.8 → 292.8 (-5.8%) | 1107.4 → 1003.4 (-9.4%) | 479.9 → 457.8 (-4.6%) |
| q51 | 628.1 → 470.5 (-25.1%) | 200.5 → 144.1 (-28.1%) | 709.8 → 459.3 (-35.3%) | 220.0 → 173.7 (-21.0%) |
| q57 | 327.4 → 309.0 (-5.6%) | 112.2 → 94.7 (-15.6%) | 500.7 → 456.6 (-8.8%) | 268.3 → 246.4 (-8.2%) |
| q67 | 1438.6 → 1253.3 (-12.9%) | 506.1 → 526.9 (+4.1%) | 1585.1 → 1289.3 (-18.7%) | 550.8 → 564.1 (+2.4%) |
| 高基数 GROUP BY | 72.2 → 73.0 (+1.1%) | 25.7 → 25.7 (+0.0%) | 82.3 → 73.4 (-10.8%) | 21.4 → 27.0 (+26.2%) |
| ORDER BY … LIMIT 100 | 83.7 → 83.0 (-0.8%) | 30.7 → 27.9 (-9.1%) | 86.6 → 87.9 (+1.5%) | 29.6 → 29.1 (-1.7%) |

同一轮的 4-worker ZeroCompile 分阶段结果：

| 阶段 | q51 | q67 | 高基数 GROUP BY | ORDER BY … LIMIT |
| --- | ---: | ---: | ---: | ---: |
| baseline | 200.5 | 506.1 | 25.7 | 30.7 |
| instances | 220.9 | 753.0 | 30.9 | 45.0 |
| aggregate | 198.9 | 644.5 | 27.2 | 43.6 |
| window | 147.7 | 720.8 | 31.9 | 49.8 |
| sort | 145.4 | 725.2 | 35.9 | 41.9 |
| final | 144.1 | 526.9 | 25.7 | 27.9 |

这些是累计阶段，不是隔离微基准。仅改实例调度并没有带来整体收益；单规格 Window
下推对 q51 的收益明显，DataFusion 直接拉取则消除了大部分调度迁移后的 q67 回退。
高基数聚合与 top-k 查询仍需结合复测判断。收尾工作移到 source 后，`sink_finish_ms`
的减少不等于计算消失；判断效果使用 Roc 总耗时和 execute 耗时。


### 9.4 交替复测与结论

完整矩阵之后，额外运行两轮 4-worker 基线/最终版交替复测；第一轮最终版先跑，第二轮
基线先跑。每个点仍为三次中最快一次，下表取两轮这些值的中位数，单位 ms。
全部 64 个复测配置通过结果校验，原始文件以 `recheck1-` / `recheck2-` 开头。

| 查询 | ZeroCompile 基线 → 最终 | 变化 | Compile 基线 → 最终 | 变化 |
| --- | ---: | ---: | ---: | ---: |
| q23a | 1015.2 → 1057.2 | +4.1% | 1202.4 → 1213.3 | +0.9% |
| q23b | 987.6 → 1025.8 | +3.9% | 1201.1 → 1245.5 | +3.7% |
| q47 | 303.9 → 291.6 | -4.0% | 444.8 → 444.9 | +0.0% |
| q51 | 201.6 → 144.2 | -28.5% | 212.8 → 161.6 | -24.1% |
| q57 | 110.3 → 98.0 | -11.2% | 258.9 → 246.2 | -4.9% |
| q67 | 498.9 → 530.4 | +6.3% | 521.2 → 557.5 | +7.0% |
| 高基数 GROUP BY | 22.1 → 26.9 | +21.2% | 21.7 → 26.0 | +19.8% |
| ORDER BY … LIMIT 100 | 30.2 → 28.9 | -4.6% | 30.1 → 29.6 | -1.3% |

主要收益集中在需要整表窗口物化的 q51：耗时减少约 24–29%；q57 减少约 5–11%。
q47 的 ZeroCompile 有小幅收益，Compile 持平。q23 约增加 1–4%，q67 约增加 6–7%。
高基数聚合的回退约 20%，绝对增加约 4–5 ms；两轮复测都存在，不能归为首轮噪声。
ORDER BY … LIMIT 的差异较小：本查询已有 local top-k，按需范围归并的额外节省有限。

因此，本实现完成并行 source 与按需收尾的结构目标，但目前只支持“窗口场景明显受益”
这一性能结论。对高基数 GROUP BY 应保留回退记录，后续再定位扫描、实例调度和 merge
的占比；本轮没有为消除单项回退而增加 eager/inline 特殊路径。单 worker 的数字仅有
首轮完整矩阵，未做两轮交替复测。未采集峰值 RSS，也未验证专门的 IO 延迟负载，不能
把设计预期当成已测的内存或公平性收益。
