# 讨论：Sink 收尾工作的并行化（`finish_on` 的去留）

状态：待讨论，未定。分支 `feat/tpcds`，写于 2026-10-15。
相关：[`tpcds-progress.md`](tpcds-progress.md)。

## 1. 背景：`finish_on` 是什么，为什么有问题

TPC-DS 性能优化中，`feat/tpcds` 分支引入了 `SinkExec::finish_on`：

```rust
// src/exec/mod.rs
pub trait SinkExec {
    fn consume(..);   // 原有：在 coordinator 上逐个接收 program 输出的 chunk
    fn combine(..);   // 原有：接收每个 worker 跑完后的 LocalState
    fn finish(..);    // 原有：输入结束后收尾
    /// 新增：同 finish，但可以把 CPU 工作交给 `cpu` 并行跑
    fn finish_on(&mut self, guard, cpu: &dyn Cpu) -> BoxFuture<Result<()>> {
        self.finish(guard)
    }
}
pub trait Cpu {
    fn run(&self, jobs: Vec<CpuJob>) -> BoxFuture<Result<()>>;
    fn parallelism(&self) -> usize;
}
```

`Pipeline::execute` 用 `ExecutorCpu` 调 `finish_on`，job 通过 pipeline 原有的
`on_cpu(executor, ..)` 跑在同一组 worker 线程上。

目前覆写它的 sink 有四个：

| Sink | 在 `finish_on` 里并行做的事 | 相关 commit |
| --- | --- | --- |
| `AggregateSinkExec` | 把各 worker 的 partial 按 key hash 分区，每个分区一个 job 合并 | `07d2e3f` |
| `TableSinkExec` | 每列的拼接和重排各一个 job | `ccf45d9` |
| `BufferSinkExec`（排序） | 按行区间分段排序，再归并 | `978830c` |
| `BufferSinkExec`（窗口） | 按 partition key 做 hash 分区，每份一个 job 排序并计算 | `978830c` |

**问题**：`finish_on` 里跑的不是 kernel，而是 sink 自己写的 Rust 闭包。
- 语义在 IR 外：分区、排序、窗口计算、聚合合并都是 `exec/` 下的手写代码，不能像
  kernel 那样单独测试、特化或替换。
- 调度在 pipeline 图外：等于在 sink 里开了一条旁路调度通道，并行度、取消、背压
  都要 sink 自己保证。

性质上和之前被否掉的 `StreamOperator` 是同一类，只是藏在了 sink 内部，违反
[`tpcds-progress.md`](tpcds-progress.md) 里的框架边界原则。结论倾向于撤掉它。

## 2. 现状：两类 sink 的数据走向

**聚合**：数据分散在各 worker 的 LocalState 里。

```
worker i: program 跑 GroupLookup / AggregateUpdate kernel
          → 结果写进自己 LocalState 里的 GroupState / AccumulatorState
结束时:   每个 LocalState → sink.combine 合并（手写 Rust：GroupIndex::intern +
          Accumulator::merge），coordinator 上串行
```

逐行更新是 kernel；合并 LocalState 的那一步不是。

**建表、排序、窗口**：数据汇总在 sink 里，LocalState 是空的。

```
worker i: program 跑完 → 输出 chunk → 交回 coordinator → sink.consume(chunk) 存进 Vec
结束时:   LocalState 什么都没有（program 不写 state）
          sink.finish 一次性处理全部行：拼接、建 hash 索引、排序或计算窗口
```

重活全在 finish 上。

## 3. 方案 A：聚合合并改成并行 combine（已确认可行）

`combine` 现在串行的原因是历史遗留：
1. 以前用 rayon 跑不了 async 代码，combine 只能留在 coordinator 上；
2. 签名是 `combine(&mut self, ..)`，需要独占 sink。

现在 job 本来就是 `on_cpu(executor, ..)` spawn 出去的 Tokio 任务，可以在 spawn
的地方顺便做 combine。调用位置见 `src/pipeline/execution.rs`：多 worker 路径在
`for local in idle { self.sink.combine(local) }` 处（第 376 行附近），单 worker
路径在第 201 行附近。

改法：
- **接口**：sink 在 start 时交出一个可共享的合并器，例如
  `fn combiner(&mut self) -> Option<Arc<dyn Combine>>`，其中
  `Combine: Send + Sync`，提供同步的 `fn combine(&self, local: LocalState) -> Result<()>`。
- **执行**：source 读到 eof 后，每个 LocalState 回到 idle 时立即 spawn 一个
  combine 任务，和还没跑完的 job 重叠。单 worker 路径行为不变。
- **聚合**：合并器内部是 P 个按 key hash 分区的 `Mutex<AggregateMergeState>`。
  每个 worker 把自己的 partial 拆成 P 份，从不同的起点轮流锁分区合并，减少锁竞争。
  （目前只有 `AggregateSinkExec` 实现了 combine，另外两个实现在测试里。）

调度留在 pipeline 执行层，sink 只负责「怎么合并」。聚合不再需要 `finish_on`。

讨论中提过但不采用的替代：两阶段聚合 pipeline。即 partial 按 key 分区导出成行，
由第二个 pipeline 用合并类 `AggregateUpdate` kind 再聚合一次。这样需要新的合并类
kind（COVAR/STDDEV 合并；COUNT DISTINCT 最好在翻译时改写成 `GROUP BY (key, value)`
后再 COUNT），以及按分区读写的 source/sink。方案 A 更简单，在不扩展 IR 的前提下就
解决了问题。

## 4. 建表、排序、窗口：两个候选方向（待定）

并行 combine 对这三个帮不上，因为它们的 LocalState 是空的。

### 方向 1：让行进入 LocalState（与聚合同一模式）

加一类写 state 的 kernel，例如 `TableAppend`，地位与 `AggregateUpdate` 相同：
program 不再把行作为输出交给 coordinator，而是由每个 worker 追加到自己的 state。
这样每个 worker 的局部工作可以放进并行 combine，finish 只剩轻量的汇总：

| | 并行 combine（每个 worker） | finish（汇总） |
| --- | --- | --- |
| 建表 | 按 key hash 分区，各分区建索引 | 拼接各分区（分区化的 table state） |
| 排序 | 把自己的行排成一个有序段 | 归并各有序段 |
| 窗口 | 按 partition key 分区 | 每个分区排序并计算，各分区互不依赖 |

代价：IR 要加这类 state kernel。排序和窗口在分区内的计算，仍需决定是 kernel 还是
exec 代码（见下）。

### 方向 2：sub-operator / 多 pipeline

在 `build_pipeline` 时由算子自己插入额外的 pipeline，例如先分区，再由下一个
pipeline 按分区处理。需要的框架设施：
- 按分区写的 sink（现有的 `exec/exchange.rs` 是广播，不分区），分区号由 program
  里的 hash kernel 算出，作为普通输出列；
- 「一个分区整体交给同一个实例」的 source，配套 sink 的 combine 只做拼接。

这是 pipeline 层的扩展，不改 IR 的计算语义；调度全部在 pipeline 图里，但改动更大。

### 两个方向都要回答的问题：分区内的排序和窗口算不算 kernel

ScalarKernel 的契约是「一行的结果只依赖这一行」，排序和窗口不满足。
- (a) IR 加一类新 kernel：输入一个完整分区，输出新 domain 上的行（排列后的行或窗口
  值），类似 DomainKernel 里的 `Sort` / `Scan`，与 Reduce 同层。可单独测试和特化。
- (b) 暂不进 IR：分区内计算留在 sink/source 的 exec 代码里。语义不比现状差，
  并行度由分区提供，不需要旁路调度。

## 5. 需要决定的事

1. 方案 A（聚合并行 combine）是否采用？
2. 建表、排序、窗口走方向 1（state kernel + 并行 combine）还是方向 2（多 pipeline）？
3. 分区内的排序和窗口走 (a) 还是 (b)？
4. 撤掉 `finish_on` 的时机：建议先做方案 A 并撤掉 `finish_on` 和 `exec::Cpu`，
   建表、排序、窗口暂时退回串行 finish，等 2 和 3 定了再做。

撤掉的代价（SF1，ZeroCompile，4 worker，机器有其他负载，数值仅供参考）：
- q51 依赖排序和窗口的并行，大约从 0.35 s 退回 1.4 s；q47、q57、q67 也会变慢；
- 建表退回串行，影响建表侧大的 join（例如 q23 的 `best_ss_customer`）；
- 聚合合并由方案 A 接替，预计不受影响。
