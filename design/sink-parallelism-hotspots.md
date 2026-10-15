# q51 / q67 并行 sink 回退定位

2026-10-15，同机 TPC-DS SF1，4 worker / 4 runtime 线程，ZeroCompile。

本文记录第 11 节串行 LocalSink 版本的诊断，保留当时的数据和代码位置。
后续分区 finish 的实现与复测见 [设计第 12 节](sink-parallelism.md#12-修订sinkexecfinish-通过-cpu-分区并行2026-10-15已实现)。

结论：q51 的主要瓶颈是 Window 全量归并、窗口值计算串行化，再叠加本地排序
并行度不足；q67 的主要瓶颈是 ROLLUP 的键重新入表和键列输出串行化。
另外，End 交接时局部聚合状态的串行析构，在 q67 上耗时约 50 ms，原有
`sink_finish` 指标完全没有计入这一项。

## 测量口径

- 基线：`37e69782efb87a6b0e8d0b415170b0b8d25e0695`，迁移后为第 11 节串行 LocalSink 实现。
- Apple M4 Pro，Rust 1.95.0，LLVM 19.1.7，默认 release，同一份本机 SF1 Parquet。
- 未加细分计时的原始二进制，Roc 单独重复 10 次，最快总耗时：

  | 查询 | 迁移前 | 迁移后 |
  | --- | ---: | ---: |
  | q51 | 201.9 ms | 404.1 ms |
  | q67 | 473.5 ms | 847.3 ms |

- 细分计时只加入临时源码副本。每进程重复 6 次，丢弃首轮；迁移后默认配置在
  实验前后各测一组，以下阶段数据为剩余 10 次的中位数，基线为 5 次中位数。
  每轮先合计同 pipeline、同阶段的多次调用，再求中位数。
- 细分计时的查询墙钟中位数：q51 198.3 → 383.0 ms；q67 492.3 → 834.1 ms。
  插桩、日志、运行顺序和调度会扰动耗时，不能把不同批次的细项与未插桩总耗时
  拼成精确的耗时等式。表内分项中位数也不保证相加等于父阶段中位数。
- 对原始二进制另做 macOS `sample`：每查询、每版本采样 6 秒，间隔 1 ms。
  采样、构建、正确性验证与计时实验分开运行。
- 旧版 Aggregate 的 `finish_on` 包含 partial 导出/分区和并行 merge/evaluate；
  新版 partial 导出在 `LocalSink::finish`，单独计时。旧版 Window 位于 BufferSink
  的 finish 回调，不能只按 sink 类型名比较。
- 多条 pipeline 可重叠；父阶段包含子阶段。禁止把它们不加区分地相加。

## q51：重点是 store 支路和最后的两个 MAX 窗口

| Pipeline / 工作 | 行数 | 本地 finish 墙钟 | 串行 sink finish | 串行部分的主要细项 |
| --- | ---: | ---: | ---: | --- |
| P3 web 聚合 | 143,043 输出 group | 1.6 ms | 15.7 ms | 键入表 5.7，accumulator merge/resize 6.4 |
| P4 web 累计 SUM | 143,043 | 20.7 ms | 7.7 ms | 归并 1.7，SUM 3.3 |
| P7 store 聚合 | 492,419 输出 group | 5.4 ms | 55.8 ms | 键入表 20.9，accumulator merge/resize 25.7，输出 9.2 |
| P8 store 累计 SUM | 492,419 | 35.7 ms | 51.5 ms | 归并 22.7，SUM 20.1 |
| P12 full join 后两个累计 MAX | 608,669 | 35.5 ms | 124.3 ms | 归并 37.4，两个 MAX 合计 70.8 |

P12 两个 MAX 共享一份排序规格，所以只做一次 heap merge；`values` 仍按窗口
成员分别遍历 608,669 行。细项中的两个 MAX、decode/scatter/encode 均为两个
窗口调用之和。

具体调用链：

```text
WindowSinkExec::finish                         src/exec/window.rs:207
  merge_sorted                               src/exec/sort.rs:158
    BinaryHeap::pop / push / sift_up
    partition/order 编码字节比较
  values                                     src/exec/window.rs:306
    partition 边界扫描
    Running::add / value                      src/exec/window.rs:453
    MAX 的 Decimal128 值编码、比较
  scatter + scalar::encode
```

采样中，限定在 Window coordinator finish 调用树内的 2,329 个样本：
`BinaryHeap::sift_up` 自身占 22.2%，`memcmp` 占 14.2%，`Running::add` 占 13.2%；
另有 28.3% 落在被内联到 Window finish 的代码中。它们与分阶段计时指向同一组
热点；这些比例是该调用树的采样占比，不是整条查询的墙钟占比。

旧版 P8、P12 的完整 Window finish 分别为 35.9、66.8 ms，包含键编码、按
partition key 分成 8 个桶、并行排序和计算、输出编码。新版仅串行 finish 就为
51.5、124.3 ms，还各有约 35 ms 的本地排序。迁移去掉了按完整窗口 partition
分配计算任务的路径，却只并行了输入 run 的排序，窗口值计算全部留在最后。

关键依赖链是 store 聚合 P7 → store window P8 → spool P9 → table P10 →
join P11 → final window P12 → TopK P13。web 支路 P3/P4 与 store 支路重叠，
不能把两支路的 finish 都算作净回退。P10 建表从旧版 17.7 降到 14.2 ms，
P13 最终 TopK finish 仅 0.23 ms；优化最终 LIMIT 100 不能解决这里的大头。

## q67：ROLLUP 键重建和输出比 SUM 合并更贵

P3 的 9 个 grouping set 合计输入约 964,303 个 partial group，输出 501,976
个 group；其中完整 8 键的 set 输出 339,570 个 group。

| P3 串行 finish 内阶段 | 中位耗时 |
| --- | ---: |
| group key Arrow 列解码为 Cell | 17.2 ms |
| 逐行 intern：编码、hash、探测、新 group 的键所有权复制 | 184.9 ms |
| accumulator merge/resize，各 set 合计 | 约 34.0 ms |
| group key 输出：从 OwnedCell 取出 Cell | 40.3 ms |
| group key 输出：字符串编码为 Arrow 列 | 48.3 ms |
| group key 输出：数字列编码 | 1.9 ms |
| 最终 GroupIndex 析构 | 22.0 ms |
| Layout 拼接各 grouping set | 8.6 ms |
| **整个 Aggregate sink finish** | **365.8 ms** |

表内省略少量 accumulator 输出、临时数组回收及管理开销；键解码/入表包含在
`aggregate.merge_all`，键输出/最终索引析构包含在 `aggregate.finish_all`，不可
再次与这两个父阶段相加。键入表与键列输出合计约 294 ms，占整个 finish 约 80%。

关键代码：

```text
AggregateSinkExec::finish                      src/exec/aggregate/mod.rs:398
  AggregateMergeState::merge
    GroupIndex::intern                        src/exec/aggregate/group_index.rs:386
      scalar::decode
      intern_row → intern_encoded             src/exec/aggregate/group_index.rs:314
        scalar::key_bytes                     src/kernel/scalar.rs:319
        hash / probe / add / bind_owner
    Accumulator::resize / merge
  AggregateMergeState::finish_columns          src/exec/aggregate/mod.rs:154
    GroupIndex::columns                       src/exec/aggregate/group_index.rs:406
      self.cells[group * columns + column].get()
      scalar::encode → UTF-8 校验 / StringViewArray 构建
    drop(GroupIndex)                          OwnedCell 的字符串存储释放
```

完整 8 键 set 的 `intern` 单独约 125.1 ms，7 键约 50.0 ms，6 键约 19.3 ms。
高基数 set 含多个字符串列，`intern_encoded` 会重新编码键，新 group 再复制
变量宽度值；输出时又按列扫描行式保存的 OwnedCell，并构造 Arrow 字符串列。
本地 partial 已做过的键表示在这条合并路径中没有直接复用。

限定在 Aggregate coordinator finish 的 1,958 个采样中，`key_bytes` 自身占
18.8%，`intern_row` 占 10.9%，`memmove` 占 9.4%，`Accumulator::merge` 占 6.2%；
同时可见 `from_utf8`、StringView builder 和 `_xzm_free`。计时与采样均表明，
只优化 SUM 的累加循环收益有限。

还有两个不能遗漏的部分：

- **P3 End 交接析构 50.5 ms**：`src/pipeline/execution.rs:471` 的循环每交接
  一个实例就退出其作用域，`state` 在 coordinator 线程析构。AggregateLocalSink
  借用导出 partial，原 GroupIndex 和 Accumulator 仍在 LocalState 中，因此所有
  局部字符串状态随后逐个释放。这发生在 `SinkExec::finish` 之前。
- **P4 RANK**：501,976 行，串行 finish 82.5 ms，其中 heap merge 33.0 ms、
  `values(Rank)` 36.2 ms、payload concat 10.4 ms。local sort 另耗 44.4 ms。
  旧版完整 Window finish 为 59.6 ms，其中 8 个 partition 桶的并行排序/计算墙钟
  30.1 ms。最终 TopK finish 只有 0.11 ms。

旧版 P3 finish 为 206.7 ms，含 parallel partial 导出/分区 90.8 ms、parallel
merge/evaluate 99.3 ms。新版仅串行 merge/evaluate/layout 就为 365.8 ms，另有
local partial 导出墙钟 51.4 ms 和串行 LocalState 析构 50.5 ms。P3、P4 直接串接，
这两段的变化可以解释绝大多数回退。

基线另有一个中间结果口径差异：旧 8 路分区合并对空 grouping set 的每个分区
都输出一个 group，q67 的 ROLLUP 中间输出为 501,983 行，多 7 个空分区 group；
新版为 501,976 行。该差异量级不足以解释性能差距；本轮最终 Top 100 对照通过。

## 本地排序没有用满四个 worker

`src/pipeline/execution.rs:289` 每 job 最多吸收 128 个已就绪 batch。
AggregateSource 每 morsel 2,048 行，因此一个 job 可吸收 262,144 行。

- q51 P4：143,043 行全部进入一个非空 run，其余三个实例为空。
- q51 P8：两个非空 run，262,144 和 230,275 行。
- q67 P4：两个非空 run，262,144 和 239,832 行。
- q51 P12 的代表轮有三个非空 run，来自两个 join 支路的输出。

旧版按完整 partition 的 hash 再拆成 8 个任务，不受这个输入批次划分限制。
新版 local sort 在上述分布下实际只能使用 1–2 个 worker。将派发上限改小可以
改善这个问题，但 run 更多后，最后的串行 heap merge 也变贵。

## 临时对照实验

同一个诊断二进制，通过环境变量切换；表中为每配置 5 次热运行的中位数。
实验前后均重复默认配置，以展示运行波动。单位 ms。

| 配置 | q51 总耗时 | q67 总耗时 |
| --- | ---: | ---: |
| 默认，实验前 | 380.4 | 832.8 |
| End 时在 worker 释放 Aggregate 局部状态 | 394.5 | 804.3 |
| job coalesce 上限 128 → 16 | 358.4 | 819.3 |
| 两项同时开启 | 361.0 | 789.3 |
| 默认，实验后 | 392.8 | 835.4 |

释放实验中，q67 coordinator 的局部状态析构从约 50 ms 降到 0.02 ms，local
finish 墙钟从约 51 增到 65 ms，总耗时减少约 30 ms。q51 这一释放项原本只有
约 1.6 ms，没有测到查询级收益。

coalesce 实验中，q51 三个 Window 的 local finish 墙钟之和由约 92 降到 45 ms；
q67 由约 44 降到 19 ms。同时串行 Window finish 变长，抵消了部分收益。
它适合后续调度优化，但无法替代跨实例窗口分区计算和聚合分区合并。

三种实验配置 × 两条查询 × Compile/ZeroCompile，**12/12** 与 DataFusion 对照
通过。性能 trace 的 `--profile` 运行不做正确性检查，验证单独执行。这些实验
没有写回生产源码，不能视为通用优化已验证。

## 后续优先级

1. q67：为 Aggregate 的高基数 set 恢复跨实例分区合并能力，并把 group key
   输出、最终索引回收包含进分区任务。只并行累加器合并会漏掉约 80% 的 finish。
2. q51：把完整 window partition 的归并与 `values` 计算交给 worker；同一
   partition 不能按任意输入 run 独立求累计值。P12 的两个 MAX 应继续共享排序。
3. 利用已有 End 的 `StateContext::take` 能力，在 worker 导出 partial 后释放
   已结束的聚合状态。先保持 Flush 仍可复用状态，再覆盖取消/错误路径。
4. 再调 coalesce 粒度及 heap 的常数开销；单纯改小批次会增加串行归并代价。

以上第 1、2 项需要兑现原设计第 9 节的跨实例工作分配，保留 LocalSink 协议也
可以做，不必让 sink 重新拥有任意 CPU 任务提交接口。

细分数据：[hotspots CSV](../benches/sink_parallelism_hotspots.csv)，
对照实验逐轮数据：[experiments CSV](../benches/sink_parallelism_experiments.csv)。
原始 trace、采样、计时补丁和复跑方法见
[profiles](../benches/sink_parallelism_profiles/README.md)。
