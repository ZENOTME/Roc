# 性能路线图：追上 DataFusion 和 DuckDB

状态：规划，各项待验证、待实现。写于 2026-10-15。
相关：[`sink-parallelism.md`](sink-parallelism.md)（第 12 节）、
[`source-parallelism.md`](source-parallelism.md)、
[`sink-parallelism-hotspots.md`](sink-parallelism-hotspots.md)、
[`tpcds-progress.md`](tpcds-progress.md)（"Known gaps and next steps"）。

本文面向实现者。每一项都按"现象和证据 → 假设 → **先做的验证** → 设计 → 验收"
组织。**先做验证，验证结论成立再实现**；验证结论不成立的，把结论写回本文，
不要实现。所有改动遵守 `tpcds-progress.md` 的框架边界：语义留在 kernel IR 和
source → program → sink 模型里，翻译器和框架不针对 TPC-DS 的具体查询做特殊处理。

## 0. 原则：不做过拟合

TPC-DS 只是用来发现问题的样本，不是优化目标本身。牺牲其他负载来换取这些查询
变快没有意义。每一项改动都必须满足：

1. **改在框架或 kernel 上。** 优先改 IR 构建和 lowering（KernelBuilder、Plan、
   Steps）、执行层、通用 kernel（哈希表、排序、字符串、聚合函数）。翻译器只做
   与前端无关的通用改写（例如去掉重复的谓词），不出现针对某个查询形状的分支。
2. **能用一句通用的话解释收益。** 例如"每行少算一次重复的子表达式"、"小表侧
   建表"。如果收益只能用"q16 变快了"来说明，就说明这不是通用改进，不要做。
3. **启发式的阈值来自微基准，而不是来自某条查询。** 例如 IN 列表从 OR 展开切换
   到集合查找的元素个数，要用不同类型、不同列表长度、不同命中率的微基准测出
   交叉点；不能按 q8 调。
4. **对代价有保护。** 运行时才能判断是否值得的优化（例如布隆过滤）必须能在
   运行中自动关闭，在"优化无效"的数据上（例如事实表不按 join 键聚集）不能比
   现在慢。
5. **不允许回退。** 验收不只看目标查询，见第 11 节：全量语句不回退、微基准覆盖
   多种数据分布，并用一个没有参与调优的负载做留出验证。

## 1. 现状

SF1，ZeroCompile，4 worker，LocalSink 迁移之后的全量运行
（[`status.md`](../integrations/datafusion/tpcds/status.md)，每条语句只跑一次，
有噪声，只用来排序，不用来比较绝对值）：

- 总耗时：Roc 13.2 s，DataFusion 6.4 s；Roc 持平或更快的语句：24 / 103。
- 差距最大的语句（Roc − DataFusion，ms）：

| 语句 | Roc | DataFusion | 差距 | 倍数 | 主要原因（见对应章节） |
| --- | ---: | ---: | ---: | ---: | --- |
| q23a | 3105 | 220 | 2885 | 14.1 | 字符串分组键、大表 semi join、串行建表（§5、§4.1、sink §12） |
| q23b | 1284 | 227 | 1056 | 5.6 | 同上 |
| q67 | 873 | 188 | 685 | 4.7 | ROLLUP 合并、窗口（sink §12、§5） |
| q51 | 474 | 90 | 385 | 5.3 | 窗口（sink §12、source、§6） |
| q14a | 513 | 207 | 306 | 2.5 | 聚合、集合运算（§5） |
| q47 | 334 | 143 | 191 | 2.3 | 窗口（§6） |
| q21 | 200 | 35 | 165 | 5.7 | 全表扫描 inventory、重复过滤（§3、§4.2） |
| q8 | 167 | 19 | 149 | 8.9 | IN 列表展开（§3.2、§3.3） |
| q78 | 273 | 125 | 148 | 2.2 | 聚合 |
| q16 | 105 | 7 | 98 | 14.6 | semi / anti join 在大表上建表（§4.1） |
| q48 | 125 | 40 | 85 | 3.1 | OR 谓词、重复过滤（§3） |
| q39a / q39b | 117 / 104 | 37 / 35 | 80 / 69 | 3.2 / 2.9 | 运行时剪枝、stddev 合并（§4.2、§5） |
| q82 | 86 | 13 | 73 | 6.7 | semi join 在 store_sales 上建表（§4.1） |
| q97 | 93 | 28 | 65 | 3.3 | full join 两侧聚合 |

Compile 模式另有编译开销：q23a/b 约 190–200 ms，q47、q57 约 150 ms
（[`sink_parallelism.csv`](../benches/sink_parallelism.csv) 的 build_ms 列），
4 worker 下 q57 的 Compile 总耗时 261 ms，ZeroCompile 只有 112 ms（§7）。

## 2. 常驻的 pipeline 级 profile（所有验证的前提）

后面每一项的验证都需要知道"时间花在哪条 pipeline、每条 pipeline 处理了多少行"。
`sink-parallelism-hotspots.md` 的分段计时是临时补丁，需要改成常驻、默认关闭的
功能。

**设计**：

- 执行层在 `Pipeline::execute` 中按 pipeline 记录（仅在开启时）：source 产出
  行数、Program 输出行数（有输出时）、各实例 run 的累计 CPU 时间、LocalSink
  consume 与 finish 的累计时间、`SinkExec::consume` 与 `finish` 的墙钟时间、
  pipeline 的开始和结束时间。
- 开关放在 `PipelineExecutionConfig`（例如 `profile: bool`），结果通过
  `PipelineGraphExecutor` 返回；关闭时不调用任何计时函数。
- TPC-DS runner 增加 `--pipelines FILE`，把每条语句每条 pipeline 一行写成 CSV，
  并附带 pipeline 的说明（source 类型、sink 类型、`explain` 的第一行）。
- 另外输出每个 Program 的 kernel 数（`KernelGraph.kernels.len()`）和 step 数，
  用于 §3 的验证。

**验收**：开启后，sink §12 和本文各项需要的数字都能从 CSV 直接得到；关闭时
TPC-DS 总耗时与现在相同（±2%）。

## 3. 翻译器和 IR 生成的浪费（改动小、收益确定，优先做）

### 3.1 扫描上的谓词被求值两次

**证据**：`--explain q21` 中每条带过滤的扫描 pipeline 都有两个相同的
`Filter`：`d0` 上一份，`d1` 上又重新计算一遍完全相同的比较。原因在
`integrations/datafusion/src/plan.rs` 的 TableScan 翻译：逻辑计划中下推到扫描的
`scan.filters` 被翻译成一个 FilterOperator（注释写着"evaluating them again is
harmless"），而 provider 报告为 Inexact 的谓词，DataFusion 会同时在上方保留
Filter 节点，于是被翻译成第二个 FilterOperator。

**验证**：用第 2 节的 profile统计全量 103 条语句中"扫描 pipeline 上相邻两个 Filter 的谓词
在结构上相等"的数量，以及去掉之后的 kernel 数变化。

**设计**：翻译 Filter 节点时，如果它的输入（跳过投影）是 TableScan，就把 Filter
的谓词按 AND 拆开，去掉与 `scan.filters` 中结构相等（`Expr` 相等）的合取项；
都去掉了就不生成这个 FilterOperator。扫描上的谓词仍然交给 DataFusion 做
row group 剪枝（`storage.rs` 的 DataSourceExec 保持不变）。

**验收**：所有扫描 pipeline 只有一次过滤；103/103 两种模式通过；q21、q48、q13、
q85 等有大量扫描过滤的语句的 execute 时间下降。

### 3.2 相同的子表达式被重复计算（需要 CSE）

**证据**：q8 的 `substr(ca_zip, 1, 5) IN (约 400 个邮编)`：翻译器把 IN 展开成
`x = a OR x = b …` 时，每一项都重新翻译 `l.expr`（`plan.rs` 中 `Expr::InList`
分支的 `l.expr.clone()`），pipeline 2 里 `Substring` 出现了 801 次、`Equal`
800 次、`And` 3195 次、`Or` 2394 次，单个 Program 约 11,000 个 kernel。

**验证**：第 2 节的 profile统计每个 Program 中"function 和输入变量都相同的纯 kernel"
的重复次数，确认 q8 之外还有哪些语句受影响。

**设计**：在 `KernelBuilder` 中对纯 kernel 做 hash-consing（通用的 CSE）：
`Constant`、纯 `Call`（不包括 `RaiseIf` 和有副作用的 kernel）、`Select`，在同一个
domain 中如果 function 和输入完全相同，就直接返回已有的输出变量，不再新增
kernel。这是 IR 构建层的通用优化，对任何翻译器都有效。翻译器中 IN 列表的展开
也改为先翻译一次 `l.expr`（无论是否有 CSE 都应该这样写）。

**验收**：q8 的 kernel 数从约 11,000 降到数百以内；q8 的 execute 时间显著下降；
所有测试通过。

### 3.3 大 IN 列表改为集合查找

**证据**：同上。即使去掉重复，400 个元素的 IN 仍然是 400 次比较加 400 次 OR。

**设计**：新增纯 kernel `ScalarFunction::InSet`（输入一个值，输出布尔），常量
集合在构建时确定：整数用排序数组或位图，字符串用哈希集合。NULL 语义与 SQL 一致：
输入为 NULL 时结果为 NULL；没有匹配且列表中含 NULL 时结果为 NULL。翻译器在列表
元素都是常量、数量超过阈值（初值 8）时使用它，否则保持现在的 OR 展开。
ZeroCompile 用原生实现；Compile 模式先调用原生函数，不需要生成代码。

**阈值**：用微基准测出 OR 展开与集合查找的交叉点，覆盖 Int32 / Int64 /
Decimal128 / Utf8View、列表长度 2–1000、命中率 0%–100%、NULL 比例 0%–50%；
阈值按类型分别确定。

**通用性**：任何前端的 `IN` 都受益；短列表保持原有路径，不会变慢。

**验收**：微基准中每种类型在阈值两侧都不比原路径慢；q8 的谓词部分在 pipeline
profile 中可以忽略不计。

### 3.4 过滤上下文中的三值逻辑展开

**证据**：`--explain q21`：`d_date BETWEEN a AND b` 在可为 NULL 的列上生成 11 个
kernel，其中 `Not`、`And`、`Or` 用来计算结果的有效位（三值逻辑）。但 Filter 只
保留结果为 TRUE 的行，NULL 和 FALSE 的处理完全相同。

**设计**：放在 IR 层，而不是翻译器里，这样与前端无关。

1. 把三值逻辑作为 IR 的基本函数：`And3`、`Or3`、`Not3`、`IsTrue`，输入输出都是
   (值, 有效位) 对，代替翻译器现在用 `Not` / `And` / `Or` 手工拼出有效位的做法。
2. lowering 时做"需求分析"：一个布尔变量如果只被 Filter、`Reduce(Any)` 或其他
   只关心 TRUE 的使用者使用，就只需要它的 TRUE 位，按以下规则递归化简：

- `T(比较) = 有效 ∧ 比较`；`F(比较) = 有效 ∧ ¬比较`
- `T(a AND b) = T(a) ∧ T(b)`；`T(a OR b) = T(a) ∨ T(b)`；`T(NOT a) = F(a)`
- `F(a AND b) = F(a) ∨ F(b)`；`F(a OR b) = F(a) ∧ F(b)`；`F(NOT a) = T(a)`
- 其他表达式退回到一般翻译，再取 `有效 ∧ 值`

3. 被投影输出或参与其他计算的布尔变量仍然计算完整的三值结果。

**通用性**：任何前端生成的谓词都受益；这是 lowering 的局部化简，不改变语义。

**验证**：在 §2 的 kernel 数统计中比较前后；在单元测试中对含 NULL 的随机输入
比较新旧翻译的过滤结果。

**验收**：BETWEEN 在可为 NULL 的列上降到 3–4 个 kernel；结果完全一致。

## 4. Join 策略

### 4.1 semi / anti join 的建表侧选择（"在保留侧建表并标记"）

**证据**：`plan.rs` 的 `join` 只对 inner join 按行数估计选择 probe 侧；semi、anti、
left、mark join 总是在不保留的一侧建表。于是：

- q16：`EXISTS` / `NOT EXISTS` 的子查询侧是完整的 catalog_sales（约 144 万行）
  和 catalog_returns，`--explain q16` 中 pipeline 3、4 就是没有过滤的整表建表；
  而外侧经过日期、州、呼叫中心过滤后只剩很少的行。DataFusion 7 ms。
- q82、q37：semi join 的子查询侧是整个 store_sales / catalog_sales。
- q23：`best_ss_customer` 约 290 万行建表只为了测试是否存在（`tpcds-progress.md`
  已知问题 4）。

**验证**：用第 2 节的 profile列出所有 semi / anti / mark join 的建表行数和 probe 行数，
找出"建表侧远大于 probe 侧"的 join 及其建表耗时（Table sink 的 finish）。

**设计**：新增一种 join 执行方式：在**保留侧**（小）建表，用另一侧（大）探测并
**标记**表中命中的行，再输出被标记（semi）或未被标记（anti）的行。在现有模型中
需要：

- kernel：`TableMark { state, marks, row }`，`has_effect`，在**本实例的**标记位图
  （一个 StateId，大小为表的行数）中把命中的表行置位。不写共享的表，遵守
  sink-parallelism 第 2 节的不变式："kernel 写入的 state 都归 pipeline 的 sink
  所有"。表本身仍然是只读的。建表侧本来就小，P 份位图的内存可以忽略。
- probe pipeline 的 sink（`MarkSinkExec`）：各实例的位图在交接时按位或合并，
  发布"表 + 合并后的标记"。
- 一条新的 pipeline：以表和标记为 source（按行段并行读取），输出被标记（semi）
  或未被标记（anti）的行，作为 join 的输出继续往下。
- 带残余条件的 semi join（q16 的 `cs1.warehouse <> cs2.warehouse`）：在 probe 侧
  对每对 (probe 行, 表行) 求残余条件，满足的才标记。
- 翻译器按行数估计选择：保留侧的估计行数小于另一侧的若干分之一时使用标记
  方式。比例阈值用微基准确定：构造两侧行数比例从 1:1000 到 1000:1、匹配率
  从 0% 到 100% 的 semi / anti join，测出两种方式的交叉点。
- **估计错误时的代价**：估计偏差会让两种方式都可能在大表上建表，最坏情况与
  现在相同，不会更差。实现时在第 2 节的 profile 中记录估计与实际的偏差。

**通用性**：这是所有 hash join 实现都有的"选择建表侧"能力（DataFusion 的
`JoinSelection` 会把 LeftSemi 换成 RightSemi），不依赖查询形状。

**验收**：微基准中每个比例下选中的方式都不比另一种慢；q16、q82、q37 的
semi join 不再整表建表；第 11 节的全量不回退。

### 4.2 运行时过滤（建表之后裁剪 probe 侧的扫描）

**证据**：q21 的 probe pipeline（`--explain q21` 的 pipeline 5）读取整张
inventory（约 1170 万行），再通过 `Reduce(Any)` 和日期表做 semi join；DataFusion
用 join 建表侧产生的动态过滤裁剪 date 有序的事实表的 row group，只有 35 ms。
q39 情况类似（`tpcds-progress.md` 已知问题 5）。

我们的条件比 DataFusion 更好：**probe pipeline 要等建表 pipeline 结束才启动**，
因此启动扫描时建表侧的键是完全已知的，不需要"动态"。

**验证**：

- 用第 2 节的 profile统计每条 probe pipeline 的扫描行数和经过第一个 join 之后的行数，
  选择率很低（< 10%）的 join 就是候选。
- 检查 SF1 数据中事实表的 row group 是否按日期键有序（每个 row group 的 min/max
  统计），以确认 row group 剪枝有效。

**设计**（分两层，可以分开做）：

1. **键范围剪枝**：建表时记录每个键列的 min / max（Table sink 的 finish 中顺带
   计算）。probe pipeline 启动扫描时，把"键 ∈ [min, max]"作为附加谓词交给
   storage，由 DataFusion 的 row group / page 剪枝使用。这只是存储层的提示，
   不改变语义（probe 程序中原有的 join 仍然执行）。需要扩展 `ScanRequest`，
   让它能携带运行时谓词，`DataFusionScan` 在 `start_scan` 时把它加到 Parquet
   的剪枝谓词上。
2. **布隆过滤**：建表时同时构建布隆过滤器（或小键域时用位图），probe 程序的
   第一个 kernel 先用它过滤，再进入 Expand。这是一个只读 state 上的纯 kernel，
   和 TableGather 同类。**必须自适应**：每个实例统计通过率，前若干个窗口的
   通过率高于阈值（初值 80%）时，本实例后续不再使用（kernel 直接放行），因为
   此时它只增加开销。

**代价保护**：

- 键范围剪枝只在扫描启动时计算一次，代价可以忽略；在不按键聚集的数据上只是
  剪不掉 row group，不会更慢。
- 布隆过滤用自适应关闭保证最坏情况接近现在。
- 验证时除了原始数据，还要用**打乱行顺序**的事实表副本跑一遍，确认在剪枝无效
  的数据上没有回退。

**通用性**：任何 hash join 的 probe 侧都适用；收益取决于数据，代价有上界。

现有的 `plan.rs::Reduction`（计划阶段的 semi join 化简）保留；运行时过滤是对它的
补充，覆盖计划阶段无法知道过滤结果的情况。

**验收**：q21、q39a/b 的扫描行数明显下降；打乱行顺序的数据上全量不回退；
第 11 节的全量不回退。

### 4.3 inner join 的行数估计

**证据**：inner join 按 `Rel.rows` 选择 probe 侧，估计来自翻译器的简单规则
（例如 semi join 取 probe 侧的一半）。

**验证**：第 2 节的 profile输出每个 join 的估计行数和实际行数，统计建表侧选错（实际建表
侧更大）的 join 数量和代价。只有选错的情况造成了可观的耗时，才做下面的设计。

**设计**：利用 Parquet 的统计和 DataFusion 的行数估计改进 `Rel.rows`；对过滤
选择率使用 DataFusion 的统计（如有）。

## 5. 聚合的内部实现

已知问题见 `tpcds-progress.md` 的 1、2 条和
[`sink-parallelism-hotspots.md`](sink-parallelism-hotspots.md) 中 q67 的分析：
合并时键入表（`GroupIndex::intern`、`scalar::key_bytes`）和键列输出占 Aggregate
finish 的约 80%；group key 以 `OwnedCell` 按行保存，输出时还要逐行取出再构造
Arrow 字符串列；每个 group 的 accumulator 约 180 字节。

sink §12 把这部分并行化，但每行的开销本身没有变。本节在 §12 完成之后做。

**验证**：

- 微基准：`GroupIndex::intern` 在 1、2、4 个字符串键、10 万到 100 万个 group 下的
  每行耗时，与 Arrow 的 `RowConverter` + 哈希表、以及 DataFusion 的
  `GroupValuesRows` 对比。
- 第 2 节的 profile给出 q23、q67、q14a 中聚合 pipeline 的本地 run 耗时和 finish 耗时。

**设计方向**（按验证结果选择）：

1. **键以列式存储**：group key 存成按列的数组加字节 arena（变长值保存偏移），
   每个 group 记录 hash。输出 key 列时直接构造 Arrow 数组，不再经过 Cell。
2. **按窗口批量编码键**：一次对整个窗口的 key 列编码和计算 hash，而不是每行
   调用 `key_bytes`。
3. **partial 携带 hash 和编码后的键**：合并时直接使用，不再重新解码、编码、算
   hash。
4. **accumulator 按函数类型存储**：每种聚合函数用自己的列式状态（例如 SUM(i128)
   就是一个 `Vec<i128>` 加计数），合并时直接按数组合并，不经过 Arrow 数组和 Cell。

**通用性**：这是哈希聚合本身的实现，与查询无关；DataFusion（`GroupValues`
按列存储）和 DuckDB（行布局加 hash 前缀）都是这个方向。

**验收**：微基准中各种键类型、基数和 NULL 比例下的每行耗时都下降，没有变慢的
组合；q67、q23、q14a 的聚合 pipeline 耗时随之下降；第 11 节的全量不回退。

## 6. 窗口和排序的内部实现

**证据**：`sink-parallelism-hotspots.md`：Window finish 中 `BinaryHeap` 归并、
`memcmp`、`Running::add` 是主要热点；`values` 对每个窗口成员都要遍历全部行；
参数先 `scalar::decode` 成 Cell，结果再 `scalar::encode`。

sink §12 和 source-parallelism 解决并行度和物化的问题，本节处理每行的开销，
在它们之后做。

**设计方向**：

- 按函数和类型特化 `values`：常见组合（ROW_NUMBER、RANK、SUM / AVG / MAX /
  MIN over Int64 / Decimal128 / Float64，以及三种帧）直接在 Arrow 原生数组上
  计算，不经过 Cell 和 `Running` 的动态分派。
- 同一种规格的多个窗口成员合并到一次遍历中（q51 的两个 MAX）。
- 分区边界和 peer 边界在归并时顺带记录（比较 partition / order 字节时已经知道是否
  相等），`values` 不再重新比较。
- 归并：桶内的 run 数很少时用两两归并或败者树代替 `BinaryHeap`。

**验证**：对每种函数和帧做微基准；用第 2 节的 profile比较 q47、q51、q57、q67 的 Window
任务耗时。

## 7. Compile 模式的编译开销

**证据**：Compile 模式的 build 时间：q23a/b 约 190–200 ms，q47、q57 约 150 ms；
4 worker 下 q57 的 Compile 总耗时 261 ms，ZeroCompile 112 ms。`tpcds-progress.md` 已知
问题 6：LLVM 在构建图时编译所有程序。

**验证**：

- 统计每条语句的 Program 数、生成的函数数，以及结构相同的函数（按 IR 文本或
  结构哈希）的重复比例。
- 测量单个函数的编译耗时分布，确认开销来自函数数量还是单个函数的大小（§3.2
  的重复子表达式也会让函数变大）。

**设计方向**（按验证结果选择）：

1. 按结构哈希缓存编译结果（进程内），相同的融合函数只编译一次。
2. 各 pipeline 的程序并行编译。
3. **分层执行**：Program 先以 ZeroCompile 方式开始运行，同时在后台编译，编译
   完成后后续的窗口切换到编译后的代码。两种模式本来就保证结果一致，切换只需要
   以 batch 为边界。

**验收**：Compile 模式在所有语句上不慢于 ZeroCompile；在计算密集的语句上更快。

## 8. 字符串处理

**证据**：`tpcds-progress.md` 已知问题 7：字符串比较和 LIKE 走逐行路径
（`Step::Text`）；q8 的 `Substring`、q23 的 `substr(i_item_desc, 1, 30)` 都是
字符串函数。

**验证**：微基准比较 `Substring`、等值比较、LIKE（前缀、后缀、包含）在 Utf8View
上的每行耗时与 arrow-rs 的计算 kernel；在 §2 的 profile 中找出字符串函数占比高的
pipeline。

**设计方向**：

- `Substring` 在 Utf8View 上直接生成新的 view（不拷贝字节，前缀不超过 12 字节时
  内联）。
- 常量比较和 LIKE 的常见形态（前缀、后缀、包含）按窗口批量执行，利用 Utf8View
  的 4 字节前缀先做快速比较。

## 9. 实施顺序

| 顺序 | 内容 | 依赖 |
| --- | --- | --- |
| 1 | §2 pipeline 级 profile | 无 |
| 2 | §3.1 重复过滤、§3.2 CSE、§3.3 IN 集合、§3.4 过滤上下文 | §2（用于验证） |
| 3 | sink-parallelism §12（分区并行收尾） | 已有设计 |
| 4 | §4.1 保留侧建表并标记、§4.2 运行时过滤 | §2 |
| 5 | source-parallelism（async 实例和收尾下推） | sink §12 |
| 6 | §5 聚合内部实现、§6 窗口内部实现 | sink §12、source |
| 7 | §7 Compile 开销、§8 字符串 | §2 |

第 2 步的改动都在翻译器和 KernelBuilder 中，范围小、风险低，可以和第 3 步并行。
每一步完成后，按第 11 节验收，结果追加到第 10 节。

## 10. 进展记录

（每完成一项，在这里追加：日期、提交、验证结论、性能变化。验证结论不成立的
项也要记录。）

## 11. 验收规则（每一项都适用）

1. **全量不回退。** 改动前后各跑一次全量 103 条语句（ZeroCompile 和 Compile，
   1 和 4 worker，`--repeat 3`，同一台机器、同一份数据）。任何语句变慢超过
   max(10%, 5 ms) 都要解释原因；不能解释的视为回退，必须先解决。报告变快、
   持平、变慢的语句数，而不只是总耗时。
2. **微基准覆盖多种数据分布。** kernel 和执行层的改动要有对应的微基准
   （`benches/`），参数至少覆盖：行数、键的基数、键类型（整数、Decimal、短 /
   长字符串）、NULL 比例、倾斜程度。改动在所有参数组合上都不能明显变慢。
3. **留出负载。** 增加一个没有用来调优的负载作为留出验证：建议用同一个
   DataFusion 集成跑 TPC-H SF1（数据用 DuckDB 的 `tpch` 扩展生成，写一个与
   `tpcds` 相同的 runner）。每个里程碑在 TPC-H 上对比改动前后，只用来检查回退，
   不用它来调参。
4. **正确性。** 103/103 两种模式通过；新增 kernel 有包含 NULL、空输入和边界值的
   单元测试，并对比原路径的结果。
