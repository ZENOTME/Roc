//! 新执行模型的设计草稿：先确定物理计划、逻辑列和 Scan 输入的契约。
//!
//! 本文件作为私有模块接入 crate，方便编辑器类型检查与跳转。
//! 方法体仅占位，不接入实际执行路径，不实现执行、编译或存储接口。
//! 用户提交的是已经绑定依赖、确定类型的物理计划，Roc 负责校验这些声明。
//!
//! 示例：
//!   Scan 定义 price、quantity。
//!   Project 引用它们，定义 amount = price * quantity。
//!   后续算子直接引用 amount 的 ColumnId。
//! 名称在构建/反序列化阶段解析，构建完成后全部通过 ColumnId 连接。
//! Column 表达带逻辑类型和 NULL 属性的列；Kernel 构建阶段再将其展开为
//! 若干内部 Variable。两个身份空间分开，通过当前 pipeline 的 ColumnBinding 连接。

// 草稿保留接口参数和未使用的定义，具体实现后再移除这些局部豁免。
#![allow(dead_code, unused_variables, clippy::new_without_default)]

use std::collections::HashMap;

use arrow_buffer::{Buffer, MutableBuffer};
use arrow_schema::DataType;

use crate::error::Result;

// ── 列：一份计划内统一的值身份 ────────────────────────────────────────

/// ID 只在所属 ColumnContext 内有意义，不作为跨计划的全局编号。
/// 字段私有：通过 context 分配，不由调用方随意构造。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ColumnId(usize);

#[derive(Clone, Debug)]
pub struct ColumnType {
    pub data_type: DataType,
    pub nullable: bool,
}

/// 一个逻辑列的声明，包含数据类型和 NULL 属性，不持有 Array、buffer 或本批行数。
/// 是否最终物化为数组，由后续执行设计决定。
#[derive(Clone, Debug)]
pub struct Column {
    pub ty: ColumnType,
}

/// 整份物理计划共享一份 context；算子和表达式通过 ColumnId 引用逻辑列。
/// 内部 Variable 使用独立的身份空间，不在这里注册。
/// columns 的下标就是 ID，长度就是下一个 ID，无需另存 inc_id。
pub struct ColumnContext {
    columns: Vec<Column>,
    names: HashMap<String, ColumnId>,
}

impl ColumnContext {
    pub fn new() -> Self {
        todo!("设计草稿")
    }

    /// 登记列身份及类型。可选名字在本 context 中必须唯一；重复命名报错。
    /// 登记不等于已有生产者：计划还需指定由哪个 Scan 或计算定义这个值。
    pub fn allocate_column(&mut self, ty: ColumnType, name: Option<String>) -> Result<ColumnId> {
        todo!("设计草稿")
    }

    pub fn get(&self, id: ColumnId) -> Result<&Column> {
        todo!("设计草稿")
    }

    /// 仅用于计划构建/反序列化。未定义名字报错，表达式中不保留字符串查找。
    /// 解析成功只表示列存在，不表示它在当前算子的输入中可见。
    pub fn resolve(&self, name: &str) -> Result<ColumnId> {
        todo!("设计草稿")
    }
}

// ── 表达式：引用已有列，明确指定计算结果绑定到哪个列 ────────────────

/// 只列出说明连接方式所需的节点；完整表达式种类后续沿用现有能力。
/// 没有 evaluate 方法；各具体表达式自己决定如何构建 Kernel。
pub enum Expression {
    Reference(ReferenceExpression),
    Add(AddExpression),
    Multiply(MultiplyExpression),
}

impl Expression {
    pub fn output(&self) -> ColumnId {
        match self {
            Self::Reference(expr) => expr.output(),
            Self::Add(expr) => expr.output(),
            Self::Multiply(expr) => expr.output(),
        }
    }

    /// 分派给具体表达式；生成输出列的绑定，不执行任何一批数据。
    ///
    /// 返回该表达式待报的错误标记（Boolean，None 表示不可能出错）。错误像 validity 一样
    /// 向上组合，由使用根表达式的算子加一个 RaiseIf：Project 每个根表达式一个，
    /// Filter 在过滤前对谓词加一个。组合规则：
    ///   - 严格函数：error = 各参数 error OR (函数错误标记 AND 输出 validity)，NULL 行上的错误被压住。
    ///   - CASE / COALESCE：error = 条件 error OR Select(条件, 各分支 error)，未选中分支的错误被压住。
    ///   - AND / OR：不保证短路，两侧 error 直接 OR。
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Option<VariableId>> {
        match self {
            Self::Reference(expr) => expr.build_kernels(builder),
            Self::Add(expr) => expr.build_kernels(builder),
            Self::Multiply(expr) => expr.build_kernels(builder),
        }
    }
}

/// 引用已有列，不额外定义结果列。
pub struct ReferenceExpression {
    pub column: ColumnId,
}

impl ReferenceExpression {
    pub fn output(&self) -> ColumnId {
        self.column
    }

    /// 查询已有 ColumnBinding，确认当前输入可见；不重新分配 Variable 或生产者，不可能出错。
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Option<VariableId>> {
        todo!("设计草稿")
    }
}

/// 计算节点自己保存参数及其定义的结果列。
pub struct AddExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub output: ColumnId,
}

impl AddExpression {
    pub fn output(&self) -> ColumnId {
        self.output
    }

    /// 先构建左右输入，再按 ColumnType 选择加法函数并读取 ColumnBinding。
    /// 输出 validity 是输入 validity 的 AND（Call(And)）；None 视为 true，只有一个输入可能为空时
    /// 复用它的 validity，均非空时无需 validity。加法本身是 Call，输出 [value, overflow]；
    /// 返回 left.error OR right.error OR (overflow AND validity)，最后绑定输出列。
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Option<VariableId>> {
        todo!("设计草稿")
    }
}

pub struct MultiplyExpression {
    pub left: Box<Expression>,
    pub right: Box<Expression>,
    pub output: ColumnId,
}

impl MultiplyExpression {
    pub fn output(&self) -> ColumnId {
        self.output
    }

    /// 与加法相同，只是函数换成乘法。
    /// 规则由表达式维护；Builder 不集中实现各类表达式的展开。
    pub fn build_kernels(&self, builder: &mut KernelBuilder<'_>) -> Result<Option<VariableId>> {
        todo!("设计草稿")
    }
}

// ── 物理计划：算子显式描述输入、输出列 ────────────────────────────────

pub struct OperatorTree {
    pub columns: ColumnContext,
    pub root: OperatorTreeNode,
}

pub struct OperatorTreeNode {
    pub operator: Box<dyn Operator>,
    pub children: Vec<OperatorTreeNode>,
}

/// 输入就是子算子的输出，算子引用哪些列由其表达式体现，因此只声明输出。
pub trait Operator {
    fn name(&self) -> &'static str;

    /// 有序输出列，不依赖子算子即可得到。用于：确定 Sink 边界编码哪些列；
    /// 校验引用只来自当前输入中可见的列；校验每个被引用的列都有生产者。
    /// 只能是子算子的输出或本算子产生的列；Filter / Limit 等可借此丢掉不再需要的列。
    fn output_columns(&self) -> Vec<ColumnId>;

    /// 沿用旧的 pipeline 切分算法，由算子负责递归构建子算子。在当前 pipeline 的
    /// KernelBuilder 中加入自己的 Kernel；设置 Source 时把输出列绑定到 root 域，
    /// 设置 Sink 时确定输出边界，改变行域的算子（如 Filter）自己切换当前域。
    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PipelineId(usize);

/// 与旧版的区别是每个 pipeline 带一个 KernelBuilder；Source / Sink 及依赖关系沿用旧结构。
pub struct PipelineGraphBuilder<'plan> {
    pipelines: Vec<KernelBuilder<'plan>>,
}

impl<'plan> PipelineGraphBuilder<'plan> {
    pub fn kernel_builder(&mut self, pipeline: PipelineId) -> &mut KernelBuilder<'plan> {
        todo!("设计草稿")
    }
}

// 用于物理绑定怎么从 Chunk 得到一个 Column 
pub struct ColumnChunkBind {
    column_id: ColumnId,
    col_index: usize,
}

/// Source 只代表存储请求描述，具体存储接口暂不在本轮设计。
pub struct ScanOperator<Source> {
    pub source: Source,

    /// 用户预先声明这些列，并将它们交给 Scan 作为值的生产者。
    /// 每项绑定的 batch.column(col_index) 对应 column_id。
    /// col_index 是扫描结果中的列位置，不一定是原始表中的列位置。
    pub outputs: Vec<ColumnChunkBind>,
}

// Scan 的 Operator 契约：inputs = []，outputs = 上面的有序列表。
// 每批输入必须满足：
//   1. 每个 col_index 在 batch 中有效，扫描返回的列布局符合绑定约定。
//   2. 对应列的类型及 NULL 数据符合 columns.get(column_id) 的声明。
//   3. 每批只更新这些列的数据绑定，不申请新的 ColumnId。
//   4. Array owner 在使用期间保持有效；buffer 适配及所有权细节后续讨论。
// 名称只参与计划绑定，不按每批 RecordBatch 的字段名字重新查找列。

/// 用 Project 说明“声明计算”和“选择输出”是两件事。
pub struct ProjectOperator {
    pub expr: Vec<Expression>,
}

// Project 的输出按 expr 顺序取各根表达式的 output()，无需再维护一份输出列表。
impl<Source> Operator for ScanOperator<Source> {
    fn name(&self) -> &'static str {
        "Scan"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        todo!("设计草稿")
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        todo!("设计草稿")
    }
}

impl Operator for ProjectOperator {
    fn name(&self) -> &'static str {
        "Project"
    }

    fn output_columns(&self) -> Vec<ColumnId> {
        todo!("设计草稿")
    }

    fn build_pipeline(
        &self,
        node: &OperatorTreeNode,
        current: PipelineId,
        graph: &mut PipelineGraphBuilder<'_>,
    ) -> Result<()> {
        todo!("设计草稿")
    }
}

// 从上面可以引出上层对执行计划的描述：
// 执行计划由 Operator 构成，Opertar 包含表达式
// 这里的 Operator 可以视为 输入 Chunk ，输出 Chunk
// Chunk 是由 Column 构成

// 本草稿只定义数据和接口，暂不展开执行或校验实现。
//
// 计划校验需要保证：
//   - 每个被使用的列有明确生产者；转发已有列不算重复定义。
//   - 引用在当前输入中可见，不能只因为 context 中存在就跨分支引用。
//   - 计算结果与输出列声明的类型一致。
//
// 列的注册名和结果字段的显示别名是不同概念。最终输出字段、Filter 的
// 列重绑定、Aggregate、Program/frame、Arrow 输出适配留待后续逐项讨论。

// ── Column → Variable：当前 pipeline 内可查询的构建期绑定 ─────────────


/// 只在所属 KernelBuilder 内有意义，与 ColumnId 独立分配。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct VariableId(usize);

/// 内部基础值类型，不含逻辑列的 nullable；有效性本身用 Boolean Variable 表达。
/// 这里只列出说明展开关系所需的类型，完整类型集合后续补齐。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VariableType {
    Int32,
    Int64,
    Boolean,
    BytePointer,
    Usize,
}

/// 当前行的一个基础值声明；不因分配 ID 就分配物化 buffer。
pub struct Variable {
    pub ty: VariableType,
    /// 所属行域；"第 i 行"只在该域内有意义。
    pub domain: DomainId,
}

pub struct VariableContext {
    variables: Vec<Variable>,
}

impl VariableContext {
    pub fn allocate_variable(&mut self, ty: VariableType, domain: DomainId) -> VariableId {
        todo!("设计草稿")
    }
}

/// 构建期 Column 到内部 Variable 的映射，不保存某一行的实际值。
/// 每个变体对应一种逻辑类型；所有 Variable 属于同一个域。
/// validity 为 Boolean，true 表示非 NULL，与 Arrow 的 validity 位图一致，入口和出口可零拷贝；
/// None 表示该列不可为 NULL。
/// 数组类型以后按 start / len 加数组 validity 展开，元素 buffer 为批级常量。
#[derive(Clone, Copy, Debug)]
pub enum ColumnVariableBinding {
    Int32 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Int64 {
        value: VariableId,
        validity: Option<VariableId>,
    },
    Boolean {
        value: VariableId,
        validity: Option<VariableId>,
    },
    /// ptr 为 BytePointer，len 为 Usize。
    Utf8 {
        ptr: VariableId,
        len: VariableId,
        validity: Option<VariableId>,
    },
}

impl ColumnVariableBinding {
    /// "逻辑类型 → Variable 组成" 的唯一规则，供 Scan 入口和表达式输出共用。
    /// nullable 时才分配 validity；不支持的 DataType 报错。
    pub fn allocate(
        ty: &ColumnType,
        variables: &mut VariableContext,
        domain: DomainId,
    ) -> Result<Self> {
        todo!("设计草稿")
    }

    pub fn validity(&self) -> Option<VariableId> {
        todo!("设计草稿")
    }
}

// ── 行域与 Kernel ─────────────────────────────────────────────────────

/// 只在所属 KernelBuilder 内有意义，与 ColumnId、VariableId 独立分配。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DomainId(usize);

/// 行域：一个行空间，其中每个 Variable 相当于一列。
///
/// 结构：
///   - root 域对应 pipeline 入口的一个 root 切片；其余每个域都由一个 DomainKernel 创建。
///   - 行域组成一棵树。非 root 域的每一行通过 parent_row 指向父域中的一行；parent_row
///     本身是该域中类型为 Usize 的 Variable，由创建它的 DomainKernel 定义。
///     它的值是切片内的下标，取决于 root 切片大小，只供执行层使用，不参与语义计算。
///   - parent_row 单调不减：子域行的顺序跟随父域，同一父行对应的子行连续。
///
/// 引用规则：
///   - 每个 Variable 属于且只属于一个行域。
///   - 一个域可以直接引用本域及祖先域的 Variable：子域第 k 行读到的是该 Variable
///     沿 parent_row 链对应到祖先域那一行的值。
///   - 兄弟域、子孙域的 Variable 不能直接引用，只能经 Union / Reduce 显式传入。
///
/// 祖先引用如何落地（融合时为别名；不融合时提前压缩到子域，或按 parent_row 延迟
/// gather）是执行策略，不影响语义。parent_row 只在需要跨域对应关系且跨越 Program
/// 边界时才物化。
pub struct RowDomain {
    /// root 域为 None。Filter / Expand / Reduce 的父域是输入域，Union 的父域是各来源域的公共祖先。
    pub parent: Option<DomainId>,
    /// root 域为 None。
    pub parent_row: Option<VariableId>,
}

/// 按行定义语义的计算单元：声明在某个行域上如何由已有 Variable 产生新 Variable、
/// 更新 State，或创建新行域。Kernel 不是具体实现；同一个 Kernel 可以单独编译为一个
/// Program，也可以与其他 Kernel 融合进同一个 Program，两者结果必须一致。
pub enum Kernel {
    Scalar(ScalarKernel),
    Domain(DomainKernel),
}

/// 在一个行域 D 内逐行执行，不改变行域；这里的 scalar 也包括更新 State 的情况。
///
/// 契约：
///   - 输入为 D 及其祖先域的 Variable 和声明的 State；输出为 D 的新 Variable 或 State 更新。
///   - 一行的结果只依赖本行（及其祖先行）的输入，不依赖同域其他行。窗口函数等跨行的
///     有序依赖属于 pipeline 断点，不由 Kernel 表达。
///   - 全函数：任何输入都产生结果，不报错。可能出错的运算（溢出检查、除零、Cast 等）
///     额外输出 Boolean 错误标记，由 RaiseIf 统一报错；RaiseIf 是唯一可以使执行失败的
///     Kernel。CASE 分支、NULL 行上的错误通过把条件 AND 进 RaiseIf 的条件来抑制。
///   - 指针型输出在所有行上（包括 NULL 行和不应计算的行）都指向合法内存。
///   - 同一 root 切片内，任何一行从 State 读到的值不能依赖其他行的写入。
///
/// NULL 不在 Kernel 内部处理：Kernel 只计算值部分，validity 的合并由 lowering 另加 Kernel 完成。
pub enum ScalarKernel {
    /// 无输入；当前域每一行输出同一个值。执行层可识别为常量而不物化，这只是优化。
    Constant {
        value: ConstantValue,
        output: VariableId,
    },
    /// 逐行的纯函数。参数个数与类型、输出个数与类型、是否可能出错都由 function 决定；
    /// 可能出错的函数在值之后多输出一个 Boolean 错误标记，如 [value, overflow]。
    Call {
        function: ScalarFunction,
        inputs: Vec<VariableId>,
        outputs: Vec<VariableId>,
    },
    /// cond 为 true 取 then，否则取 otherwise；用于 CASE、COALESCE 和 NULL 行的安全默认值。
    Select {
        cond: VariableId,
        then: VariableId,
        otherwise: VariableId,
        output: VariableId,
    },
    /// 任意一行 cond 为 true 时执行失败；不保证报告的是哪一行、哪一个错误。
    RaiseIf { cond: VariableId, error: KernelError },
    /// 按 key 查找或插入分组，输出分组下标。字段待 State 模型确定。
    GroupLookup,
    /// 把输入累加到分组的聚合状态。字段待 State 模型确定。
    AggregateUpdate,
}

impl ScalarKernel {
    /// 读取的 Variable，用于可见性校验和 Program 边界推导。
    pub fn inputs(&self) -> Vec<VariableId> {
        todo!("设计草稿")
    }

    /// 产生的 Variable，用于单一生产者校验。
    pub fn outputs(&self) -> Vec<VariableId> {
        todo!("设计草稿")
    }
}

/// 字面量的值部分；NULL 字面量展开为任意合法值加上 Constant(false) 的 validity，因此不需要 Null 变体。
pub enum ConstantValue {
    Int32(i32),
    Int64(i64),
    Boolean(bool),
    Utf8(String),
}

/// 算术、比较、Cast、三值逻辑 AND / OR / NOT 等，随需要补充。
pub enum ScalarFunction {}

/// RaiseIf 报告的错误种类，随可能出错的函数补充。
pub enum KernelError {}

/// 创建新行域。
///
/// 共同契约：
///   - 新域的每一行通过 parent_row 指向声明的父域，parent_row 单调不减。
///   - 新域的行数在执行时确定。Filter、Reduce 和互不相交的 Union 相对 root 切片有
///     静态上界，可按上界预分配；Expand 之下的域没有静态上界，物化时需要按需增长。
///   - DomainKernel 只定义新域的行及其对应关系，不负责复制祖先域的 Variable。
///
/// 第一版只实现 Filter。
pub enum DomainKernel {
    /// 收窄：新域包含 predicate 为 true 的行，父域为 predicate 所在的域。
    /// predicate 不可为 NULL；可空条件需先在构建期与其 validity 合并。
    Filter {
        predicate: VariableId,
        output: DomainId,
    },
    /// 展开：输入域每行产生 0..n 行，父域为输入域。
    /// outputs 是展开产生的新域 Variable（如数组元素、build 侧的列），由 kind 决定。
    Expand {
        input: DomainId,
        kind: ExpandKind,
        outputs: Vec<VariableId>,
        output: DomainId,
    },
    /// 合并：新域的行是各 arm 来源域的行，按公共祖先 ancestor 的行顺序排列；
    /// 父域为 ancestor。来源域不是新域的祖先，其 Variable 必须经 arm 显式传入。
    /// 例：CASE 的各分支子域合并回来；LEFT JOIN 中匹配行与补 NULL 行合并。
    Union {
        ancestor: DomainId,
        arms: Vec<UnionArm>,
        /// 属于新域；arm.values[i] 在新域中成为 outputs[i]。
        outputs: Vec<VariableId>,
        output: DomainId,
    },
    /// 归约：parent 域的每一行在新域中恰好对应一行，parent_row 为恒等映射；
    /// 新域每行的值由 descendant 中映射到该父行的所有行归约而来，没有对应行时取各
    /// 归约的空值。例：半连接、反连接（Count 后再 Filter）；对数组元素计算后求和。
    /// 与 GROUP BY 不同：这是批内按父行的局部归约，不跨切片，也不使用 State。
    Reduce {
        parent: DomainId,
        descendant: DomainId,
        /// 输入属于 descendant。
        aggregates: Vec<(ReduceKind, VariableId)>,
        /// 属于新域，与 aggregates 一一对应。
        outputs: Vec<VariableId>,
        output: DomainId,
    },
}

/// Union 的一个来源。
pub struct UnionArm {
    /// 必须是 Union 的 ancestor 的后代。
    pub domain: DomainId,
    /// 属于 domain，与 Union 的 outputs 一一对应。
    pub values: Vec<VariableId>,
}

/// Join 探测、unnest 等具体展开方式，后续定义。
pub enum ExpandKind {}

/// Count、Sum、Any 等归约方式，随需要补充。
pub enum ReduceKind {}

/// 每个 pipeline 单独构建，只负责注册、分配和校验，不认识任何具体 Kernel；
/// 加入哪些 Kernel 由各 Expression 和 Operator 自己决定。
/// 一个 Column 跨 pipeline 后可重新绑定到新的内部 Variable；绑定不是计划全局属性。
pub struct KernelBuilder<'plan> {
    columns: &'plan ColumnContext,
    /// 下标是 ColumnId；None 表示当前上下文尚未绑定该列。
    column_bindings: Vec<Option<ColumnVariableBinding>>,
    /// 下标是 VariableId，与上面的 ColumnId 空间独立。
    variables: VariableContext,
    /// 下标是 DomainId；第 0 个是 root 域。
    domains: Vec<RowDomain>,
    /// add_scalar_kernel 加入的 Kernel 所属的域；pipeline 中后续算子也在此域构建。
    current: DomainId,
    /// 每个 Kernel 及其所属的域。所属域必须显式记录：RaiseIf、AggregateUpdate 等
    /// Kernel 的输入可能全部来自祖先域，无法从输入推断它在哪些行上执行。
    kernels: Vec<(DomainId, Kernel)>,
}

impl<'plan> KernelBuilder<'plan> {
    /// 创建时只有 root 域，当前域为 root。
    pub fn new(columns: &'plan ColumnContext) -> Self {
        todo!("设计草稿")
    }

    pub fn current_domain(&self) -> DomainId {
        todo!("设计草稿")
    }

    /// 只能切换到 root 或已由 DomainKernel 产生的域。是否切换、何时切换由调用方决定：
    /// Filter 之后进入子域；CASE 分支进入子域后再回到父域；Union / Reduce 之后进入新域。
    pub fn set_current_domain(&mut self, domain: DomainId) -> Result<()> {
        todo!("设计草稿")
    }

    /// 声明以 parent 为父域的新域，并在新域中分配它的 parent_row。
    /// 声明后新域尚无生产者，必须随后由 add_domain_kernel 产生。
    pub fn allocate_domain(&mut self, parent: DomainId) -> DomainId {
        todo!("设计草稿")
    }

    /// 域需显式给出：DomainKernel 的输出 Variable 属于尚未生效、也不是当前域的新域。
    /// 普通情况传 current_domain()。分配本身不代表已有生产者。
    pub fn allocate_variable(&mut self, ty: VariableType, domain: DomainId) -> VariableId {
        todo!("设计草稿")
    }

    /// Kernel 属于当前域。校验：输入属于当前域或其祖先域；输出属于当前域且尚无生产者。
    pub fn add_scalar_kernel(&mut self, kernel: ScalarKernel) -> Result<()> {
        todo!("设计草稿")
    }

    /// 校验：产生的域已声明且尚无生产者，父域符合变体要求（Filter 为 predicate 所在的域，
    /// Union 为各 arm 的公共祖先等）；输入可见性及输出 Variable 的归属同上。不切换当前域。
    pub fn add_domain_kernel(&mut self, kernel: DomainKernel) -> Result<()> {
        todo!("设计草稿")
    }

    /// 读取逻辑类型以选择展开规则；列存在不代表已经绑定或在当前输入中可见。
    pub fn column_type(&self, column: ColumnId) -> Result<&ColumnType> {
        todo!("设计草稿")
    }

    /// 未绑定，或所绑定的 Variable 不属于当前域及其祖先域时报错。
    pub fn column_binding(&self, column: ColumnId) -> Result<&ColumnVariableBinding> {
        todo!("设计草稿")
    }

    /// 由生产者登记输出；校验列类型、各 Variable 类型和有效性约定，拒绝重复定义。
    /// 转发列直接复用已有绑定，不再次登记；分配 Variable 本身不代表它已有生产者。
    pub fn bind_column(&mut self, column: ColumnId, binding: ColumnVariableBinding) -> Result<()> {
        todo!("设计草稿")
    }
}

// 例：nullable Int64 的 a、b 分别绑定到 {value: v0, validity: v1}、
// {value: v2, validity: v3}。Add 自己构建 v4 = v1 AND v3，以及有效时的
// v5 = v0 + v2，再将输出列 c 绑定到 {value: v5, validity: v4}。
// NULL 传播不是 Builder 的统一规则：IS NULL、COALESCE、布尔 AND/OR 各有语义。
//
// Utf8 的 Arrow 批量布局是 offsets/bytes/validity；Scan 的读取逻辑根据当前
// 行偏移产生 ptr/len/validity，再登记 ColumnBinding。字符串表达式只查询这些
// Variable，不必重复解析 Arrow 布局；计算产生的字符串也遵守同一绑定契约。
// ptr 的内存所有者必须在消费期间保持有效，ColumnBinding 本身不持有数据内存。
// 融合时这些 Variable 可留在生成函数的局部值中；物化方式不由绑定表决定。

// ── 内部物化存储：与计划层 Column 的身份空间分开 ──────────────────────

/// 物化后的只读基础数据缓冲区，可共享 Scan 输入或已完成的输出内存。
/// 属于后续内部 Variable 层，不直接表示完整的逻辑 Column。
/// 不重复保存类型；元素类型及编码由内部 Variable 声明和 Kernel 契约确定。
/// buffer.len() 是已初始化的字节数，不是行数；行数及 bitmap 位偏移另行设计。
/// ColumnBinding 描述列的当前行展开；哪些 Variable 需要物化及其布局后续确定。
pub struct VariableBuffer {
    buffer: Buffer,
}

/// 独占的可写基础数据缓冲区，用于 Program 的中间结果和输出。
/// 调用前准备容量并绑定指针；扩容后必须重新绑定，避免使用失效地址。
/// 只将已初始化的输出范围计入 buffer.len()。
pub struct MutableVariableBuffer {
    buffer: MutableBuffer,
}

impl VariableBuffer {
    /// 尝试零拷贝取得可写所有权。底层内存必须独占，且偏移、分配方式满足
    /// Buffer::into_mutable 的条件；失败返回原容器，不隐式复制数据。
    pub fn try_into_mutable(self) -> std::result::Result<MutableVariableBuffer, Self> {
        todo!("设计草稿")
    }
}

impl MutableVariableBuffer {
    /// 消耗可写容器，零拷贝转交为可共享的只读容器。
    pub fn freeze(self) -> VariableBuffer {
        todo!("设计草稿")
    }
}

// Scan 在计划层绑定 batch 列与 ColumnId；后续按列布局提取 Arrow Buffer，
// 绑定为内部 VariableVector 时可共享内存，无需先转成可写内存。
// 执行上下文内的后续 Program 也可只读借用 MutableVariableVector，保留其容量
// 用于下一批；只读借用期间不能同时写入或扩容，不必每经过一个 Program 就 freeze。
// 此处只确定容器及转换契约，具体访问接口和逻辑列的物理布局后续再设计。

// ── 构建示例：Scan → Project → Project ─────────────────────────────────

/// 只演示计划构建；ColumnContext 的方法仍然是占位，暂不能运行本函数。
///
/// 输入 batch 的固定布局：
///   column(0): price    Int64，单位为分
///   column(1): quantity Int64
///   column(2): fee      Int64，单位为分
///
/// 数据流：
///   Scan    → [price, quantity, fee]
///   Project → [amount = price * quantity, fee]
///   Project → [total = amount + fee]
///
/// 例如一行 (price=100, quantity=3, fee=20) 的最终结果应为 total=320。
/// 下方只声明依赖，不读取数据，也不生成 Program。
pub fn build_operator_tree_example() -> Result<OperatorTree> {
    let mut columns = ColumnContext::new();
    let int64 = ColumnType {
        data_type: DataType::Int64,
        nullable: false,
    };

    // 1. 用户或反序列化适配层登记 Scan 将要定义的列。
    //    注册名用于后续解析引用；类型由上层计划给定。
    let price = columns.allocate_column(int64.clone(), Some("price".into()))?;
    let quantity = columns.allocate_column(int64.clone(), Some("quantity".into()))?;
    let fee = columns.allocate_column(int64.clone(), Some("fee".into()))?;

    // 2. 声明实际 batch 列位置与 ColumnId 的绑定。source 用字符串代指存储请求，
    //    并不在这里规定存储 API；适配器需按上面的固定布局返回 batch。
    let scan = ScanOperator {
        source: "orders",
        outputs: vec![
            ColumnChunkBind {
                column_id: price,
                col_index: 0,
            },
            ColumnChunkBind {
                column_id: quantity,
                col_index: 1,
            },
            ColumnChunkBind {
                column_id: fee,
                col_index: 2,
            },
        ],
    };
    let scan_node = OperatorTreeNode {
        operator: Box::new(scan),
        children: vec![],
    };

    // 3. 计算节点定义 amount；引用节点沿用 price、quantity 的 ID。
    //    fee 通过 Reference 显式转发，下游才能继续引用它。
    let amount = columns.allocate_column(int64.clone(), Some("amount".into()))?;
    let project = ProjectOperator {
        expr: vec![
            Expression::Multiply(MultiplyExpression {
                left: Box::new(Expression::Reference(ReferenceExpression { column: price })),
                right: Box::new(Expression::Reference(ReferenceExpression {
                    column: quantity,
                })),
                output: amount,
            }),
            Expression::Reference(ReferenceExpression { column: fee }),
        ],
    };
    let project_node = OperatorTreeNode {
        operator: Box::new(project),
        children: vec![scan_node],
    };

    // 4. 模拟下一段序列化计划按名字引用上游输出。名字只在此处解析；
    //    创建出来的表达式仍只保存 ColumnId。
    let amount_ref = columns.resolve("amount")?;
    let fee_ref = columns.resolve("fee")?;
    let total = columns.allocate_column(int64, Some("total".into()))?;
    let final_project = ProjectOperator {
        expr: vec![Expression::Add(AddExpression {
            left: Box::new(Expression::Reference(ReferenceExpression {
                column: amount_ref,
            })),
            right: Box::new(Expression::Reference(ReferenceExpression {
                column: fee_ref,
            })),
            output: total,
        })],
    };

    // 5. 最下游算子成为树的根；context 随整棵树一起交给后续构建阶段。
    Ok(OperatorTree {
        columns,
        root: OperatorTreeNode {
            operator: Box::new(final_project),
            children: vec![project_node],
        },
    })
}
