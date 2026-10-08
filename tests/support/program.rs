#![allow(dead_code, unused_imports)]
//! Test/benchmark construction helpers, not runtime expression abstractions.
pub use roc::program::Value;
use roc::{
    error::Result,
    expr::scalar::*,
    program::{ProcessProgram, ProgramBuilder},
};
pub trait ProgramTestExt {
    fn program(&self) -> Result<ProcessProgram>;
}
impl ProgramTestExt for ScalarExpression {
    fn program(&self) -> Result<ProcessProgram> {
        let mut builder = ProgramBuilder::default();
        let input = builder.value();
        let output = self.emit(&mut builder, input)?;
        builder.build_value(input, output)
    }
}
macro_rules! descriptor {
    ($($ty:ty),*)=>{$(impl ProgramTestExt for $ty{fn program(&self)->Result<ProcessProgram>{ScalarExpression::from(self.clone()).program()}})*};
}
descriptor!(
    ReferenceExpression,
    ConstantExpression,
    FunctionExpression,
    CastExpression,
    ConjunctionExpression,
    NotExpression,
    CaseExpression,
    CoalesceExpression
);
pub fn projection_program(projection: roc::operator::Projection) -> Result<ProcessProgram> {
    let mut builder = ProgramBuilder::default();
    let input = builder.value();
    let output = builder.emit_project(input, &projection)?;
    builder.build_batch(input, Some(output))
}
pub fn project(
    program: &mut ProcessProgram,
    input: &roc::exec::Batch,
) -> Result<arrow::record_batch::RecordBatch> {
    let roc::exec::ProcessResult::NeedMoreInput(output) = program.execute(input)? else {
        unreachable!()
    };
    output.into_record_batch()
}

pub fn build_program(expr: ScalarExprRef) -> Result<ProcessProgram> {
    expr.program()
}
