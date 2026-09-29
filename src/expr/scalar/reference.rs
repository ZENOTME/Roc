/// A physical column position in the input batch, never a catalog identifier.
#[derive(Clone, Debug)]
pub struct BoundReferenceExpression {
    index: usize,
}
impl BoundReferenceExpression {
    pub fn new(index: usize) -> Self {
        Self { index }
    }
    pub fn index(&self) -> usize {
        self.index
    }
}
