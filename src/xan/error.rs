pub struct InvalidArityErrorContext {
    pub expected: usize,
    pub got: usize,
}

pub enum EvaluationError {
    InvalidArity(InvalidArityErrorContext),
    ColumnOutOfRange(usize),
    UnknownVariable(String),
    UnknownFunction(String),
    UnicodeDecodeError,
    Cast,
}
