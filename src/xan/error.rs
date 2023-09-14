use xan::types::ColumIndexation;

pub enum PrepareError {
    ParseError(String),
    ColumnNotFound(ColumIndexation),
}

pub struct StrictArityErrorContext {
    pub expected: usize,
    pub got: usize,
}

pub struct MinArityErrorContext {
    pub min_expected: usize,
    pub got: usize,
}

pub struct RangeArityErrorContext {
    pub min_expected: usize,
    pub max_expected: usize,
    pub got: usize,
}

pub enum InvalidArity {
    Strict(StrictArityErrorContext),
    Min(MinArityErrorContext),
    Range(RangeArityErrorContext),
}

pub enum EvaluationError {
    IllegalBinding,
    InvalidArity(InvalidArity),
    ColumnOutOfRange(usize),
    UnknownVariable(String),
    UnknownFunction(String),
    InvalidPath,
    NotImplemented,
    UnicodeDecodeError,
    CannotOpenFile(String),
    CannotReadFile(String),
    CannotCompare,
    Cast,
}

impl EvaluationError {
    pub fn from_invalid_arity(expected: usize, got: usize) -> Self {
        Self::InvalidArity(InvalidArity::Strict(StrictArityErrorContext {
            expected,
            got,
        }))
    }

    pub fn from_invalid_min_arity(min_expected: usize, got: usize) -> Self {
        Self::InvalidArity(InvalidArity::Min(MinArityErrorContext {
            min_expected,
            got,
        }))
    }

    pub fn from_range_arity(min_expected: usize, max_expected: usize, got: usize) -> Self {
        Self::InvalidArity(InvalidArity::Range(RangeArityErrorContext {
            min_expected,
            max_expected,
            got,
        }))
    }
}
