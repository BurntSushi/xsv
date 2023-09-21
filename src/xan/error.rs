use std::fmt::Display;

use super::types::ColumIndexationBy;

#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum PrepareError {
    ParseError(String),
    ColumnNotFound(ColumIndexationBy),
}

impl Display for PrepareError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ColumnNotFound(indexation) => match indexation {
                ColumIndexationBy::Name(name) => write!(f, "cannot find column \"{}\"", name),
                ColumIndexationBy::Pos(pos) => write!(f, "column {} out of range", pos),
                ColumIndexationBy::NameAndNth((name, nth)) => {
                    write!(f, "cannot find column (\"{}\", {})", name, nth)
                }
            },
            Self::ParseError(_) => write!(f, "could not parse expression"),
        }
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct StrictArityErrorContext {
    pub expected: usize,
    pub got: usize,
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct MinArityErrorContext {
    pub min_expected: usize,
    pub got: usize,
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct RangeArityErrorContext {
    pub min_expected: usize,
    pub max_expected: usize,
    pub got: usize,
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum InvalidArity {
    Strict(StrictArityErrorContext),
    Min(MinArityErrorContext),
    Range(RangeArityErrorContext),
}

#[cfg_attr(test, derive(Debug, PartialEq))]
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
    Custom(String),
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

impl Display for EvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath => write!(f, "invalid posix path"),
            Self::InvalidArity(arity) => match arity {
                _ => write!(f, "invalid arity"),
            },
            Self::CannotOpenFile(path) => {
                write!(f, "cannot open file {}", path)
            }
            Self::CannotReadFile(path) => write!(f, "cannot read file {}", path),
            Self::UnknownFunction(name) => write!(f, "unknown function \"{}\"", name),
            Self::Custom(msg) => write!(f, "{}", msg),
            Self::IllegalBinding => write!(f, "illegal binding"),
            Self::Cast => write!(f, "casting error"),
            Self::ColumnOutOfRange(idx) => write!(f, "column \"{}\" is out of range", idx),
            Self::UnknownVariable(name) => write!(f, "unknown variable \"{}\"", name),
            Self::NotImplemented => write!(f, "not implemented"),
            Self::CannotCompare => write!(f, "invalid comparison between mixed arguments"),
            Self::UnicodeDecodeError => write!(f, "unicode decode error"),
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum RunError {
    Prepare(PrepareError),
    Evaluation(EvaluationError),
}
