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
            Self::ParseError(expr) => write!(f, "could not parse expression: {}", expr),
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
pub struct SpecifiedBindingError {
    pub function_name: String,
    pub arg_index: Option<usize>,
    pub reason: BindingError,
}

impl Display for SpecifiedBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.arg_index {
            Some(i) => write!(
                f,
                "error when binding arg n°{} for \"{}\": {}",
                i + 1,
                self.function_name,
                self.reason.to_string()
            ),
            None => write!(
                f,
                "error when binding expression: {}",
                self.reason.to_string()
            ),
        }
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum BindingError {
    IllegalBinding,
    ColumnOutOfRange(usize),
    UnicodeDecodeError,
    UnknownVariable(String),
}

impl Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IllegalBinding => write!(f, "illegal binding"),
            Self::ColumnOutOfRange(idx) => write!(f, "column \"{}\" is out of range", idx),
            Self::UnknownVariable(name) => write!(f, "unknown variable \"{}\"", name),
            Self::UnicodeDecodeError => write!(f, "unicode decode error"),
        }
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct SpecifiedCallError {
    pub function_name: String,
    pub reason: CallError,
}

impl Display for SpecifiedCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "error when calling function \"{}\": {}",
            self.function_name,
            self.reason.to_string()
        )
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum CallError {
    InvalidArity(InvalidArity),
    UnknownFunction(String),
    InvalidPath,
    NotImplemented(String),
    CannotOpenFile(String),
    CannotReadFile(String),
    Cast((String, String)),
    Custom(String),
}

impl CallError {
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

impl Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPath => write!(f, "invalid posix path"),
            Self::InvalidArity(arity) => match arity {
                InvalidArity::Min(context) => write!(
                    f,
                    "expected at least {} arguments but got {}",
                    context.min_expected, context.got
                ),
                InvalidArity::Strict(context) => write!(
                    f,
                    "expected {} arguments but got {}",
                    context.expected, context.got
                ),
                InvalidArity::Range(context) => write!(
                    f,
                    "expected between {} and {} arguments but got {}",
                    context.min_expected, context.max_expected, context.got
                ),
            },
            Self::CannotOpenFile(path) => {
                write!(f, "cannot open file {}", path)
            }
            Self::CannotReadFile(path) => write!(f, "cannot read file {}", path),
            Self::UnknownFunction(_) => write!(f, "unknown function"),
            Self::Custom(msg) => write!(f, "{}", msg),
            Self::Cast((from_type, to_type)) => write!(
                f,
                "cannot safely cast from type \"{}\" to type \"{}\"",
                from_type, to_type
            ),
            Self::NotImplemented(t) => {
                write!(f, "not implemented for values of type \"{}\" as of yet", t)
            }
        }
    }
}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum EvaluationError {
    Binding(SpecifiedBindingError),
    Call(SpecifiedCallError),
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Binding(err) => err.fmt(f),
            Self::Call(err) => err.fmt(f),
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum RunError {
    Prepare(PrepareError),
    Evaluation(EvaluationError),
}
