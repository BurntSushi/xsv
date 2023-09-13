use flate2::read::GzDecoder;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use xan::error::{EvaluationError, InvalidArityErrorContext};

pub enum Number {
    Float(f64),
    Integer(i64),
}

#[derive(Clone, Debug)]
pub enum DynamicValue {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    None,
}

impl DynamicValue {
    pub fn serialize(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Float(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Boolean(value) => String::from(if *value { "true" } else { "false" }),
            Self::None => "".to_string(),
        }
    }

    fn cast_to_string(&self) -> Result<String, EvaluationError> {
        Ok(self.serialize())
    }

    fn cast_to_bool(&self) -> Result<bool, EvaluationError> {
        Ok(match self {
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value.total_cmp(&0f64).is_eq(),
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        })
    }

    fn cast_to_float(&self) -> Result<f64, EvaluationError> {
        Ok(match self {
            Self::String(string) => match string.parse::<f64>() {
                Err(_) => return Err(EvaluationError::Cast),
                Ok(value) => value,
            },
            Self::Float(value) => *value,
            Self::Integer(value) => *value as f64,
            Self::Boolean(value) => {
                if *value {
                    1.0
                } else {
                    0.0
                }
            }
            Self::None => return Err(EvaluationError::Cast),
        })
    }

    fn cast_to_integer(&self) -> Result<i64, EvaluationError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Err(_) => match string.parse::<f64>() {
                    Err(_) => return Err(EvaluationError::Cast),
                    Ok(value) => value.trunc() as i64,
                },
                Ok(value) => value,
            },
            Self::Float(value) => value.trunc() as i64,
            Self::Integer(value) => *value,
            Self::Boolean(value) => *value as i64,
            Self::None => return Err(EvaluationError::Cast),
        })
    }

    fn cast_to_number(&self) -> Result<Number, EvaluationError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Ok(value) => Number::Integer(value),
                Err(_) => match string.parse::<f64>() {
                    Ok(value) => Number::Float(value),
                    Err(_) => return Err(EvaluationError::Cast),
                },
            },
            Self::Integer(value) => Number::Integer(*value),
            Self::Float(value) => Number::Float(*value),
            Self::Boolean(value) => Number::Integer(*value as i64),
            _ => return Err(EvaluationError::Cast),
        })
    }
}

impl PartialEq for DynamicValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            DynamicValue::None => match other {
                DynamicValue::None => true,
                _ => false,
            },
            DynamicValue::Boolean(self_value) => match other {
                DynamicValue::Boolean(other_value) => self_value.eq(other_value),
                _ => match other.cast_to_bool() {
                    Err(_) => false,
                    Ok(other_value) => self_value.eq(&other_value),
                },
            },
            DynamicValue::String(self_value) => match other {
                DynamicValue::String(other_value) => self_value.eq(other_value),
                _ => match other.cast_to_string() {
                    Err(_) => false,
                    Ok(other_value) => self_value.eq(&other_value),
                },
            },
            DynamicValue::Integer(self_value) => match other {
                DynamicValue::Integer(other_value) => self_value.eq(other_value),
                _ => match other.cast_to_integer() {
                    Err(_) => false,
                    Ok(other_value) => self_value.eq(&other_value),
                },
            },
            DynamicValue::Float(self_value) => match other {
                DynamicValue::Float(other_value) => self_value.eq(other_value),
                _ => match other.cast_to_float() {
                    Err(_) => false,
                    Ok(other_value) => self_value.eq(&other_value),
                },
            },
        }
    }
}

impl PartialOrd for DynamicValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            DynamicValue::None => match other {
                DynamicValue::None => Some(Ordering::Equal),
                _ => None,
            },
            DynamicValue::Boolean(self_value) => match other {
                DynamicValue::Boolean(other_value) => Some(self_value.cmp(other_value)),
                _ => match other.cast_to_bool() {
                    Err(_) => None,
                    Ok(other_value) => Some(self_value.cmp(&other_value)),
                },
            },
            DynamicValue::String(self_value) => match other {
                DynamicValue::String(other_value) => Some(self_value.cmp(other_value)),
                _ => match other.cast_to_string() {
                    Err(_) => None,
                    Ok(other_value) => Some(self_value.cmp(&other_value)),
                },
            },
            DynamicValue::Integer(self_value) => match other {
                DynamicValue::Integer(other_value) => Some(self_value.cmp(other_value)),
                _ => match other.cast_to_integer() {
                    Err(_) => None,
                    Ok(other_value) => Some(self_value.cmp(&other_value)),
                },
            },
            DynamicValue::Float(self_value) => match other {
                DynamicValue::Float(other_value) => self_value.partial_cmp(other_value),
                _ => match other.cast_to_float() {
                    Err(_) => None,
                    Ok(other_value) => self_value.partial_cmp(&other_value),
                },
            },
        }
    }
}

// Arity helpers
fn validate_arity(args: &Vec<DynamicValue>, expected: usize) -> Result<(), EvaluationError> {
    if args.len() != expected {
        Err(EvaluationError::InvalidArity(InvalidArityErrorContext {
            expected,
            got: args.len(),
        }))
    } else {
        Ok(())
    }
}

fn validate_min_arity(args: &Vec<DynamicValue>, min: usize) -> Result<(), EvaluationError> {
    if args.len() < min {
        Err(EvaluationError::InvalidArity(InvalidArityErrorContext {
            expected: min,
            got: args.len(),
        }))
    } else {
        Ok(())
    }
}

// String transformations
pub fn trim(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.trim(),
    )))
}

pub fn lower(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.to_lowercase(),
    )))
}

pub fn upper(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.to_uppercase(),
    )))
}

pub fn len(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::Integer(args[0].cast_to_string()?.len() as i64))
}

// String queries
pub fn count(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;

    Ok(DynamicValue::Integer(
        args[0]
            .cast_to_string()?
            .matches(&args[1].cast_to_string()?)
            .count() as i64,
    ))
}

// Arithmetics
pub fn add(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_min_arity(args, 2)?;

    let mut sum = Number::Integer(0);

    for arg in args {
        sum = match arg.cast_to_number()? {
            Number::Integer(n) => match sum {
                Number::Integer(s) => Number::Integer(s + n),
                Number::Float(s) => Number::Float(s + (n as f64)),
            },
            Number::Float(n) => match sum {
                Number::Integer(s) => Number::Float((s as f64) + n),
                Number::Float(s) => Number::Float(s + n),
            },
        }
    }

    Ok(match sum {
        Number::Integer(value) => DynamicValue::Integer(value),
        Number::Float(value) => DynamicValue::Float(value),
    })
}

// Utilities
pub fn coalesce(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    for arg in args {
        if arg.cast_to_bool()? {
            return Ok(arg.clone());
        }
    }

    Ok(DynamicValue::None)
}

pub fn concat(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    let mut result = String::new();

    for arg in args {
        result.push_str(&arg.cast_to_string()?);
    }

    Ok(DynamicValue::String(result))
}

// Comparison
// TODO: distinguish between numbers and strings
pub fn eq(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;
    Ok(DynamicValue::Boolean(args[0].eq(&args[1])))
}

// IO
pub fn pathjoin(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_min_arity(args, 2)?;

    let mut path = PathBuf::new();

    for arg in args {
        path.push(arg.cast_to_string()?);
    }

    let path = String::from(path.to_str().ok_or(EvaluationError::InvalidPath)?);

    Ok(DynamicValue::String(path))
}

pub fn read(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;

    let path = args[0].cast_to_string()?;

    // TODO: handle encoding
    let mut file = match File::open(&path) {
        Err(_) => return Err(EvaluationError::CannotOpenFile(path)),
        Ok(f) => f,
    };

    let mut buffer = String::new();

    if path.ends_with(".gz") {
        let mut gz = GzDecoder::new(file);
        gz.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path))?;
    } else {
        file.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path))?;
    }

    Ok(DynamicValue::String(buffer))
}
