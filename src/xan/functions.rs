use flate2::read::GzDecoder;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use xan::error::{EvaluationError, InvalidArityErrorContext};

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
                Err(_) => return Err(EvaluationError::Cast),
                Ok(value) => value,
            },
            Self::Float(value) => value.trunc() as i64,
            Self::Integer(value) => *value,
            Self::Boolean(value) => *value as i64,
            Self::None => return Err(EvaluationError::Cast),
        })
    }
}

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
pub fn eq(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;

    let left = &args[0];
    let right = &args[1];

    Ok(DynamicValue::Boolean(match left {
        DynamicValue::Boolean(left_value) => match right {
            DynamicValue::Boolean(right_value) => left_value == right_value,
            _ => left_value == &right.cast_to_bool()?,
        },
        DynamicValue::None => match right {
            DynamicValue::None => true,
            _ => false,
        },
        DynamicValue::String(left_value) => match right {
            DynamicValue::String(right_value) => left_value == right_value,
            DynamicValue::None => false,
            _ => left_value == &right.cast_to_string()?,
        },
        DynamicValue::Integer(left_value) => match right {
            DynamicValue::Integer(right_value) => left_value == right_value,
            DynamicValue::None => false,
            _ => left_value == &right.cast_to_integer()?,
        },
        DynamicValue::Float(left_value) => match right {
            DynamicValue::Float(right_value) => left_value == right_value,
            DynamicValue::None => false,
            _ => left_value == &right.cast_to_float()?,
        },
    }))
}

// IO
pub fn pathjoin(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
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

// TODO: rayon, encoding support
