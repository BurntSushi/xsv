use flate2::read::GzDecoder;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::File;
use std::io::Read;
use std::ops::Add;
use std::path::PathBuf;

use xan::error::EvaluationError;

pub fn call(name: &str, args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    match name {
        "add" => add(args),
        "and" => and(args),
        "coalesce" => coalesce(args),
        "concat" => concat(args),
        "count" => count(args),
        "eq" => number_compare(args, Ordering::is_eq),
        "len" => len(args),
        "lower" => lower(args),
        "not" => not(args),
        "or" => or(args),
        "pathjoin" => pathjoin(args),
        "read" => read(args),
        "trim" => trim(args),
        "typeof" => type_of(args),
        "upper" => upper(args),
        _ => Err(EvaluationError::UnknownFunction(name.to_string())),
    }
}

fn downgrade_float(f: f64) -> Option<i64> {
    let t = f.trunc();

    if f - t <= f64::EPSILON {
        return Some(t as i64);
    }

    None
}

enum DynamicNumber {
    Float(f64),
    Integer(i64),
}

impl PartialEq for DynamicNumber {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::Float(self_value) => match other {
                Self::Float(other_value) => self_value == other_value,
                Self::Integer(other_value) => *self_value == (*other_value as f64),
            },
            Self::Integer(self_value) => match other {
                Self::Float(other_value) => (*self_value as f64) == *other_value,
                Self::Integer(other_value) => self_value == other_value,
            },
        }
    }
}

impl PartialOrd for DynamicNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Self::Float(self_value) => match other {
                Self::Float(other_value) => self_value.partial_cmp(other_value),
                Self::Integer(other_value) => self_value.partial_cmp(&(*other_value as f64)),
            },
            Self::Integer(self_value) => match other {
                Self::Float(other_value) => (*other_value as f64).partial_cmp(other_value),
                Self::Integer(other_value) => Some(self_value.cmp(other_value)),
            },
        }
    }
}

impl DynamicNumber {
    fn to_dynamic_value(self) -> DynamicValue {
        match self {
            Self::Float(value) => DynamicValue::Float(value),
            Self::Integer(value) => DynamicValue::Integer(value),
        }
    }
}

impl Add for DynamicNumber {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Self::Integer(a) => match rhs {
                Self::Integer(b) => Self::Integer(a + b),
                Self::Float(b) => Self::Float((a as f64) + b),
            },
            Self::Float(a) => match rhs {
                Self::Integer(b) => Self::Float(a + (b as f64)),
                Self::Float(b) => Self::Float(a + b),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum DynamicValue {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    None,
}

// TODO: find a way to avoid cloning also here
impl DynamicValue {
    pub fn type_of(&self) -> &str {
        match self {
            Self::String(_) => "string",
            Self::Float(_) => "float",
            Self::Integer(_) => "integer",
            Self::Boolean(_) => "boolean",
            Self::None => "none",
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Float(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Boolean(value) => String::from(if *value { "true" } else { "false" }),
            Self::None => "".to_string(),
        }
    }

    pub fn cast_to_string(&self) -> Result<String, EvaluationError> {
        Ok(self.serialize())
    }

    pub fn cast_to_bool(&self) -> Result<bool, EvaluationError> {
        Ok(match self {
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value.total_cmp(&0f64).is_eq(),
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        })
    }

    pub fn cast_to_float(&self) -> Result<f64, EvaluationError> {
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

    pub fn cast_to_integer(&self) -> Result<i64, EvaluationError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Err(_) => match string.parse::<f64>() {
                    Err(_) => return Err(EvaluationError::Cast),
                    Ok(value) => match downgrade_float(value) {
                        Some(safe_downgraded_value) => safe_downgraded_value,
                        None => return Err(EvaluationError::Cast),
                    },
                },
                Ok(value) => value,
            },
            Self::Float(value) => match downgrade_float(*value) {
                Some(safe_downgraded_value) => safe_downgraded_value,
                None => return Err(EvaluationError::Cast),
            },
            Self::Integer(value) => *value,
            Self::Boolean(value) => *value as i64,
            Self::None => return Err(EvaluationError::Cast),
        })
    }

    fn cast_to_number(&self) -> Result<DynamicNumber, EvaluationError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Ok(value) => DynamicNumber::Integer(value),
                Err(_) => match string.parse::<f64>() {
                    Ok(value) => DynamicNumber::Float(value),
                    Err(_) => return Err(EvaluationError::Cast),
                },
            },
            Self::Integer(value) => DynamicNumber::Integer(*value),
            Self::Float(value) => DynamicNumber::Float(*value),
            Self::Boolean(value) => DynamicNumber::Integer(*value as i64),
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
        Err(EvaluationError::from_invalid_arity(expected, args.len()))
    } else {
        Ok(())
    }
}

fn validate_min_arity(args: &Vec<DynamicValue>, min: usize) -> Result<(), EvaluationError> {
    if args.len() < min {
        Err(EvaluationError::from_invalid_min_arity(min, args.len()))
    } else {
        Ok(())
    }
}

// String transformations
fn trim(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.trim(),
    )))
}

fn lower(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.to_lowercase(),
    )))
}

fn upper(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.to_uppercase(),
    )))
}

fn len(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::Integer(args[0].cast_to_string()?.len() as i64))
}

// String queries
fn count(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;

    Ok(DynamicValue::Integer(
        args[0]
            .cast_to_string()?
            .matches(&args[1].cast_to_string()?)
            .count() as i64,
    ))
}

// Arithmetics
fn add(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_min_arity(args, 2)?;

    let mut sum = DynamicNumber::Integer(0);

    for arg in args {
        sum = sum + arg.cast_to_number()?;
    }

    Ok(sum.to_dynamic_value())
}

// Utilities
fn coalesce(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    for arg in args {
        if arg.cast_to_bool()? {
            return Ok(arg.clone());
        }
    }

    Ok(DynamicValue::None)
}

fn concat(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    let mut result = String::new();

    for arg in args {
        result.push_str(&arg.cast_to_string()?);
    }

    Ok(DynamicValue::String(result))
}

// Boolean
fn not(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;

    Ok(DynamicValue::Boolean(!args[0].cast_to_bool()?))
}

fn and(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;

    Ok(DynamicValue::Boolean(
        args[0].cast_to_bool()? && args[1].cast_to_bool()?,
    ))
}

fn or(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 2)?;

    Ok(DynamicValue::Boolean(
        args[0].cast_to_bool()? || args[1].cast_to_bool()?,
    ))
}

// Comparison
fn number_compare<F>(args: &Vec<DynamicValue>, validate: F) -> Result<DynamicValue, EvaluationError>
where
    F: FnOnce(Ordering) -> bool,
{
    validate_arity(args, 2)?;

    let a = args[0].cast_to_number()?;
    let b = args[1].cast_to_number()?;

    Ok(DynamicValue::Boolean(match a.partial_cmp(&b) {
        Some(ordering) => validate(ordering),
        None => false,
    }))
}

// IO
fn pathjoin(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_min_arity(args, 2)?;

    let mut path = PathBuf::new();

    for arg in args {
        path.push(arg.cast_to_string()?);
    }

    let path = String::from(path.to_str().ok_or(EvaluationError::InvalidPath)?);

    Ok(DynamicValue::String(path))
}

fn read(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
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

// Introspection
fn type_of(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(args[0].type_of())))
}
