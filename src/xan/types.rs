use std::borrow::Cow;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BTreeMap;
use std::convert::{From, TryInto};
use std::ops::Add;

use csv;

use super::error::EvaluationError;
use super::utils::downgrade_float;

#[derive(Debug, PartialEq, Clone)]
pub enum ColumIndexation {
    ByName(String),
    ByNameAndNth((String, usize)),
    ByPos(usize),
}

impl ColumIndexation {
    pub fn find_column_index(&self, headers: &csv::ByteRecord) -> Option<usize> {
        match self {
            Self::ByPos(i) => {
                if i >= &headers.len() {
                    None
                } else {
                    Some(*i)
                }
            }
            Self::ByName(name) => {
                let name_bytes = name.as_bytes();

                for (i, cell) in headers.iter().enumerate() {
                    if cell == name_bytes {
                        return Some(i);
                    }
                }

                None
            }
            Self::ByNameAndNth((name, pos)) => {
                let mut c = *pos;

                let name_bytes = name.as_bytes();

                for (i, cell) in headers.iter().enumerate() {
                    if cell == name_bytes {
                        if c == 0 {
                            return Some(i);
                        }
                        c -= 1;
                    }
                }

                None
            }
        }
    }
}

pub enum DynamicNumber {
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
                Self::Float(other_value) => (*self_value as f64).partial_cmp(other_value),
                Self::Integer(other_value) => Some(self_value.cmp(other_value)),
            },
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

#[derive(Debug, Clone, PartialEq)]
pub enum DynamicValue {
    List(Vec<DynamicValue>),
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    None,
}

impl DynamicValue {
    pub fn type_of(&self) -> &str {
        match self {
            Self::List(_) => "list",
            Self::String(_) => "string",
            Self::Float(_) => "float",
            Self::Integer(_) => "integer",
            Self::Boolean(_) => "boolean",
            Self::None => "none",
        }
    }

    pub fn serialize_as_bytes(&self, plural_separator: &[u8]) -> Cow<[u8]> {
        match self {
            Self::List(list) => {
                let mut bytes: Vec<u8> = Vec::new();

                for value in list {
                    bytes.extend_from_slice(&value.serialize_as_bytes(plural_separator));
                    bytes.extend_from_slice(plural_separator);
                }

                if bytes.len() > 0 {
                    bytes.truncate(bytes.len() - plural_separator.len());
                }

                Cow::Owned(bytes)
            }
            Self::String(value) => Cow::Borrowed(value.as_bytes()),
            Self::Float(value) => Cow::Owned(value.to_string().into_bytes()),
            Self::Integer(value) => Cow::Owned(value.to_string().into_bytes()),
            Self::Boolean(value) => {
                if *value {
                    Cow::Borrowed(b"true")
                } else {
                    Cow::Borrowed(b"false")
                }
            }
            Self::None => Cow::Borrowed(b""),
        }
    }

    // pub fn as_bytes(&self) -> Cow<[u8]> {
    //     match self {
    //         Self::List(_) => unimplemented!(),
    //         Self::String(value) => Cow::Borrowed(value.as_bytes()),
    //         Self::Float(value) => Cow::Owned(value.to_string().into_bytes()),
    //         Self::Integer(value) => Cow::Owned(value.to_string().into_bytes()),
    //         Self::Boolean(value) => {
    //             if *value {
    //                 Cow::Borrowed(b"true")
    //             } else {
    //                 Cow::Borrowed(b"false")
    //             }
    //         }
    //         Self::None => Cow::Borrowed(b""),
    //     }
    // }

    pub fn try_as_str(&self) -> Result<Cow<str>, EvaluationError> {
        Ok(match self {
            Self::List(_) => return Err(EvaluationError::Cast),
            Self::String(value) => Cow::Borrowed(value),
            Self::Float(value) => Cow::Owned(value.to_string()),
            Self::Integer(value) => Cow::Owned(value.to_string()),
            Self::Boolean(value) => {
                if *value {
                    Cow::Owned("true".to_string())
                } else {
                    Cow::Owned("false".to_string())
                }
            }
            Self::None => Cow::Owned("".to_string()),
        })
    }

    pub fn try_as_list(&self) -> Result<&Vec<DynamicValue>, EvaluationError> {
        match self {
            Self::List(list) => Ok(list),
            _ => Err(EvaluationError::Cast),
        }
    }

    pub fn try_as_number(&self) -> Result<DynamicNumber, EvaluationError> {
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

    pub fn truthy(&self) -> bool {
        match self {
            Self::List(value) => value.len() > 0,
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value == &0.0,
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        }
    }
}

impl From<&str> for DynamicValue {
    fn from(value: &str) -> Self {
        DynamicValue::String(value.to_string())
    }
}

impl<'a> From<Cow<'a, str>> for DynamicValue {
    fn from(value: Cow<str>) -> Self {
        DynamicValue::String(value.into_owned())
    }
}

impl From<String> for DynamicValue {
    fn from(value: String) -> Self {
        DynamicValue::String(value)
    }
}

impl From<Vec<DynamicValue>> for DynamicValue {
    fn from(value: Vec<DynamicValue>) -> Self {
        DynamicValue::List(value)
    }
}

impl From<bool> for DynamicValue {
    fn from(value: bool) -> Self {
        DynamicValue::Boolean(value)
    }
}

impl From<usize> for DynamicValue {
    fn from(value: usize) -> Self {
        DynamicValue::Integer(value as i64)
    }
}

impl From<DynamicNumber> for DynamicValue {
    fn from(value: DynamicNumber) -> Self {
        match value {
            DynamicNumber::Integer(value) => DynamicValue::Integer(value),
            DynamicNumber::Float(value) => DynamicValue::Float(value),
        }
    }
}

impl TryInto<i64> for DynamicValue {
    type Error = EvaluationError;

    fn try_into(self) -> Result<i64, Self::Error> {
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
            Self::Float(value) => match downgrade_float(value) {
                Some(safe_downgraded_value) => safe_downgraded_value,
                None => return Err(EvaluationError::Cast),
            },
            Self::Integer(value) => value,
            Self::Boolean(value) => value as i64,
            _ => return Err(EvaluationError::Cast),
        })
    }
}

impl TryInto<f64> for DynamicValue {
    type Error = EvaluationError;

    fn try_into(self) -> Result<f64, Self::Error> {
        Ok(match self {
            Self::String(string) => match string.parse::<f64>() {
                Err(_) => return Err(EvaluationError::Cast),
                Ok(value) => value,
            },
            Self::Float(value) => value,
            Self::Integer(value) => value as f64,
            Self::Boolean(value) => {
                if value {
                    1.0
                } else {
                    0.0
                }
            }
            _ => return Err(EvaluationError::Cast),
        })
    }
}

impl TryInto<DynamicNumber> for DynamicValue {
    type Error = EvaluationError;

    fn try_into(self) -> Result<DynamicNumber, Self::Error> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Ok(value) => DynamicNumber::Integer(value),
                Err(_) => match string.parse::<f64>() {
                    Ok(value) => DynamicNumber::Float(value),
                    Err(_) => return Err(EvaluationError::Cast),
                },
            },
            Self::Integer(value) => DynamicNumber::Integer(value),
            Self::Float(value) => DynamicNumber::Float(value),
            Self::Boolean(value) => DynamicNumber::Integer(value as i64),
            _ => return Err(EvaluationError::Cast),
        })
    }
}

pub type BoundArgument<'a> = Cow<'a, DynamicValue>;
pub type EvaluationResult<'a> = Result<BoundArgument<'a>, EvaluationError>;
pub type Variables<'a> = BTreeMap<&'a str, DynamicValue>;

pub struct BoundArguments<'a> {
    stack: Vec<BoundArgument<'a>>,
}

impl<'a> BoundArguments<'a> {
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.stack.len()
    }

    pub fn push(&mut self, arg: BoundArgument<'a>) {
        self.stack.push(arg);
    }

    pub fn validate_min_arity(&self, min: usize) -> Result<(), EvaluationError> {
        if self.len() < min {
            Err(EvaluationError::from_invalid_min_arity(min, self.len()))
        } else {
            Ok(())
        }
    }

    pub fn get1(&'a self) -> Result<&'a BoundArgument, EvaluationError> {
        match self.stack.get(0) {
            None => Err(EvaluationError::from_invalid_arity(1, 0)),
            Some(value) => {
                if self.len() > 1 {
                    return Err(EvaluationError::from_invalid_arity(1, self.len()));
                }

                Ok(value)
            }
        }
    }

    pub fn get2(&self) -> Result<(&Cow<DynamicValue>, &Cow<DynamicValue>), EvaluationError> {
        match self.stack.get(0) {
            None => Err(EvaluationError::from_invalid_arity(2, 0)),
            Some(a) => match self.stack.get(1) {
                None => Err(EvaluationError::from_invalid_arity(2, 1)),
                Some(b) => {
                    if self.len() > 2 {
                        return Err(EvaluationError::from_invalid_arity(2, self.len()));
                    }

                    Ok((a, b))
                }
            },
        }
    }

    pub fn get1_as_str(&'a self) -> Result<Cow<'a, str>, EvaluationError> {
        self.get1().and_then(|value| value.try_as_str())
    }

    pub fn get2_as_str(&self) -> Result<(Cow<str>, Cow<str>), EvaluationError> {
        let (a, b) = self.get2()?;

        Ok((a.try_as_str()?, b.try_as_str()?))
    }

    pub fn get2_as_numbers(&self) -> Result<(DynamicNumber, DynamicNumber), EvaluationError> {
        let (a, b) = self.get2()?;
        Ok((a.try_as_number()?, b.try_as_number()?))
    }

    // pub fn iter(&self) -> BoundArgumentsIterator {
    //     BoundArgumentsIterator(self.stack.iter())
    // }
}

pub struct BoundArgumentsIterator<'a>(std::slice::Iter<'a, BoundArgument<'a>>);

impl<'a> Iterator for BoundArgumentsIterator<'a> {
    type Item = &'a BoundArgument<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct BoundArgumentsIntoIterator<'a>(std::vec::IntoIter<BoundArgument<'a>>);

impl<'a> Iterator for BoundArgumentsIntoIterator<'a> {
    type Item = BoundArgument<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a> IntoIterator for BoundArguments<'a> {
    type Item = BoundArgument<'a>;
    type IntoIter = BoundArgumentsIntoIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BoundArgumentsIntoIterator(self.stack.into_iter())
    }
}
