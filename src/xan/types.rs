use std::borrow::Cow;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::convert::{From, Into, TryInto};
use std::ops::Add;

use csv;

use xan::error::EvaluationError;
use xan::utils::{downgrade_float, pop2};

#[derive(Debug, PartialEq)]
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

#[derive(Debug, Clone)]
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

    // TODO: rework
    pub fn as_bytes(&self) -> Cow<[u8]> {
        match self {
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

    pub fn as_str(&self) -> Cow<str> {
        match self {
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
        }
    }

    pub fn truthy(&self) -> bool {
        match self {
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value == &0.0,
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        }
    }

    // pub fn into_string(self) -> String {
    //     self.into()
    // }

    // pub fn into_bool(self) -> bool {
    //     self.into()
    // }

    // pub fn try_into_integer(self) -> Result<i64, EvaluationError> {
    //     self.try_into()
    // }

    // pub fn try_into_float(self) -> Result<f64, EvaluationError> {
    //     self.try_into()
    // }

    // pub fn try_into_number(self) -> Result<DynamicNumber, EvaluationError> {
    //     self.try_into()
    // }
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

// impl From<bool> for DynamicValue {
//     fn from(value: bool) -> Self {
//         DynamicValue::Boolean(value)
//     }
// }

// impl From<usize> for DynamicValue {
//     fn from(value: usize) -> Self {
//         DynamicValue::Integer(value as i64)
//     }
// }

// impl From<String> for DynamicValue {
//     fn from(value: String) -> Self {
//         DynamicValue::String(value)
//     }
// }

// impl From<DynamicNumber> for DynamicValue {
//     fn from(value: DynamicNumber) -> Self {
//         match value {
//             DynamicNumber::Integer(value) => DynamicValue::Integer(value),
//             DynamicNumber::Float(value) => DynamicValue::Float(value),
//         }
//     }
// }

// impl Into<bool> for DynamicValue {
//     fn into(self) -> bool {
//         match self {
//             Self::String(value) => value.len() > 0,
//             Self::Float(value) => value == 0.0,
//             Self::Integer(value) => value != 0,
//             Self::Boolean(value) => value,
//             Self::None => false,
//         }
//     }
// }

// impl TryInto<i64> for DynamicValue {
//     type Error = EvaluationError;

//     fn try_into(self) -> Result<i64, Self::Error> {
//         Ok(match self {
//             Self::String(string) => match string.parse::<i64>() {
//                 Err(_) => match string.parse::<f64>() {
//                     Err(_) => return Err(EvaluationError::Cast),
//                     Ok(value) => match downgrade_float(value) {
//                         Some(safe_downgraded_value) => safe_downgraded_value,
//                         None => return Err(EvaluationError::Cast),
//                     },
//                 },
//                 Ok(value) => value,
//             },
//             Self::Float(value) => match downgrade_float(value) {
//                 Some(safe_downgraded_value) => safe_downgraded_value,
//                 None => return Err(EvaluationError::Cast),
//             },
//             Self::Integer(value) => value,
//             Self::Boolean(value) => value as i64,
//             Self::None => return Err(EvaluationError::Cast),
//         })
//     }
// }

// impl TryInto<f64> for DynamicValue {
//     type Error = EvaluationError;

//     fn try_into(self) -> Result<f64, Self::Error> {
//         Ok(match self {
//             Self::String(string) => match string.parse::<f64>() {
//                 Err(_) => return Err(EvaluationError::Cast),
//                 Ok(value) => value,
//             },
//             Self::Float(value) => value,
//             Self::Integer(value) => value as f64,
//             Self::Boolean(value) => {
//                 if value {
//                     1.0
//                 } else {
//                     0.0
//                 }
//             }
//             Self::None => return Err(EvaluationError::Cast),
//         })
//     }
// }

// impl TryInto<DynamicNumber> for DynamicValue {
//     type Error = EvaluationError;

//     fn try_into(self) -> Result<DynamicNumber, Self::Error> {
//         Ok(match self {
//             Self::String(string) => match string.parse::<i64>() {
//                 Ok(value) => DynamicNumber::Integer(value),
//                 Err(_) => match string.parse::<f64>() {
//                     Ok(value) => DynamicNumber::Float(value),
//                     Err(_) => return Err(EvaluationError::Cast),
//                 },
//             },
//             Self::Integer(value) => DynamicNumber::Integer(value),
//             Self::Float(value) => DynamicNumber::Float(value),
//             Self::Boolean(value) => DynamicNumber::Integer(value as i64),
//             _ => return Err(EvaluationError::Cast),
//         })
//     }
// }

// impl Into<String> for DynamicValue {
//     fn into(self) -> String {
//         match self {
//             Self::String(value) => value,
//             Self::Float(value) => value.to_string(),
//             Self::Integer(value) => value.to_string(),
//             Self::Boolean(value) => (if value { "true" } else { "false" }).to_string(),
//             Self::None => "".to_string(),
//         }
//     }
// }

pub type EvaluationResult = Result<DynamicValue, EvaluationError>;

pub struct BoundArguments<'a> {
    stack: Vec<Cow<'a, DynamicValue>>,
}

impl<'a> BoundArguments<'a> {
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.stack.len()
    }

    pub fn push(&mut self, arg: Cow<'a, DynamicValue>) {
        self.stack.push(arg);
    }

    pub fn validate_min_arity(&self, min: usize) -> Result<(), EvaluationError> {
        if self.len() < min {
            Err(EvaluationError::from_invalid_min_arity(min, self.len()))
        } else {
            Ok(())
        }
    }

    // pub fn get1(&mut self) -> EvaluationResult {
    //     match self.stack.pop() {
    //         None => Err(EvaluationError::from_invalid_arity(1, 0)),
    //         Some(value) => {
    //             if self.len() > 1 {
    //                 return Err(EvaluationError::from_invalid_arity(1, self.len()));
    //             }

    //             Ok(value)
    //         }
    //     }
    // }

    // TODO: error will be incorrect on subsequent pops
    pub fn pop1(mut self) -> Result<Cow<'a, DynamicValue>, EvaluationError> {
        match self.stack.pop() {
            None => Err(EvaluationError::from_invalid_arity(1, 0)),
            Some(value) => {
                if self.len() > 1 {
                    return Err(EvaluationError::from_invalid_arity(1, self.len()));
                }

                Ok(value)
            }
        }
    }

    pub fn pop1_str(self) -> Result<Cow<'a, str>, EvaluationError> {
        self.pop1().map(|value| value.as_str())
    }

    // pub fn pop1_bool(self) -> Result<bool, EvaluationError> {
    //     self.pop1().map(|value| value.into_bool())
    // }

    // pub fn pop2(mut self) -> Result<(DynamicValue, DynamicValue), EvaluationError> {
    //     match pop2(&mut self.stack) {
    //         None => Err(EvaluationError::from_invalid_arity(2, self.len())),
    //         Some(t) => {
    //             if self.len() > 2 {
    //                 return Err(EvaluationError::from_invalid_arity(2, self.len()));
    //             }

    //             Ok(t)
    //         }
    //     }
    // }

    // pub fn pop2_str(self) -> Result<(Cow<'a, str>, Cow<'a, str>), EvaluationError> {
    //     self.pop2().map(|(a, b)| (a.into_str(), b.into_str()))
    // }

    // pub fn pop2_bool(self) -> Result<(bool, bool), EvaluationError> {
    //     self.pop2().map(|(a, b)| (a.into_bool(), b.into_bool()))
    // }

    // pub fn pop2_number(self) -> Result<(DynamicNumber, DynamicNumber), EvaluationError> {
    //     let (a, b) = self.pop2()?;

    //     let a = a.try_into_number()?;
    //     let b = b.try_into_number()?;

    //     Ok((a, b))
    // }
}

// impl IntoIterator for BoundArguments {
//     type Item = DynamicValue;
//     type IntoIter = <Vec<DynamicValue> as IntoIterator>::IntoIter;

//     fn into_iter(self) -> Self::IntoIter {
//         self.stack.into_iter()
//     }
// }
