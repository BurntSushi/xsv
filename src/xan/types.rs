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

                return None;
            }
            Self::ByNameAndNth((name, pos)) => {
                let mut i: usize = 0;
                let mut c = *pos;

                let name_bytes = name.as_bytes();

                for cell in headers {
                    if cell == name_bytes {
                        if c == 0 {
                            return Some(i);
                        }
                        c -= 1;
                    }

                    i += 1;
                }

                return None;
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

    pub fn truthy(&self) -> bool {
        match self {
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value == &0.0,
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        }
    }

    pub fn serialize(self) -> String {
        self.into()
    }

    pub fn into_string(self) -> String {
        self.into()
    }

    // pub fn cast_to_string(&self) -> Option<String> {
    //     Some(self.serialize())
    // }

    // pub fn cast_to_float(&self) -> Option<f64> {
    //     Some(match self {
    //         Self::String(string) => match string.parse::<f64>() {
    //             Err(_) => return None,
    //             Ok(value) => value,
    //         },
    //         Self::Float(value) => *value,
    //         Self::Integer(value) => *value as f64,
    //         Self::Boolean(value) => {
    //             if *value {
    //                 1.0
    //             } else {
    //                 0.0
    //             }
    //         }
    //         Self::None => return None,
    //     })
    // }

    // pub fn cast_to_integer(&self) -> Option<i64> {
    //     Some(match self {
    //         Self::String(string) => match string.parse::<i64>() {
    //             Err(_) => match string.parse::<f64>() {
    //                 Err(_) => return None,
    //                 Ok(value) => match downgrade_float(value) {
    //                     Some(safe_downgraded_value) => safe_downgraded_value,
    //                     None => return None,
    //                 },
    //             },
    //             Ok(value) => value,
    //         },
    //         Self::Float(value) => match downgrade_float(*value) {
    //             Some(safe_downgraded_value) => safe_downgraded_value,
    //             None => return None,
    //         },
    //         Self::Integer(value) => *value,
    //         Self::Boolean(value) => *value as i64,
    //         Self::None => return None,
    //     })
    // }

    // fn cast_to_number(&self) -> Option<DynamicNumber> {
    //     Some(match self {
    //         Self::String(string) => match string.parse::<i64>() {
    //             Ok(value) => DynamicNumber::Integer(value),
    //             Err(_) => match string.parse::<f64>() {
    //                 Ok(value) => DynamicNumber::Float(value),
    //                 Err(_) => return None,
    //             },
    //         },
    //         Self::Integer(value) => DynamicNumber::Integer(*value),
    //         Self::Float(value) => DynamicNumber::Float(*value),
    //         Self::Boolean(value) => DynamicNumber::Integer(*value as i64),
    //         _ => return None,
    //     })
    // }
}

impl From<usize> for DynamicValue {
    fn from(value: usize) -> Self {
        DynamicValue::Integer(value as i64)
    }
}

impl From<String> for DynamicValue {
    fn from(value: String) -> Self {
        DynamicValue::String(value)
    }
}

impl From<&str> for DynamicValue {
    fn from(value: &str) -> Self {
        DynamicValue::String(String::from(value))
    }
}

impl Into<bool> for DynamicValue {
    fn into(self) -> bool {
        match self {
            Self::String(value) => value.len() > 0,
            Self::Float(value) => value == 0.0,
            Self::Integer(value) => value != 0,
            Self::Boolean(value) => value,
            Self::None => false,
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
            Self::None => return Err(EvaluationError::Cast),
        })
    }
}

impl Into<String> for DynamicValue {
    fn into(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Float(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Boolean(value) => String::from(if value { "true" } else { "false" }),
            Self::None => "".to_string(),
        }
    }
}

// impl PartialEq for DynamicValue {
//     fn eq(&self, other: &Self) -> bool {
//         match self {
//             DynamicValue::None => match other {
//                 DynamicValue::None => true,
//                 _ => false,
//             },
//             DynamicValue::Boolean(self_value) => match other {
//                 DynamicValue::Boolean(other_value) => self_value.eq(other_value),
//                 _ => match other.cast_to_bool() {
//                     Err(_) => false,
//                     Ok(other_value) => self_value.eq(&other_value),
//                 },
//             },
//             DynamicValue::String(self_value) => match other {
//                 DynamicValue::String(other_value) => self_value.eq(other_value),
//                 _ => match other.cast_to_string() {
//                     Err(_) => false,
//                     Ok(other_value) => self_value.eq(&other_value),
//                 },
//             },
//             DynamicValue::Integer(self_value) => match other {
//                 DynamicValue::Integer(other_value) => self_value.eq(other_value),
//                 _ => match other.cast_to_integer() {
//                     Err(_) => false,
//                     Ok(other_value) => self_value.eq(&other_value),
//                 },
//             },
//             DynamicValue::Float(self_value) => match other {
//                 DynamicValue::Float(other_value) => self_value.eq(other_value),
//                 _ => match other.cast_to_float() {
//                     Err(_) => false,
//                     Ok(other_value) => self_value.eq(&other_value),
//                 },
//             },
//         }
//     }
// }

// impl PartialOrd for DynamicValue {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         match self {
//             DynamicValue::None => match other {
//                 DynamicValue::None => Some(Ordering::Equal),
//                 _ => None,
//             },
//             DynamicValue::Boolean(self_value) => match other {
//                 DynamicValue::Boolean(other_value) => Some(self_value.cmp(other_value)),
//                 _ => match other.cast_to_bool() {
//                     Err(_) => None,
//                     Ok(other_value) => Some(self_value.cmp(&other_value)),
//                 },
//             },
//             DynamicValue::String(self_value) => match other {
//                 DynamicValue::String(other_value) => Some(self_value.cmp(other_value)),
//                 _ => match other.cast_to_string() {
//                     Err(_) => None,
//                     Ok(other_value) => Some(self_value.cmp(&other_value)),
//                 },
//             },
//             DynamicValue::Integer(self_value) => match other {
//                 DynamicValue::Integer(other_value) => Some(self_value.cmp(other_value)),
//                 _ => match other.cast_to_integer() {
//                     Err(_) => None,
//                     Ok(other_value) => Some(self_value.cmp(&other_value)),
//                 },
//             },
//             DynamicValue::Float(self_value) => match other {
//                 DynamicValue::Float(other_value) => self_value.partial_cmp(other_value),
//                 _ => match other.cast_to_float() {
//                     Err(_) => None,
//                     Ok(other_value) => self_value.partial_cmp(&other_value),
//                 },
//             },
//         }
//     }
// }

pub type EvaluationResult = Result<DynamicValue, EvaluationError>;

pub struct BoundArguments {
    stack: Vec<DynamicValue>,
}

impl BoundArguments {
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    pub fn len(&self) -> usize {
        self.stack.len()
    }

    pub fn push(&mut self, arg: DynamicValue) {
        self.stack.push(arg);
    }

    pub fn get1(&mut self) -> EvaluationResult {
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

    pub fn get1_string(&mut self) -> Result<String, EvaluationError> {
        self.get1().map(|value| value.into_string())
    }

    pub fn get2(&mut self) -> Result<(DynamicValue, DynamicValue), EvaluationError> {
        match pop2(&mut self.stack) {
            None => Err(EvaluationError::from_invalid_arity(2, self.len())),
            Some(t) => {
                if self.len() > 2 {
                    return Err(EvaluationError::from_invalid_arity(2, self.len()));
                }

                Ok(t)
            }
        }
    }

    pub fn get2_string(&mut self) -> Result<(String, String), EvaluationError> {
        self.get2().map(|(a, b)| (a.into_string(), b.into_string()))
    }
}

impl IntoIterator for BoundArguments {
    type Item = DynamicValue;
    type IntoIter = <Vec<DynamicValue> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.stack.into_iter()
    }
}
