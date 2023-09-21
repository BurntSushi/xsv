use std::borrow::Cow;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::collections::BTreeMap;
use std::convert::From;
use std::ops::{Add, Mul, Sub};

use csv;

use super::error::{CallError, EvaluationError};
use super::utils::downgrade_float;

#[derive(Debug, PartialEq, Clone)]
pub enum ColumIndexationBy {
    Name(String),
    NameAndNth((String, usize)),
    Pos(usize),
}

impl ColumIndexationBy {
    pub fn find_column_index(&self, headers: &csv::ByteRecord) -> Option<usize> {
        match self {
            Self::Pos(i) => {
                if i >= &headers.len() {
                    None
                } else {
                    Some(*i)
                }
            }
            Self::Name(name) => {
                let name_bytes = name.as_bytes();

                for (i, cell) in headers.iter().enumerate() {
                    if cell == name_bytes {
                        return Some(i);
                    }
                }

                None
            }
            Self::NameAndNth((name, pos)) => {
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

fn apply_op<F1, F2>(a: DynamicNumber, b: DynamicNumber, op_int: F1, op_float: F2) -> DynamicNumber
where
    F1: FnOnce(i64, i64) -> i64,
    F2: FnOnce(f64, f64) -> f64,
{
    match a {
        DynamicNumber::Integer(a) => match b {
            DynamicNumber::Integer(b) => DynamicNumber::Integer(op_int(a, b)),
            DynamicNumber::Float(b) => DynamicNumber::Float(op_float(a as f64, b)),
        },
        DynamicNumber::Float(a) => match b {
            DynamicNumber::Integer(b) => DynamicNumber::Float(op_float(a, b as f64)),
            DynamicNumber::Float(b) => DynamicNumber::Float(op_float(a, b)),
        },
    }
}

impl Add for DynamicNumber {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        apply_op(self, rhs, Add::<i64>::add, Add::<f64>::add)
    }
}

impl Sub for DynamicNumber {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        apply_op(self, rhs, Sub::<i64>::sub, Sub::<f64>::sub)
    }
}

impl Mul for DynamicNumber {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        apply_op(self, rhs, Mul::<i64>::mul, Mul::<f64>::mul)
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

                if !bytes.is_empty() {
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

    pub fn try_as_str(&self) -> Result<Cow<str>, CallError> {
        Ok(match self {
            Self::List(_) => {
                return Err(CallError::Cast(("list".to_string(), "string".to_string())))
            }
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

    pub fn try_as_list(&self) -> Result<&Vec<DynamicValue>, CallError> {
        match self {
            Self::List(list) => Ok(list),
            value => Err(CallError::Cast((
                value.type_of().to_string(),
                "list".to_string(),
            ))),
        }
    }

    pub fn try_as_number(&self) -> Result<DynamicNumber, CallError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Ok(value) => DynamicNumber::Integer(value),
                Err(_) => match string.parse::<f64>() {
                    Ok(value) => DynamicNumber::Float(value),
                    Err(_) => {
                        return Err(CallError::Cast((
                            "string".to_string(),
                            "number".to_string(),
                        )))
                    }
                },
            },
            Self::Integer(value) => DynamicNumber::Integer(*value),
            Self::Float(value) => DynamicNumber::Float(*value),
            Self::Boolean(value) => DynamicNumber::Integer(*value as i64),
            value => {
                return Err(CallError::Cast((
                    value.type_of().to_string(),
                    "number".to_string(),
                )))
            }
        })
    }

    pub fn try_as_usize(&self) -> Result<usize, CallError> {
        Ok(match self {
            Self::String(string) => match string.parse::<usize>() {
                Err(_) => {
                    return Err(CallError::Cast((
                        "string".to_string(),
                        "unsigned_number".to_string(),
                    )))
                }
                Ok(value) => value,
            },
            Self::Float(value) => match downgrade_float(*value) {
                Some(safe_downgraded_value) => {
                    if safe_downgraded_value >= 0 {
                        safe_downgraded_value as usize
                    } else {
                        return Err(CallError::Cast((
                            "float".to_string(),
                            "unsigned_number".to_string(),
                        )));
                    }
                }
                None => {
                    return Err(CallError::Cast((
                        "float".to_string(),
                        "unsigned_number".to_string(),
                    )))
                }
            },
            Self::Integer(value) => {
                if value >= &0 {
                    (*value) as usize
                } else {
                    return Err(CallError::Cast((
                        "integer".to_string(),
                        "unsigned_number".to_string(),
                    )));
                }
            }
            Self::Boolean(value) => (*value) as usize,
            _ => {
                return Err(CallError::Cast((
                    "boolean".to_string(),
                    "unsigned_number".to_string(),
                )))
            }
        })
    }

    pub fn try_as_i64(&self) -> Result<i64, CallError> {
        Ok(match self {
            Self::String(string) => match string.parse::<i64>() {
                Err(_) => {
                    return Err(CallError::Cast((
                        "string".to_string(),
                        "integer".to_string(),
                    )))
                }
                Ok(value) => value,
            },
            Self::Float(value) => match downgrade_float(*value) {
                Some(safe_downgraded_value) => safe_downgraded_value,
                None => {
                    return Err(CallError::Cast((
                        "float".to_string(),
                        "integer".to_string(),
                    )))
                }
            },
            Self::Integer(value) => *value,
            Self::Boolean(value) => (*value) as i64,
            value => {
                return Err(CallError::Cast((
                    value.type_of().to_string(),
                    "integer".to_string(),
                )))
            }
        })
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Self::List(value) => !value.is_empty(),
            Self::String(value) => !value.is_empty(),
            Self::Float(value) => value == &0.0,
            Self::Integer(value) => value != &0,
            Self::Boolean(value) => *value,
            Self::None => false,
        }
    }

    pub fn is_falsey(&self) -> bool {
        !self.is_truthy()
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

impl From<char> for DynamicValue {
    fn from(value: char) -> Self {
        DynamicValue::String(value.to_string())
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

impl<T> From<Option<T>> for DynamicValue
where
    T: Into<DynamicValue>,
{
    fn from(option: Option<T>) -> Self {
        match option {
            None => DynamicValue::None,
            Some(value) => value.into(),
        }
    }
}

pub type BoundArgument<'a> = Cow<'a, DynamicValue>;
pub type EvaluationResult<'a> = Result<BoundArgument<'a>, EvaluationError>;
pub type Variables<'a> = BTreeMap<&'a str, DynamicValue>;

pub struct BoundArguments<'a> {
    stack: Vec<BoundArgument<'a>>,
}

impl<'a> BoundArguments<'a> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            stack: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.stack.len()
    }

    pub fn push(&mut self, arg: BoundArgument<'a>) {
        self.stack.push(arg);
    }

    pub fn validate_arity(&self, expected: usize) -> Result<(), CallError> {
        if self.len() != expected {
            Err(CallError::from_invalid_arity(expected, self.len()))
        } else {
            Ok(())
        }
    }

    pub fn validate_min_arity(&self, min: usize) -> Result<(), CallError> {
        if self.len() < min {
            Err(CallError::from_invalid_min_arity(min, self.len()))
        } else {
            Ok(())
        }
    }

    pub fn validate_min_max_arity(&self, min: usize, max: usize) -> Result<(), CallError> {
        if self.len() < min || self.len() > max {
            Err(CallError::from_range_arity(min, max, self.len()))
        } else {
            Ok(())
        }
    }

    pub fn getn_opt(&'a self, n: usize) -> Vec<Option<&'a BoundArgument>> {
        let mut selection: Vec<Option<&BoundArgument>> = Vec::new();

        for i in 0..n {
            selection.push(self.stack.get(i));
        }

        selection
    }

    pub fn get1(&'a self) -> Result<&'a BoundArgument, CallError> {
        match self.stack.get(0) {
            None => Err(CallError::from_invalid_arity(1, 0)),
            Some(value) => {
                if self.len() > 1 {
                    return Err(CallError::from_invalid_arity(1, self.len()));
                }

                Ok(value)
            }
        }
    }

    pub fn get2(&self) -> Result<(&Cow<DynamicValue>, &Cow<DynamicValue>), CallError> {
        match self.stack.get(0) {
            None => Err(CallError::from_invalid_arity(2, 0)),
            Some(a) => match self.stack.get(1) {
                None => Err(CallError::from_invalid_arity(2, 1)),
                Some(b) => {
                    if self.len() > 2 {
                        return Err(CallError::from_invalid_arity(2, self.len()));
                    }

                    Ok((a, b))
                }
            },
        }
    }

    pub fn get1_as_str(&'a self) -> Result<Cow<'a, str>, CallError> {
        self.get1().and_then(|value| value.try_as_str())
    }

    pub fn get1_as_bool(&'a self) -> Result<bool, CallError> {
        self.get1().map(|value| value.is_truthy())
    }

    pub fn get2_as_str(&self) -> Result<(Cow<str>, Cow<str>), CallError> {
        let (a, b) = self.get2()?;

        Ok((a.try_as_str()?, b.try_as_str()?))
    }

    pub fn get2_as_numbers(&self) -> Result<(DynamicNumber, DynamicNumber), CallError> {
        let (a, b) = self.get2()?;
        Ok((a.try_as_number()?, b.try_as_number()?))
    }

    pub fn get2_as_bool(&self) -> Result<(bool, bool), CallError> {
        let (a, b) = self.get2()?;
        Ok((a.is_truthy(), b.is_truthy()))
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
