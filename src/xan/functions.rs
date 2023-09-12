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

pub fn trim(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::String(String::from(
        args[0].cast_to_string()?.trim(),
    )))
}

pub fn len(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    validate_arity(args, 1)?;
    Ok(DynamicValue::Integer(args[0].cast_to_string()?.len() as i64))
}

pub fn coalesce(args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    for arg in args {
        if arg.cast_to_bool()? {
            return Ok(arg.clone());
        }
    }

    Ok(DynamicValue::None)
}
