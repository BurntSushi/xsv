use std::borrow::Cow;
use std::collections::BTreeMap;

use csv::ByteRecord;

use xan::error::{EvaluationError, PrepareError};
use xan::functions::call;
use xan::parser::{parse, Argument, Pipeline};
use xan::types::{BoundArguments, ColumIndexation, DynamicValue, EvaluationResult};

type Variables<'a> = BTreeMap<&'a str, DynamicValue<'a>>;

enum ConcreteArgument<'a> {
    Variable(Cow<'a, str>),
    Column(usize),
    StringLiteral(Cow<'a, str>),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    Call(ConcreteFunctionCall<'a>),
    Null,
    Underscore,
}

impl<'a> ConcreteArgument<'a> {
    fn bind(
        &self,
        record: &'a ByteRecord,
        last_value: Cow<DynamicValue<'a>>,
        _variables: &'a Variables,
    ) -> EvaluationResult {
        Ok(match self {
            Self::StringLiteral(value) => DynamicValue::String(value.clone()),
            Self::FloatLiteral(value) => DynamicValue::Float(*value),
            Self::IntegerLiteral(value) => DynamicValue::Integer(*value),
            Self::BooleanLiteral(value) => DynamicValue::Boolean(*value),
            Self::Underscore => (*last_value).clone(),
            Self::Null => DynamicValue::None,
            Self::Column(index) => match record.get(*index) {
                None => return Err(EvaluationError::ColumnOutOfRange(*index)),
                Some(cell) => match std::str::from_utf8(cell) {
                    Err(_) => return Err(EvaluationError::UnicodeDecodeError),
                    Ok(value) => DynamicValue::String(Cow::Borrowed(value)),
                },
            },
            // Self::Variable(name) => match variables.get(name.as_ref()) {
            //     Some(value) => *value,
            //     None => return Err(EvaluationError::UnknownVariable(name.into_owned())),
            // },
            Self::Variable(_) => unimplemented!(),
            Self::Call(_) => return Err(EvaluationError::IllegalBinding),
        })
    }
}

pub struct ConcreteFunctionCall<'a> {
    name: String,
    args: Vec<ConcreteArgument<'a>>,
}

type ConcretePipeline<'a> = Vec<ConcreteFunctionCall<'a>>;

fn concretize_argument<'a>(
    argument: Argument,
    headers: &'a ByteRecord,
    reserved: &'a Vec<String>,
) -> Result<ConcreteArgument<'a>, PrepareError> {
    Ok(match argument {
        Argument::Underscore => ConcreteArgument::Underscore,
        Argument::Null => ConcreteArgument::Null,
        Argument::BooleanLiteral(v) => ConcreteArgument::BooleanLiteral(v),
        Argument::FloatLiteral(v) => ConcreteArgument::FloatLiteral(v),
        Argument::IntegerLiteral(v) => ConcreteArgument::IntegerLiteral(v),
        Argument::StringLiteral(v) => ConcreteArgument::StringLiteral(Cow::Owned(v)),
        Argument::Identifier(name) => {
            if reserved.contains(&name) {
                ConcreteArgument::Variable(Cow::Owned(name))
            } else {
                let indexation = ColumIndexation::ByName(name);

                match indexation.find_column_index(headers) {
                    Some(index) => ConcreteArgument::Column(index),
                    None => return Err(PrepareError::ColumnNotFound(indexation)),
                }
            }
        }
        Argument::Indexation(indexation) => match indexation.find_column_index(headers) {
            Some(index) => ConcreteArgument::Column(index),
            None => return Err(PrepareError::ColumnNotFound(indexation)),
        },
        Argument::Call(call) => {
            let mut concrete_args = Vec::new();

            for arg in call.args {
                concrete_args.push(concretize_argument(arg, headers, reserved)?);
            }

            ConcreteArgument::Call(ConcreteFunctionCall {
                name: call.name,
                args: concrete_args,
            })
        }
    })
}

fn concretize_pipeline<'a>(
    pipeline: Pipeline,
    headers: &'a ByteRecord,
    reserved: &'a Vec<String>,
) -> Result<ConcretePipeline<'a>, PrepareError> {
    let mut concrete_pipeline: ConcretePipeline = Vec::new();

    for function_call in pipeline {
        let mut concrete_arguments: Vec<ConcreteArgument> = Vec::new();

        for argument in function_call.args {
            concrete_arguments.push(concretize_argument(argument, headers, reserved)?);
        }

        concrete_pipeline.push(ConcreteFunctionCall {
            name: function_call.name.clone(),
            args: concrete_arguments,
        });
    }

    Ok(concrete_pipeline)
}

pub fn prepare<'a>(
    code: &'a str,
    headers: &'a ByteRecord,
    reserved: &'a Vec<String>,
) -> Result<ConcretePipeline<'a>, PrepareError> {
    match parse(code) {
        Err(_) => Err(PrepareError::ParseError(code.to_string())),
        Ok(pipeline) => concretize_pipeline(pipeline, headers, reserved),
    }
}

fn eval_function<'a>(
    function_call: &'a ConcreteFunctionCall,
    record: &'a ByteRecord,
    last_value: Cow<DynamicValue<'a>>,
    variables: &'a Variables,
) -> EvaluationResult<'a> {
    let mut bound_args = BoundArguments::new();

    for arg in function_call.args.iter() {
        match arg {
            ConcreteArgument::Call(sub_function_call) => {
                bound_args.push(traverse(
                    sub_function_call,
                    record,
                    last_value.clone(),
                    variables,
                )?);
            }
            _ => bound_args.push(arg.bind(record, last_value.clone(), variables)?),
        }
    }

    call(&function_call.name, bound_args)
}

fn eval<'a>(
    arg: &'a ConcreteArgument,
    record: &'a ByteRecord,
    last_value: Cow<DynamicValue<'a>>,
    variables: &'a Variables,
) -> EvaluationResult<'a> {
    match arg {
        ConcreteArgument::Call(function_call) => {
            eval_function(function_call, record, last_value, variables)
        }
        _ => arg.bind(record, last_value, variables),
    }
}

fn traverse<'a>(
    function_call: &'a ConcreteFunctionCall,
    record: &'a ByteRecord,
    last_value: Cow<DynamicValue<'a>>,
    variables: &'a Variables,
) -> EvaluationResult<'a> {
    // Branching
    if function_call.name == *"if" {
        let arity = function_call.args.len();

        if arity < 2 || arity > 3 {
            return Err(EvaluationError::from_range_arity(2, 3, arity));
        }

        let condition = &function_call.args[0];
        let result = eval(condition, record, last_value.clone(), variables)?;

        let mut branch: Option<&ConcreteArgument> = None;

        if result.truthy() {
            branch = Some(&function_call.args[1]);
        } else if arity == 3 {
            branch = Some(&function_call.args[2]);
        }

        match branch {
            None => Ok(DynamicValue::None),
            Some(arg) => eval(arg, record, last_value, variables),
        }
    }
    // Regular call
    else {
        eval_function(function_call, record, last_value, variables)
    }
}

pub fn interpret<'a>(
    pipeline: &'a ConcretePipeline,
    record: &'a ByteRecord,
    variables: &'a Variables,
) -> EvaluationResult<'a> {
    let mut last_value = Cow::Borrowed(&DynamicValue::None);

    for function_call in pipeline {
        last_value = Cow::Owned(traverse(function_call, record, last_value, variables)?);
    }

    Ok(last_value.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpret() -> Result<(), ()> {
        match prepare("trim", &ByteRecord::new(), &Vec::new()) {
            Err(_) => Err(()),
            Ok(pipeline) => {
                let variables = BTreeMap::new();

                match interpret(&pipeline, &ByteRecord::new(), &variables) {
                    Err(_) => return Err(()),
                    Ok(value) => assert_eq!(String::from(value.into_str()), String::new()),
                }

                Ok(())
            }
        }
    }
}
