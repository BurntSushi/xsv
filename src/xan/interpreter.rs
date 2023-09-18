use std::borrow::Cow;

use csv::ByteRecord;

use xan::error::{EvaluationError, PrepareError};
use xan::functions::call;
use xan::parser::{parse, Argument, Pipeline};
use xan::types::{BoundArguments, ColumIndexation, DynamicValue, EvaluationResult, Variables};

// TODO: unfurling the pipeline for the first argument should clone less
// NOTE: unfurling = cutting sequence until there is an underscore as first step
// NOTE: then it means renesting the sequence if there is a single underscore reference
enum ConcreteArgument {
    Variable(String),
    Column(usize),
    StringLiteral(DynamicValue),
    FloatLiteral(DynamicValue),
    IntegerLiteral(DynamicValue),
    BooleanLiteral(DynamicValue),
    Call(ConcreteFunctionCall),
    Null,
    Underscore,
}

impl ConcreteArgument {
    fn bind<'a>(
        &'a self,
        record: &'a ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> EvaluationResult {
        Ok(match self {
            Self::StringLiteral(value) => Cow::Borrowed(value),
            Self::FloatLiteral(value) => Cow::Borrowed(value),
            Self::IntegerLiteral(value) => Cow::Borrowed(value),
            Self::BooleanLiteral(value) => Cow::Borrowed(value),
            Self::Underscore => Cow::Borrowed(last_value),
            Self::Null => Cow::Owned(DynamicValue::None),
            Self::Column(index) => match record.get(*index) {
                None => return Err(EvaluationError::ColumnOutOfRange(*index)),
                Some(cell) => match std::str::from_utf8(cell) {
                    Err(_) => return Err(EvaluationError::UnicodeDecodeError),
                    Ok(value) => Cow::Owned(DynamicValue::from(value)),
                },
            },
            Self::Variable(name) => match variables.get::<str>(name) {
                Some(value) => Cow::Borrowed(value),
                None => return Err(EvaluationError::UnknownVariable(name.clone())),
            },
            Self::Call(_) => return Err(EvaluationError::IllegalBinding),
        })
    }
}

pub struct ConcreteFunctionCall {
    name: String,
    args: Vec<ConcreteArgument>,
}

type ConcretePipeline = Vec<ConcreteFunctionCall>;

fn concretize_argument(
    argument: Argument,
    headers: &ByteRecord,
    reserved: &Vec<&str>,
) -> Result<ConcreteArgument, PrepareError> {
    Ok(match argument {
        Argument::Underscore => ConcreteArgument::Underscore,
        Argument::Null => ConcreteArgument::Null,
        Argument::BooleanLiteral(v) => ConcreteArgument::BooleanLiteral(DynamicValue::Boolean(v)),
        Argument::FloatLiteral(v) => ConcreteArgument::FloatLiteral(DynamicValue::Float(v)),
        Argument::IntegerLiteral(v) => ConcreteArgument::IntegerLiteral(DynamicValue::Integer(v)),
        Argument::StringLiteral(v) => ConcreteArgument::StringLiteral(DynamicValue::String(v)),
        Argument::Identifier(name) => {
            if reserved.contains(&name.as_str()) {
                ConcreteArgument::Variable(name)
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

fn concretize_pipeline(
    pipeline: Pipeline,
    headers: &ByteRecord,
    reserved: &Vec<&str>,
) -> Result<ConcretePipeline, PrepareError> {
    let mut concrete_pipeline: ConcretePipeline = Vec::new();

    for function_call in pipeline {
        let mut concrete_arguments: Vec<ConcreteArgument> = Vec::new();

        for argument in function_call.args {
            concrete_arguments.push(concretize_argument(argument, headers, reserved)?);
        }

        concrete_pipeline.push(ConcreteFunctionCall {
            name: function_call.name,
            args: concrete_arguments,
        });
    }

    Ok(concrete_pipeline)
}

pub fn prepare(
    code: &str,
    headers: &ByteRecord,
    reserved: &Vec<&str>,
) -> Result<ConcretePipeline, PrepareError> {
    match parse(code) {
        Err(_) => Err(PrepareError::ParseError(code.to_string())),
        Ok(pipeline) => concretize_pipeline(pipeline, headers, reserved),
    }
}

fn eval_function<'a>(
    function_call: &ConcreteFunctionCall,
    record: &ByteRecord,
    last_value: &DynamicValue,
    variables: &Variables,
) -> EvaluationResult<'a> {
    let mut bound_args = BoundArguments::new();

    for arg in function_call.args.iter() {
        match arg {
            ConcreteArgument::Call(sub_function_call) => {
                bound_args.push(traverse(sub_function_call, record, last_value, variables)?);
            }
            _ => bound_args.push(arg.bind(record, last_value, variables)?),
        }
    }

    call(&function_call.name, bound_args)
}

fn eval<'a>(
    arg: &'a ConcreteArgument,
    record: &'a ByteRecord,
    last_value: &'a DynamicValue,
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
    last_value: &'a DynamicValue,
    variables: &'a Variables,
) -> EvaluationResult<'a> {
    // Branching
    if function_call.name == *"if" {
        let arity = function_call.args.len();

        if arity < 2 || arity > 3 {
            return Err(EvaluationError::from_range_arity(2, 3, arity));
        }

        let condition = &function_call.args[0];
        let result = eval(condition, record, last_value, variables)?;

        let mut branch: Option<&ConcreteArgument> = None;

        if result.truthy() {
            branch = Some(&function_call.args[1]);
        } else if arity == 3 {
            branch = Some(&function_call.args[2]);
        }

        match branch {
            None => Ok(Cow::Owned(DynamicValue::None)),
            Some(arg) => eval(arg, record, last_value, variables),
        }
    }
    // Regular call
    else {
        eval_function(function_call, record, last_value, variables)
    }
}

pub fn interpret(
    pipeline: &ConcretePipeline,
    record: &ByteRecord,
    variables: &Variables,
) -> Result<DynamicValue, EvaluationError> {
    let mut last_value = DynamicValue::None;

    for function_call in pipeline {
        let wrapped_value = traverse(function_call, record, &last_value, variables)?;

        if let Cow::Borrowed(_) = wrapped_value {
            panic!("value should not be borrowed here!")
        }

        last_value = wrapped_value.into_owned();
    }

    Ok(last_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpret() -> Result<(), ()> {
        match prepare("trim", &ByteRecord::new(), &Vec::new()) {
            Err(_) => Err(()),
            Ok(pipeline) => {
                let variables = Variables::new();

                match interpret(&pipeline, &ByteRecord::new(), &variables) {
                    Err(_) => return Err(()),
                    Ok(value) => assert_eq!(value.truthy(), false),
                }

                Ok(())
            }
        }
    }
}
