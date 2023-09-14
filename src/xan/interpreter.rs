use std::collections::BTreeMap;

use csv::ByteRecord;

use xan::error::EvaluationError;
use xan::functions::{
    add, coalesce, concat, count, eq, len, lower, pathjoin, read, trim, type_of, upper,
    DynamicValue,
};
use xan::parser::{parse, Argument, IndexationInfo, Pipeline};

enum ConcreteArgument {
    Variable(String),
    Column(usize),
    StringLiteral(String),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    Call(ConcreteFunctionCall),
    Underscore,
}

// TODO: investigate cows, or make dynamic values hold pointers
impl ConcreteArgument {
    fn bind(
        &self,
        record: &ByteRecord,
        last_value: &DynamicValue,
        variables: &BTreeMap<&String, DynamicValue>,
    ) -> Result<DynamicValue, EvaluationError> {
        Ok(match self {
            Self::StringLiteral(value) => DynamicValue::String(value.clone()),
            Self::FloatLiteral(value) => DynamicValue::Float(*value),
            Self::IntegerLiteral(value) => DynamicValue::Integer(*value),
            Self::BooleanLiteral(value) => DynamicValue::Boolean(*value),
            Self::Underscore => last_value.clone(),
            Self::Column(index) => match record.get(*index) {
                None => return Err(EvaluationError::ColumnOutOfRange(*index)),
                Some(cell) => match String::from_utf8(cell.to_vec()) {
                    Err(_) => return Err(EvaluationError::UnicodeDecodeError),
                    Ok(value) => DynamicValue::String(value),
                },
            },
            Self::Variable(name) => match variables.get(name) {
                Some(value) => value.clone(),
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

fn call(name: &str, args: &Vec<DynamicValue>) -> Result<DynamicValue, EvaluationError> {
    match name {
        "add" => add(&args),
        "coalesce" => coalesce(&args),
        "concat" => concat(&args),
        "count" => count(&args),
        "eq" => eq(&args),
        "len" => len(&args),
        "lower" => lower(&args),
        "pathjoin" => pathjoin(&args),
        "read" => read(&args),
        "trim" => trim(&args),
        "typeof" => type_of(&args),
        "upper" => upper(&args),
        _ => Err(EvaluationError::UnknownFunction(name.to_string())),
    }
}

type ConcretePipeline = Vec<ConcreteFunctionCall>;

fn find_column_index(header: &ByteRecord, name: &str, pos: usize) -> Option<usize> {
    let mut i: usize = 0;
    let mut pos = pos;

    let name_bytes = name.as_bytes();

    for cell in header {
        if cell == name_bytes {
            if pos == 0 {
                return Some(i);
            }
            pos -= 1;
        }

        i += 1;
    }

    None
}

// TODO: create a concretization error enum
fn concretize_argument(
    argument: Argument,
    header: &ByteRecord,
    reserved: &Vec<String>,
) -> Result<ConcreteArgument, IndexationInfo> {
    Ok(match argument {
        Argument::Underscore => ConcreteArgument::Underscore,
        Argument::BooleanLiteral(v) => ConcreteArgument::BooleanLiteral(v),
        Argument::FloatLiteral(v) => ConcreteArgument::FloatLiteral(v),
        Argument::IntegerLiteral(v) => ConcreteArgument::IntegerLiteral(v),
        Argument::StringLiteral(v) => ConcreteArgument::StringLiteral(v),
        Argument::Identifier(name) => {
            if reserved.contains(&name) {
                ConcreteArgument::Variable(name)
            } else {
                match find_column_index(header, &name, 0) {
                    Some(index) => ConcreteArgument::Column(index),
                    None => return Err(IndexationInfo { name: name, pos: 0 }),
                }
            }
        }
        Argument::Indexation(info) => match find_column_index(header, &info.name, info.pos) {
            Some(index) => ConcreteArgument::Column(index),
            None => return Err(info),
        },
        Argument::Call(call) => {
            let mut concrete_args = Vec::new();

            for arg in call.args {
                concrete_args.push(concretize_argument(arg, header, reserved)?);
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
    header: &ByteRecord,
    reserved: &Vec<String>,
) -> Result<ConcretePipeline, IndexationInfo> {
    let mut concrete_pipeline: ConcretePipeline = Vec::new();

    for function_call in pipeline {
        let mut concrete_arguments: Vec<ConcreteArgument> = Vec::new();

        for argument in function_call.args {
            concrete_arguments.push(concretize_argument(argument, header, reserved)?);
        }

        concrete_pipeline.push(ConcreteFunctionCall {
            name: function_call.name.clone(),
            args: concrete_arguments,
        });
    }

    Ok(concrete_pipeline)
}

// TODO: write this better
// TODO: enum for errors
pub fn prepare(
    code: &str,
    header: &ByteRecord,
    reserved: &Vec<String>,
) -> Result<ConcretePipeline, ()> {
    match parse(code) {
        Err(_) => Err(()),
        Ok(pipeline) => match concretize_pipeline(pipeline, header, reserved) {
            Err(_) => Err(()),
            Ok(concrete_pipeline) => Ok(concrete_pipeline),
        },
    }
}

fn eval_function(
    function_call: &ConcreteFunctionCall,
    record: &ByteRecord,
    last_value: &DynamicValue,
    variables: &BTreeMap<&String, DynamicValue>,
) -> Result<DynamicValue, EvaluationError> {
    let mut bound_args: Vec<DynamicValue> = Vec::new();

    for arg in function_call.args.iter() {
        match arg {
            ConcreteArgument::Call(sub_function_call) => {
                bound_args.push(traverse(&sub_function_call, record, last_value, variables)?);
            }
            _ => bound_args.push(arg.bind(record, last_value, variables)?),
        }
    }

    call(&function_call.name, &bound_args)
}

fn eval(
    arg: &ConcreteArgument,
    record: &ByteRecord,
    last_value: &DynamicValue,
    variables: &BTreeMap<&String, DynamicValue>,
) -> Result<DynamicValue, EvaluationError> {
    match arg {
        ConcreteArgument::Call(function_call) => {
            eval_function(function_call, record, last_value, variables)
        }
        _ => arg.bind(record, last_value, variables),
    }
}

fn traverse(
    function_call: &ConcreteFunctionCall,
    record: &ByteRecord,
    last_value: &DynamicValue,
    variables: &BTreeMap<&String, DynamicValue>,
) -> Result<DynamicValue, EvaluationError> {
    // Branching
    if function_call.name == "if".to_string() {
        let arity = function_call.args.len();

        if arity < 2 || arity > 3 {
            return Err(EvaluationError::from_range_arity(2, 3, arity));
        }

        let condition = &function_call.args[0];
        let result = eval(condition, record, last_value, variables)?;

        let mut branch: Option<&ConcreteArgument> = None;

        if result.cast_to_bool()? {
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

pub fn interpret(
    pipeline: &ConcretePipeline,
    record: &ByteRecord,
    variables: &BTreeMap<&String, DynamicValue>,
) -> Result<DynamicValue, EvaluationError> {
    let mut last_value = DynamicValue::None;

    for function_call in pipeline {
        last_value = traverse(function_call, record, &last_value, variables)?;
    }

    Ok(last_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpret() -> Result<(), ()> {
        let pipeline = prepare("trim", &ByteRecord::new(), &Vec::new())?;
        let variables = BTreeMap::new();

        match interpret(&pipeline, &ByteRecord::new(), &variables) {
            Err(_) => return Err(()),
            Ok(value) => assert_eq!(value.serialize(), String::new()),
        }

        Ok(())
    }
}
