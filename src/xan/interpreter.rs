use csv::ByteRecord;

use xan::error::EvaluationError;
use xan::functions::{coalesce, concat, count, eq, len, trim, DynamicValue};
use xan::parser::{parse, Argument, IndexationInfo, Pipeline};

enum ConcreteArgument {
    Variable(String),
    Column(usize),
    StringLiteral(String),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    Underscore,
}

// TODO: handle variables
// TODO: investigate cows
impl ConcreteArgument {
    fn bind(
        &self,
        record: &ByteRecord,
        last_value: &DynamicValue,
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
            Self::Variable(name) => return Err(EvaluationError::UnknownVariable(name.clone())),
        })
    }
}

pub struct ConcreteFunctionCall {
    name: String,
    args: Vec<ConcreteArgument>,
}

impl ConcreteFunctionCall {
    fn bind(
        &self,
        record: &ByteRecord,
        last_value: &DynamicValue,
    ) -> Result<Vec<DynamicValue>, EvaluationError> {
        let mut bound_args: Vec<DynamicValue> = Vec::new();

        for arg in self.args.iter() {
            bound_args.push(arg.bind(record, last_value)?);
        }

        Ok(bound_args)
    }

    fn call(
        &self,
        record: &ByteRecord,
        last_value: &DynamicValue,
    ) -> Result<DynamicValue, EvaluationError> {
        let args = self.bind(record, last_value)?;

        match self.name.as_ref() {
            "coalesce" => coalesce(&args),
            "concat" => concat(&args),
            "count" => count(&args),
            "eq" => eq(&args),
            "len" => len(&args),
            "trim" => trim(&args),
            _ => Err(EvaluationError::UnknownFunction(self.name.clone())),
        }
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

pub fn interpret(
    pipeline: &ConcretePipeline,
    record: &ByteRecord,
) -> Result<DynamicValue, EvaluationError> {
    let mut last_value = DynamicValue::None;

    for function_call in pipeline {
        last_value = function_call.call(&record, &last_value)?;
    }

    Ok(last_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpret() -> Result<(), ()> {
        let pipeline = prepare("trim", &ByteRecord::new(), &Vec::new())?;

        match interpret(&pipeline, &ByteRecord::new()) {
            Err(_) => return Err(()),
            Ok(value) => assert_eq!(value.serialize(), String::new()),
        }

        Ok(())
    }
}
