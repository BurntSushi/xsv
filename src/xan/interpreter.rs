use csv::ByteRecord;

use xan::parser::{Argument, IndexationInfo, Pipeline};

enum DynamicValue {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    None,
}

enum ConcreteArgument {
    Variable(String),
    Column(usize),
    StringLiteral(String),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    Underscore,
}

struct ConcreteFunctionCall {
    name: String,
    args: Vec<ConcreteArgument>,
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
