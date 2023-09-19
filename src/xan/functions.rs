use flate2::read::GzDecoder;
use std::borrow::Cow;
use std::cmp::{Ordering, PartialOrd};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use super::error::EvaluationError;
use super::types::{BoundArguments, DynamicValue, EvaluationResult};

type FunctionResult = Result<DynamicValue, EvaluationError>;

// TODO: contains, startswith, endswith, comp, str comp, add, sub, lte, deburr, const
// TODO: parse most likely and cast functions
// TODO: --ignore-errors or print error or report error
pub fn call<'a>(name: &str, args: BoundArguments) -> EvaluationResult<'a> {
    (match name {
        "add" => add(args),
        "and" => and(args),
        "coalesce" => coalesce(args),
        "concat" => concat(args),
        "count" => count(args),
        "eq" => number_compare(args, Ordering::is_eq),
        "join" => join(args),
        "len" => len(args),
        "lower" => lower(args),
        "not" => not(args),
        "or" => or(args),
        "pathjoin" => pathjoin(args),
        "read" => read(args),
        "split" => split(args),
        "trim" => trim(args),
        "typeof" => type_of(args),
        "upper" => upper(args),
        _ => Err(EvaluationError::UnknownFunction(name.to_string())),
    })
    .map(Cow::Owned)
}

// Strings
fn trim(args: BoundArguments) -> FunctionResult {
    Ok(DynamicValue::from(args.get1_as_str()?.trim()))
}

fn split(args: BoundArguments) -> FunctionResult {
    let (to_split, pattern) = args.get2_as_str()?;
    let splitted: Vec<DynamicValue> = to_split.split(&*pattern).map(DynamicValue::from).collect();

    Ok(DynamicValue::from(splitted))
}

fn lower(args: BoundArguments) -> FunctionResult {
    Ok(DynamicValue::from(args.get1_as_str()?.to_lowercase()))
}

fn upper(args: BoundArguments) -> FunctionResult {
    Ok(DynamicValue::from(args.get1_as_str()?.to_uppercase()))
}

fn len(args: BoundArguments) -> FunctionResult {
    let arg = args.get1()?;

    Ok(DynamicValue::from(match arg.as_ref() {
        DynamicValue::List(list) => list.len(),
        _ => arg.try_as_str()?.len(),
    }))
}

fn count(args: BoundArguments) -> FunctionResult {
    let (string, pattern) = args.get2_as_str()?;

    Ok(DynamicValue::from(string.matches(pattern.as_ref()).count()))
}

fn concat(args: BoundArguments) -> FunctionResult {
    args.validate_min_arity(1)?;

    let mut args_iter = args.into_iter();
    let first = args_iter.next().unwrap();

    match first.as_ref() {
        DynamicValue::List(list) => {
            let mut result: Vec<DynamicValue> = list.clone();

            for arg in args_iter {
                result.push(arg.as_ref().clone());
            }

            Ok(DynamicValue::List(result))
        }
        value => {
            let mut result = String::new();
            result.push_str(&value.try_as_str()?);

            for arg in args_iter {
                result.push_str(&arg.try_as_str()?);
            }

            Ok(DynamicValue::from(result))
        }
    }
}

// Lists
fn join(args: BoundArguments) -> FunctionResult {
    let (arg1, arg2) = args.get2()?;

    let list = arg1.try_as_list()?;
    let joiner = arg2.try_as_str()?;

    let mut string_list: Vec<Cow<str>> = Vec::new();

    for value in list {
        string_list.push(value.try_as_str()?);
    }

    Ok(DynamicValue::from(string_list.join(&joiner)))
}

// Arithmetics
fn add(args: BoundArguments) -> FunctionResult {
    let (a, b) = args.get2_as_numbers()?;
    Ok(DynamicValue::from(a + b))
}

// Utilities
fn coalesce(args: BoundArguments) -> FunctionResult {
    for arg in args {
        if arg.truthy() {
            return Ok(arg.into_owned());
        }
    }

    Ok(DynamicValue::None)
}

// Boolean
fn not(args: BoundArguments) -> FunctionResult {
    Ok(DynamicValue::from(!args.get1_as_bool()?))
}

fn and(args: BoundArguments) -> FunctionResult {
    let (a, b) = args.get2_as_bool()?;
    Ok(DynamicValue::from(a && b))
}

fn or(args: BoundArguments) -> FunctionResult {
    let (a, b) = args.get2_as_bool()?;
    Ok(DynamicValue::from(a || b))
}

// Comparison
fn number_compare<F>(args: BoundArguments, validate: F) -> FunctionResult
where
    F: FnOnce(Ordering) -> bool,
{
    let (a, b) = args.get2_as_numbers()?;

    Ok(DynamicValue::from(match a.partial_cmp(&b) {
        Some(ordering) => validate(ordering),
        None => false,
    }))
}

// IO
fn pathjoin(args: BoundArguments) -> FunctionResult {
    args.validate_min_arity(2)?;

    let mut path = PathBuf::new();

    for arg in args {
        path.push(arg.try_as_str()?.as_ref());
    }

    let path = String::from(path.to_str().ok_or(EvaluationError::InvalidPath)?);

    Ok(DynamicValue::from(path))
}

fn read(args: BoundArguments) -> FunctionResult {
    let path = args.get1_as_str()?;

    // TODO: handle encoding
    let mut file = match File::open(path.as_ref()) {
        Err(_) => return Err(EvaluationError::CannotOpenFile(path.into_owned())),
        Ok(f) => f,
    };

    let mut buffer = String::new();

    if path.ends_with(".gz") {
        let mut gz = GzDecoder::new(file);
        gz.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path.into_owned()))?;
    } else {
        file.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path.into_owned()))?;
    }

    Ok(DynamicValue::from(buffer))
}

// Introspection
fn type_of(args: BoundArguments) -> FunctionResult {
    Ok(DynamicValue::from(args.get1()?.type_of()))
}
