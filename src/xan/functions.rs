use flate2::read::GzDecoder;
use std::cmp::{Ordering, PartialOrd};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use xan::error::EvaluationError;
use xan::types::{BoundArguments, DynamicValue, EvaluationResult};

// TODO: contains, startswith, endswith, comp, str comp, add, sub, lte, etc.
// TODO: test variable bindings
// TODO: parse most likely and cast functions
// TODO: -p and --ignore-errors
// TODO: never clone strings, implement Into and TryInto, Vec of arguments
// TODO: try bound arguments containing Rc or Arc of dynamic values instead
// shall be owned so we can safely transform them as needed on the fly for no cost and own them
pub fn call(name: &str, args: BoundArguments) -> EvaluationResult {
    match name {
        "add" => add(args),
        "and" => and(args),
        "coalesce" => coalesce(args),
        "concat" => concat(args),
        "count" => count(args),
        "eq" => number_compare(args, Ordering::is_eq),
        "len" => len(args),
        "lower" => lower(args),
        "not" => not(args),
        "or" => or(args),
        "pathjoin" => pathjoin(args),
        "read" => read(args),
        "trim" => trim(args),
        "typeof" => type_of(args),
        "upper" => upper(args),
        _ => Err(EvaluationError::UnknownFunction(name.to_string())),
    }
}

// String transformations
fn trim(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1_string()?.trim()))
}

fn lower(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1_string()?.to_lowercase()))
}

fn upper(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1_string()?.to_uppercase()))
}

fn len(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1_string()?.len()))
}

// Strings
fn count(mut args: BoundArguments) -> EvaluationResult {
    let (string, pattern) = args.get2_string()?;

    Ok(DynamicValue::from(string.matches(&pattern).count()))
}

fn concat(args: BoundArguments) -> EvaluationResult {
    let mut result = String::new();

    for arg in args {
        result.push_str(&arg.into_string());
    }

    Ok(DynamicValue::from(result))
}

// Arithmetics
fn add(mut args: BoundArguments) -> EvaluationResult {
    let (a, b) = args.get2_number()?;

    return Ok(DynamicValue::from(a + b));
}

// Utilities
fn coalesce(args: BoundArguments) -> EvaluationResult {
    for arg in args {
        if arg.truthy() {
            return Ok(arg);
        }
    }

    Ok(DynamicValue::None)
}

// Boolean
fn not(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(!args.get1_bool()?))
}

fn and(mut args: BoundArguments) -> EvaluationResult {
    let (a, b) = args.get2_bool()?;
    Ok(DynamicValue::from(a && b))
}

fn or(mut args: BoundArguments) -> EvaluationResult {
    let (a, b) = args.get2_bool()?;
    Ok(DynamicValue::from(a || b))
}

// Comparison
fn number_compare<F>(mut args: BoundArguments, validate: F) -> EvaluationResult
where
    F: FnOnce(Ordering) -> bool,
{
    let (a, b) = args.get2_number()?;

    Ok(DynamicValue::from(match a.partial_cmp(&b) {
        Some(ordering) => validate(ordering),
        None => false,
    }))
}

// IO
fn pathjoin(args: BoundArguments) -> EvaluationResult {
    args.validate_min_arity(2)?;

    let mut path = PathBuf::new();

    for arg in args {
        path.push(arg.into_string());
    }

    let path = String::from(path.to_str().ok_or(EvaluationError::InvalidPath)?);

    Ok(DynamicValue::from(path))
}

fn read(mut args: BoundArguments) -> EvaluationResult {
    let path = args.get1_string()?;

    // TODO: handle encoding
    let mut file = match File::open(&path) {
        Err(_) => return Err(EvaluationError::CannotOpenFile(path)),
        Ok(f) => f,
    };

    let mut buffer = String::new();

    if path.ends_with(".gz") {
        let mut gz = GzDecoder::new(file);
        gz.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path))?;
    } else {
        file.read_to_string(&mut buffer)
            .map_err(|_| EvaluationError::CannotReadFile(path))?;
    }

    Ok(DynamicValue::String(buffer))
}

// Introspection
fn type_of(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1()?.type_of()))
}
