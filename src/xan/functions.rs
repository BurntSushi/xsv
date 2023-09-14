use flate2::read::GzDecoder;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::fs::File;
use std::io::Read;
use std::ops::Add;
use std::path::PathBuf;

use xan::error::EvaluationError;
use xan::types::{BoundArguments, DynamicNumber, DynamicValue, EvaluationResult};

// TODO: contains, startswith, endswith, comp, str comp, add, sub etc.
// TODO: test variable bindings
// TODO: parse most likely and cast functions
// TODO: -p and --ignore-errors
// TODO: never clone strings, implement Into and TryInto, Vec of arguments
// TODO: try bound arguments containing Rc or Arc of dynamic values instead
// shall be owned so we can safely transform them as needed on the fly for no cost and own them
pub fn call(name: &str, args: BoundArguments) -> EvaluationResult {
    match name {
        // "add" => add(args),
        // "and" => and(args),
        "coalesce" => coalesce(args),
        // "concat" => concat(args),
        "count" => count(args),
        // "eq" => number_compare(args, Ordering::is_eq),
        "len" => len(args),
        "lower" => lower(args),
        // "not" => not(args),
        // "or" => or(args),
        // "pathjoin" => pathjoin(args),
        // "read" => read(args),
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

// String queries
fn count(mut args: BoundArguments) -> EvaluationResult {
    let (string, pattern) = args.get2_string()?;

    Ok(DynamicValue::from(string.matches(&pattern).count()))
}

// Arithmetics
// fn add(args: BoundArguments) -> EvaluationResult {
//     validate_min_arity(args, 2)?;

//     let mut sum = DynamicNumber::Integer(0);

//     for arg in args {
//         sum = sum + arg.cast_to_number()?;
//     }

//     Ok(sum.to_dynamic_value())
// }

// Utilities
fn coalesce(args: BoundArguments) -> EvaluationResult {
    for arg in args {
        if arg.truthy() {
            return Ok(arg);
        }
    }

    Ok(DynamicValue::None)
}

// fn concat(args: BoundArguments) -> EvaluationResult {
//     let mut result = String::new();

//     for arg in args {
//         result.push_str(&arg.cast_to_string()?);
//     }

//     Ok(DynamicValue::String(result))
// }

// Boolean
// fn not(args: BoundArguments) -> EvaluationResult {
//     validate_arity(args, 1)?;

//     Ok(DynamicValue::Boolean(!args[0].cast_to_bool()?))
// }

// fn and(args: BoundArguments) -> EvaluationResult {
//     validate_arity(args, 2)?;

//     Ok(DynamicValue::Boolean(
//         args[0].cast_to_bool()? && args[1].cast_to_bool()?,
//     ))
// }

// fn or(args: BoundArguments) -> EvaluationResult {
//     validate_arity(args, 2)?;

//     Ok(DynamicValue::Boolean(
//         args[0].cast_to_bool()? || args[1].cast_to_bool()?,
//     ))
// }

// Comparison
// fn number_compare<F>(args: BoundArguments, validate: F) -> EvaluationResult
// where
//     F: FnOnce(Ordering) -> bool,
// {
//     validate_arity(args, 2)?;

//     let a = args[0].cast_to_number()?;
//     let b = args[1].cast_to_number()?;

//     Ok(DynamicValue::Boolean(match a.partial_cmp(&b) {
//         Some(ordering) => validate(ordering),
//         None => false,
//     }))
// }

// IO
// fn pathjoin(args: BoundArguments) -> EvaluationResult {
//     validate_min_arity(args, 2)?;

//     let mut path = PathBuf::new();

//     for arg in args {
//         path.push(arg.cast_to_string()?);
//     }

//     let path = String::from(path.to_str().ok_or(EvaluationError::InvalidPath)?);

//     Ok(DynamicValue::String(path))
// }

// fn read(args: BoundArguments) -> EvaluationResult {
//     validate_arity(args, 1)?;

//     let path = args[0].cast_to_string()?;

//     // TODO: handle encoding
//     let mut file = match File::open(&path) {
//         Err(_) => return Err(EvaluationError::CannotOpenFile(path)),
//         Ok(f) => f,
//     };

//     let mut buffer = String::new();

//     if path.ends_with(".gz") {
//         let mut gz = GzDecoder::new(file);
//         gz.read_to_string(&mut buffer)
//             .map_err(|_| EvaluationError::CannotReadFile(path))?;
//     } else {
//         file.read_to_string(&mut buffer)
//             .map_err(|_| EvaluationError::CannotReadFile(path))?;
//     }

//     Ok(DynamicValue::String(buffer))
// }

// Introspection
fn type_of(mut args: BoundArguments) -> EvaluationResult {
    Ok(DynamicValue::from(args.get1()?.type_of()))
}
