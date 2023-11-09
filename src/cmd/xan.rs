use std::borrow::Cow;
use std::convert::TryFrom;

use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
use select::SelectColumns;
use util::ImmutableRecordHelpers;
use xan::{eval, prepare, DynamicValue, EvaluationError, Variables};
use CliError;
use CliResult;

macro_rules! xan_cheatsheet {
    () => {
        "
xsv script language cheatsheet (use --functions for comprehensive list of
available functions):

  . Indexing a column by name:
        'trim(col)'

  . Indexing a column by name even with spaces:
        'trim(Name of film)'

  . Indexing column with characters forbidden in identifies (e.g. commas):
        'trim(row[\"Name, of film\"])'

  . Indexing column by index (0-based):
        'trim(row[2])'

  . Indexing a column by name and 0-based nth (for duplicate headers):
        'trim(row[\"col\", 1])'

  . Integer literals:
        'add(1, count)'

  . Boolean literals (true or false):
        'coalesce(count, true)'

  . Null literals:
        'coalesce(null, count)'

  . Float literals:
        'mul(0.5, count)'

  . String literals (can use single or double quotes):
        'concat(name, \"-\", surname)'

  . Regex literals:
        'match(name, /john/)'

  . Case-insensitive regex literals:
        'match(name, /john/i)'

  . Accessing current row index:
        'add(%index, 1)'

  . Nesting function calls:
        'add(sub(col1, col2), mul(col3, col4))'

  . Piping (underscore \"_\" becomes a reference to previous result):
        'trim(name) | lower(_) | add(count, len(_))'

        is the same as:

        'add(count, len(lower(trim(name))))'

  . Piping shorthand for unary functions:
        'trim(name) | lower'

        is the same as:

        'trim(name) | lower(_)'

   . Basic branching (also consider using the \"coalesce\" function for simple cases):
        'if(lt(count, 4), trim(name), trim(surname))'

Misc notes:

  . This is a minimal interpreted language with dynamic typing,
    which means functions will usually cast values around to
    make them fit expectations. Use the `typeof` function if
    you feel lost.
"
    };
}

macro_rules! xan_function_list {
    () => {
        "
# Available functions

(use --cheatsheet for a reminder of how the scripting language works)

## Arithmetics

    - abs(x) -> number
        Return absolute value of number.

    - add(x, y) -> number
        Add two numbers.

    - div(x, y) -> number
        Divide two numbers.

    - idiv(x, y) -> number
        Integer division of two numbers.

    - mul(x, y) -> number
        Multiply x & y.

    - sub(x, y) -> number
        Subtract x & y.

## Boolean operations & branching

    - and(a, b) -> bool
        Perform boolean AND operation.

    - if(cond, then, else?) -> T
        Evaluate condition and switch to correct branch.

    - not(a) -> bool
        Perform boolean NOT operation.

    - or(a, b) -> bool
        Perform boolean OR operation.

## Comparison

    - eq(x, y) -> bool
        Test numerical equality.

    - gt(x, y) -> bool
        Test numerical x > y.

    - gte(x, y) -> bool
        Test numerical x >= y.

    - lt(x, y)
        Test numerical x < y.

    - lte(x, y)
        Test numerical x > y.

    - neq(x, y) -> bool
        Test numerical x != y.

    - s_eq(s1, s2) -> bool
        Test sequence equality.

    - s_gt(s1, s2) -> bool
        Test sequence s1 > s2.

    - s_gte(s1, s2) -> bool
        Test sequence s1 >= s2.

    - s_lt(s1, s2) -> bool
        Test sequence s1 < s2.

    - s_gte(s1, s2) -> bool
        Test sequence s1 <= s2.

    - s_neq(s1, s2) -> bool
        Test sequence s1 != s2.

## String & sequence helpers

    - concat(string, *strings) -> string
        Concatenate given strings into a single one.

    - contains(seq, subseq) -> bool
        Find if subseq can be found in seq.

    - count(seq, pattern) -> int
        Count number of times pattern appear in seq.

    - endswith(string, pattern) -> bool
        Test if string ends with pattern.

    - first(seq) -> T
        Get first element of sequence.

    - get(seq, index) -> T
        Get nth element of sequence (can use negative indexing).

    - join(seq, sep) -> string
        Join sequence by separator.

    - last(seq) -> T
        Get last element of sequence.

    - len(seq) -> int
        Get length of sequence.

    - ltrim(string, pattern?) -> string
        Trim string of leading whitespace or
        provided characters.

    - lower(string) -> string
        Lowercase string.

    - match(string, regex) -> bool
        Return whether regex pattern matches string.

    - replace(string, pattern, replacement) -> string
        Replace pattern in string. Can use a regex.

    - rtrim(string, pattern?) -> string
        Trim string of trailing whitespace or
        provided characters.

    - slice(seq, start, end?) -> seq
        Return slice of sequence.

    - split(string, sep, max?) -> list
        Split a string by separator.

    - startswith(string, pattern) -> bool
        Test if string starts with pattern.

    - trim(string, pattern?) -> string
        Trim string of leading & trailing whitespace or
        provided characters.

    - unidecode(string) -> string
        Convert string to ascii as well as possible.

    - upper(string) -> string
        Uppercase string.

## Utils

    - coalesce(*args) -> T
        Return first truthy value.

    - err(msg) -> error
        Make the expression return a custom error.

    - typeof(value) -> string
        Return type of value.

    - val(value) -> T
        Return a value as-is. Useful to return constants.

## IO & path wrangling

    - abspath(string) -> string
        Return absolute & canonicalized path.

    - isfile(string) -> bool
        Return whether the given path is an existing file on disk.

    - pathjoin(string, *strings) -> string
        Join multiple paths correctly.

    - read(path, encoding?, errors?) -> string
        Read file at path. Default encoding is \"utf-8\".
        Default error handling policy is \"replace\", and can be
        one of \"replace\", \"ignore\" or \"strict\".

## Random

    - uuid() -> string
        Return a uuid v4.

"
    };
}

pub enum XanMode {
    Map,
    Filter,
    Transform,
    Flatmap,
}

impl XanMode {
    fn is_map(&self) -> bool {
        match self {
            Self::Map => true,
            _ => false,
        }
    }

    fn is_transform(&self) -> bool {
        match self {
            Self::Transform => true,
            _ => false,
        }
    }

    fn cannot_report(&self) -> bool {
        match self {
            Self::Filter | Self::Flatmap => true,
            _ => false,
        }
    }
}

pub enum XanErrorPolicy {
    Panic,
    Report,
    Ignore,
    Log,
}

impl XanErrorPolicy {
    fn will_report(&self) -> bool {
        match self {
            Self::Report => true,
            _ => false,
        }
    }

    pub fn from_restricted(value: &str) -> Result<Self, CliError> {
        Ok(match value {
            "panic" => Self::Panic,
            "ignore" => Self::Ignore,
            "log" => Self::Log,
            _ => {
                return Err(CliError::Other(format!(
                    "unknown error policy \"{}\"",
                    value
                )))
            }
        })
    }
}

impl TryFrom<String> for XanErrorPolicy {
    type Error = CliError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(match value.as_str() {
            "panic" => Self::Panic,
            "report" => Self::Report,
            "ignore" => Self::Ignore,
            "log" => Self::Log,
            _ => {
                return Err(CliError::Other(format!(
                    "unknown error policy \"{}\"",
                    value
                )))
            }
        })
    }
}

pub struct XanCmdArgs {
    pub print_cheatsheet: bool,
    pub print_functions: bool,
    pub target_column: Option<String>,
    pub rename_column: Option<String>,
    pub map_expr: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub no_headers: bool,
    pub delimiter: Option<Delimiter>,
    pub threads: Option<usize>,
    pub error_policy: XanErrorPolicy,
    pub error_column_name: Option<String>,
    pub mode: XanMode,
}

pub fn handle_eval_result<'a, 'b>(
    args: &'a XanCmdArgs,
    index: usize,
    record: &'b mut csv::ByteRecord,
    eval_result: Result<DynamicValue, EvaluationError>,
    replace: Option<usize>,
) -> Result<Vec<Cow<'b, csv::ByteRecord>>, String> {
    let mut records_to_emit: Vec<Cow<csv::ByteRecord>> = Vec::new();

    match eval_result {
        Ok(value) => match args.mode {
            XanMode::Filter => {
                if value.is_truthy() {
                    records_to_emit.push(Cow::Borrowed(record));
                }
            }
            XanMode::Map => {
                record.push_field(&value.serialize_as_bytes(b"|"));

                if args.error_policy.will_report() {
                    record.push_field(b"");
                }

                records_to_emit.push(Cow::Borrowed(record));
            }
            XanMode::Transform => {
                let mut record =
                    record.replace_at(replace.unwrap(), &value.serialize_as_bytes(b"|"));

                if args.error_policy.will_report() {
                    record.push_field(b"");
                }

                records_to_emit.push(Cow::Owned(record));
            }
            XanMode::Flatmap => 'm: {
                if value.is_falsey() {
                    break 'm;
                }

                for subvalue in value.flat_iter() {
                    let cell = subvalue.serialize_as_bytes(b"|");

                    let new_record = if let Some(idx) = replace {
                        record.replace_at(idx, &cell)
                    } else {
                        record.append(&cell)
                    };

                    records_to_emit.push(Cow::Owned(new_record));
                }
            }
        },
        Err(err) => match args.error_policy {
            XanErrorPolicy::Ignore => {
                let value = DynamicValue::None.serialize_as_bytes(b"|");

                if args.mode.is_map() {
                    record.push_field(&value);
                    records_to_emit.push(Cow::Borrowed(record));
                } else if args.mode.is_transform() {
                    let record = record.replace_at(replace.unwrap(), &value);
                    records_to_emit.push(Cow::Owned(record));
                }
            }
            XanErrorPolicy::Report => {
                if args.mode.cannot_report() {
                    unreachable!();
                }

                let value = DynamicValue::None.serialize_as_bytes(b"|");

                if args.mode.is_map() {
                    record.push_field(&value);
                    record.push_field(err.to_string().as_bytes());
                    records_to_emit.push(Cow::Borrowed(record));
                } else if args.mode.is_transform() {
                    let mut record = record.replace_at(replace.unwrap(), &value);
                    record.push_field(err.to_string().as_bytes());
                    records_to_emit.push(Cow::Owned(record));
                }
            }
            XanErrorPolicy::Log => {
                eprintln!("Row n°{}: {}", index + 1, err);

                let value = DynamicValue::None.serialize_as_bytes(b"|");

                if args.mode.is_map() {
                    record.push_field(&value);
                    records_to_emit.push(Cow::Borrowed(record));
                } else if args.mode.is_transform() {
                    let record = record.replace_at(replace.unwrap(), &value);
                    records_to_emit.push(Cow::Owned(record));
                }
            }
            XanErrorPolicy::Panic => {
                return Err(format!("Row n°{}: {}", index + 1, err));
            }
        },
    };

    Ok(records_to_emit)
}

pub fn run_xan_cmd(args: XanCmdArgs) -> CliResult<()> {
    if args.print_cheatsheet {
        println!(xan_cheatsheet!());
        return Ok(());
    }

    if args.print_functions {
        println!(xan_function_list!());
        return Ok(());
    }

    let mut rconfig = Config::new(&args.input)
        .delimiter(args.delimiter)
        .no_headers(args.no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.output).writer()?;

    let mut headers = csv::ByteRecord::new();
    let mut modified_headers = csv::ByteRecord::new();
    let mut must_write_headers = false;
    let mut column_to_replace: Option<usize> = None;
    let mut map_expr = args.map_expr.clone();

    if !args.no_headers {
        headers = rdr.byte_headers()?.clone();
        modified_headers = headers.clone();

        if !headers.is_empty() {
            must_write_headers = true;

            if args.mode.is_map() {
                if let Some(target_column) = &args.target_column {
                    modified_headers.push_field(target_column.as_bytes());
                }
            } else if args.mode.is_transform() {
                if let Some(name) = &args.target_column {
                    rconfig = rconfig.select(SelectColumns::parse(name)?);
                    let idx = rconfig.single_selection(&headers)?;

                    if let Some(renamed) = &args.rename_column {
                        modified_headers = modified_headers.replace_at(idx, renamed.as_bytes());
                    }

                    column_to_replace = Some(idx);

                    // NOTE: binding implicit last value to target column value
                    map_expr = format!("val(row[{}]) | {}", idx, map_expr);
                }
            }

            if args.error_policy.will_report() {
                if let Some(error_column_name) = &args.error_column_name {
                    modified_headers.push_field(error_column_name.as_bytes());
                }
            }
        }
    }

    let pipeline = prepare(&map_expr, &headers)?;

    if must_write_headers {
        wtr.write_byte_record(&modified_headers)?;
    }

    if let Some(threads) = args.threads {
        rdr.into_byte_records()
            .enumerate()
            .parallel_map_custom(
                |o| o.threads(threads),
                move |(i, record)| -> CliResult<(
                    usize,
                    csv::ByteRecord,
                    Result<DynamicValue, EvaluationError>,
                )> {
                    let record = record?;
                    let mut variables = Variables::new();
                    variables.insert("index", DynamicValue::Integer(i as i64));

                    let eval_result = eval(&pipeline, &record, &variables);

                    Ok((i, record, eval_result))
                },
            )
            .try_for_each(|result| -> CliResult<()> {
                let (i, mut record, eval_result) = result?;
                let records_to_emit =
                    handle_eval_result(&args, i, &mut record, eval_result, column_to_replace)?;

                for record_to_emit in records_to_emit {
                    wtr.write_byte_record(&record_to_emit)?;
                }
                Ok(())
            })?;

        return Ok(wtr.flush()?);
    }

    let mut record = csv::ByteRecord::new();
    let mut variables = Variables::new();
    let mut i: usize = 0;

    while rdr.read_byte_record(&mut record)? {
        variables.insert("index", DynamicValue::Integer(i as i64));

        let eval_result = eval(&pipeline, &record, &variables);
        let records_to_emit =
            handle_eval_result(&args, i, &mut record, eval_result, column_to_replace)?;

        for record_to_emit in records_to_emit {
            wtr.write_byte_record(&record_to_emit)?;
        }

        i += 1;
    }

    Ok(wtr.flush()?)
}
