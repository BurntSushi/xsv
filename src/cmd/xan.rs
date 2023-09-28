use std::convert::TryFrom;

use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
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
}

impl XanMode {
    fn is_map(&self) -> bool {
        match self {
            Self::Map => true,
            _ => false,
        }
    }

    fn is_filter(&self) -> bool {
        match self {
            Self::Filter => true,
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
    pub new_column: Option<String>,
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

pub fn handle_eval_result<W: std::io::Write>(
    args: &XanCmdArgs,
    index: usize,
    record: &mut csv::ByteRecord,
    eval_result: Result<DynamicValue, EvaluationError>,
    writer: &mut csv::Writer<W>,
) -> CliResult<()> {
    let mut should_write_row = true;

    match eval_result {
        Ok(value) => {
            if args.mode.is_filter() {
                if value.is_falsey() {
                    should_write_row = false;
                }
            } else {
                record.push_field(&value.serialize_as_bytes(b"|"));

                if args.error_policy.will_report() {
                    record.push_field(b"");
                }
            }
        }
        Err(err) => match args.error_policy {
            XanErrorPolicy::Ignore => {
                if args.mode.is_map() {
                    let value = DynamicValue::None;
                    record.push_field(&value.serialize_as_bytes(b"|"));
                }
            }
            XanErrorPolicy::Report => {
                if args.mode.is_filter() {
                    unreachable!();
                }

                record.push_field(b"");
                record.push_field(err.to_string().as_bytes());
            }
            XanErrorPolicy::Log => {
                eprintln!("Row n°{}: {}", index + 1, err);

                if args.mode.is_map() {
                    let value = DynamicValue::None;
                    record.push_field(&value.serialize_as_bytes(b"|"));
                }
            }
            XanErrorPolicy::Panic => {
                Err(format!("Row n°{}: {}", index + 1, err))?;
            }
        },
    };

    if should_write_row {
        writer.write_byte_record(record)?;
    }

    Ok(())
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

    let rconfig = Config::new(&args.input)
        .delimiter(args.delimiter)
        .no_headers(args.no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.output).writer()?;

    let mut headers = csv::ByteRecord::new();
    let mut must_write_headers = false;

    if !args.no_headers {
        headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            must_write_headers = true;

            if args.mode.is_map() {
                if let Some(new_column) = &args.new_column {
                    headers.push_field(new_column.as_bytes());
                }
            }

            if args.error_policy.will_report() {
                if let Some(error_column_name) = &args.error_column_name {
                    headers.push_field(error_column_name.as_bytes());
                }
            }
        }
    }

    let pipeline = prepare(&args.map_expr, &headers)?;

    if must_write_headers {
        wtr.write_byte_record(&headers)?;
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
                handle_eval_result(&args, i, &mut record, eval_result, &mut wtr)?;
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
        handle_eval_result(&args, i, &mut record, eval_result, &mut wtr)?;
        i += 1;
    }

    Ok(wtr.flush()?)
}
