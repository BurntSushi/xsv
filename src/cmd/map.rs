use csv;

use crate::{xan::interpret, CliError};
use config::{Config, Delimiter};
use util;
use xan::{prepare, EvaluationError};
use CliResult;

impl From<EvaluationError> for CliError {
    fn from(err: EvaluationError) -> CliError {
        CliError::Other(
            (match err {
                EvaluationError::InvalidPath => "invalid path",
                EvaluationError::InvalidArity(_) => "invalid arity",
                EvaluationError::CannotOpenFile => "cannot open file",
                EvaluationError::CannotReadFile => "cannot read file",
                _ => "evaluation error",
            })
            .to_string(),
        )
    }
}

impl From<()> for CliError {
    fn from(_: ()) -> CliError {
        CliError::Other("unknown error".to_string())
    }
}

static USAGE: &'static str = "
TODO map

Usage:
    xsv map [options] <operations> <column> [<input>]
    xsv map --help

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be interpreted
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_column: String,
    arg_operations: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = csv::ByteRecord::new();

    if !args.flag_no_headers {
        headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            headers.push_field(args.arg_column.as_bytes());
            wtr.write_byte_record(&headers)?;
        }
    }

    let pipeline = prepare(&args.arg_operations, &headers, &Vec::new())?;

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        let value = interpret(&pipeline, &record)?;
        record.push_field(value.serialize().as_bytes());
        wtr.write_byte_record(&record)?;
    }

    Ok(wtr.flush()?)
}
