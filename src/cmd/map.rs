use csv;

use config::{Config, Delimiter};
use util;
use xan::prepare;
use CliResult;

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
    flag_rename: Option<String>,
    flag_new_column: Option<String>,
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

    if !args.flag_no_headers {
        let mut headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            headers.push_field(args.arg_column.as_bytes());
            wtr.write_byte_record(&headers)?;
        }
    }

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        record.push_field(b"");
        wtr.write_byte_record(&record)?;
    }

    Ok(wtr.flush()?)
}
