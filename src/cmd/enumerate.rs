use csv;

use config::{Config, Delimiter};
use util::{self, ImmutableRecordHelpers};
use CliResult;

static USAGE: &str = "
Enumerate a CSV file by preprending an index column to each row.

Usage:
    xsv enum [options] [<input>]
    xsv enum --help

enum options:
    -c, --column-name <arg>  Name of the column to prepend. [default: index].
    -S, --start <arg>        Number to count from. [default: 0].

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not considered as being
                           the file header.
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_delimiter: Option<Delimiter>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_start: i64,
    flag_column_name: String,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = conf.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    if !args.flag_no_headers {
        let headers = rdr
            .byte_headers()?
            .prepend(args.flag_column_name.as_bytes());

        wtr.write_byte_record(&headers)?;
    }

    let mut record = csv::ByteRecord::new();
    let mut counter = args.flag_start;

    while rdr.read_byte_record(&mut record)? {
        wtr.write_byte_record(&record.prepend(counter.to_string().as_bytes()))?;
        counter += 1;
    }

    Ok(wtr.flush()?)
}
