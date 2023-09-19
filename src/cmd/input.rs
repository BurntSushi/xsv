use csv;

use config::{Config, Delimiter};
use util;
use CliResult;

static USAGE: &str = "
Read CSV data with special quoting rules.

Generally, all xsv commands support basic options like specifying the delimiter
used in CSV data. This does not cover all possible types of CSV data. For
example, some CSV files don't use '\"' for quotes or use different escaping
styles.

Usage:
    xsv input [options] [<input>]

input options:
    --quote <arg>          The quote character to use. [default: \"]
    --escape <arg>         The escape character to use. When not specified,
                           quotes are escaped by doubling them.
    --no-quoting           Disable quoting completely.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
    flag_quote: Delimiter,
    flag_escape: Option<Delimiter>,
    flag_no_quoting: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(true)
        .quote(args.flag_quote.as_byte());
    let wconfig = Config::new(&args.flag_output);

    if let Some(escape) = args.flag_escape {
        rconfig = rconfig.escape(Some(escape.as_byte())).double_quote(false);
    }
    if args.flag_no_quoting {
        rconfig = rconfig.quoting(false);
    }

    let mut rdr = rconfig.reader()?;
    let mut wtr = wconfig.writer()?;
    let mut row = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut row)? {
        wtr.write_record(&row)?;
    }
    wtr.flush()?;
    Ok(())
}
