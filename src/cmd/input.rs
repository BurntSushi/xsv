use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
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

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
    flag_quote: Delimiter,
    flag_escape: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(true);
    let wconfig = Config::new(&args.flag_output);
    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(wconfig.writer());

    rdr = rdr.quote(args.flag_quote.as_byte());
    if let Some(escape) = args.flag_escape {
        rdr = rdr.escape(Some(escape.as_byte())).double_quote(false);
    }
    for r in rdr.byte_records() {
        try!(wtr.write(try!(r).into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
