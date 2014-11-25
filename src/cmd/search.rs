use regex::Regex;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Filters CSV data by whether the given regex matches a row.

The regex is applied to each field in each row, and if any field matches,
then the row is written to the output. The columns to search can be limited
with the '--select' flag (but the full row is still written to the output if
there is a match).

Usage:
    xsv search [options] <regex> [<input>]
    xsv search --help

search options:
    -s, --select <arg>     Select the columns to search. See 'xsv select -h'
                           for the full syntax.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    arg_regex: String,
    flag_select: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let pattern = try!(Regex::new(args.arg_regex[]));
    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers)
                         .select(args.flag_select);

    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(Config::new(&args.flag_output).writer());

    let headers = try!(rdr.byte_headers());
    let nsel = try!(rconfig.normal_selection(headers[]));

    try!(rconfig.write_headers(&mut rdr, &mut wtr));
    for row in rdr.records() {
        let row = try!(row);
        if nsel.select(row.iter()).any(|f| pattern.is_match(f[])) {
            try!(wtr.write(row.iter().map(|f| f[])));
        }
    }
    Ok(try!(wtr.flush()))
}
