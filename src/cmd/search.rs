use regex::Regex;

use docopt;

use {CliError, CliResult};
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

docopt!(Args, "
Filters CSV data by whether the given regex matches a row.

The regex is applied to each field in each row, and if any field matches,
then the row is written to the output. The columns to search can be limited
with the `--select` flag (but the full row is still written to the output if
there is a match).

Usage:
    xsv search [options] <regex> [<input>]
    xsv search --help

search options:
    -s, --select <arg>  Column selection. Each column can be referenced
                        by its column name or index, starting at 1.
                        Specify multiple columns by separating them with
                        a comma. Specify a range of columns with `-`.
                        Each column will have the regex applied to it.
                        If not supplied, all columns in each row will be
                        searched.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   arg_regex: String,
   flag_delimiter: Delimiter, flag_select: SelectColumns)

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(argv));
    let pattern = try!(Regex::new(args.arg_regex[])
                             .map_err(CliError::from_str));
    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers)
                         .select(args.flag_select);

    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| Config::new(args.flag_output).writer());

    let headers = try!(csv| rdr.byte_headers());
    let nsel = try!(str| rconfig.normal_selection(headers[]));

    try!(csv| rconfig.write_headers(&mut rdr, &mut wtr));
    for row in rdr.records() {
        let row = try!(csv| row);
        if nsel.select(row.iter()).any(|f| pattern.is_match(f[])) {
            try!(csv| wtr.write(row.iter().map(|f| f[])));
        }
    }
    try!(csv| wtr.flush());
    Ok(())
}
