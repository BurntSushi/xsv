use std::iter;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Sorts CSV data lexicographically.

Note that this (currently) requires reading all of the CSV data into memory.

Usage:
    xsv sort [options] [<input>]

sort options:
    -s, --select <arg>     Select a subset of columns to sort.
                           See 'xsv select --help' for the format details.

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
    flag_select: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers)
                         .select(args.flag_select);

    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(Config::new(args.flag_output).writer());

    let headers = try!(rdr.byte_headers());
    let sel = try!(rconfig.selection(headers[]));

    let mut all = try!(rdr.byte_records().collect::<Result<Vec<_>, _>>());
    all.sort_by(|r1, r2| {
        // TODO: Numeric sorting. The tricky part, IMO, is figuring out
        // how to expose it in the CLI interface. Not sure of the right
        // answer at the moment.
        iter::order::cmp(sel.select(r1[]), sel.select(r2[]))
    });

    try!(rconfig.write_headers(&mut rdr, &mut wtr));
    for r in all.into_iter() {
        try!(wtr.write_bytes(r.into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
