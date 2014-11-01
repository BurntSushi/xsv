use std::iter;

use docopt;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

docopt!(Args, "
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
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_select: SelectColumns)

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(argv));

    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers)
                         .select(args.flag_select);

    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| Config::new(args.flag_output).writer());

    let headers = try!(csv| rdr.byte_headers());
    let sel = try!(str| rconfig.selection(headers[]));

    let mut all = try!(csv| rdr.byte_records().collect::<Result<Vec<_>, _>>());
    all.sort_by(|r1, r2| {
        // TODO: Numeric sorting. The tricky part, IMO, is figuring out
        // how to expose it in the CLI interface. Not sure of the right
        // answer at the moment.
        iter::order::cmp(sel.select(r1[]), sel.select(r2[]))
    });

    try!(csv| rconfig.write_headers(&mut rdr, &mut wtr));
    for r in all.into_iter() {
        try!(csv| wtr.write_bytes(r.into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}
