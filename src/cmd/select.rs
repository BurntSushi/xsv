use docopt;

use types::{CliError, CsvConfig, Delimiter, SelectColumns};
use util;

docopt!(Args, "
Usage:
    xsv select [options] <selection> [<input>]
    xsv select --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, arg_selection: SelectColumns)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());

    let rconfig = CsvConfig::new(args.arg_input)
                            .delimiter(args.flag_delimiter)
                            .no_headers(args.flag_no_headers);

    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output).writer());

    let headers = try!(csv| rdr.byte_headers());
    let sel = try!(str| args.arg_selection.selection(&rconfig, headers[]));

    if !args.flag_no_headers {
        try!(csv| wtr.write_bytes(sel.select(headers[])));
    }
    for r in rdr.byte_records() {
        // TODO: I don't think we can do any better here. Since selection
        // operates on indices, some kind of allocation is probably required.
        try!(csv| wtr.write_bytes(sel.select(try!(csv| r)[])))
    }
    try!(csv| wtr.flush());
    Ok(())
}
