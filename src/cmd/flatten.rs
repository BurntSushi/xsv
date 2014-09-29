use std::io;

use docopt;
use tabwriter::TabWriter;

use types::{CliError, CsvConfig, Delimiter};
use util;

docopt!(Args, "
Prints flattened records such that fields are labeled separated
by a new line. This mode is particularly useful for viewing one
record at a time.

There is also a condensed view (-c or --condensed) that will shorten
the contents of each field to provide a summary view.

Usage:
    xcsv flatten [options] [<input>]

flatten options:
    -c, --condensed <arg>  Limits the length (in bytes) of each field to the
                           value specified.
    -s, --separator <arg>  A string of characters to write after each record.
                           When non-empty, a new line is automatically
                           appended to the separator.
                           [default: #]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let rconfig = CsvConfig::new(args.arg_input.clone())
                            .delimiter(args.flag_delimiter)
                            .no_headers(args.flag_no_headers);
    let mut rdr = try!(io| rconfig.reader());
    let headers = try!(csv| rdr.byte_headers());

    let mut wtr: Box<Writer> =
        if false {
            box io::stdout() as Box<Writer>
        } else {
            box TabWriter::new(io::stdout()) as Box<Writer>
        };
    let mut first = true;
    for r in rdr.byte_records() {
        if !first && !args.flag_separator.is_empty() {
            try!(io| wtr.write_str(args.flag_separator.as_slice()));
            try!(io| wtr.write_u8(b'\n'));
        }
        first = false;
        for (header, field) in headers.iter().zip(try!(csv| r).into_iter()) {
            try!(io| wtr.write(header[]));
            try!(io| wtr.write_u8(b'\t'));
            try!(io| wtr.write(field[]));
            try!(io| wtr.write_u8(b'\n'));
        }
    }
    try!(io| wtr.flush());
    Ok(())
}
