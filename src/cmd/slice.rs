use std::u64;

use docopt;

use types::{CliError, CsvConfig, Delimiter};
use util;

docopt!(Args, "
Returns the rows in the range specified (starting at 0, half-open interval).
The range does not include headers.

If the start of the range isn't specified, then the slice starts from the
first record in the CSV data.

If the end of the range isn't specified, then the slice continues to the last
record in the CSV data.

Usage:
    xcsv slice [options] [<input>]

slice options:
    -s, --start <arg>      The index of the record to slice from.
    -e, --end <arg>        The index of the record to slice to.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_start: Option<u64>, flag_end: Option<u64>)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());

    let start = args.flag_start.unwrap_or(0);
    let end = args.flag_end.unwrap_or(u64::MAX);
    if start > end {
        return Err(CliError::from_str(format!(
            "The end of the range ({:u}) must be greater than or\n\
             equal to the start of the range ({:u}).", end, start)));
    }

    let rconfig = CsvConfig::new(args.arg_input)
                            .delimiter(args.flag_delimiter)
                            .no_headers(args.flag_no_headers);
    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output).writer());
    try!(csv| rconfig.write_headers(&mut rdr, &mut wtr));

    let mut it = rdr.byte_records()
                    .skip(start as uint)
                    .take((end - start) as uint);
    for r in it {
        try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}
