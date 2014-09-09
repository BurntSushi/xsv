use std::uint;

use csv;
use docopt;

use types::{CliError, Delimiter, InputReader, OutputWriter};
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
    --one                  The range is interpreted as a closed interval
                           starting at 1. e.g., [1, 4] is equivalent to
                           [0, 4) when this flag is enabled.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter, flag_delimiter: Delimiter,
   flag_start: Option<uint>, flag_end: Option<uint>)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let (start, end) =
        try!(if args.flag_one { inc_range(&args) } else { zero_range(&args) });
    if start > end {
        return Err(CliError::from_str(format!(
            "The end of the range ({:u}) must be greater than or\n\
             equal to the start of the range ({:u}).", end, start)));
    }

    let mut rdr = csv_reader!(args);
    let mut wtr = csv::Encoder::to_writer(args.flag_output);
    csv_write_headers!(args, rdr, wtr);
    for r in rdr.iter_bytes().skip(start).take(end - start) {
        ctry!(wtr.record_bytes(ctry!(r).move_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}

fn zero_range(args: &Args) -> Result<(uint, uint), CliError> {
    Ok((args.flag_start.unwrap_or(0), args.flag_end.unwrap_or(uint::MAX)))
}

fn inc_range(args: &Args) -> Result<(uint, uint), CliError> {
    let start = args.flag_start.unwrap_or(1);
    let end = args.flag_end.unwrap_or(::std::uint::MAX);
    match (start, end) {
        (0, _) | (_, 0) => {
            Err(CliError::from_str("The first record starts at 1 (not 0)."))
        }
        _ => Ok((start-1, end)),
    }
}
