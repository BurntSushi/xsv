use docopt;

use {get_args, CliError};
use types::{Delimiter, InputReader, OutputWriter};
use util;

docopt!(Args, "
Usage:
    xcsv fmt [options] [<input>]

fmt options:
    -t, --out-delimiter <arg>  The delimiter to use to format the CSV data.
                               [default: ,]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names.
    -d, --delimiter <arg>  The field delimiter to use.
                           Must be a single character. [default: ,]
    -f, --flexible         When set, records may be of varying length.
                           When not set, an error will occur when xcsv
                           encounters records of varying length in the
                           same CSV file.
    -c, --crlf             Use '\\r\\n' line endings in the output.
", arg_input: InputReader, flag_output: OutputWriter,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(get_args());

    let mut rdr = csv_reader!(args);
    let mut wtr = csv_writer!(args).separator(args.flag_out_delimiter.to_byte());
    if !args.flag_no_headers {
        ctry!(wtr.record_bytes(ctry!(rdr.headers_bytes()).move_iter()));
    }
    for r in rdr.iter_bytes() {
        ctry!(wtr.record_bytes(ctry!(r).move_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}
