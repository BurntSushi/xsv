use csv;
use docopt;

use types::{CliError, Delimiter, InputReader, OutputWriter};
use util;

docopt!(Args, "
Usage:
    xcsv fmt [options] [<input>]

fmt options:
    -t, --out-delimiter <arg>  The field delimiter for writing CSV data.
                               [default: ,]
    --crlf                     Use '\\r\\n' line endings in the output.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());

    let mut rdr = csv_reader!(args);
    let mut wtr = csv::Writer::from_writer(args.flag_output)
                  .delimiter(args.flag_out_delimiter.to_byte())
                  .crlf(args.flag_crlf);
    csv_write_headers!(args, rdr, wtr);
    for r in rdr.byte_records() {
        ctry!(wtr.write_bytes(ctry!(r).into_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}
