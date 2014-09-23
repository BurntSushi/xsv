use csv;
use docopt;
use tabwriter::TabWriter;

use types::{CliError, Delimiter, InputReader, OutputWriter};
use util;

docopt!(Args, "
Outputs CSV data as a table with columns in alignment.

This will not work well if the CSV data contains large fields.

Note that formatting a table requires buffering all CSV data into memory.
Therefore, you should use the 'slice' command to trim down large CSV data
before formatting it with this command.

Usage:
    xcsv table [options] [<input>]

table options:
    -w, --width <arg>      The minimum width of each column.
                           [default: 2]
    -p, --pad <arg>        The minimum number of spaces between each column.
                           [default: 2]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter, flag_delimiter: Delimiter,
   flag_width: uint, flag_pad: uint)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let mut rdr = csv_reader!(args);

    let tw = TabWriter::new(args.flag_output)
                           .minwidth(args.flag_width)
                           .padding(args.flag_pad);
    let mut wtr = csv::Writer::from_writer(tw).delimiter(b'\t');
    csv_write_headers!(args, rdr, wtr);
    for r in rdr.byte_records() {
        ctry!(wtr.write_bytes(ctry!(r).into_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}
