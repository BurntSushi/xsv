use std::io;

use csv;
use docopt;
use tabwriter::TabWriter;

use types::{CliError, Delimiter, InputReader};
use util;

docopt!(Args, "
Prints the fields of the first row in the CSV data.

These names can be used in commands like 'select' to refer to columns in the
CSV data.

Usage:
    xcsv headers [options] [<input>]

headers options:
    -j, --just-names       Only show the header names (hide column index).

Common options:
    -h, --help             Display this message
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let mut rdr = csv::Decoder::from_reader(args.arg_input)
                               .separator(args.flag_delimiter.to_byte());
    let mut wtr: Box<Writer> =
        if args.flag_just_names {
            box io::stdout() as Box<Writer>
        } else {
            box TabWriter::new(io::stdout()) as Box<Writer>
        };
    for (i, header) in ctry!(rdr.headers_bytes()).iter().enumerate() {
        if !args.flag_just_names {
            ctry!(wtr.write_str(i.to_string().as_slice()));
            ctry!(wtr.write_u8(b'\t'));
        }
        ctry!(wtr.write(header.as_slice()));
        ctry!(wtr.write_u8(b'\n'));
    }
    ctry!(wtr.flush());
    Ok(())
}
