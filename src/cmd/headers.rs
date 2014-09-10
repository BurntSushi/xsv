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
    xcsv headers [options] [<input>...]

headers options:
    -j, --just-names       Only show the header names (hide column index).
                           This is automatically enabled if more than one
                           input is given.
    --intersect            Shows the intersection of all headers in all of
                           the inputs given.

Common options:
    -h, --help             Display this message
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Vec<InputReader>, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let mut args: Args = try!(util::get_args());
    let mut wtr: Box<Writer> =
        if args.flag_just_names {
            box io::stdout() as Box<Writer>
        } else {
            box TabWriter::new(io::stdout()) as Box<Writer>
        };

    if args.arg_input.is_empty() {
        args.arg_input.push(ctry!(InputReader::new(None))); // stdin
    }
    ctry!(util::at_most_one_stdin(args.arg_input.as_slice()));

    let num_inputs = args.arg_input.len();
    let mut headers = vec!();
    for inp in args.arg_input.move_iter() {
        let mut rdr = csv::Decoder::from_reader(inp)
                                   .separator(args.flag_delimiter.to_byte());
        for header in ctry!(rdr.headers_bytes()).move_iter() {
            if !args.flag_intersect || !headers.contains(&header) {
                headers.push(header);
            }
        }
    }
    for (i, header) in headers.move_iter().enumerate() {
        if num_inputs == 1 && !args.flag_just_names {
            ctry!(wtr.write_str(i.to_string().as_slice()));
            ctry!(wtr.write_u8(b'\t'));
        }
        ctry!(wtr.write(header.as_slice()));
        ctry!(wtr.write_u8(b'\n'));
    }
    ctry!(wtr.flush());
    Ok(())
}
