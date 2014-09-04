use csv;
use docopt;

use {get_args, CliError};
use types::{Delimiter, InputReader, OutputWriter};

docopt!(Args, "
Usage:
    xcsv fmt [options] [<input>]

Options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter to use.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(get_args());

    let mut rdr = csv::Decoder::from_reader(args.arg_input);
    let mut wtr = csv::Encoder::to_writer(args.flag_output)
                                .separator(args.flag_delimiter.to_byte());
    for r in rdr.iter_bytes() {
        ctry!(wtr.record_bytes(ctry!(r).move_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}
