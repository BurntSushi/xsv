use std::io;

use csv;
use docopt;

use {char_to_u8, get_args, CliError};

docopt!(Args, "
Usage:
    xcsv fmt [options] [<input>]

Options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter to use.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_delimiter: char)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(get_args());
    println!("Args: {}", args);

    let delimiter = ctry!(char_to_u8(args.flag_delimiter));

    let mut rdr = csv::Decoder::from_reader(::stdin_or_file(args.arg_input));
    let mut wtr = csv::Encoder::to_writer(io::stdout())
                               .separator(delimiter);
    for r in rdr.iter_bytes() {
        ctry!(wtr.record_bytes(ctry!(r).move_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}
