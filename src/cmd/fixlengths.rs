use std::io;

use csv;
use docopt;

use types::{CliError, Delimiter, InputReader, OutputWriter};
use util;

docopt!(Args, "
Transforms CSV data so that all records have the same length. The length is the
length of the longest record in the data. Records with smaller lengths are
padded with empty fields.

This requires two complete scans of the CSV data: one for determining the
record size and one for the actual transform. Because of this, the input
given must be a file and not stdin.

Usage:
    xcsv fixlengths [options] <input>

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let mut args: Args = try!(util::get_args());
    if !args.arg_input.is_seekable() {
        return Err(CliError::from_str(
            "<stdin> cannot be used in this command. \
             Please specify a file path."));
    }

    let maxlen = reader(args.arg_input.by_ref(), &args.flag_delimiter)
                 .iter_bytes()
                 .map(|r|r.unwrap_or(vec!()).len())
                 .max().unwrap_or(0);

    ctry!(ctry!(args.arg_input.file_ref()).seek(0, io::SeekSet));

    let mut rdr = reader(args.arg_input.by_ref(), &args.flag_delimiter);
    let mut wtr = csv::Encoder::to_writer(args.flag_output);
    for r in rdr.iter_bytes() {
        let mut r = ctry!(r);
        for i in range(r.len(), maxlen) {
            r.push(csv::ByteString::from_bytes::<Vec<u8>>(vec!()));
        }
        ctry!(wtr.record_bytes(r.move_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}

fn reader<R: Reader>(rdr: R, delim: &Delimiter) -> csv::Decoder<R> {
    csv::Decoder::from_reader(rdr)
                 .separator(delim.to_byte())
                 .no_headers()
                 .enforce_same_length(false)
}
