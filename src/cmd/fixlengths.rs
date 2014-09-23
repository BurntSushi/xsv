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

Alternatively, if --length is set, then all records are forced to that length.
This requires a single pass and can be done with stdin.

Usage:
    xcsv fixlengths [options] [<input>]

fixlengths options:
    -l, --length <arg>     Forcefully set the length of each record. If a
                           record is not the size given, then it is truncated
                           or expanded as appropriate.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter,
   flag_length: Option<uint>)

pub fn main() -> Result<(), CliError> {
    let mut args: Args = try!(util::get_args());
    let length = match args.flag_length {
        Some(length) => {
            if length == 0 {
                return Err(CliError::from_str(
                    "Length must be greater than 0."));
            }
            length
        }
        None => {
            if !args.arg_input.is_seekable() {
                return Err(CliError::from_str(
                    "<stdin> cannot be used in this command. \
                     Please specify a file path."));
            }
            let maxlen = reader(args.arg_input.by_ref(), &args.flag_delimiter)
                         .byte_records()
                         .map(|r|r.unwrap_or(vec!()).len())
                         .max().unwrap_or(0);
            ctry!(ctry!(args.arg_input.file_ref()).seek(0, io::SeekSet));
            maxlen
        }
    };

    let mut rdr = reader(args.arg_input.by_ref(), &args.flag_delimiter);
    let mut wtr = csv::Writer::from_writer(args.flag_output);
    for r in rdr.byte_records() {
        let mut r = ctry!(r);
        if length >= r.len() {
            for i in range(r.len(), length) {
                r.push(util::empty_field());
            }
        } else {
            r.truncate(length);
        }
        ctry!(wtr.write_bytes(r.into_iter()));
    }
    ctry!(wtr.flush());
    Ok(())
}

fn reader<R: Reader>(rdr: R, delim: &Delimiter) -> csv::Reader<R> {
    csv::Reader::from_reader(rdr)
                .delimiter(delim.to_byte())
                .no_headers()
                .flexible(true)
}
