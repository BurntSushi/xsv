use csv;
use docopt;

use types::{CliError, Delimiter, InputReader, OutputWriter};
use util;

docopt!(Args, "
Concatenates CSV data by column or by row.

When concatenating by column, the columns will be written in the same order
as the inputs given. The number of rows in the result is always equivalent to
to the minimum number of rows across all given CSV data. (This behavior can
be reversed with the '--pad' flag.)

When concatenating by row, all CSV data must have the same number of columns.
If you need to rearrange the columns or fix the lengths of records, use the
'slice' or 'fixlengths' commands. Also, only the headers of the *first* CSV
data given are used. Headers in subsequent inputs are ignored. (This behavior
can be disabled with --no-headers.)

Usage:
    xcsv cat (rows | columns) [options] [<input>...]

cat options:
    -p, --pad              When concatenating columns, this flag will cause
                           all records to appear. It will pad each row if
                           other CSV data isn't long enough.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Note that this has no effect when
                           concatenating columns.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Vec<InputReader>, flag_output: OutputWriter,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter,
   flag_length: Option<uint>)

pub fn main() -> Result<(), CliError> {
    let mut args: Args = try!(util::get_args());
    let mut wtr = csv::Encoder::to_writer(args.flag_output.by_ref());
    if args.arg_input.is_empty() {
        args.arg_input.push(ctry!(InputReader::new(None))); // stdin
    }
    ctry!(util::at_most_one_stdin(args.arg_input.as_slice()));

    if args.cmd_rows {
        for (i, inp) in args.arg_input.move_iter().enumerate() {
            let mut rdr = csv_reader!(args, inp);
            if !args.flag_no_headers && i == 0 {
                csv_write_headers!(args, rdr, wtr);
            }
            for r in rdr.iter_bytes() {
                ctry!(wtr.record_bytes(ctry!(r).move_iter()));
            }
        }
    } else if args.cmd_columns {
        let delim = args.flag_delimiter.to_byte();
        let mut rdrs: Vec<csv::Decoder<InputReader>> =
            args.arg_input
                .move_iter().map(|inp| column_reader(inp, delim)).collect();
        // Find the lengths of each record. If a length varies, then an error
        // will occur so we can rely on the first length being the correct one.
        let mut lengths = vec!();
        for rdr in rdrs.mut_iter() {
            lengths.push(ctry!(rdr.headers_bytes()).len());
        }
        'OUTER: loop {
            let mut records: Vec<Vec<csv::ByteString>> = vec!();
            let mut num_done = 0;
            for (rdr, &len) in rdrs.mut_iter().zip(lengths.iter()) {
                match rdr.record_bytes() {
                    Err(ref e) if e.is_eof() => {
                        num_done += 1;
                        if args.flag_pad {
                            // This can probably be optimized by pre-allocating.
                            // It would avoid the intermediate `Vec`.
                            let pad = Vec::from_elem(len, util::empty_field());
                            records.push(pad);
                        } else {
                            break 'OUTER;
                        }
                    }
                    err @ Err(_) => { ctry!(err); }
                    Ok(next) => records.push(next),
                }
            }
            // Only needed when `--pad` is set.
            if num_done >= rdrs.len() {
                break 'OUTER;
            }
            let row = records.as_slice().concat_vec();
            ctry!(wtr.record_bytes(row.move_iter()));
        }
    } else {
        unreachable!();
    }
    ctry!(wtr.flush());
    Ok(())
}

fn column_reader<R: Reader>(rdr: R, delim: u8) -> csv::Decoder<R> {
    // We always set no_headers here because there's no need to distinguish.
    csv::Decoder::from_reader(rdr).separator(delim).no_headers()
}
