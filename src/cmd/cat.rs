use csv;
use docopt;

use types::{CliError, CsvConfig, Delimiter};
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
    xcsv cat --help

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
", arg_input: Vec<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output).writer());
    let configs = try!(str| util::many_configs(args.arg_input.as_slice(),
                                              args.flag_delimiter,
                                              args.flag_no_headers));

    if args.cmd_rows {
        for (i, conf) in configs.into_iter().enumerate() {
            let mut rdr = try!(io| conf.reader());
            if i == 0 {
                try!(csv| conf.write_headers(&mut rdr, &mut wtr));
            }
            for r in rdr.byte_records() {
                try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
            }
        }
    } else if args.cmd_columns {
        let mut rdrs = try!(io| configs.into_iter()
                                      .map(|conf| conf.no_headers(true)
                                                      .reader())
                                      .collect::<Result<Vec<_>, _>>());

        // Find the lengths of each record. If a length varies, then an error
        // will occur so we can rely on the first length being the correct one.
        let mut lengths = vec!();
        for rdr in rdrs.iter_mut() {
            lengths.push(try!(csv| rdr.byte_headers()).len());
        }

        let mut iters: Vec<_> = rdrs.iter_mut()
                                    .map(|rdr| rdr.byte_records())
                                    .collect();
        'OUTER: loop {
            let mut records: Vec<Vec<csv::ByteString>> = vec!();
            let mut num_done = 0;
            for (iter, &len) in iters.iter_mut().zip(lengths.iter()) {
                match iter.next() {
                    None => {
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
                    Some(Err(err)) => return Err(CliError::from_csv(err)),
                    Some(Ok(next)) => records.push(next),
                }
            }
            // Only needed when `--pad` is set.
            if num_done >= iters.len() {
                break 'OUTER;
            }
            let row = records.as_slice().concat_vec();
            try!(csv| wtr.write_bytes(row.into_iter()));
        }
    } else {
        unreachable!();
    }
    try!(csv| wtr.flush());
    Ok(())
}
