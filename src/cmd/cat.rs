use std::error::FromError;

use csv;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Concatenates CSV data by column or by row.

When concatenating by column, the columns will be written in the same order as
the inputs given. The number of rows in the result is always equivalent to to
the minimum number of rows across all given CSV data. (This behavior can be
reversed with the '--pad' flag.)

When concatenating by row, all CSV data must have the same number of columns.
If you need to rearrange the columns or fix the lengths of records, use the
'select' or 'fixlengths' commands. Also, only the headers of the *first* CSV
data given are used. Headers in subsequent inputs are ignored. (This behavior
can be disabled with --no-headers.)

Usage:
    xsv cat rows    [options] [<input>...]
    xsv cat columns [options] [<input>...]
    xsv cat --help

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
                           Must be a single character. (default: ,)
";

#[deriving(RustcDecodable)]
struct Args {
    cmd_rows: bool,
    cmd_columns: bool,
    arg_input: Vec<String>,
    flag_pad: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    if args.cmd_rows {
        args.cat_rows()
    } else if args.cmd_columns {
        args.cat_columns()
    } else {
        unreachable!();
    }
}

impl Args {
    fn configs(&self) -> CliResult<Vec<Config>> {
        util::many_configs(self.arg_input.as_slice(),
                           self.flag_delimiter,
                           self.flag_no_headers)
             .map_err(FromError::from_error)
    }

    fn cat_rows(&self) -> CliResult<()> {
        let mut wtr = try!(Config::new(&self.flag_output).writer());
        for (i, conf) in try!(self.configs()).into_iter().enumerate() {
            let mut rdr = try!(conf.reader());
            if i == 0 {
                try!(conf.write_headers(&mut rdr, &mut wtr));
            }
            for r in rdr.byte_records() {
                try!(wtr.write(try!(r).into_iter()));
            }
        }
        wtr.flush().map_err(FromError::from_error)
    }

    fn cat_columns(&self) -> CliResult<()> {
        let mut wtr = try!(Config::new(&self.flag_output).writer());
        let mut rdrs = try!(try!(self.configs())
                                .into_iter()
                                .map(|conf| conf.no_headers(true).reader())
                                .collect::<Result<Vec<_>, _>>());

        // Find the lengths of each record. If a length varies, then an error
        // will occur so we can rely on the first length being the correct one.
        let mut lengths = vec!();
        for rdr in rdrs.iter_mut() {
            lengths.push(try!(rdr.byte_headers()).len());
        }

        let mut iters = rdrs.iter_mut()
                            .map(|rdr| rdr.byte_records())
                            .collect::<Vec<_>>();
        'OUTER: loop {
            let mut records: Vec<Vec<csv::ByteString>> = vec!();
            let mut num_done = 0;
            for (iter, &len) in iters.iter_mut().zip(lengths.iter()) {
                match iter.next() {
                    None => {
                        num_done += 1;
                        if self.flag_pad {
                            // This can probably be optimized by
                            // pre-allocating. It would avoid the intermediate
                            // `Vec`.
                            let pad = Vec::from_elem(len, util::empty_field());
                            records.push(pad);
                        } else {
                            break 'OUTER;
                        }
                    }
                    Some(Err(err)) => return fail!(err),
                    Some(Ok(next)) => records.push(next),
                }
            }
            // Only needed when `--pad` is set.
            // When not set, the OUTER loop breaks when the shortest iterator
            // is exhausted.
            if num_done >= iters.len() {
                break 'OUTER;
            }
            let row = records.as_slice().concat_vec();
            try!(wtr.write(row.into_iter()));
        }
        wtr.flush().map_err(FromError::from_error)
    }
}
