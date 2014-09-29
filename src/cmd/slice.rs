use std::io;

use csv::index::Indexed;
use docopt;

use types::{CliError, CsvConfig, Delimiter};
use util;

docopt!(Args, "
Returns the rows in the range specified (starting at 0, half-open interval).
The range does not include headers.

If the start of the range isn't specified, then the slice starts from the
first record in the CSV data.

If the end of the range isn't specified, then the slice continues to the last
record in the CSV data.

Usage:
    xcsv slice [options] [<input>]

slice options:
    -s, --start <arg>      The index of the record to slice from.
    -e, --end <arg>        The index of the record to slice to.
    -l, --len <arg>        The length of the slice (can be used instead
                           of --end).
    -i, --index <arg>      Slice a single record (shortcut for -s N -l 1).

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_index: Option<u64>,
   flag_start: Option<u64>, flag_end: Option<u64>, flag_len: Option<u64>)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let rconfig = CsvConfig::new(args.arg_input.clone())
                            .delimiter(args.flag_delimiter)
                            .no_headers(args.flag_no_headers);
    match try!(io| rconfig.indexed()) {
        None => main_no_index(args, rconfig),
        Some(idxed) => main_index(args, rconfig, idxed),
    }
}

fn main_no_index(args: Args, rconfig: CsvConfig) -> Result<(), CliError> {
    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output.clone()).writer());
    try!(csv| rconfig.write_headers(&mut rdr, &mut wtr));

    let (start, end) = try!(str| args.range());
    let mut it = rdr.byte_records()
                    .skip(start as uint)
                    .take((end - start) as uint);
    for r in it {
        try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}

fn main_index(args: Args, rconfig: CsvConfig,
              mut idxed: Indexed<io::File, io::File>)
             -> Result<(), CliError> {
    let mut wtr = try!(io| CsvConfig::new(args.flag_output.clone()).writer());
    try!(csv| rconfig.write_headers(idxed.csv(), &mut wtr));

    let (start, end) = try!(str| args.range());
    try!(csv| idxed.seek(start));
    let mut it = idxed.csv().byte_records()
                            .take((end - start) as uint);
    for r in it {
        try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}

impl Args {
    fn range(&self) -> Result<(u64, u64), String> {
        util::range(self.flag_start, self.flag_end,
                    self.flag_len, self.flag_index)
    }
}
