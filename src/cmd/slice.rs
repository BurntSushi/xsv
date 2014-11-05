use std::io::File;

use csv::index::Indexed;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Returns the rows in the range specified (starting at 0, half-open interval).
The range does not include headers.

If the start of the range isn't specified, then the slice starts from the
first record in the CSV data.

If the end of the range isn't specified, then the slice continues to the last
record in the CSV data.

Usage:
    xsv slice [options] [<input>]

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
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    flag_start: Option<uint>,
    flag_end: Option<uint>,
    flag_len: Option<uint>,
    flag_index: Option<uint>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    match try!(args.rconfig().indexed()) {
        None => args.no_index(),
        Some(idxed) => args.with_index(idxed),
    }
}

impl Args {
    fn no_index(&self) -> CliResult<()> {
        let mut rdr = try!(self.rconfig().reader());
        let mut wtr = try!(self.wconfig().writer());
        try!(self.rconfig().write_headers(&mut rdr, &mut wtr));

        let (start, end) = try!(self.range());
        let mut it = rdr.byte_records().skip(start).take(end - start);
        for r in it {
            try!(wtr.write_bytes(try!(r).into_iter()));
        }
        try!(wtr.flush());
        Ok(())
    }

    fn with_index(&self, mut idx: Indexed<File, File>) -> CliResult<()> {
        let mut wtr = try!(self.wconfig().writer());
        try!(self.rconfig().write_headers(idx.csv(), &mut wtr));

        let (start, end) = try!(self.range());
        try!(idx.seek(start as u64));
        let mut it = idx.csv().byte_records().take(end - start);
        for r in it {
            try!(wtr.write_bytes(try!(r).into_iter()));
        }
        try!(wtr.flush());
        Ok(())
    }

    fn range(&self) -> Result<(uint, uint), String> {
        util::range(self.flag_start, self.flag_end,
                    self.flag_len, self.flag_index)
    }

    fn rconfig(&self) -> Config {
        Config::new(self.arg_input.clone())
               .delimiter(self.flag_delimiter)
               .no_headers(self.flag_no_headers)
    }

    fn wconfig(&self) -> Config {
        Config::new(self.flag_output.clone())
    }
}
