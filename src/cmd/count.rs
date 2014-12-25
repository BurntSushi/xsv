use csv::NextField;

use CliResult;
use config::{Delimiter, Config};
use util;

static USAGE: &'static str = "
Prints a count of the number of records in the CSV data.

Note that the count will not include the header row (unless --no-headers is
given).

Usage:
    xsv count [options] [<input>]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be included in
                           the count.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[deriving(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let conf = Config::new(&args.arg_input)
                      .delimiter(args.flag_delimiter)
                      .no_headers(args.flag_no_headers);

    let count =
        match try!(conf.indexed()) {
            Some(idx) => idx.count(),
            None => {
                let mut rdr = try!(conf.reader());
                let mut count = 0u64;
                while !rdr.done() {
                    loop {
                        match rdr.next_field() {
                            NextField::EndOfCsv => break,
                            NextField::EndOfRecord => { count += 1; break; }
                            NextField::Error(err) => return fail!(err),
                            NextField::Data(_) => {}
                        }
                    }
                }
                if !conf.no_headers && count > 0 {
                    count - 1
                } else {
                    count
                }
            }
        };
    Ok(println!("{}", count))
}
