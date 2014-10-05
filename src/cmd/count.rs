use docopt;

use types::{CliError, Delimiter, CsvConfig};
use util;

docopt!(Args, "
Prints a count of the number of records in the CSV data.

Usage:
    xsv count [options] [<input>]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let conf = CsvConfig::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);

    let mut count =
        match try!(conf.indexed()) {
            Some(idx) => idx.count(),
            None => {
                let mut rdr = try!(io| conf.reader());
                let mut count = 0u64;
                while !rdr.done() {
                    loop {
                        match rdr.next_field() {
                            None => break,
                            Some(r) => { try!(csv| r); }
                        }
                    }
                    count += 1;
                }
                count
            }
        };
    if !args.flag_no_headers {
        count -= 1;
    }
    println!("{:u}", count);
    Ok(())
}
