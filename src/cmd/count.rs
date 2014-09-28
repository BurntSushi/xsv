use docopt;

use types::{CliError, Delimiter, CsvConfig};
use util;

docopt!(Args, "
Prints a count of the number of records in the CSV data.

Usage:
    xcsv count [options] [<input>]

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

    match try!(io| conf.index_files()) {
        None => {
            let mut rdr = try!(io| conf.reader());
            let mut count = 0u;
            while !rdr.done() {
                for field in rdr { let _ = try!(csv| field); }
                count += 1;
            }
            println!("{:u}", count);
        }
        Some((_, mut idx_file)) => {
            let stat = try!(io| idx_file.stat());
            assert_eq!(stat.size % 8, 0);
            println!("{:u}", stat.size / 8);
        }
    }
    Ok(())
}
