use docopt;

use types::{CliError, Delimiter, InputReader};
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
", arg_input: InputReader, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    let mut rdr = csv_reader!(args);
    println!("{:u}", rdr.iter_bytes().count());
    Ok(())
}
