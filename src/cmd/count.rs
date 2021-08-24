use csv;

use CliResult;
use config::{Delimiter, Config};
use util;

static USAGE: &str = "
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

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let count =
        match conf.indexed()? {
            Some(idx) => idx.count(),
            None => {
                let mut rdr = conf.reader()?;
                let mut count = 0u64;
                let mut record = csv::ByteRecord::new();
                while rdr.read_byte_record(&mut record)? {
                    count += 1;
                }
                count
            }
        };
    Ok(println!("{}", count))
}
