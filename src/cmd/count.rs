use CliResult;
use config::{Delimiter, Config};
use util;

static USAGE: &'static str = "
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
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let conf = Config::new(args.arg_input)
                      .delimiter(args.flag_delimiter)
                      .no_headers(args.flag_no_headers);

    let count =
        match try!(conf.indexed()) {
            Some(idx) => idx.count(),
            None => {
                let mut rdr = try!(conf.reader());
                let mut count = 0u64;
                let mut seen_field = false;
                while !rdr.done() {
                    loop {
                        match rdr.next_field() {
                            None => break,
                            Some(r) => { seen_field = true; try!(r); }
                        }
                    }
                    if seen_field { count += 1; }
                }
                if !args.flag_no_headers && count > 0 {
                    count - 1
                } else {
                    count
                }
            }
        };
    println!("{}", count);
    Ok(())
}
