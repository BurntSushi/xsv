use std::borrow::Cow;
use std::io::{self, Write};

use tabwriter::TabWriter;

use crate::CliResult;
use crate::config::{Config, Delimiter};
use crate::util;

static USAGE: &str = "
Prints flattened records such that fields are labeled separated by a new line.
This mode is particularly useful for viewing one record at a time. Each
record is separated by a special '#' character (on a line by itself), which
can be changed with the --separator flag.

There is also a condensed view (-c or --condense) that will shorten the
contents of each field to provide a summary view.

Usage:
    xsv flatten [options] [<input>]

flatten options:
    -c, --condense <arg>  Limits the length of each field to the value
                           specified. If the field is UTF-8 encoded, then
                           <arg> refers to the number of code points.
                           Otherwise, it refers to the number of bytes.
    -s, --separator <arg>  A string of characters to write after each record.
                           When non-empty, a new line is automatically
                           appended to the separator.
                           [default: #]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. When set, the name of each field
                           will be its index.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_condense: Option<usize>,
    flag_separator: String,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let mut rdr = rconfig.reader()?;
    let headers = rdr.byte_headers()?.clone();

    let mut wtr = TabWriter::new(io::stdout());
    let mut first = true;
    for r in rdr.byte_records() {
        if !first && !args.flag_separator.is_empty() {
            writeln!(&mut wtr, "{}", args.flag_separator)?;
        }
        first = false;
        let r = r?;
        for (i, (header, field)) in headers.iter().zip(&r).enumerate() {
            if rconfig.no_headers {
                write!(&mut wtr, "{}", i)?;
            } else {
                wtr.write_all(&header)?;
            }
            wtr.write_all(b"\t")?;
            wtr.write_all(&*util::condense(
                Cow::Borrowed(&*field), args.flag_condense))?;
            wtr.write_all(b"\n")?;
        }
    }
    wtr.flush()?;
    Ok(())
}
