use std::borrow::Cow;
use std::io::{self, Write};

use tabwriter::TabWriter;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
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

#[derive(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    flag_condense: Option<usize>,
    flag_separator: String,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);
    let mut rdr = try!(rconfig.reader());
    let headers = try!(rdr.byte_headers());

    let mut wtr = TabWriter::new(io::stdout());
    let mut first = true;
    for r in rdr.byte_records() {
        if !first && !args.flag_separator.is_empty() {
            try!(writeln!(&mut wtr, "{}", args.flag_separator));
        }
        first = false;
        let r = try!(r).into_iter();
        for (i, (header, field)) in headers.iter().zip(r).enumerate() {
            if rconfig.no_headers {
                try!(write!(&mut wtr, "{}", i));
            } else {
                try!(wtr.write_all(&header));
            }
            try!(wtr.write_all(b"\t"));
            try!(wtr.write_all(&*util::condense(Cow::Borrowed(&*field),
                                                args.flag_condense)));
            try!(wtr.write_all(b"\n"));
        }
    }
    try!(wtr.flush());
    Ok(())
}
