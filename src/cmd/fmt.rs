use csv;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Formats CSV data with a custom delimiter or CRLF line endings.

Generally, all commands in xsv output CSV data in a default format, which is
the same as the default format for reading CSV data. This makes it easy to
pipe multiple xsv commands together. However, you may want the final result to
have a specific delimiter or record separator, and this is where 'xsv fmt' is
useful.

Usage:
    xsv fmt [options] [<input>]

fmt options:
    -t, --out-delimiter <arg>  The field delimiter for writing CSV data.
                               (default: ,)
    --crlf                     Use '\\r\\n' line endings in the output.
    --ascii                    Use ASCII field and record separators.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[deriving(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    flag_out_delimiter: Option<Delimiter>,
    flag_crlf: bool,
    flag_ascii: bool,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(true);
    let wconfig = Config::new(&args.flag_output)
                         .delimiter(args.flag_out_delimiter)
                         .crlf(args.flag_crlf);
    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(wconfig.writer());

    if args.flag_ascii {
        wtr = wtr.delimiter(b'\x1f')
                 .record_terminator(csv::RecordTerminator::Any(b'\x1e'));
    }
    for r in rdr.byte_records() {
        try!(wtr.write(try!(r).into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
