use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Usage:
    xsv fmt [options] [<input>]

fmt options:
    -t, --out-delimiter <arg>  The field delimiter for writing CSV data.
                               [default: ,]
    --crlf                     Use '\\r\\n' line endings in the output.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    flag_out_delimiter: Delimiter,
    flag_crlf: bool,
    flag_output: Option<String>,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(true);
    let wconfig = Config::new(args.flag_output)
                         .delimiter(args.flag_out_delimiter)
                         .crlf(args.flag_crlf);
    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(wconfig.writer());

    for r in rdr.byte_records() {
        try!(wtr.write_bytes(try!(r).into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
