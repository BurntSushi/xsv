use docopt;

use CliResult;
use config::{Config, Delimiter};
use util;

docopt!(Args, "
Usage:
    xsv fmt [options] [<input>]

fmt options:
    -t, --out-delimiter <arg>  The field delimiter for writing CSV data.
                               [default: ,]
    --crlf                     Use '\\r\\n' line endings in the output.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_out_delimiter: Delimiter)

pub fn main(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(argv));

    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);
    let wconfig = Config::new(args.flag_output)
                         .delimiter(args.flag_out_delimiter)
                         .crlf(args.flag_crlf);
    let mut rdr = try!(io| rconfig.reader());
    let mut wtr = try!(io| wconfig.writer());

    try!(csv| wconfig.write_headers(&mut rdr, &mut wtr));
    for r in rdr.byte_records() {
        try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}
