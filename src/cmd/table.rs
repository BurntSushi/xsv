use tabwriter::TabWriter;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Outputs CSV data as a table with columns in alignment.

This will not work well if the CSV data contains large fields.

Note that formatting a table requires buffering all CSV data into memory.
Therefore, you should use the 'slice' command to trim down large CSV data
before formatting it with this command.

Usage:
    xsv table [options] [<input>]

table options:
    -w, --width <arg>      The minimum width of each column.
                           [default: 2]
    -p, --pad <arg>        The minimum number of spaces between each column.
                           [default: 2]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    flag_width: uint,
    flag_pad: uint,
    flag_output: Option<String>,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(true);
    let wconfig = Config::new(&args.flag_output).delimiter(Delimiter(b'\t'));

    let tw = TabWriter::new(try!(wconfig.io_writer()))
                       .minwidth(args.flag_width)
                       .padding(args.flag_pad);
    let mut wtr = wconfig.from_writer(tw);
    let mut rdr = try!(rconfig.reader());

    for r in rdr.byte_records() {
        try!(wtr.write_bytes(try!(r).into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
