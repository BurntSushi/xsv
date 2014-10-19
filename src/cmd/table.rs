use docopt;
use tabwriter::TabWriter;

use CliResult;
use config::{Config, Delimiter};
use util;

docopt!(Args, "
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
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_width: uint, flag_pad: uint)

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(argv));

    let rconfig = Config::new(args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);
    let wconfig = Config::new(args.flag_output).delimiter(Delimiter(b'\t'));

    let tw = TabWriter::new(try!(io| wconfig.io_writer()))
                       .minwidth(args.flag_width)
                       .padding(args.flag_pad);
    let mut wtr = wconfig.from_writer(tw);
    let mut rdr = try!(io| rconfig.reader());

    try!(csv| rconfig.write_headers(&mut rdr, &mut wtr));
    for r in rdr.byte_records() {
        try!(csv| wtr.write_bytes(try!(csv| r).into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}
