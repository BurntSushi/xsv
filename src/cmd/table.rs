use std::borrow::Cow;

use csv;
use tabwriter::TabWriter;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &str = "
Outputs CSV data as a table with columns in alignment.

This will not work well if the CSV data contains large fields.

Note that formatting a table requires buffering all CSV data into memory.
Therefore, you should use the 'sample' or 'slice' command to trim down large
CSV data before formatting it with this command.

Usage:
    xsv table [options] [<input>]

table options:
    -w, --width <arg>      The minimum width of each column.
                           [default: 2]
    -p, --pad <arg>        The minimum number of spaces between each column.
                           [default: 2]
    -c, --condense <arg>  Limits the length of each field to the value
                           specified. If the field is UTF-8 encoded, then
                           <arg> refers to the number of code points.
                           Otherwise, it refers to the number of bytes.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_width: usize,
    flag_pad: usize,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
    flag_condense: Option<usize>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(true);
    let wconfig = Config::new(&args.flag_output)
        .delimiter(Some(Delimiter(b'\t')));

    let tw = TabWriter::new(wconfig.io_writer()?)
        .minwidth(args.flag_width)
        .padding(args.flag_pad);
    let mut wtr = wconfig.from_writer(tw);
    let mut rdr = rconfig.reader()?;

    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {
        wtr.write_record(record.iter().map(|f| {
            util::condense(Cow::Borrowed(f), args.flag_condense)
        }))?;
    }
    wtr.flush()?;
    Ok(())
}
