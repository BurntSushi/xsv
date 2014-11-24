use std::io::{BufferedWriter, File};

use csv;

use CliResult;
use config::Delimiter;
use util;

static USAGE: &'static str = "
Creates an index of the given CSV data, which can make other operations like
slicing, splitting and gathering statistics much faster.

Note that this does not accept CSV data on stdin. You must give a file
path. The index is created at 'path/to/input.csv.idx'. The index will be
automatically used by commands that can benefit from it. If the original CSV
data changes after the index is made, commands that try to use it will result
in an error (you have to regenerate the index before it can be used again).

Usage:
    xsv index [options] <input>
    xsv index --help

index options:
    -o, --output <file>    Write index to <file> instead of <input>.idx.
                           Generally, this is not currently useful because
                           the only way to use an index is if it is specially
                           named <input>.idx.

Common options:
    -h, --help             Display this message
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[deriving(Decodable)]
struct Args {
    arg_input: String,
    flag_output: Option<String>,
    flag_delimiter: Delimiter,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let pcsv = Path::new(args.arg_input.as_slice());
    let pidx = match args.flag_output {
        None => util::idx_path(&pcsv),
        Some(p) => Path::new(p),
    };
    let rdr = csv::Reader::from_reader(try!(File::open(&pcsv)))
                          .delimiter(args.flag_delimiter.to_byte());
    let idx = BufferedWriter::new(try!(File::create(&pidx)));
    let _ = try!(csv::index::create(rdr, idx));
    Ok(())
}
