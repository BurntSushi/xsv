use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use csv_index::RandomAccessSimple;

use config::{Config, Delimiter};
use util;
use CliResult;

static USAGE: &str = "
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
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: String,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let pidx = match args.flag_output {
        None => util::idx_path(Path::new(&args.arg_input)),
        Some(p) => PathBuf::from(&p),
    };

    let rconfig = Config::new(&Some(args.arg_input)).delimiter(args.flag_delimiter);
    let mut rdr = rconfig.reader_file()?;
    let mut wtr = io::BufWriter::new(fs::File::create(pidx)?);
    RandomAccessSimple::create(&mut rdr, &mut wtr)?;
    Ok(())
}
