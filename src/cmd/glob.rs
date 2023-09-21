use csv;
use glob::glob;

use config::Config;
use util;
use CliResult;

static USAGE: &str = "
Create a CSV file from the matches of a glob pattern. Matches will
be stored in a \"path\" column.

Usage:
    xsv glob [options] <pattern>
    xsv glob --help

glob options:
    -a, --absolute         Yield absolute paths.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
";

#[derive(Deserialize)]
struct Args {
    arg_pattern: String,
    flag_absolute: bool,
    flag_output: Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut record = csv::ByteRecord::new();
    record.push_field(b"path");

    wtr.write_byte_record(&record)?;

    for entry in glob(&args.arg_pattern)? {
        let mut entry = entry?;

        if args.flag_absolute {
            entry = entry.canonicalize()?;
        }

        let path = entry.to_str().unwrap();
        record.clear();
        record.push_field(path.as_bytes());

        wtr.write_byte_record(&record)?;
    }

    Ok(wtr.flush()?)
}
