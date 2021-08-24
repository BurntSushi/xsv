use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &str = "
Reverses rows of CSV data.

Useful for cases when there is no column that can be used for sorting in reverse order,
or when keys are not unique and order of rows with the same key needs to be preserved.

Note that this requires reading all of the CSV data into memory.

Usage:
    xsv reverse [options] [<input>]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be reversed with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;

    let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
    all.reverse();

    let mut wtr = Config::new(&args.flag_output).writer()?;
    rconfig.write_headers(&mut rdr, &mut wtr)?;
    for r in all.into_iter() {
        wtr.write_byte_record(&r)?;
    }
    Ok(wtr.flush()?)
}
