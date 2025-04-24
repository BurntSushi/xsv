use csv;

use CliResult;
use CliError;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Rename columns from CSV data efficiently.

This command allows you to select columns in CSV data, using the same
features as the select command, however in addition you can specify new
names for all the columns you have selected.

  rename the first and fourth columns as 'col1' and 'col2 respectively:
  $ xsv rename 1,4 col1 col2

  rename the columns 'Selected Username' and 'Grade' as 'id' and 'ps0'
  respectively:
  $ xsv rename 'Selected Username,Grade' id ps0

Usage:
    xsv rename [options] [--] <selection> <renamed>... [<input>]
    xsv rename --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
    arg_renamed: Vec<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_selection);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    
    if !rconfig.no_headers {
        if args.arg_renamed.len() != sel.len() {
            return Err(CliError::Other(format!(
                    "While renaming, selected {} columns, but only provided {} new names.",
                    sel.len(), args.arg_renamed.len())))
        } else {
            wtr.write_record(args.arg_renamed)?;

        }
    }
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {
        wtr.write_record(sel.iter().map(|&i| &record[i]))?;
    }
    wtr.flush()?;
    Ok(())
}
