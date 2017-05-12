use csv;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Select columns from CSV data efficiently.

This command lets you manipulate the columns in CSV data. You can re-order
them, duplicate them or drop them. Columns can be referenced by index or by
name if there is a header row (duplicate column names can be disambiguated with
more indexing). Finally, column ranges can be specified.

  Select the first and fourth columns:
  $ xsv select 1,4

  Select the first 4 columns (by index and by name):
  $ xsv select 1-4
  $ xsv select Header1-Header4

  Ignore the first 2 columns (by range and by omission):
  $ xsv select 3-
  $ xsv select '!1-2'

  Select the third column named 'Foo':
  $ xsv select 'Foo[2]'

  Re-order and duplicate columns arbitrarily:
  $ xsv select 3-1,Header3-Header1,Header1,Foo[2],Header1

  Quote column names that conflict with selector syntax:
  $ xsv select '\"Date - Opening\",\"Date - Actual Closing\"'

Usage:
    xsv select [options] [--] <selection> [<input>]
    xsv select --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
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
        wtr.write_record(sel.iter().map(|&i| &headers[i]))?;
    }
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {
        wtr.write_record(sel.iter().map(|&i| &record[i]))?;
    }
    wtr.flush()?;
    Ok(())
}
