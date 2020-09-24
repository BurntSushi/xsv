use csv;

use CliResult;
use config::{Delimiter, Config};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Explodes a row into multiple ones by splitting a column value based on the
given separator.

For instance the following CSV:

name,colors
John,blue|yellow
Mary,red

Can be exploded on the \"colors\" <column> based on the \"|\" <separator> to:

name,colors
John,blue
John,yellow
Mary,red

Usage:
    xsv explode [options] <column> <separator> [<input>]

explode options:
    -r, --rename <name>    New name for the exploded column.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_column: SelectColumns,
    arg_separator: String,
    arg_input: Option<String>,
    flag_rename: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn replace_column_value(record: &csv::StringRecord, column_index: usize, new_value: &String)
                           -> csv::StringRecord {
    record
        .into_iter()
        .enumerate()
        .map(|(i, v)| if i == column_index { new_value } else { v })
        .collect()
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let column_index = *sel.iter().next().unwrap();

    let mut headers = rdr.headers()?.clone();

    if let Some(new_name) = args.flag_rename {
        headers = replace_column_value(&headers, column_index, &new_name);
    }

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {
        for val in record[column_index].split(&args.arg_separator) {
            let new_record = replace_column_value(&record, column_index, &val.to_owned());
            wtr.write_record(&new_record)?;
        }
    }

    Ok(wtr.flush()?)
}
