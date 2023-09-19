use std::collections::HashMap;

use csv;

use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &str = "
Pseudonymise the value of the given column by replacing them by an
incremental identifier.

Usage:
    xsv pseudo [options] <column> [<input>]
    xsv pseudo --help

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
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn replace_column_value(
    record: &csv::StringRecord,
    column_index: usize,
    new_value: &String,
) -> csv::StringRecord {
    record
        .into_iter()
        .enumerate()
        .map(|(i, v)| if i == column_index { new_value } else { v })
        .collect()
}

type Values = HashMap<String, u64>;

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

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();
    let mut values = Values::new();
    let mut counter: u64 = 0;

    while rdr.read_record(&mut record)? {
        let value = record[column_index].to_owned();

        match values.get(&value) {
            Some(id) => {
                record = replace_column_value(&record, column_index, &id.to_string());
            }
            None => {
                values.insert(value, counter);
                record = replace_column_value(&record, column_index, &counter.to_string());
                counter += 1;
            }
        }

        wtr.write_record(&record)?;
    }

    Ok(wtr.flush()?)
}
