use csv;

use config::{Config, Delimiter};
use select::SelectColumns;
use util::{self, ImmutableRecordHelpers};
use CliResult;

static USAGE: &str = "
Implodes a CSV file by collapsing multiple consecutive rows into a single one
where the values of a column are joined using the given separator.

This is the reverse of the explode command.

For instance the following CSV:

name,color
John,blue
John,yellow
Mary,red

Can be imploded on the \"color\" <column> using the \"|\" <separator> to produce:

name,color
John,blue|yellow
Mary,red

Usage:
    xsv implode [options] <column> <separator> [<input>]
    xsv implode --help

implode options:
    -r, --rename <name>    New name for the diverging column.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
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

fn compare_but_for_col(first: &csv::ByteRecord, second: &csv::ByteRecord, except: usize) -> bool {
    first.iter().zip(second.iter()).enumerate().all(
        |(i, (a, b))| {
            if i == except {
                true
            } else {
                a == b
            }
        },
    )
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
    let column_index = rconfig.single_selection(&headers)?;

    let mut headers = rdr.headers()?.clone();

    if let Some(new_name) = args.flag_rename {
        headers = headers.replace_at(column_index, &new_name);
    }

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }

    let sep = args.arg_separator;
    let mut previous: Option<csv::ByteRecord> = None;
    let mut accumulator: Vec<Vec<u8>> = Vec::new();

    for result in rdr.into_byte_records() {
        let record = result?;

        if let Some(previous_record) = previous.as_ref() {
            if !compare_but_for_col(&record, previous_record, column_index) {
                // Flushing
                let value = accumulator.join(sep.as_bytes());
                let imploded_record = previous_record.replace_at(column_index, &value);
                wtr.write_byte_record(&imploded_record)?;

                accumulator.clear();
            }
        }

        accumulator.push(record[column_index].to_vec());
        previous = Some(record);
    }

    // Flushing last instance
    if !accumulator.is_empty() {
        let value = accumulator.join(sep.as_bytes());
        let imploded_record = previous.unwrap().replace_at(column_index, &value);
        wtr.write_byte_record(&imploded_record)?;
    }

    Ok(wtr.flush()?)
}
