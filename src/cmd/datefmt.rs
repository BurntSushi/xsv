use chrono_tz::Tz;
use csv;

use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &str = r#"
Add a column with the date from a CSV column in a specified format and timezone

Usage:
    xsv datefmt [options] <column> [<input>]
    xsv datefmt --help

datefmt options:
    -c, --new-column <name>   Name of the column to create.
                              Will default to "formatted_date".
    --infmt <format>          Input date format. See
                              https://docs.rs/chrono/latest/chrono/format/strftime/
                              for accepted date formats.
                              If not provided, the format will
                              be infered using dateparser.
    --outfmt <format>         Output date format. See
                              https://docs.rs/chrono/latest/chrono/format/strftime/
                              for accepted date formats.
                              Will default to ISO 8601/RFC 3339 format.
    --intz <tz>               Timezone of the input column.
                              Will default to "UTC".
    --outtz <tz>              Timezone of the output column.
                              Will default to "UTC".

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be interpreted
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. [default: ,]
"#;

#[derive(Deserialize, Debug)]
struct Args {
    arg_column: SelectColumns,
    arg_input: Option<String>,
    flag_new_column: Option<String>,
    flag_infmt: Option<String>,
    flag_outfmt: Option<String>,
    flag_intz: Option<String>,
    flag_outtz: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let input_tz: Tz = util::parse_timezone(args.flag_intz)?;
    let output_tz: Tz = util::parse_timezone(args.flag_outtz)?;

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let column_index = *sel.iter().next().unwrap();

    if !rconfig.no_headers {
        headers.push_field(
            args.flag_new_column
                .map_or("formatted_date".to_string(), |name| name)
                .as_bytes(),
        );
        wtr.write_byte_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {
        let cell = record[column_index].to_owned();

        let parsed_date = util::parse_date(&cell, input_tz, &args.flag_infmt);

        if let Ok(date) = parsed_date {
            if let Some(ref date_format) = args.flag_outfmt {
                let formatted_date = date
                    .with_timezone(&output_tz)
                    .format(date_format)
                    .to_string();

                record.push_field(&formatted_date);
            } else {
                record.push_field(&date.with_timezone(&output_tz).to_string());
            }
        } else {
            record.push_field("");
        }

        wtr.write_record(&record)?;
    }

    Ok(wtr.flush()?)
}
