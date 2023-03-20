use csv;
use uuid::Uuid;

use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &'static str = r#"
Add a new column enumerating the lines of a CSV file. This can be useful to keep
track of a specific line order, give a unique identifier to each line or even
make a copy of the contents of a column.

The enum function can currently be used to perform the following tasks:

  Add an incremental identifier to each of the lines:
    $ xsv enum file.csv

  Add a uuid v4 to each of the lines:
    $ xsv enum --uuid file.csv

  Create a new column filled with a given value:
    $ xsv enum --constant 0

  Copy the contents of a column to a new one:
    $ xsv enum --copy names

  Finally, note that you should also be able to shuffle the lines of a CSV file
  by sorting on the generated uuids:
    $ xsv enum uuid file.csv | xsv sort -s uuid > shuffled.csv

Usage:
    xsv enum [options] [<input>]
    xsv enum --help

enum options:
    -c, --new-column <name>  Name of the column to create.
                             Will default to "index".
    --constant <value>       Fill a new column with the given value.
                             Changes the default column name to "constant".
    --copy <column>          Name of a column to copy.
                             Changes the default column name to "{column}_copy".
    --uuid                   When set, the column will be populated with
                             uuids (v4) instead of the incremental identifer.
                             Changes the default column name to "uuid".

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be interpreted
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
"#;

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_new_column: Option<String>,
    flag_constant: Option<String>,
    flag_copy: Option<SelectColumns>,
    flag_uuid: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = rdr.byte_headers()?.clone();

    let mut copy_index = 0;
    let mut copy_operation = false;

    if let Some(column_name) = args.flag_copy {
        rconfig = rconfig.select(column_name);
        let sel = rconfig.selection(&headers)?;
        copy_index = *sel.iter().next().unwrap();
        copy_operation = true;
    }

    if !rconfig.no_headers {
        if let Some(column_name) = &args.flag_new_column {
            headers.push_field(column_name.as_bytes());
        } else if args.flag_uuid {
            headers.push_field(b"uuid");
        } else if let Some(_) = &args.flag_constant {
            headers.push_field(b"constant");
        } else if copy_operation {
            let current_header = String::from_utf8(headers[copy_index].to_vec())
                .expect("Could not parse cell as utf-8!");
            headers.push_field(format!("{}_copy", current_header).as_bytes());
        } else {
            headers.push_field(b"index");
        };

        wtr.write_record(&headers)?;
    }

    let mut record = csv::ByteRecord::new();
    let mut counter: u64 = 0;

    while rdr.read_byte_record(&mut record)? {
        if let Some(constant_value) = &args.flag_constant {
            record.push_field(constant_value.as_bytes());
        } else if copy_operation {
            record.push_field(&record[copy_index].to_vec());
        } else if args.flag_uuid {
            let id = Uuid::new_v4();
            record.push_field(
                id.to_hyphenated()
                    .encode_lower(&mut Uuid::encode_buffer())
                    .as_bytes(),
            );
        } else {
            record.push_field(counter.to_string().as_bytes());
            counter += 1;
        }
        wtr.write_byte_record(&record)?;
    }

    Ok(wtr.flush()?)
}
