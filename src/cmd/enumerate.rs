use csv;
use uuid::Uuid;

use CliResult;
use config::{Delimiter, Config};
use util;

static USAGE: &'static str = "
Add a new column enumerating the lines of a CSV file. This is useful to keep
track of a specific line order or give a unique identifier to each line.

You should also be able to shuffle the lines of a CSV file by sorting on
the generated uuids:

  $ xsv enumerate uuid file.csv | xsv sort -s uuid > shuffled.csv

Usage:
    xsv enumerate [options] [<input>]
    xsv enumerate --help

enumerate options:
    -c, --new-column <name>  Name of the column to create.
                             Will default to \"index\".
    --constant <value>       Fill a new column with the given value.
                             Changes the default column name to \"constant\".
    --uuid                   When set, the column will be populated with
                             uuids (v4) instead of the incremental identifer.
                             Changes the default column name to \"uuid\".

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be interpreted
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_new_column: Option<String>,
    flag_constant: Option<String>,
    flag_uuid: bool,
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
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = rdr.byte_headers()?.clone();

    let new_column_name = match (&args.flag_new_column, args.flag_uuid) {
        (Some(column_name), _) => column_name.as_bytes(),
        (None, false) => b"index",
        (None, true) => b"uuid"
    };

    if !rconfig.no_headers {
        headers.push_field(new_column_name);
        wtr.write_record(&headers)?;
    }

    let mut record = csv::ByteRecord::new();
    let mut counter: u64 = 0;

    while rdr.read_byte_record(&mut record)? {
        if let Some(constant_value) = &args.flag_constant {
            record.push_field(constant_value.as_bytes());
        }
        else if args.flag_uuid {
            let id = Uuid::new_v4();
            record.push_field(id.to_hyphenated().encode_lower(&mut Uuid::encode_buffer()).as_bytes());
        }
        else {
            record.push_field(counter.to_string().as_bytes());
            counter += 1;
        }
        wtr.write_byte_record(&record)?;
    }

    Ok(wtr.flush()?)
}
