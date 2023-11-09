use csv;

use config::{Config, Delimiter};
use select::SelectColumns;
use util::{self, ImmutableRecordHelpers};
use CliResult;

static USAGE: &str = "
Explodes a row into multiple ones by splitting a column value based on the
given separator.

This is the reverse of the implode command.

For instance the following CSV:

name,colors
John,blue|yellow
Mary,red

Can be exploded on the \"colors\" <column> based on the \"|\" <separator> to:

name,colors
John,blue
John,yellow
Mary,red

Note that given file needs to be UTF-8 encoded if given separator is more than
one byte long.

Usage:
    xsv explode [options] <column> <separator> [<input>]
    xsv explode --help

explode options:
    -r, --rename <name>    New name for the exploded column.

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
    let sep_is_single_byte = sep.len() == 1;

    if sep_is_single_byte {
        let sep = &sep.as_bytes()[0];

        let mut record = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut record)? {
            for val in record[column_index].split(|b| b == sep) {
                let new_record = record.replace_at(column_index, val);
                wtr.write_record(&new_record)?;
            }
        }
    } else {
        let mut record = csv::StringRecord::new();

        while rdr.read_record(&mut record)? {
            for val in record[column_index].split(&sep) {
                let new_record = record.replace_at(column_index, val);
                wtr.write_record(&new_record)?;
            }
        }
    }

    Ok(wtr.flush()?)
}
