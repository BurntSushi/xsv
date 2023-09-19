use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
use util;
use xan::{eval, prepare, DynamicValue, Variables};
use CliResult;

static USAGE: &str = "
The map command evaluates an expression for each row of the given CSV file and
output the row with an added column containing the result of beforementioned
expression.

For instance, given the following CSV file:

a,b
1,4
5,2

The following command:

$ xsv map 'add(a, b)' c

Will produce the following result:

a,b,c
1,4,5
5,2,7

Usage:
    xsv map [options] <operations> <column> [<input>]
    xsv map --help

map options:
    -t, --threads <threads>  Number of threads to use in order to run the
                             computations in parallel. Only useful if you
                             perform heavy stuff such as reading files etc.

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be evaled
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_column: String,
    arg_operations: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_threads: Option<usize>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = csv::ByteRecord::new();

    if !args.flag_no_headers {
        headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            headers.push_field(args.arg_column.as_bytes());
            wtr.write_byte_record(&headers)?;
        }
    }

    let reserved = vec!["index"];

    let pipeline = prepare(&args.arg_operations, &headers, &reserved)?;

    if let Some(threads) = args.flag_threads {
        rdr.into_byte_records()
            .enumerate()
            .parallel_map_custom(
                |o| o.threads(threads),
                move |(i, record)| -> CliResult<csv::ByteRecord> {
                    let mut record = record?;
                    let mut variables = Variables::new();
                    variables.insert(&"index", DynamicValue::Integer(i as i64));

                    let value = eval(&pipeline, &record, &variables)?;
                    record.push_field(&value.serialize_as_bytes(b"|"));

                    Ok(record)
                },
            )
            .try_for_each(|result| -> CliResult<()> {
                let record = result?;
                wtr.write_byte_record(&record)?;
                Ok(())
            })?;

        return Ok(wtr.flush()?);
    }

    let mut record = csv::ByteRecord::new();
    let mut variables = Variables::new();
    let mut i = 0;

    while rdr.read_byte_record(&mut record)? {
        variables.insert("index", DynamicValue::Integer(i));
        let value = eval(&pipeline, &record, &variables)?;
        record.push_field(&value.serialize_as_bytes(b"|"));
        wtr.write_byte_record(&record)?;
        i += 1;
    }

    Ok(wtr.flush()?)
}
