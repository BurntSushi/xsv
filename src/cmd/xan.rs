use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
use xan::{eval, prepare, DynamicValue, Variables};
use CliResult;

pub struct XanCmdArgs {
    pub column: String,
    pub map_expr: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub no_headers: bool,
    pub delimiter: Option<Delimiter>,
    pub threads: Option<usize>,
}

pub fn run_xan_cmd(args: XanCmdArgs) -> CliResult<()> {
    let rconfig = Config::new(&args.input)
        .delimiter(args.delimiter)
        .no_headers(args.no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.output).writer()?;

    let mut headers = csv::ByteRecord::new();

    if !args.no_headers {
        headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            headers.push_field(args.column.as_bytes());
            wtr.write_byte_record(&headers)?;
        }
    }

    let reserved = vec!["index"];

    let pipeline = prepare(&args.map_expr, &headers, &reserved)?;

    if let Some(threads) = args.threads {
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
