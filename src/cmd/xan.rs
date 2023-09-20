use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
use xan::{eval, prepare, DynamicValue, EvaluationError, Variables};
use CliResult;

pub enum XanMode {
    Map,
    Filter,
}

impl XanMode {
    fn is_map(&self) -> bool {
        match self {
            Self::Map => true,
            _ => false,
        }
    }

    fn is_filter(&self) -> bool {
        match self {
            Self::Filter => true,
            _ => false,
        }
    }
}

pub enum XanErrorPolicy {
    Panic,
    Report,
    Ignore,
}

impl XanErrorPolicy {
    fn will_report(&self) -> bool {
        match self {
            Self::Report => true,
            _ => false,
        }
    }
}

impl From<String> for XanErrorPolicy {
    fn from(value: String) -> Self {
        match value.as_str() {
            "panic" => Self::Panic,
            "report" => Self::Report,
            "ignore" => Self::Ignore,
            _ => unreachable!(),
        }
    }
}

pub struct XanCmdArgs {
    pub column: String,
    pub map_expr: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub no_headers: bool,
    pub delimiter: Option<Delimiter>,
    pub threads: Option<usize>,
    pub error_policy: XanErrorPolicy,
    pub error_column_name: String,
    pub mode: XanMode,
}

pub fn handle_eval_result(
    error_policy: &XanErrorPolicy,
    record: &mut csv::ByteRecord,
    eval_result: Result<DynamicValue, EvaluationError>,
) -> CliResult<()> {
    match eval_result {
        Ok(value) => {
            record.push_field(&value.serialize_as_bytes(b"|"));

            if error_policy.will_report() {
                record.push_field(b"");
            }
        }
        Err(err) => match error_policy {
            XanErrorPolicy::Ignore => {
                let value = DynamicValue::None;
                record.push_field(&value.serialize_as_bytes(b"|"));
            }
            XanErrorPolicy::Report => {
                record.push_field(b"");
                record.push_field(err.to_string().as_bytes());
            }
            XanErrorPolicy::Panic => {
                Err(err)?;
            }
        },
    };

    Ok(())
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
            let mut should_write_headers = false;

            if args.mode.is_map() {
                headers.push_field(args.column.as_bytes());
                should_write_headers = true;
            }

            if args.error_policy.will_report() {
                headers.push_field(args.error_column_name.as_bytes());
                should_write_headers = true;
            }

            if should_write_headers {
                wtr.write_byte_record(&headers)?;
            }
        }
    }

    let reserved = vec!["index"];

    let pipeline = prepare(&args.map_expr, &headers, &reserved)?;

    if let Some(threads) = args.threads {
        rdr.into_byte_records()
            .enumerate()
            .parallel_map_custom(
                |o| o.threads(threads),
                move |(i, record)| -> CliResult<(csv::ByteRecord, Result<DynamicValue, EvaluationError>)> {
                    let record = record?;
                    let mut variables = Variables::new();
                    variables.insert(&"index", DynamicValue::Integer(i as i64));

                    let eval_result = eval(&pipeline, &record, &variables);

                    Ok((record, eval_result))
                },
            )
            .try_for_each(|result| -> CliResult<()> {
                let (mut record, eval_result) = result?;
                handle_eval_result(&args.error_policy, &mut record, eval_result)?;
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

        let eval_result = eval(&pipeline, &record, &variables);
        handle_eval_result(&args.error_policy, &mut record, eval_result)?;

        wtr.write_byte_record(&record)?;
        i += 1;
    }

    Ok(wtr.flush()?)
}
