use std::convert::TryFrom;

use csv;
use pariter::IteratorExt;

use config::{Config, Delimiter};
use xan::{eval, prepare, DynamicValue, EvaluationError, Variables};
use CliError;
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
    Log,
}

impl XanErrorPolicy {
    fn will_report(&self) -> bool {
        match self {
            Self::Report => true,
            _ => false,
        }
    }

    pub fn from_restricted(value: &str) -> Result<Self, CliError> {
        Ok(match value {
            "panic" => Self::Panic,
            "ignore" => Self::Ignore,
            "log" => Self::Log,
            _ => {
                return Err(CliError::Other(format!(
                    "unknown error policy \"{}\"",
                    value
                )))
            }
        })
    }
}

impl TryFrom<String> for XanErrorPolicy {
    type Error = CliError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(match value.as_str() {
            "panic" => Self::Panic,
            "report" => Self::Report,
            "ignore" => Self::Ignore,
            "log" => Self::Log,
            _ => {
                return Err(CliError::Other(format!(
                    "unknown error policy \"{}\"",
                    value
                )))
            }
        })
    }
}

pub struct XanCmdArgs {
    pub new_column: Option<String>,
    pub map_expr: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub no_headers: bool,
    pub delimiter: Option<Delimiter>,
    pub threads: Option<usize>,
    pub error_policy: XanErrorPolicy,
    pub error_column_name: Option<String>,
    pub mode: XanMode,
}

pub fn handle_eval_result<W: std::io::Write>(
    args: &XanCmdArgs,
    index: usize,
    record: &mut csv::ByteRecord,
    eval_result: Result<DynamicValue, EvaluationError>,
    writer: &mut csv::Writer<W>,
) -> CliResult<()> {
    let mut should_write_row = true;

    match eval_result {
        Ok(value) => {
            if args.mode.is_filter() {
                if value.is_falsey() {
                    should_write_row = false;
                }
            } else {
                record.push_field(&value.serialize_as_bytes(b"|"));

                if args.error_policy.will_report() {
                    record.push_field(b"");
                }
            }
        }
        Err(err) => match args.error_policy {
            XanErrorPolicy::Ignore => {
                if args.mode.is_map() {
                    let value = DynamicValue::None;
                    record.push_field(&value.serialize_as_bytes(b"|"));
                }
            }
            XanErrorPolicy::Report => {
                if args.mode.is_filter() {
                    unreachable!();
                }

                record.push_field(b"");
                record.push_field(err.to_string().as_bytes());
            }
            XanErrorPolicy::Log => {
                eprintln!("Row n°{}: {}", index + 1, err.to_string());

                if args.mode.is_map() {
                    let value = DynamicValue::None;
                    record.push_field(&value.serialize_as_bytes(b"|"));
                }
            }
            XanErrorPolicy::Panic => {
                Err(err)?;
            }
        },
    };

    if should_write_row {
        writer.write_byte_record(record)?;
    }

    Ok(())
}

pub fn run_xan_cmd(args: XanCmdArgs) -> CliResult<()> {
    let rconfig = Config::new(&args.input)
        .delimiter(args.delimiter)
        .no_headers(args.no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.output).writer()?;

    let mut headers = csv::ByteRecord::new();
    let mut must_write_headers = false;

    if !args.no_headers {
        headers = rdr.byte_headers()?.clone();

        if !headers.is_empty() {
            must_write_headers = true;

            if args.mode.is_map() {
                if let Some(new_column) = &args.new_column {
                    headers.push_field(new_column.as_bytes());
                }
            }

            if args.error_policy.will_report() {
                if let Some(error_column_name) = &args.error_column_name {
                    headers.push_field(error_column_name.as_bytes());
                }
            }
        }
    }

    let reserved = vec!["index"];

    let pipeline = prepare(&args.map_expr, &headers, &reserved)?;

    if must_write_headers {
        wtr.write_byte_record(&headers)?;
    }

    if let Some(threads) = args.threads {
        rdr.into_byte_records()
            .enumerate()
            .parallel_map_custom(
                |o| o.threads(threads),
                move |(i, record)| -> CliResult<(
                    usize,
                    csv::ByteRecord,
                    Result<DynamicValue, EvaluationError>,
                )> {
                    let record = record?;
                    let mut variables = Variables::new();
                    variables.insert(&"index", DynamicValue::Integer(i as i64));

                    let eval_result = eval(&pipeline, &record, &variables);

                    Ok((i, record, eval_result))
                },
            )
            .try_for_each(|result| -> CliResult<()> {
                let (i, mut record, eval_result) = result?;
                handle_eval_result(&args, i, &mut record, eval_result, &mut wtr)?;
                Ok(())
            })?;

        return Ok(wtr.flush()?);
    }

    let mut record = csv::ByteRecord::new();
    let mut variables = Variables::new();
    let mut i: usize = 0;

    while rdr.read_byte_record(&mut record)? {
        variables.insert("index", DynamicValue::Integer(i as i64));

        let eval_result = eval(&pipeline, &record, &variables);
        handle_eval_result(&args, i, &mut record, eval_result, &mut wtr)?;
        i += 1;
    }

    Ok(wtr.flush()?)
}
