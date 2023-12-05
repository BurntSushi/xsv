use csv;
use serde_json::Value;
use std::borrow::Cow;
use std::fs;
use std::io::{self, BufRead, BufReader};

use config::Config;
use util;
use CliResult;

static USAGE: &str = "
Converts a newline-delimited JSON file (.ndjson or .jsonl, typically) into
a CSV file.

The command tries to do its best but since it is not possible to
straightforwardly convert JSON lines to CSV, the process might lose some complex
fields from the input.

Also, it will fail if the JSON documents are not consistent with one another,
as the first JSON line will be use to infer the headers of the CSV output.

Usage:
    xsv jsonl [options] [<input>]
    xsv jsonl --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
}

fn recurse_to_infer_headers(value: &Value, headers: &mut Vec<Vec<String>>, path: Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map.iter() {
                match value {
                    Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                        let mut full_path = path.clone();
                        full_path.push(key.to_string());

                        headers.push(full_path);
                    }
                    Value::Object(_) => {
                        let mut new_path = path.clone();
                        new_path.push(key.to_string());

                        recurse_to_infer_headers(value, headers, new_path);
                    }
                    _ => {}
                }
            }
        }
        _ => {
            headers.push(vec![String::from("value")]);
        }
    }
}

fn infer_headers(value: &Value) -> Option<Vec<Vec<String>>> {
    let mut headers: Vec<Vec<String>> = Vec::new();

    recurse_to_infer_headers(value, &mut headers, Vec::new());

    Some(headers)
}

fn get_value_at_path(value: &Value, path: &[String]) -> Option<Value> {
    let mut current = value;

    for key in path.iter() {
        match current.get(key) {
            Some(new_value) => {
                current = new_value;
            }
            None => {
                return None;
            }
        }
    }

    Some(current.to_owned())
}

fn json_line_to_csv_record(value: &Value, headers: &Vec<Vec<String>>) -> csv::StringRecord {
    let mut record = csv::StringRecord::new();

    for path in headers {
        let value = get_value_at_path(value, path);

        if let Some(value) = value {
            record.push_field(&match value {
                Value::Bool(v) => Cow::Borrowed(if v { "true" } else { "false" }),
                Value::Number(v) => Cow::Owned(v.to_string()),
                Value::String(v) => Cow::Owned(v),
                _ => Cow::Borrowed(""),
            });
        } else {
            record.push_field("");
        }
    }

    record
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let rdr: Box<dyn BufRead> = match args.arg_input {
        None => Box::new(BufReader::new(io::stdin())),
        Some(p) => Box::new(BufReader::new(fs::File::open(p)?)),
    };

    let mut headers: Vec<Vec<String>> = Vec::new();
    let mut headers_emitted: bool = false;

    for line in rdr.lines() {
        let value: Value = serde_json::from_str(&line?).expect("Could not parse line as JSON!");

        if !headers_emitted {
            if let Some(h) = infer_headers(&value) {
                headers = h;

                let headers_formatted =
                    headers.iter().map(|v| v.join(".")).collect::<Vec<String>>();
                let headers_record = csv::StringRecord::from(headers_formatted);
                wtr.write_record(&headers_record)?;
            }

            headers_emitted = true;
        }

        let record = json_line_to_csv_record(&value, &headers);
        wtr.write_record(&record)?;
    }

    Ok(())
}
