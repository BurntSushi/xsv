use csv;
use std::fs;
use std::io::{self, BufRead, BufReader};
use serde_json::Value;

use CliResult;
use config::{Delimiter, Config};
use util;

static USAGE: &'static str = "
Converts a newline-delimited JSON file (.ndjson or .jsonl, typically) into
a CSV file.

The command tries to do its best but since it is not possible to
straightforwardly convert jsonl to CSV, the process might lose some complex
fields from the input.

Also, it will fail if the JSON documents are not consistent with one another.

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

fn infer_headers(value: Value) -> Option<Vec<String>> {
    let mut headers: Vec<String> = Vec::new();

    return Some(headers);
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rdr: Box<dyn BufRead> = match args.arg_input {
        None => Box::new(BufReader::new(io::stdin())),
        Some(p) => Box::new(BufReader::new(fs::File::open(p)?))
    };

    for line in rdr.lines() {
        let value: Value = serde_json::from_str(&line?)
            .expect("Could not parse line as JSON!");

        println!("{:?}", value);
    }

    Ok(())
}
