use std::io;

use config::{Config, Delimiter};
use util;
use CliResult;

use crate::CliError;

static USAGE: &'static str = "
Reverses rows of CSV data.

Useful for cases when there is no column that can be used for sorting in reverse order,
or when keys are not unique and order of rows with the same key needs to be preserved.

This function is memory efficient by default but only for seekable inputs (ones with
the possibility to randomly access data). Others must be read using --in-memory flag.

Usage:
    xsv reverse [options] [<input>]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be reversed with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
    -m, --in-memory        Load the csv content in memory before reversing it. Can
                           be useful for streamed inputs such as stdin.
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_in_memory: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let ref mut rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(true);

    return if args.flag_in_memory {
        run_without_memory_efficiency(rconfig, args)
    } else {
        run_with_memory_efficiency(rconfig, args)
    };
}

fn run_with_memory_efficiency(rconfig: &mut Config, args: Args) -> CliResult<()> {
    rconfig.no_headers = true;

    let mut config_csv_reader = rconfig.reader()?;
    let headers_size = if args.flag_no_headers {
        0
    } else {
        let _ = config_csv_reader.byte_headers();
        let position = config_csv_reader.position();
        position.clone().byte()
    };

    let reverse_reader = rconfig.io_reader_for_reverse_reading(headers_size as u64);
    match reverse_reader {
        Err(_) => {
            let msg = format!(
                "can't use provided input : needs to be loaded in the RAM using -m, --in-memory flag"
            );
            Err(CliError::from(io::Error::new(
                io::ErrorKind::Unsupported,
                msg,
            )))
        }
        Ok(rr) => {
            let mut wtr = Config::new(&args.flag_output).writer()?;
            let mut reverse_csv_reader = rconfig.from_reader(rr);

            if !args.flag_no_headers && headers_size > 0 {
                let headers = config_csv_reader.byte_headers()?;
                wtr.write_byte_record(&headers)?;
            }

            for r in reverse_csv_reader.byte_records() {
                match r {
                    Ok(record) => {
                        let new_record: Vec<Vec<u8>> = record
                            .iter()
                            .rev()
                            .map(|b| b.iter().rev().map(|c| *c).collect())
                            .collect();
                        wtr.write_record(new_record)?;
                    }
                    Err(_) => {}
                }
            }

            Ok(wtr.flush()?)
        }
    }
}

fn run_without_memory_efficiency(rconfig: &mut Config, args: Args) -> CliResult<()> {
    rconfig.no_headers = args.flag_no_headers;

    let mut reader = rconfig.reader()?;
    let mut all = reader.byte_records().collect::<Result<Vec<_>, _>>()?;
    all.reverse();

    let mut wtr = Config::new(&args.flag_output).writer()?;
    rconfig.write_headers(&mut reader, &mut wtr)?;

    for r in all.into_iter() {
        wtr.write_byte_record(&r)?;
    }

    Ok(wtr.flush()?)
}
