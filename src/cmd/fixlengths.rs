use std::cmp;



use crate::CliResult;
use crate::config::{Config, Delimiter};
use crate::util;

static USAGE: &str = "
Transforms CSV data so that all records have the same length. The length is
the length of the longest record in the data (not counting trailing empty fields,
but at least 1). Records with smaller lengths are padded with empty fields.

This requires two complete scans of the CSV data: one for determining the
record size and one for the actual transform. Because of this, the input
given must be a file and not stdin.

Alternatively, if --length is set, then all records are forced to that length.
This requires a single pass and can be done with stdin.

Usage:
    xsv fixlengths [options] [<input>]

fixlengths options:
    -l, --length <arg>     Forcefully set the length of each record. If a
                           record is not the size given, then it is truncated
                           or expanded as appropriate.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_length: Option<usize>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let config = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(true)
        .flexible(true);
    let length = match args.flag_length {
        Some(length) => {
            if length == 0 {
                return fail!("Length must be greater than 0.");
            }
            length
        }
        None => {
            if config.is_std() {
                return fail!("<stdin> cannot be used in this command. \
                              Please specify a file path.");
            }
            let mut maxlen = 0usize;
            let mut rdr = config.reader()?;
            let mut record = csv::ByteRecord::new();
            while rdr.read_byte_record(&mut record)? {
                let mut nonempty_count = 0;
                for (index, field) in record.iter().enumerate() {
                    if index == 0 || !field.is_empty() {
                        nonempty_count = index+1;
                    }
                }
                maxlen = cmp::max(maxlen, nonempty_count);
            }
            maxlen
        }
    };

    let mut rdr = config.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;
    for r in rdr.byte_records() {
        let mut r = r?;
        if length >= r.len() {
            for _ in r.len()..length {
                r.push_field(b"");
            }
        } else {
            r.truncate(length);
        }
        wtr.write_byte_record(&r)?;
    }
    wtr.flush()?;
    Ok(())
}
