use std::cmp;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Transforms CSV data so that all records have the same length. The length is
the length of the longest record in the data. Records with smaller lengths are
padded with empty fields.

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

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    flag_length: Option<uint>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
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
            let mut maxlen = 0u;
            let mut rdr = try!(config.reader());
            while !rdr.done() {
                let mut count = 0u;
                loop {
                    match rdr.next_field().into_iter_result() {
                        None => break,
                        Some(r) => { try!(r); }
                    }
                    count += 1;
                }
                maxlen = cmp::max(maxlen, count);
            }
            maxlen
        }
    };

    let mut rdr = try!(config.reader());
    let mut wtr = try!(Config::new(&args.flag_output).writer());
    for r in rdr.byte_records() {
        let mut r = try!(r);
        if length >= r.len() {
            for _ in range(r.len(), length) {
                r.push(util::empty_field());
            }
        } else {
            r.truncate(length);
        }
        try!(wtr.write_bytes(r.into_iter()));
    }
    try!(wtr.flush());
    Ok(())
}
