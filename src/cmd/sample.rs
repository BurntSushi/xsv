use std::rand::Rng;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Randomly samples CSV data uniformly using memory proportional to the size of
the sample.

This command is intended to provide a means to sample from a CSV data set that
is too big to fit into memory (for example, for use with commands like 'xsv
frequency' or 'xsv stats'). It will however visit every CSV record exactly
once, which is necessary to provide a uniform random sample. If you wish to
limit the number of records visited, use the 'xsv slice' command to pipe into
'xsv sample'.

Usage:
    xsv sample [options] <sample-size> [<input>]
    xsv sample --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will be consider as part of
                           the population to sample from. (When not set, the
                           first row is the header row and will always appear
                           in the output.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[deriving(Decodable)]
struct Args {
    arg_input: Option<String>,
    arg_sample_size: uint,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);

    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(Config::new(&args.flag_output).writer());

    // The following algorithm has been adapted from:
    // http://en.wikipedia.org/wiki/Reservoir_sampling

    // Fill the reservoir.
    let mut reservoir = Vec::with_capacity(args.arg_sample_size);
    {
        // Scope `rdr.byte_records()` so that the mutable borrow gets dropped.
        let mut records = rdr.byte_records().enumerate();
        for (_, row) in records.by_ref().take(reservoir.capacity()) {
            reservoir.push(try!(row));
        }

        // Now do the sampling.
        let mut rng = ::std::rand::task_rng();
        for (i, row) in records {
            let random = rng.gen_range(0, i+1);
            if random < args.arg_sample_size {
                reservoir[random] = try!(row);
            }
        }
    }

    try!(rconfig.write_headers(&mut rdr, &mut wtr));
    for row in reservoir.into_iter() {
        try!(wtr.write_bytes(row.into_iter()));
    }
    Ok(try!(wtr.flush()))
}
