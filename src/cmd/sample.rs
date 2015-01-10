use std::rand::Rng;

use csv::{self, ByteString};
use csv::index::Indexed;

use CliResult;
use config::{Config, Delimiter};
use util;

static USAGE: &'static str = "
Randomly samples CSV data uniformly using memory proportional to the size of
the sample.

When an index is present, this command will use random indexing if the sample
size is less than 10% of the total number of records. This allows for efficient
sampling such that the entire CSV file is not parsed.

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

#[derive(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    arg_sample_size: u64,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers);
    let sample_size = args.arg_sample_size;

    let mut wtr = try!(Config::new(&args.flag_output).writer());
    let sampled = match try!(rconfig.indexed()) {
        Some(mut idx) => {
            if do_random_access(sample_size, idx.count()) {
                try!(rconfig.write_headers(idx.csv(), &mut wtr));
                try!(sample_random_access(&mut idx, sample_size))
            } else {
                let mut rdr = try!(rconfig.reader());
                try!(rconfig.write_headers(&mut rdr, &mut wtr));
                try!(sample_reservoir(&mut rdr, sample_size))
            }
        }
        _ => {
            let mut rdr = try!(rconfig.reader());
            try!(rconfig.write_headers(&mut rdr, &mut wtr));
            try!(sample_reservoir(&mut rdr, sample_size))
        }
    };
    for row in sampled.into_iter() {
        try!(wtr.write(row.into_iter()));
    }
    Ok(try!(wtr.flush()))
}

fn sample_random_access<R: Reader + Seek, I: Reader + Seek>
                       (idx: &mut Indexed<R, I>, sample_size: u64)
                       -> CliResult<Vec<Vec<ByteString>>> {
    let mut all_indices = range(0, idx.count()).collect::<Vec<_>>();
    let mut rng = ::std::rand::thread_rng();
    rng.shuffle(&mut *all_indices);

    let mut sampled = Vec::with_capacity(sample_size as usize);
    for i in all_indices.into_iter().take(sample_size as usize) {
        try!(idx.seek(i));
        let mut rdr = idx.csv();
        sampled.push(try!(rdr.byte_records().next().unwrap()));
    }
    Ok(sampled)
}

fn sample_reservoir<R: Reader>
                   (rdr: &mut csv::Reader<R>, sample_size: u64)
                   -> CliResult<Vec<Vec<ByteString>>> {
    // The following algorithm has been adapted from:
    // http://en.wikipedia.org/wiki/Reservoir_sampling
    let mut reservoir = Vec::with_capacity(sample_size as usize);
    let mut records = rdr.byte_records().enumerate();
    for (_, row) in records.by_ref().take(reservoir.capacity()) {
        reservoir.push(try!(row));
    }

    // Now do the sampling.
    let mut rng = ::std::rand::thread_rng();
    for (i, row) in records {
        let random = rng.gen_range(0, i+1);
        if random < sample_size as usize {
            reservoir[random] = try!(row);
        }
    }
    Ok(reservoir)
}

fn do_random_access(sample_size: u64, total: u64) -> bool {
    sample_size <= (total / 10)
}
