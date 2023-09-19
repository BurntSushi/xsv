use std::io;

use csv;
use rand::Rng;

use config::{Config, Delimiter};
use index::Indexed;
use util;
use CliResult;

static USAGE: &str = "
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

sample options:
    --seed <number>        RNG seed.

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

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_sample_size: u64,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_seed: Option<usize>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let sample_size = args.arg_sample_size;

    let mut wtr = Config::new(&args.flag_output).writer()?;
    let sampled = match rconfig.indexed()? {
        Some(mut idx) => {
            if do_random_access(sample_size, idx.count()) {
                rconfig.write_headers(&mut *idx, &mut wtr)?;
                sample_random_access(&mut idx, sample_size)?
            } else {
                let mut rdr = rconfig.reader()?;
                rconfig.write_headers(&mut rdr, &mut wtr)?;
                sample_reservoir(&mut rdr, sample_size, args.flag_seed)?
            }
        }
        _ => {
            let mut rdr = rconfig.reader()?;
            rconfig.write_headers(&mut rdr, &mut wtr)?;
            sample_reservoir(&mut rdr, sample_size, args.flag_seed)?
        }
    };
    for row in sampled.into_iter() {
        wtr.write_byte_record(&row)?;
    }
    Ok(wtr.flush()?)
}

fn sample_random_access<R, I>(
    idx: &mut Indexed<R, I>,
    sample_size: u64,
) -> CliResult<Vec<csv::ByteRecord>>
where
    R: io::Read + io::Seek,
    I: io::Read + io::Seek,
{
    let mut all_indices = (0..idx.count()).collect::<Vec<_>>();
    let mut rng = ::rand::thread_rng();
    rng.shuffle(&mut all_indices);

    let mut sampled = Vec::with_capacity(sample_size as usize);
    for i in all_indices.into_iter().take(sample_size as usize) {
        idx.seek(i)?;
        sampled.push(idx.byte_records().next().unwrap()?);
    }
    Ok(sampled)
}

fn sample_reservoir<R: io::Read>(
    rdr: &mut csv::Reader<R>,
    sample_size: u64,
    seed: Option<usize>,
) -> CliResult<Vec<csv::ByteRecord>> {
    // The following algorithm has been adapted from:
    // https://en.wikipedia.org/wiki/Reservoir_sampling
    let mut reservoir = Vec::with_capacity(sample_size as usize);
    let mut records = rdr.byte_records().enumerate();
    for (_, row) in records.by_ref().take(reservoir.capacity()) {
        reservoir.push(row?);
    }

    // Seeding rng
    let mut rng = util::acquire_rng(seed);

    // Now do the sampling.
    for (i, row) in records {
        let random = rng.gen_range(0, i + 1);
        if random < sample_size as usize {
            reservoir[random] = row?;
        }
    }
    Ok(reservoir)
}

fn do_random_access(sample_size: u64, total: u64) -> bool {
    sample_size <= (total / 10)
}
