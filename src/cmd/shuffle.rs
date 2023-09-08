use byteorder::{ByteOrder, LittleEndian};
use csv;
use rand::{Rng, SeedableRng, StdRng};
use std::io::SeekFrom;

use config::{Config, Delimiter};
use util;
use CliResult;

// TODO: add --in-memory
static USAGE: &'static str = "
Shuffle the given CSV file. Requires memory proportional to the
number of rows of the file (approx. 2 u64 per row).

Since this command needs random access in the input file, it
does not work with stdin or piping.

Usage:
    xsv shuffle [options] [<input>]
    xsv shuffle --help

shuffle options:
    --seed <number>        RNG seed.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be included in
                           the count.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_seed: Option<isize>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let wconf = Config::new(&args.flag_output);

    // Seeding rng
    let mut rng: StdRng = match args.flag_seed {
        None => StdRng::from_rng(rand::thread_rng()).unwrap(),
        Some(seed) => {
            let mut buf = [0u8; 32];
            LittleEndian::write_u64(&mut buf, seed as u64);
            SeedableRng::from_seed(buf)
        }
    };

    let mut positions: Vec<(u64, usize)> = Vec::new();
    let mut output_wtr = wconf.io_writer()?;

    {
        let mut rdr = rconf.reader()?;

        if !args.flag_no_headers {
            let header = rdr.byte_headers()?;

            if !header.is_empty() {
                let mut wtr = csv::Writer::from_writer(vec![]);
                wtr.write_record(header)?;

                let binary_header = wtr
                    .into_inner()
                    .expect("error while serializing binary header");

                output_wtr.write(&binary_header[..binary_header.len() - 1])?;
            }
        }

        let mut record = csv::ByteRecord::new();
        let mut last_pos: u64 = rdr.position().byte();

        while rdr.read_byte_record(&mut record)? {
            let pos = rdr.position().byte();
            positions.push((last_pos, (pos - last_pos) as usize));
            last_pos = pos;
        }

        rng.shuffle(&mut positions);
    }

    let mut input_rdr = rconf.io_reader_for_random_access()?;
    let mut reading_buffer: Vec<u8> = Vec::new();

    for (byte_offset, length) in positions {
        input_rdr.seek(SeekFrom::Start(byte_offset))?;

        reading_buffer
            .try_reserve(length)
            .expect("not enough memory");

        unsafe {
            reading_buffer.set_len(length);
        }

        input_rdr.read_exact(&mut reading_buffer)?;
        output_wtr.write_all(&reading_buffer)?;
    }

    Ok(output_wtr.flush()?)
}
