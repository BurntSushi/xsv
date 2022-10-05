use std::fs;
use std::io;
use std::path::Path;

use channel;
use csv;
use threadpool::ThreadPool;

use CliResult;
use config::{Config, Delimiter};
use index::Indexed;
use util::{self, FilenameTemplate};

static USAGE: &'static str = "
Splits the given CSV data into chunks.

The files are written to the directory given with the name '{start}.csv',
where {start} is the index of the first record of the chunk (starting at 0).

Usage:
    xsv split [options] <outdir> [<input>]
    xsv split --help

split options:
    -s, --size <arg>       The number of records to write into each chunk.
                           [default: 500]
    -j, --jobs <arg>       The number of spliting jobs to run in parallel.
                           This only works when the given CSV data has
                           an index already created. Note that a file handle
                           is opened for each job.
                           When set to '0', the number of jobs is set to the
                           number of CPUs detected.
                           [default: 0]
    --filename <filename>  A filename template to use when constructing
                           the names of the output files.  The string '{}'
                           will be replaced by a value based on the value
                           of the field, but sanitized for shell safety.
                           [default: {}.csv]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Otherwise, the first row will
                           appear in all chunks as the header row.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_outdir: String,
    flag_size: usize,
    flag_jobs: usize,
    flag_filename: FilenameTemplate,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    if args.flag_size == 0 {
        return fail!("--size must be greater than 0.");
    }
    fs::create_dir_all(&args.arg_outdir)?;

    match args.rconfig().indexed()? {
        Some(idx) => args.parallel_split(idx),
        None => args.sequential_split(),
    }
}

impl Args {
    fn sequential_split(&self) -> CliResult<()> {
        let rconfig = self.rconfig();
        let mut rdr = rconfig.reader()?;
        let headers = rdr.byte_headers()?.clone();

        let mut wtr = self.new_writer(&headers, 0)?;
        let mut i = 0;
        let mut row = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut row)? {
            if i > 0 && i % self.flag_size == 0 {
                wtr.flush()?;
                wtr = self.new_writer(&headers, i)?;
            }
            wtr.write_byte_record(&row)?;
            i += 1;
        }
        wtr.flush()?;
        Ok(())
    }

    fn parallel_split(
        &self,
        idx: Indexed<fs::File, fs::File>,
    ) -> CliResult<()> {
        let nchunks = util::num_of_chunks(
            idx.count() as usize, self.flag_size);
        let pool = ThreadPool::new(self.njobs());
        let (tx, rx) = channel::bounded::<()>(0);
        for i in 0..nchunks {
            let args = self.clone();
            let tx = tx.clone();
            pool.execute(move || {
                let conf = args.rconfig();
                let mut idx = conf.indexed().unwrap().unwrap();
                let headers = idx.byte_headers().unwrap().clone();
                let mut wtr = args
                    .new_writer(&headers, i * args.flag_size)
                    .unwrap();

                idx.seek((i * args.flag_size) as u64).unwrap();
                for row in idx.byte_records().take(args.flag_size) {
                    let row = row.unwrap();
                    wtr.write_byte_record(&row).unwrap();
                }
                wtr.flush().unwrap();
                drop(tx);
            });
        }
        drop(tx);
        rx.recv();
        Ok(())
    }

    fn new_writer(
        &self,
        headers: &csv::ByteRecord,
        start: usize,
    ) -> CliResult<csv::Writer<Box<dyn io::Write+'static>>> {
        let dir = Path::new(&self.arg_outdir);
        let path = dir.join(self.flag_filename.filename(&format!("{}", start)));
        let spath = Some(path.display().to_string());
        let mut wtr = Config::new(&spath).writer()?;
        if !self.rconfig().no_headers {
            wtr.write_record(headers)?;
        }
        Ok(wtr)
    }

    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
    }

    fn njobs(&self) -> usize {
        if self.flag_jobs == 0 {
            util::num_cpus()
        } else {
            self.flag_jobs
        }
    }
}
