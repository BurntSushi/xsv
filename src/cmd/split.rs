use std::old_io as io;
use std::old_io::fs::mkdir_recursive;
use std::os;

use csv;
use csv::index::Indexed;

use CliResult;
use config::{Config, Delimiter};
use util;

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

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Otherwise, the first row will
                           appear in all chunks as the header row.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    arg_outdir: String,
    flag_size: usize,
    flag_jobs: usize,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    if args.flag_size == 0 {
        return fail!("--size must be greater than 0.");
    }
    try!(mkdir_recursive(&Path::new(&*args.arg_outdir), io::ALL_PERMISSIONS));

    match try!(args.rconfig().indexed()) {
        Some(idx) => args.parallel_split(idx),
        None => args.sequential_split(),
    }
}

impl Args {
    fn sequential_split(&self) -> CliResult<()> {
        let rconfig = self.rconfig();
        let mut rdr = try!(rconfig.reader());
        let headers = try!(rdr.byte_headers());

        let mut wtr = try!(self.new_writer(&*headers, 0));
        for (i, row) in rdr.byte_records().enumerate() {
            if i > 0 && i % self.flag_size == 0 {
                try!(wtr.flush());
                wtr = try!(self.new_writer(&*headers, i));
            }
            let row = try!(row);
            try!(wtr.write(row.into_iter()));
        }
        try!(wtr.flush());
        Ok(())
    }

    fn parallel_split(&self, idx: Indexed<io::File, io::File>)
                     -> CliResult<()> {
        use threadpool::ThreadPool;
        use std::sync::mpsc::channel;

        let nchunks = util::num_of_chunks(idx.count() as usize,
                                          self.flag_size);
        let pool = ThreadPool::new(self.njobs());
        let (tx, rx) = channel();
        for i in range(0, nchunks) {
            let args = self.clone();
            let tx = tx.clone();
            pool.execute(move || {
                let conf = args.rconfig();
                let mut idx = conf.indexed().unwrap().unwrap();
                let headers = idx.csv().byte_headers().unwrap();
                let mut wtr = args.new_writer(&*headers, i * args.flag_size)
                                  .unwrap();

                idx.seek((i * args.flag_size) as u64).unwrap();
                for row in idx.csv().byte_records().take(args.flag_size) {
                    let row = row.unwrap();
                    wtr.write(row.into_iter()).unwrap();
                }
                wtr.flush().unwrap();
                tx.send(()).unwrap();
            });
        }
        drop(tx);
        for _ in rx.iter() {}
        Ok(())
    }

    fn new_writer(&self, headers: &[csv::ByteString], start: usize)
                 -> CliResult<csv::Writer<Box<io::Writer+'static>>> {
        let dir = Path::new(self.arg_outdir.clone());
        let path = dir.join(format!("{}.csv", start));
        let spath = Some(path.display().to_string());
        let mut wtr = try!(Config::new(&spath).writer());
        if !self.rconfig().no_headers {
            try!(wtr.write(headers.iter().map(|f| &**f)));
        }
        Ok(wtr)
    }

    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
               .delimiter(self.flag_delimiter)
               .no_headers(self.flag_no_headers)
    }

    fn njobs(&self) -> usize {
        if self.flag_jobs == 0 { os::num_cpus() } else { self.flag_jobs }
    }
}
