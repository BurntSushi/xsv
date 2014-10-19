use std::io;
use std::io::fs::mkdir_recursive;

use csv;
use csv::index::Indexed;
use docopt;

use CliResult;
use config::{Config, Delimiter};
use util;

docopt!(Args deriving Clone, "
Splits the given CSV data into chunks.

The files are written to the directory given with the name '{start}.csv',
where {start} is the index of the first record of the chunk (starting at 0).

Usage:
    xsv split [options] <outdir> [<input>]

split options:
    -s, --size <arg>       The number of records to write into each chunk.
                           [default: 500]
    -j, --jobs <arg>       The number of spliting jobs to run in parallel.
                           This only works when the given CSV data has
                           an index already created. Note that a file handle
                           is opened for each job.
                           [default: 12]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Note that this has no effect when
                           concatenating columns.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, arg_outdir: String, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_size: u64, flag_jobs: uint)

pub fn main() -> CliResult<()> {
    let args: Args = try!(util::get_args());
    try!(io| mkdir_recursive(&Path::new(args.arg_outdir[]),
                             io::AllPermissions));

    match try!(args.rconfig().indexed()) {
        Some(idx) => args.parallel_split(idx),
        None => args.sequential_split(),
    }
}

impl Args {
    fn sequential_split(&self) -> CliResult<()> {
        let rconfig = self.rconfig();
        let mut rdr = try!(io| rconfig.reader());
        let headers = try!(csv| rdr.byte_headers());

        let mut wtr = try!(self.new_writer(headers[], 0));
        for (i, row) in rdr.byte_records().enumerate() {
            if i > 0 && i as u64 % self.flag_size == 0 {
                try!(csv| wtr.flush());
                wtr = try!(self.new_writer(headers[], i as u64));
            }
            let row = try!(csv| row);
            try!(csv| wtr.write_bytes(row.into_iter()));
        }
        try!(csv| wtr.flush());
        Ok(())
    }

    fn parallel_split(&self, idx: Indexed<io::File, io::File>)
                     -> CliResult<()> {
        use std::sync::TaskPool;

        let nchunks = util::num_of_chunks(idx.count(), self.flag_size);
        let mut pool = TaskPool::new(self.flag_jobs, || { proc(i) i });
        for i in range(0, nchunks) {
            let args = self.clone();
            pool.execute(proc(_) {
                let conf = args.rconfig();
                let mut idx = conf.indexed().unwrap().unwrap();
                let headers = idx.csv().byte_headers().unwrap();
                let mut wtr = args.new_writer(headers[], i * args.flag_size)
                                  .unwrap();

                idx.seek(i * args.flag_size).unwrap();
                let writenum = args.flag_size as uint;
                for row in idx.csv().byte_records().take(writenum) {
                    let row = row.unwrap();
                    wtr.write_bytes(row.into_iter()).unwrap();
                }
                wtr.flush().unwrap();
            });
        }
        Ok(())
    }

    fn new_writer(&self, headers: &[csv::ByteString], start: u64)
                 -> CliResult<csv::Writer<Box<io::Writer+'static>>> {
        let dir = Path::new(self.arg_outdir.clone());
        let path = dir.join(format!("{}.csv", start));
        let spath = Some(path.display().to_string());
        let mut wtr = try!(io| Config::new(spath).writer());
        if !self.flag_no_headers {
            try!(csv| wtr.write_bytes(headers.iter().map(|f| f[])));
        }
        Ok(wtr)
    }

    fn rconfig(&self) -> Config {
        Config::new(self.arg_input.clone())
               .delimiter(self.flag_delimiter)
               .no_headers(self.flag_no_headers)
    }
}
