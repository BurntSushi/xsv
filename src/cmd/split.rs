use std::io;
use std::io::fs::mkdir_recursive;

use csv;
use csv::index::Indexed;
use docopt;

use types::{CliError, CsvConfig, Delimiter};
use util;

docopt!(Args, "
Splits the given CSV data into chunks.

The files are written to the directory given with the name '{start}-{end}.csv',
where {start} and {end} is the half-open interval corresponding to the records
in the chunk.

Usage:
    xsv split [options] <outdir> [<input>]

split options:
    -s, --size <arg>       The number of records to write into each chunk.
                           [default: 500]
    -j, --jobs <arg>       The number of spliting jobs to run in parallel.
                           Note that this only works when the given CSV data
                           has an index already created.
                           [default: 4]

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

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    try!(io| mkdir_recursive(&Path::new(args.arg_outdir[]),
                             io::AllPermissions));

    match try!(args.rconfig().indexed()) {
        Some(idx) => args.parallel_split(idx),
        None => args.sequential_split(),
    }
}

impl Args {
    fn sequential_split(&self) -> Result<(), CliError> {
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
                     -> Result<(), CliError> {
        use std::sync::TaskPool;

        let mut nchunks = idx.count() / self.flag_size;
        if idx.count() % self.flag_size != 0 {
            nchunks += 1;
        }
        let mut pool = TaskPool::new(self.flag_jobs, || { proc(i) i });
        for i in range(0, nchunks) {
            let conf = self.rconfig();
            let args = self.clone();
            pool.execute(proc(_) {
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
                 -> Result<csv::Writer<Box<io::Writer+'static>>, CliError> {
        let dir = Path::new(self.arg_outdir.clone());
        let path = dir.join(format!("{}.csv", start));
        let spath = Some(path.display().to_string());
        let mut wtr = try!(io| CsvConfig::new(spath).writer());
        if !self.flag_no_headers {
            try!(csv| wtr.write_bytes(headers.iter().map(|f| f[])));
        }
        Ok(wtr)
    }

    fn rconfig(&self) -> CsvConfig {
        CsvConfig::new(self.arg_input.clone())
                  .delimiter(self.flag_delimiter)
                  .no_headers(self.flag_no_headers)
    }
}

// This is gross. Any way around it?
impl Clone for Args {
    fn clone(&self) -> Args {
        Args {
            flag_size: self.flag_size,
            cmd_split: self.cmd_split,
            flag_output: self.flag_output.clone(),
            flag_no_headers: self.flag_no_headers,
            flag_delimiter: self.flag_delimiter.clone(),
            flag_help: self.flag_help,
            arg_input: self.arg_input.clone(),
            arg_outdir: self.arg_outdir.clone(),
            flag_jobs: self.flag_jobs,
        }
    }
}
