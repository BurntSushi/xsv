#![allow(dead_code, unused_imports)]

use std::cell::RefCell;
use std::default::Default;
use std::fmt;
use std::from_str::{FromStr, from_str};
use std::io::{mod, File};
use std::os;
use std::str;

use csv::{mod, ByteString};
use csv::index::Indexed;
use docopt;
use stats::{Frequencies, merge_all};

use CliResult;
use config::{Config, Delimiter};
use select::{SelectColumns, Selection};
use util;

docopt!(Args deriving Clone, "
Compute a frequency table on CSV data.

The frequency table is formatted as CSV data:

    field,value,count

Since this computes an exact frequency table, memory proportional to the
cardinality of each column is required.

Usage:
    xsv frequency [options] [<input>]

frequency options:
    -s, --select <arg>     Select a subset of columns to compute frequencies
                           for. See 'xsv select --help' for the format
                           details. This is provided here because piping 'xsv
                           select' into 'xsv frequencies' will disable the use
                           of indexing.
    -l, --limit <arg>      Limit the frequency table to the N most common
                           items. Set to '0' to disable a limit.
                           [default: 10]
    -a, --asc              Sort the frequency tables in ascending order by
                           count.
    --no-nulls             Don't include NULLs in the frequency table.
    -j, --jobs <arg>       The number of jobs to run in parallel.
                           This works better when the given CSV data has
                           an index already created. Note that a file handle
                           is opened for each job.
                           When set to '0', the number of jobs is set to the
                           number of CPUs detected.
                           [default: 0]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. i.e., They will be included
                           in statistics.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_jobs: u64,
   flag_select: SelectColumns, flag_limit: uint)

pub fn main() -> CliResult<()> {
    let args: Args = try!(util::get_args());

    let mut wtr = try!(io| Config::new(args.flag_output.clone()).writer());
    let (headers, tables) = try!(match try!(args.rconfig().indexed()) {
        None => args.sequential_ftables(),
        Some(idx) => {
            if args.flag_jobs == 1 {
                args.sequential_ftables()
            } else {
                args.parallel_ftables(idx)
            }
        }
    });

    try!(csv| wtr.write(vec!["field", "value", "count"].into_iter()));
    for (header, ftab) in headers.iter().zip(tables.into_iter()) {
        for (value, count) in args.counts(&ftab).into_iter() {
            let count = count.to_string();
            let row = vec![header[], value[], count.as_bytes()];
            try!(csv| wtr.write_bytes(row.into_iter()));
        }
    }
    Ok(())
}

type ByteRow = Vec<ByteString>;
type Headers = ByteRow;
type FTable = Frequencies<ByteString>;
type FTables = Vec<Frequencies<ByteString>>;

impl Args {
    fn rconfig(&self) -> Config {
        Config::new(self.arg_input.clone())
               .delimiter(self.flag_delimiter)
               .no_headers(self.flag_no_headers)
               .select(self.flag_select.clone())
    }

    fn counts<'a>(&self, ftab: &'a FTable) -> Vec<(&'a ByteString, u64)> {
        let mut counts = if self.flag_asc {
            ftab.least_frequent()
        } else {
            ftab.most_frequent()
        };
        if self.flag_limit > 0 {
            counts = counts.into_iter().take(self.flag_limit).collect();
        }
        counts
    }

    fn sequential_ftables(&self) -> CliResult<(Headers, FTables)> {
        let mut rdr = try!(io| self.rconfig().reader());
        let (headers, sel) = try!(self.sel_headers(&mut rdr));
        Ok((headers, try!(self.ftables(&sel, rdr.byte_records()))))
    }

    fn parallel_ftables(&self, idx: Indexed<io::File, io::File>)
                       -> CliResult<(Headers, FTables)> {
        use std::comm::channel;
        use std::sync::TaskPool;

        let mut rdr = try!(io| self.rconfig().reader());
        let (headers, sel) = try!(self.sel_headers(&mut rdr));

        let chunk_size = idx.count() / self.njobs();
        let nchunks = util::num_of_chunks(idx.count(), chunk_size);

        let mut pool = TaskPool::new(self.njobs() as uint, || { proc(_) () });
        let (send, recv) = channel();
        for i in range(0, nchunks) {
            let (send, args, sel) = (send.clone(), self.clone(), sel.clone());
            pool.execute(proc(_) {
                let mut idx = args.rconfig().indexed().unwrap().unwrap();
                idx.seek(i * chunk_size).unwrap();
                let it = idx.csv().byte_records().take(chunk_size as uint);
                send.send(args.ftables(&sel, it).unwrap());
            });
        }
        drop(send);
        Ok((headers, merge_all(recv.iter()).unwrap()))
    }

    fn ftables<I: Iterator<csv::CsvResult<ByteRow>>>
              (&self, sel: &Selection, mut it: I)
              -> CliResult<FTables> {
        let null = ByteString::from_bytes(b"NULL");
        let nsel = sel.normal();
        let mut tabs = Vec::from_fn(nsel.len(), |_| Frequencies::new());
        for row in it {
            let row = try!(csv| row);
            for (i, field) in nsel.select(row.into_iter()).enumerate() {
                if !field.is_empty() {
                    tabs.get_mut(i).add(field);
                } else {
                    if !self.flag_no_nulls {
                        tabs.get_mut(i).add(null.clone());
                    }
                }
            }
        }
        Ok(tabs)
    }

    fn sel_headers<R: Reader>(&self, rdr: &mut csv::Reader<R>)
                  -> CliResult<(ByteRow, Selection)> {
        let headers = try!(csv| rdr.byte_headers());
        let sel = try!(str| self.rconfig().selection(headers[]));
        Ok((sel.select(headers[]).map(ByteString::from_bytes).collect(), sel))
    }

    fn njobs(&self) -> u64 {
        if self.flag_jobs == 0 { os::num_cpus() as u64 } else { self.flag_jobs }
    }
}
