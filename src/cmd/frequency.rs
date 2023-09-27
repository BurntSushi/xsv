use std::fs;
use std::io;

use channel;
use csv;
use stats::{merge_all, Frequencies};
use threadpool::ThreadPool;

use config::{Config, Delimiter};
use index::Indexed;
use select::{SelectColumns, Selection};
use util;
use CliResult;

static USAGE: &str = "
Compute a frequency table on CSV data.

The frequency table is formatted as CSV data:

    field,value,count

By default, there is a row for the N most frequent values for each field in the
data. The order and number of values can be tweaked with --asc and --limit,
respectively.

Since this computes an exact frequency table, memory proportional to the
cardinality of each column is required.

Usage:
    xsv frequency [options] [<input>]

frequency options:
    -s, --select <arg>     Select a subset of columns to compute frequencies
                           for. See 'xsv select --help' for the format
                           details. This is provided here because piping 'xsv
                           select' into 'xsv frequency' will disable the use
                           of indexing.
    -l, --limit <arg>      Limit the frequency table to the N most common
                           items. Set to <=0 to disable a limit. It is combined
                           with -t/--threshold.
                           [default: 10]
    -t, --threshold <arg>  If set, won't return items having a count less than
                           this given threshold. It is combined with -l/--limit.
    -a, --asc              Sort the frequency tables in ascending order by
                           count. The default is descending order.
    -N, --no-extra         Don't include null & remaining counts.
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
    -n, --no-headers       When set, the first row will NOT be included
                           in the frequency table. Additionally, the 'field'
                           column will be 1-based indices instead of header
                           names.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_limit: usize,
    flag_threshold: Option<u64>,
    flag_asc: bool,
    flag_no_extra: bool,
    flag_jobs: usize,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

#[derive(Clone, Deserialize, PartialEq)]
enum DomainMax {
    Max,
    Total,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = args.rconfig();

    let args_clone = args.clone();

    let mut wtr = Config::new(&args.flag_output).writer()?;
    let (headers, tables, row_count) = match args.rconfig().indexed()? {
        Some(ref mut idx) if args.njobs() > 1 => args.parallel_ftables(idx),
        _ => args.sequential_ftables(),
    }?;

    wtr.write_record(vec!["field", "value", "count"])?;
    let head_ftables = headers.into_iter().zip(tables);
    for (i, (header, ftab)) in head_ftables.enumerate() {
        let header = if rconfig.no_headers {
            (i + 1).to_string().into_bytes()
        } else {
            header.to_vec()
        };

        let mut seen_count: u64 = 0;

        for (value, count) in args_clone.counts(&ftab).into_iter() {
            seen_count += count;
            let count = count.to_string();
            let row = vec![&*header, &*value, count.as_bytes()];
            wtr.write_record(row)?;
        }

        let remaining = row_count - seen_count;

        if !args.flag_no_extra && remaining > 0 {
            wtr.write_record(vec![&*header, b"<rest>", remaining.to_string().as_bytes()])?;
        }
    }

    Ok(wtr.flush()?)
}

type ByteString = Vec<u8>;
type Headers = csv::ByteRecord;
type FTable = Frequencies<Vec<u8>>;
type FTables = Vec<Frequencies<Vec<u8>>>;

impl Args {
    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.flag_select.clone())
    }

    fn counts(&self, ftab: &FTable) -> Vec<(ByteString, u64)> {
        let mut counts = if self.flag_asc {
            ftab.least_frequent()
        } else {
            ftab.most_frequent()
        };
        if self.flag_limit > 0 {
            counts = counts.into_iter().take(self.flag_limit).collect();
        }
        let counts = counts.into_iter().map(|(bs, c)| {
            if b"" == &**bs {
                (b"<null>"[..].to_vec(), c)
            } else {
                (bs.clone(), c)
            }
        });

        if let Some(t) = self.flag_threshold {
            let counts = counts.filter(|(_, c)| *c >= t);
            return counts.collect();
        }

        counts.collect()
    }

    fn sequential_ftables(&self) -> CliResult<(Headers, FTables, u64)> {
        let mut rdr = self.rconfig().reader()?;
        let (headers, sel) = self.sel_headers(&mut rdr)?;
        let (ftables, count) = self.ftables(&sel, rdr.byte_records())?;
        Ok((headers, ftables, count))
    }

    fn parallel_ftables(
        &self,
        idx: &mut Indexed<fs::File, fs::File>,
    ) -> CliResult<(Headers, FTables, u64)> {
        let mut rdr = self.rconfig().reader()?;
        let (headers, sel) = self.sel_headers(&mut rdr)?;

        if idx.count() == 0 {
            return Ok((headers, vec![], 0));
        }

        let chunk_size = util::chunk_size(idx.count() as usize, self.njobs());
        let nchunks = util::num_of_chunks(idx.count() as usize, chunk_size);

        let pool = ThreadPool::new(self.njobs());
        let (send, recv) = channel::bounded(0);
        for i in 0..nchunks {
            let (send, args, sel) = (send.clone(), self.clone(), sel.clone());
            pool.execute(move || {
                let mut idx = args.rconfig().indexed().unwrap().unwrap();
                idx.seek((i * chunk_size) as u64).unwrap();
                let it = idx.byte_records().take(chunk_size);
                let (ftable, _) = args.ftables(&sel, it).unwrap();
                send.send(ftable);
            });
        }
        drop(send);
        Ok((headers, merge_all(recv).unwrap(), idx.count()))
    }

    fn ftables<I>(&self, sel: &Selection, it: I) -> CliResult<(FTables, u64)>
    where
        I: Iterator<Item = csv::Result<csv::ByteRecord>>,
    {
        let null = &b""[..].to_vec();
        let nsel = sel.normal();
        let mut tabs: Vec<_> = (0..nsel.len()).map(|_| Frequencies::new()).collect();
        let mut count = 0;
        for row in it {
            let row = row?;
            count += 1;
            for (i, field) in nsel.select(row.into_iter()).enumerate() {
                let field = trim(field.to_vec());
                if !field.is_empty() {
                    tabs[i].add(field);
                } else if !self.flag_no_extra {
                    tabs[i].add(null.clone());
                }
            }
        }
        Ok((tabs, count))
    }

    fn sel_headers<R: io::Read>(
        &self,
        rdr: &mut csv::Reader<R>,
    ) -> CliResult<(csv::ByteRecord, Selection)> {
        let headers = rdr.byte_headers()?;
        let sel = self.rconfig().selection(headers)?;
        Ok((sel.select(headers).map(|h| h.to_vec()).collect(), sel))
    }

    fn njobs(&self) -> usize {
        if self.flag_jobs == 0 {
            util::num_cpus()
        } else {
            self.flag_jobs
        }
    }
}

fn trim(bs: ByteString) -> ByteString {
    match String::from_utf8(bs) {
        Ok(s) => s.trim().as_bytes().to_vec(),
        Err(bs) => bs.into_bytes(),
    }
}
