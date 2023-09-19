use std::borrow::ToOwned;
use std::cmp;
use std::default::Default;
use std::fmt;
use std::fs;
use std::io;
use std::iter::{repeat, FromIterator};
use std::str::{self, FromStr};

use channel;
use colored::Colorize;
use csv;
use stats::{merge_all, Commute, MinMax, OnlineStats, Unsorted};
use threadpool::ThreadPool;
use unicode_width::UnicodeWidthStr;

use config::{Config, Delimiter};
use index::Indexed;
use select::{SelectColumns, Selection};
use util;
use CliResult;

use self::FieldType::{TFloat, TInteger, TNull, TUnicode, TUnknown};

static USAGE: &str = "
Computes basic statistics on CSV data.

Basic statistics includes mean, median, mode, standard deviation, sum, max and
min values. Note that some statistics are expensive to compute, so they must
be enabled explicitly. By default, the following statistics are reported for
*every* column in the CSV data: mean, max, min and standard deviation. The
default set of statistics corresponds to statistics that can be computed
efficiently on a stream of data (i.e., constant memory).

Computing statistics on a large file can be made much faster if you create
an index for it first with 'xsv index'.

Usage:
    xsv stats [options] [<input>]

stats options:
    -s, --select <arg>     Select a subset of columns to compute stats for.
                           See 'xsv select --help' for the format details.
                           This is provided here because piping 'xsv select'
                           into 'xsv stats' will disable the use of indexing.
    --everything           Show all statistics available.
    --mode                 Show the mode.
                           This requires storing all CSV data in memory.
    --cardinality          Show the cardinality.
                           This requires storing all CSV data in memory.
    --median               Show the median.
                           This requires storing all CSV data in memory.
    --nulls                Include NULLs in the population size for computing
                           mean and standard deviation. Also include them in
                           the histogram outputed by '--pretty'.
    -j, --jobs <arg>       The number of jobs to run in parallel.
                           This works better when the given CSV data has
                           an index already created. Note that a file handle
                           is opened for each job.
                           When set to '0', the number of jobs is set to the
                           number of CPUs detected.
                           [default: 0]
    --pretty               Prints histograms.
    --screen-size <arg>    The size used to output the histogram. Set to '0',
                           it will use the shell size (default). The minimum
                           size is 80.
    --precision <arg>      The number of digit to keep after the comma. Has to be less
                           than 20. Default is 2.
    --bins <arg>           The number of bins in the distribution. Default is 10.
    --nans                 Include Unknown and Unicode in the histogram outputed
                           with '--pretty'.
    --min <arg>            The minimum from which we start to display
                           the histogram. When not set, will take the
                           minimum from the csv file.
    --max <arg>            The maximum from which we start to display
                           the histogram. When not set, will take the
                           maximum from the csv file.


Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. i.e., They will be included
                           in statistics.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_everything: bool,
    flag_mode: bool,
    flag_cardinality: bool,
    flag_median: bool,
    flag_nulls: bool,
    flag_jobs: usize,
    flag_pretty: bool,
    flag_screen_size: Option<usize>,
    flag_precision: Option<u8>,
    flag_bins: Option<u64>,
    flag_nans: Option<bool>,
    flag_max: Option<f64>,
    flag_min: Option<f64>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    if !args.flag_pretty
        && (args.flag_min.is_some()
            || args.flag_max.is_some()
            || args.flag_nans.is_some()
            || args.flag_bins.is_some()
            || args.flag_precision.is_some()
            || args.flag_screen_size.is_some())
    {
        return fail!("`--screen-size`, `--precision`, `--bins`, `--nans`, `--max` and `--min` can only be used with `--pretty`");
    }

    let mut wtr = Config::new(&args.flag_output).writer()?;
    let (headers, stats) = match args.rconfig().indexed()? {
        None => args.sequential_stats(),
        Some(idx) => {
            if args.flag_jobs == 1 {
                args.sequential_stats()
            } else {
                args.parallel_stats(idx)
            }
        }
    }?;
    let stats = stats;

    if !args.flag_pretty {
        let stats = args.stats_to_records(stats);
        wtr.write_record(&args.stat_headers())?;
        let fields = headers.iter().zip(stats);
        for (i, (header, stat)) in fields.enumerate() {
            let header = if args.flag_no_headers {
                i.to_string().into_bytes()
            } else {
                header.to_vec()
            };
            let stat = stat.iter().map(|f| f.as_bytes());
            wtr.write_record(vec![&*header].into_iter().chain(stat))?;
        }
        wtr.flush()?;
        return Ok(());
    }

    if args.flag_output.is_none() {
        let fields = headers.iter().zip(stats);
        for (i, (header, stat)) in fields.enumerate() {
            let header = if args.flag_no_headers {
                i.to_string()
            } else {
                String::from_utf8(header.to_vec()).unwrap()
            };
            args.print_histogram(stat, header)?;
        }
        return Ok(());
    }

    let stats_record = args.stats_to_records(stats.clone());
    wtr.write_record(&args.stat_headers())?;
    let fields = headers
        .iter()
        .zip(stats_record)
        .zip(stats)
        .map(|((x, y), z)| (x, y, z));
    for (i, (header, stat_record, stat)) in fields.enumerate() {
        let header_record = if args.flag_no_headers {
            i.to_string().into_bytes()
        } else {
            header.to_vec()
        };
        let stat_record = stat_record.iter().map(|f| f.as_bytes());
        wtr.write_record(vec![&*header_record].into_iter().chain(stat_record))?;

        let header = String::from_utf8(header_record).unwrap();
        args.print_histogram(stat, header)?;
    }
    wtr.flush()?;
    Ok(())
}

impl Args {
    fn sequential_stats(&self) -> CliResult<(csv::ByteRecord, Vec<Stats>)> {
        let mut rdr = self.rconfig().reader()?;
        let (headers, sel) = self.sel_headers(&mut rdr)?;
        let stats = self.compute(&sel, rdr.byte_records())?;
        Ok((headers, stats))
    }

    fn parallel_stats(
        &self,
        idx: Indexed<fs::File, fs::File>,
    ) -> CliResult<(csv::ByteRecord, Vec<Stats>)> {
        // N.B. This method doesn't handle the case when the number of records
        // is zero correctly. So we use `sequential_stats` instead.
        if idx.count() == 0 {
            return self.sequential_stats();
        }

        let mut rdr = self.rconfig().reader()?;
        let (headers, sel) = self.sel_headers(&mut rdr)?;

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
                send.send(args.compute(&sel, it).unwrap());
            });
        }
        drop(send);
        Ok((headers, merge_all(recv).unwrap_or_default()))
    }

    fn stats_to_records(&self, stats: Vec<Stats>) -> Vec<csv::StringRecord> {
        let mut records: Vec<_> = repeat(csv::StringRecord::new()).take(stats.len()).collect();
        let pool = ThreadPool::new(self.njobs());
        let mut results = vec![];
        for mut stat in stats.into_iter() {
            let (send, recv) = channel::bounded(0);
            results.push(recv);
            pool.execute(move || {
                send.send(stat.to_record());
            });
        }
        for (i, recv) in results.into_iter().enumerate() {
            records[i] = recv.recv().unwrap();
        }
        records
    }

    fn compute<I>(&self, sel: &Selection, it: I) -> CliResult<Vec<Stats>>
    where
        I: Iterator<Item = csv::Result<csv::ByteRecord>>,
    {
        let mut stats = self.new_stats(sel.len());
        for row in it {
            let row = row?;
            for (i, field) in sel.select(&row).enumerate() {
                stats[i].add(field);
            }
        }
        Ok(stats)
    }

    fn sel_headers<R: io::Read>(
        &self,
        rdr: &mut csv::Reader<R>,
    ) -> CliResult<(csv::ByteRecord, Selection)> {
        let headers = rdr.byte_headers()?.clone();
        let sel = self.rconfig().selection(&headers)?;
        Ok((csv::ByteRecord::from_iter(sel.select(&headers)), sel))
    }

    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.flag_select.clone())
    }

    fn njobs(&self) -> usize {
        if self.flag_jobs == 0 {
            util::num_cpus()
        } else {
            self.flag_jobs
        }
    }

    fn new_stats(&self, record_len: usize) -> Vec<Stats> {
        repeat(Stats::new(WhichStats {
            include_nulls: self.flag_nulls,
            sum: true,
            range: true,
            dist: true,
            cardinality: self.flag_cardinality || self.flag_everything,
            median: self.flag_median || self.flag_everything,
            histogram: self.flag_pretty,
            mode: self.flag_mode || self.flag_everything,
        }))
        .take(record_len)
        .collect()
    }

    fn stat_headers(&self) -> csv::StringRecord {
        let mut fields = vec![
            "field",
            "type",
            "sum",
            "min",
            "max",
            "min_length",
            "max_length",
            "mean",
            "stddev",
        ];
        let all = self.flag_everything;
        if self.flag_median || all {
            fields.push("median");
        }
        if self.flag_mode || all {
            fields.push("mode");
        }
        if self.flag_cardinality || all {
            fields.push("cardinality");
        }
        csv::StringRecord::from(fields)
    }

    fn bins_construction(
        &self,
        min: f64,
        max: f64,
        nb_bins: u64,
    ) -> CliResult<(Vec<(f64, f64, u64)>, f64)> {
        let mut bins: Vec<(f64, f64, u64)> = Vec::new();
        let size_interval = ((max - min) / nb_bins as f64).abs();
        let mut min_interval = min;
        let mut max_interval = min_interval + size_interval;
        bins.push((min_interval, max_interval, 0));
        for _ in 1..nb_bins {
            if size_interval == 0.0 {
                break;
            }
            min_interval = max_interval;
            max_interval = min_interval + size_interval;
            bins.push((min_interval, max_interval, 0));
        }
        Ok((bins, size_interval))
    }

    fn print_histogram(&self, stat: Stats, header: String) -> CliResult<()> {
        if let Some(min) = self.flag_min {
            if let Some(max) = self.flag_max {
                if max < min {
                    return fail!("min must be less than max");
                }
            }
        }
        let screen_size = self.flag_screen_size.unwrap_or(0);
        let precision = self.flag_precision.unwrap_or(2);
        if precision >= 20 {
            return fail!("precision must be greater than 20");
        }
        let nb_bins = self.flag_bins.unwrap_or(10);
        let nans = self.flag_nans.unwrap_or(false);

        let mut bar = Bar {
            header: header.clone(),
            screen_size,
            lines_total: 0,
            lines_total_str: String::new(),
            legend_str_len: 0,
            size_bar_cols: 0,
            size_labels: 0,
            longest_bar: 0,
        };

        let (histo, nb_int_float, nb_nans, nb_nulls) = match stat.histogram {
            Some(h) => (h.values, h.nb_int_float, h.nb_nans, h.nb_nulls),
            None => {
                let error_mess = format!(
                    "There are only NULLs, Unknown or Unicode values in the \"{}\" column.",
                    bar.header
                );
                println!("{}\n", error_mess.yellow().bold());
                return Ok(());
            }
        };
        bar.lines_total = nb_int_float;
        if self.flag_nulls {
            bar.lines_total += nb_nulls;
        }
        if nans {
            bar.lines_total += nb_nans;
        }

        let minmax = stat.minmax.unwrap();
        let min = match (self.flag_min, minmax.floats.min()) {
            (None, None) => {
                let error_mess = format!("There are {} NULLs, and {} Unknown or Unicode values in the \"{}\" column ({} lines).", nb_nulls, nb_nans, bar.header, format_number(nb_int_float + nb_nans + nb_nulls));
                println!("{}\n", error_mess.yellow().bold());
                return Ok(());
            }
            (Some(min), _) => min,
            (None, Some(min)) => *min,
        };
        let max = match (self.flag_max, minmax.floats.max()) {
            (None, None) => {
                let error_mess = format!("There are {} NULLs, and {} Unknown or Unicode values in the \"{}\" column ({} lines).", nb_nulls, nb_nans, bar.header, format_number(nb_int_float + nb_nans + nb_nulls));
                println!("{}\n", error_mess.yellow().bold());
                return Ok(());
            }
            (Some(max), _) => max,
            (None, Some(max)) => *max,
        };
        if min > max {
            return fail!("Can't output the histograms because min is greater than max");
        }

        let max_label_len = cmp::max(
            cmp::max(
                cmp::max(
                    format_number_float(min, precision, false).chars().count(),
                    format_number_float(max, precision, true).chars().count(),
                ),
                UnicodeWidthStr::width(&header[..]),
            ),
            5,
        );

        match bar.update_sizes(max_label_len) {
            Ok(1) => {
                return Ok(());
            }
            Ok(_) => {}
            Err(e) => return fail!(e),
        };

        let (mut bins, size_interval) = match self.bins_construction(min, max, nb_bins) {
            Ok((bins, size_interval)) => (bins, size_interval),
            Err(e) => return fail!(e),
        };

        let mut lines_done = 0;
        for value in histo.into_iter() {
            if value > max || value < min {
                continue;
            }
            let temp = (value - min) / size_interval;
            let mut pos = temp.floor() as usize;
            if pos as f64 == temp && pos != 0 {
                pos -= 1;
            }
            bins[pos].2 += 1;
            lines_done += 1;
        }
        if lines_done == 0 {
            let error_mess = format!("There are {} NULLs and {} Unknown or Unicode values in the \"{}\" column ({} lines).", nb_nulls, nb_nans, header, format_number(nb_int_float + nb_nans + nb_nulls));
            println!("{}\n", error_mess.yellow().bold());
            return Ok(());
        }

        bar.longest_bar = bar.lines_total as usize;
        bar.print_title();

        let mut j = 0;
        for res in bins.into_iter() {
            let interval = format_number_float(res.0, precision, false);
            bar.print_bar(interval, res.2, j);
            j += 1;
        }
        if nb_nans != 0 && nans {
            lines_done += nb_nans;
            bar.print_bar("NaNs".to_string(), nb_nans, j);
            j += 1;
        }
        if nb_nulls != 0 && self.flag_nulls {
            lines_done += nb_nulls;
            bar.print_bar("NULLs".to_string(), nb_nulls, j);
        }

        let resume = " ".repeat(bar.size_labels + 1)
            + "Distribution for "
            + &format_number(lines_done)
            + "/"
            + &bar.lines_total_str
            + " lines.";
        println!("{}\n", resume.yellow().bold());
        Ok(())
    }
}

#[derive(Clone)]
struct Histogram {
    values: Vec<f64>,
    nb_int_float: u64,
    nb_nans: u64,
    nb_nulls: u64,
}

impl Commute for Histogram {
    fn merge(&mut self, other: Histogram) {
        for v in other.values {
            self.values.push(v);
        }
        self.nb_int_float += other.nb_int_float;
        self.nb_nans += other.nb_nans;
        self.nb_nulls += other.nb_nulls;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WhichStats {
    include_nulls: bool,
    sum: bool,
    range: bool,
    dist: bool,
    cardinality: bool,
    median: bool,
    histogram: bool,
    mode: bool,
}

impl Commute for WhichStats {
    fn merge(&mut self, other: WhichStats) {
        assert_eq!(*self, other);
    }
}

#[derive(Clone)]
struct Stats {
    typ: FieldType,
    sum: Option<TypedSum>,
    minmax: Option<TypedMinMax>,
    online: Option<OnlineStats>,
    mode: Option<Unsorted<Vec<u8>>>,
    median: Option<Unsorted<f64>>,
    histogram: Option<Histogram>,
    which: WhichStats,
}

impl Stats {
    fn new(which: WhichStats) -> Stats {
        let (mut sum, mut minmax, mut online, mut mode, mut median, mut histogram) =
            (None, None, None, None, None, None);
        if which.sum {
            sum = Some(Default::default());
        }
        if which.range {
            minmax = Some(Default::default());
        }
        if which.dist {
            online = Some(Default::default());
        }
        if which.mode || which.cardinality {
            mode = Some(Default::default());
        }
        if which.median {
            median = Some(Default::default());
        }
        if which.histogram {
            histogram = Some(Histogram {
                values: Default::default(),
                nb_int_float: Default::default(),
                nb_nans: Default::default(),
                nb_nulls: Default::default(),
            })
        };
        Stats {
            typ: Default::default(),
            sum,
            minmax,
            online,
            mode,
            median,
            histogram,
            which,
        }
    }

    fn add(&mut self, sample: &[u8]) {
        let sample_type = FieldType::from_sample(sample);
        self.typ.merge(sample_type);

        self.sum.as_mut().map(|v| v.add(sample_type, sample));
        self.minmax.as_mut().map(|v| v.add(sample_type, sample));
        self.mode.as_mut().map(|v| v.add(sample.to_vec()));
        match sample_type {
            TUnknown => match self.histogram.as_mut() {
                None => {}
                Some(h) => h.nb_nans += 1,
            },
            TNull => {
                if self.which.include_nulls {
                    if let Some(v) = self.online.as_mut() {
                        v.add_null();
                    }
                }
                match self.histogram.as_mut() {
                    None => {}
                    Some(h) => h.nb_nulls += 1,
                }
            }
            TUnicode => match self.histogram.as_mut() {
                None => {}
                Some(h) => h.nb_nans += 1,
            },
            TFloat | TInteger => {
                let n = from_bytes::<f64>(sample).unwrap();
                if let Some(v) = self.median.as_mut() {
                    v.add(n);
                }
                match self.histogram.as_mut() {
                    None => {}
                    Some(h) => {
                        h.nb_int_float += 1;
                        h.values.push(n);
                    }
                }
                self.online.as_mut().map(|v| {
                    v.add(n);
                });
            }
        }
    }

    fn to_record(&mut self) -> csv::StringRecord {
        let typ = self.typ;
        let mut pieces = vec![];
        let empty = || "".to_owned();

        pieces.push(self.typ.to_string());
        match self.sum.as_ref().and_then(|sum| sum.show(typ)) {
            Some(sum) => {
                pieces.push(sum);
            }
            None => {
                pieces.push(empty());
            }
        }
        match self.minmax.as_ref().and_then(|mm| mm.show(typ)) {
            Some(mm) => {
                pieces.push(mm.0);
                pieces.push(mm.1);
            }
            None => {
                pieces.push(empty());
                pieces.push(empty());
            }
        }
        match self.minmax.as_ref().and_then(|mm| mm.len_range()) {
            Some(mm) => {
                pieces.push(mm.0);
                pieces.push(mm.1);
            }
            None => {
                pieces.push(empty());
                pieces.push(empty());
            }
        }

        if !self.typ.is_number() {
            pieces.push(empty());
            pieces.push(empty());
        } else {
            match self.online {
                Some(ref v) => {
                    pieces.push(v.mean().to_string());
                    pieces.push(v.stddev().to_string());
                }
                None => {
                    pieces.push(empty());
                    pieces.push(empty());
                }
            }
        }
        match self.median.as_mut().and_then(|v| v.median()) {
            None => {
                if self.which.median {
                    pieces.push(empty());
                }
            }
            Some(v) => {
                pieces.push(v.to_string());
            }
        }
        match self.mode.as_mut() {
            None => {
                if self.which.mode {
                    pieces.push(empty());
                }
                if self.which.cardinality {
                    pieces.push(empty());
                }
            }
            Some(ref mut v) => {
                if self.which.mode {
                    let lossy = |s: Vec<u8>| -> String { String::from_utf8_lossy(&s).into_owned() };
                    pieces.push(v.mode().map_or("N/A".to_owned(), lossy));
                }
                if self.which.cardinality {
                    pieces.push(v.cardinality().to_string());
                }
            }
        }
        csv::StringRecord::from(pieces)
    }
}

impl Commute for Stats {
    fn merge(&mut self, other: Stats) {
        self.typ.merge(other.typ);
        self.sum.merge(other.sum);
        self.minmax.merge(other.minmax);
        self.online.merge(other.online);
        self.mode.merge(other.mode);
        self.median.merge(other.median);
        self.histogram.merge(other.histogram);
        self.which.merge(other.which);
    }
}

#[derive(Clone, Copy, PartialEq, Default)]
enum FieldType {
    TUnknown,
    // The default is the most specific type.
    // Type inference proceeds by assuming the most specific type and then
    // relaxing the type as counter-examples are found.
    #[default]
    TNull,
    TUnicode,
    TFloat,
    TInteger,
}

impl FieldType {
    fn from_sample(sample: &[u8]) -> FieldType {
        if sample.is_empty() {
            return TNull;
        }
        let string = match str::from_utf8(sample) {
            Err(_) => return TUnknown,
            Ok(s) => s,
        };
        if string.parse::<i64>().is_ok() {
            return TInteger;
        }
        if string.parse::<f64>().is_ok() {
            return TFloat;
        }
        TUnicode
    }

    fn is_number(&self) -> bool {
        *self == TFloat || *self == TInteger
    }
}

impl Commute for FieldType {
    fn merge(&mut self, other: FieldType) {
        *self = match (*self, other) {
            (TUnicode, TUnicode) => TUnicode,
            (TFloat, TFloat) => TFloat,
            (TInteger, TInteger) => TInteger,
            // Null does not impact the type.
            (TNull, any) | (any, TNull) => any,
            // There's no way to get around an unknown.
            (TUnknown, _) | (_, TUnknown) => TUnknown,
            // Integers can degrate to floats.
            (TFloat, TInteger) | (TInteger, TFloat) => TFloat,
            // Numbers can degrade to Unicode strings.
            (TUnicode, TFloat) | (TFloat, TUnicode) => TUnicode,
            (TUnicode, TInteger) | (TInteger, TUnicode) => TUnicode,
        };
    }
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TUnknown => write!(f, "Unknown"),
            TNull => write!(f, "NULL"),
            TUnicode => write!(f, "Unicode"),
            TFloat => write!(f, "Float"),
            TInteger => write!(f, "Integer"),
        }
    }
}

/// TypedSum keeps a rolling sum of the data seen.
///
/// It sums integers until it sees a float, at which point it sums floats.
#[derive(Clone, Default)]
struct TypedSum {
    integer: i64,
    float: Option<f64>,
}

impl TypedSum {
    fn add(&mut self, typ: FieldType, sample: &[u8]) {
        if sample.is_empty() {
            return;
        }
        match typ {
            TFloat => {
                let float: f64 = from_bytes::<f64>(sample).unwrap();
                match self.float {
                    None => {
                        self.float = Some((self.integer as f64) + float);
                    }
                    Some(ref mut f) => {
                        *f += float;
                    }
                }
            }
            TInteger => {
                if let Some(ref mut float) = self.float {
                    *float += from_bytes::<f64>(sample).unwrap();
                } else {
                    self.integer += from_bytes::<i64>(sample).unwrap();
                }
            }
            _ => {}
        }
    }

    fn show(&self, typ: FieldType) -> Option<String> {
        match typ {
            TNull | TUnicode | TUnknown => None,
            TInteger => Some(self.integer.to_string()),
            TFloat => Some(self.float.unwrap_or(0.0).to_string()),
        }
    }
}

impl Commute for TypedSum {
    fn merge(&mut self, other: TypedSum) {
        match (self.float, other.float) {
            (Some(f1), Some(f2)) => self.float = Some(f1 + f2),
            (Some(f1), None) => self.float = Some(f1 + (other.integer as f64)),
            (None, Some(f2)) => self.float = Some((self.integer as f64) + f2),
            (None, None) => self.integer += other.integer,
        }
    }
}

/// TypedMinMax keeps track of minimum/maximum values for each possible type
/// where min/max makes sense.
#[derive(Clone, Default)]
struct TypedMinMax {
    strings: MinMax<Vec<u8>>,
    str_len: MinMax<usize>,
    integers: MinMax<i64>,
    floats: MinMax<f64>,
}

impl TypedMinMax {
    fn add(&mut self, typ: FieldType, sample: &[u8]) {
        self.str_len.add(sample.len());
        if sample.is_empty() {
            return;
        }
        self.strings.add(sample.to_vec());
        match typ {
            TUnicode | TUnknown | TNull => {}
            TFloat => {
                let n = str::from_utf8(sample)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap();
                self.floats.add(n);
                self.integers.add(n as i64);
            }
            TInteger => {
                let n = str::from_utf8(sample)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap();
                self.integers.add(n);
                self.floats.add(n as f64);
            }
        }
    }

    fn len_range(&self) -> Option<(String, String)> {
        match (self.str_len.min(), self.str_len.max()) {
            (Some(min), Some(max)) => Some((min.to_string(), max.to_string())),
            _ => None,
        }
    }

    fn show(&self, typ: FieldType) -> Option<(String, String)> {
        match typ {
            TNull => None,
            TUnicode | TUnknown => match (self.strings.min(), self.strings.max()) {
                (Some(min), Some(max)) => {
                    let min = String::from_utf8_lossy(min).to_string();
                    let max = String::from_utf8_lossy(max).to_string();
                    Some((min, max))
                }
                _ => None,
            },
            TInteger => match (self.integers.min(), self.integers.max()) {
                (Some(min), Some(max)) => Some((min.to_string(), max.to_string())),
                _ => None,
            },
            TFloat => match (self.floats.min(), self.floats.max()) {
                (Some(min), Some(max)) => Some((min.to_string(), max.to_string())),
                _ => None,
            },
        }
    }
}

impl Commute for TypedMinMax {
    fn merge(&mut self, other: TypedMinMax) {
        self.strings.merge(other.strings);
        self.str_len.merge(other.str_len);
        self.integers.merge(other.integers);
        self.floats.merge(other.floats);
    }
}

fn from_bytes<T: FromStr>(bytes: &[u8]) -> Option<T> {
    str::from_utf8(bytes).ok().and_then(|s| s.parse().ok())
}

fn format_number(count: u64) -> String {
    let mut count_str = count.to_string();
    let count_len = count_str.chars().count();

    if count_len < 3 {
        return count_str;
    }

    let count_chars: Vec<char> = count_str.chars().collect();

    count_str = count_chars[0].to_string();
    for k in 1..count_len {
        if k % 3 == count_len % 3 {
            count_str += ",";
        }
        count_str += &count_chars[k].to_string();
    }

    count_str
}

fn format_number_float(count: f64, mut precision: u8, ceil: bool) -> String {
    let mut count = count;
    if ceil {
        count = ceil_float(count, precision);
    } else {
        count = floor_float(count, precision);
    }
    let neg = count < 0.0;
    let mut count_str = count.abs().to_string();
    let mut count_str_len = count_str.chars().count();
    let mut count_str_int_len = count_str_len;
    if let Some(idx) = count_str.find('.') {
        count_str_int_len = idx;
    }
    let count_chars: Vec<char> = count_str.chars().collect();
    count_str = count_chars[0].to_string();
    for k in 1..count_str_int_len {
        if k % 3 == count_str_int_len % 3 {
            count_str += ",";
        }
        count_str += &count_chars[k].to_string();
    }
    if precision != 0 {
        count_str += ".";
        if count_str_int_len == count_str_len {
            count_str_len += 1;
        }
        precision += 1;
    }
    for k in (count_str_int_len + 1)..count_str_len {
        count_str += &count_chars[k].to_string();
    }
    if (count_str_len - count_str_int_len) < precision as usize {
        count_str += &"0".repeat(precision as usize - (count_str_len - count_str_int_len));
    }
    if neg {
        count_str = "-".to_string() + &count_str;
    }

    count_str
}

fn ceil_float(value: f64, precision: u8) -> f64 {
    let mul = if precision == 1 {
        1.0
    } else {
        u64::pow(10, precision as u32) as f64
    };

    (value * mul).ceil() / mul
}

fn floor_float(value: f64, precision: u8) -> f64 {
    let mul = if precision == 1 {
        1.0
    } else {
        u64::pow(10, precision as u32) as f64
    };

    (value * mul).floor() / mul
}

struct Bar {
    header: String,
    screen_size: usize,
    lines_total: u64,
    lines_total_str: String,
    legend_str_len: usize,
    size_bar_cols: usize,
    size_labels: usize,
    longest_bar: usize,
}

impl Bar {
    fn update_sizes(&mut self, max_str_len: usize) -> CliResult<u64> {
        if self.screen_size == 0 {
            if let Some(size) = termsize::get() {
                self.screen_size = size.cols as usize;
            }
        }
        if self.screen_size < 80 {
            self.screen_size = 80;
        }

        self.lines_total_str = format_number(self.lines_total);
        let line_total_str_len = self.lines_total_str.chars().count();
        // legend is the right part. 17 corresponds to the minimal number of characters (`nb_lines | 100.00`)
        self.legend_str_len = 17;
        if line_total_str_len > 8 {
            self.legend_str_len += line_total_str_len - 8;
        }

        if self.screen_size <= (self.legend_str_len + 2) {
            return fail!(format!(
                "Too many lines in the input, we are not able to output the histogram."
            ));
        }

        self.size_bar_cols = (self.screen_size - (self.legend_str_len + 1)) / 3 * 2;
        self.size_labels = self.screen_size - (self.legend_str_len + 1) - (self.size_bar_cols + 1);
        if self.size_labels <= max_str_len {
            let error_mess = format!("The labels can't be printed for the \"{}\" column (screen_size too small or precision too big), we are not able to output the histogram.", self.header);
            println!("{}\n", error_mess.yellow().bold());
            return Ok(1);
        }
        Ok(0)
    }

    fn print_title(&mut self) {
        let mut legend = "nb_lines | %     ".to_string();
        legend = " ".repeat(self.legend_str_len - 17) + &legend;

        self.header = " ".repeat(self.size_labels - UnicodeWidthStr::width(&self.header[..]))
            + &self.header
            + &" ".repeat(self.size_bar_cols);
        println!(
            "{}\u{200E}  {}",
            self.header.yellow().bold(),
            legend.yellow().bold()
        );
    }

    fn print_bar(&mut self, value: String, count: u64, j: usize) {
        let square_chars = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"];

        let value = " ".repeat(self.size_labels - value.chars().count()) + &value.to_string();
        let mut count_str = format_number(count);
        count_str = (" ".repeat(cmp::max(self.legend_str_len - 9, 8) - count_str.chars().count()))
            + &count_str;

        let mut nb_square = count as usize * self.size_bar_cols / self.longest_bar;
        let mut bar_str = square_chars[8].repeat(nb_square);

        let count_float = count as f64 * self.size_bar_cols as f64 / self.longest_bar as f64;
        let remainder = ((count_float - nb_square as f64) * 8.0) as usize;
        bar_str += square_chars[remainder % 8];
        if remainder % 8 != 0 {
            nb_square += 1;
        }
        let empty = ".".repeat(self.size_bar_cols - nb_square);

        let colored_bar_str = if j % 2 == 0 {
            bar_str.dimmed().white()
        } else {
            bar_str.white()
        };

        println!(
            "{} {}{} {} | {}",
            value,
            &colored_bar_str,
            &empty,
            &count_str,
            &format!("{:.2}", (count as f64 * 100.0 / self.lines_total as f64))
        );
    }
}
