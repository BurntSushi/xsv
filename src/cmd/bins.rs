use csv;

use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &str = "
TODO...

Usage:
    xsv bins [options] [<input>]
    xsv bins --help

bins options:
    -s, --select <arg>     Select a subset of columns to compute bins
                           for. See 'xsv select --help' for the format
                           details.
    --bins <number>        Number of bins. Will default to ceil(sqrt(n)).
    --min <min>            Hardcoded min value.
    --max <max>            Hardcoded max value.
    --nulls                Include nulls count in output.
    --nans                 Include nans count in outpit.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be included in
                           the count.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

// TODO: normalize, scale etc.

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_output: Option<String>,
    flag_nulls: bool,
    flag_nans: bool,
    flag_bins: Option<usize>,
    flag_min: Option<f64>,
    flag_max: Option<f64>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = conf.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = conf.selection(&headers)?;

    let mut all_series: Vec<Series> = sel.iter().map(|i| Series::new(*i)).collect();

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        for (cell, series) in sel.select(&record).zip(all_series.iter_mut()) {
            let cell = std::str::from_utf8(cell).unwrap();
            series.add(cell);
        }
    }

    wtr.write_record(vec![
        "field",
        "value",
        "lower_bound",
        "upper_bound",
        "count",
    ])?;

    for series in all_series {
        match series.bins(args.flag_bins) {
            None => continue,
            Some(bins) => {
                for bin in bins {
                    wtr.write_record(vec![
                        &headers[series.column],
                        format!(">= {}, < {}", bin.lower_bound, bin.upper_bound).as_bytes(),
                        bin.lower_bound.to_string().as_bytes(),
                        bin.upper_bound.to_string().as_bytes(),
                        bin.count.to_string().as_bytes(),
                    ])?;
                }
            }
        }

        if args.flag_nans && series.nans > 0 {
            wtr.write_record(vec![
                &headers[series.column],
                b"NaN",
                b"",
                b"",
                series.nans.to_string().as_bytes(),
            ])?;
        }

        if args.flag_nulls && series.nulls > 0 {
            wtr.write_record(vec![
                &headers[series.column],
                b"NULL",
                b"",
                b"",
                series.nulls.to_string().as_bytes(),
            ])?;
        }
    }

    Ok(wtr.flush()?)
}

#[derive(Debug)]
struct SeriesStats {
    extent: Option<(f64, f64)>,
}

impl SeriesStats {
    pub fn min(&self) -> Option<f64> {
        match self.extent {
            None => None,
            Some(extent) => Some(extent.0),
        }
    }

    pub fn max(&self) -> Option<f64> {
        match self.extent {
            None => None,
            Some(extent) => Some(extent.1),
        }
    }

    pub fn width(&self) -> Option<f64> {
        match self.extent {
            None => None,
            Some(extent) => Some(extent.1 - extent.0),
        }
    }
}

#[derive(Debug)]
struct Bin {
    lower_bound: f64,
    upper_bound: f64,
    count: usize,
}

#[derive(Debug)]
struct Series {
    column: usize,
    numbers: Vec<f64>,
    count: usize,
    nans: usize,
    nulls: usize,
}

impl Series {
    pub fn new(column: usize) -> Self {
        Series {
            column,
            numbers: Vec::new(),
            count: 0,
            nans: 0,
            nulls: 0,
        }
    }

    pub fn add(&mut self, cell: &str) {
        self.count += 1;

        let cell = cell.trim();

        if cell.is_empty() {
            self.nulls += 1;
            return;
        }

        match cell.parse::<f64>() {
            Ok(float) => {
                self.numbers.push(float);
            }
            Err(_) => {
                self.nans += 1;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.numbers.len()
    }

    pub fn stats(&self) -> SeriesStats {
        let mut extent: Option<(f64, f64)> = None;

        for n in self.numbers.iter() {
            let n = *n;

            extent = match extent {
                None => Some((n, n)),
                Some(m) => Some((f64::min(n, m.0), f64::max(n, m.1))),
            };
        }

        SeriesStats { extent }
    }

    pub fn naive_optimal_bin_count(&self) -> usize {
        (self.len() as f64).sqrt().ceil() as usize
    }

    pub fn bins(&self, count: Option<usize>) -> Option<Vec<Bin>> {
        let stats = self.stats();
        let count = count.unwrap_or(self.naive_optimal_bin_count());

        let mut bins: Vec<Bin> = Vec::with_capacity(count);

        let width = match stats.width() {
            None => return None,
            Some(w) => w,
        };

        let mut lower_bound = stats.min().unwrap();

        for _ in 0..count {
            let upper_bound = lower_bound + width / count as f64;

            bins.push(Bin {
                lower_bound,
                upper_bound,
                count: 0,
            });

            lower_bound = upper_bound;
        }

        for n in self.numbers.iter() {
            let bin_index = ((n / width).floor() as usize) % count;
            bins[bin_index].count += 1;
        }

        Some(bins)
    }
}
