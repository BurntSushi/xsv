use csv;

use config::{Config, Delimiter};
use rayon::slice::ParallelSliceMut;
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &str = "
Discretize selection of columns containing continuous data into bins.

The bins table is formatted as CSV data:

    field,value,lower_bound,upper_bound,count

Usage:
    xsv bins [options] [<input>]
    xsv bins --help

bins options:
    -s, --select <arg>     Select a subset of columns to compute bins
                           for. See 'xsv select --help' for the format
                           details.
    --bins <number>        Number of bins. Will default to using Freedman-Diaconis.
                           rule.
    --min <min>            Override min value.
    --max <max>            Override max value.
    -N, --no-extra         Don't include, nulls, nans and out-of-bounds counts.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the file will be considered as having no
                           headers.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_output: Option<String>,
    flag_no_extra: bool,
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
            series.add(cell, &args.flag_min, &args.flag_max);
        }
    }

    wtr.write_record(vec![
        "field",
        "value",
        "lower_bound",
        "upper_bound",
        "count",
    ])?;

    let mut formatter = util::acquire_number_formatter();

    for series in all_series.iter_mut() {
        match series.bins(args.flag_bins, &args.flag_min, &args.flag_max) {
            None => continue,
            Some(bins) => {
                let mut bins_iter = bins.iter().peekable();

                while let Some(bin) = bins_iter.next() {
                    let (lower_bound, upper_bound) = match series.data_type {
                        DataType::Float => (bin.lower_bound, bin.upper_bound),
                        DataType::Integer => (bin.lower_bound.ceil(), bin.upper_bound.ceil()),
                    };

                    let lower_bound = util::pretty_print_float(&mut formatter, lower_bound);
                    let upper_bound = util::pretty_print_float(&mut formatter, upper_bound);

                    let label_format = match bins_iter.peek() {
                        None => format!(">= {} <= {}", lower_bound, upper_bound),
                        Some(_) => format!(">= {} < {}", lower_bound, upper_bound),
                    };

                    wtr.write_record(vec![
                        &headers[series.column],
                        label_format.as_bytes(),
                        bin.lower_bound.to_string().as_bytes(),
                        bin.upper_bound.to_string().as_bytes(),
                        bin.count.to_string().as_bytes(),
                    ])?;
                }
            }
        }

        if !args.flag_no_extra && series.nans > 0 {
            wtr.write_record(vec![
                &headers[series.column],
                b"<NaN>",
                b"",
                b"",
                series.nans.to_string().as_bytes(),
            ])?;
        }

        if !args.flag_no_extra && series.nulls > 0 {
            wtr.write_record(vec![
                &headers[series.column],
                b"<null>",
                b"",
                b"",
                series.nulls.to_string().as_bytes(),
            ])?;
        }

        if !args.flag_no_extra && series.out_of_bounds > 0 {
            wtr.write_record(vec![
                &headers[series.column],
                b"<rest>",
                b"",
                b"",
                series.out_of_bounds.to_string().as_bytes(),
            ])?;
        }
    }

    Ok(wtr.flush()?)
}

fn compute_rectified_iqr(numbers: &Vec<f64>, stats: &SeriesStats) -> Option<f64> {
    if numbers.len() < 4 {
        None
    } else {
        let q1 = (numbers.len() as f64 * 0.25).floor() as usize;
        let q3 = (numbers.len() as f64 * 0.75).floor() as usize;

        let mut q1 = numbers[q1];
        let mut q3 = numbers[q3];

        // Translating to avoid non-positive issues
        let offset = stats.min().unwrap() + 1.0;

        q1 += offset;
        q3 += offset;

        let iqr = q3 - q1;

        Some(iqr)
    }
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
}

#[derive(Debug)]
struct Bin {
    lower_bound: f64,
    upper_bound: f64,
    count: usize,
}

#[derive(Debug)]
enum DataType {
    Integer,
    Float,
}

#[derive(Debug)]
struct Series {
    column: usize,
    numbers: Vec<f64>,
    count: usize,
    nans: usize,
    nulls: usize,
    out_of_bounds: usize,
    data_type: DataType,
}

impl Series {
    pub fn new(column: usize) -> Self {
        Series {
            column,
            numbers: Vec::new(),
            count: 0,
            nans: 0,
            nulls: 0,
            out_of_bounds: 0,
            data_type: DataType::Integer,
        }
    }

    pub fn add(&mut self, cell: &str, min: &Option<f64>, max: &Option<f64>) {
        self.count += 1;

        let cell = cell.trim();

        if cell.is_empty() {
            self.nulls += 1;
            return;
        }

        match cell.parse::<f64>() {
            Ok(float) => {
                if let Some(m) = min {
                    if float < *m {
                        self.out_of_bounds += 1;
                        return;
                    }
                } else if let Some(m) = max {
                    if float > *m {
                        self.out_of_bounds += 1;
                        return;
                    }
                }

                if float.fract() != 0.0 {
                    self.data_type = DataType::Float;
                }

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
        usize::min((self.len() as f64).sqrt().ceil() as usize, 50)
    }

    pub fn freedman_diaconis(&mut self, width: f64, stats: &SeriesStats) -> Option<usize> {
        self.numbers.par_sort_unstable_by(|a, b| a.total_cmp(b));

        compute_rectified_iqr(&self.numbers, stats).and_then(|iqr| {
            if iqr == 0.0 {
                return None;
            }

            let bin_width = 2.0 * (iqr / (self.numbers.len() as f64).cbrt());

            Some((width / bin_width).ceil() as usize)
        })
    }

    pub fn optimal_bin_count(&mut self, width: f64, stats: &SeriesStats) -> usize {
        usize::max(
            2,
            self.freedman_diaconis(width, stats)
                .unwrap_or_else(|| self.naive_optimal_bin_count()),
        )
    }

    pub fn bins(
        &mut self,
        count: Option<usize>,
        min: &Option<f64>,
        max: &Option<f64>,
    ) -> Option<Vec<Bin>> {
        if self.len() < 1 {
            return None;
        }

        let stats = self.stats();

        let min = min.unwrap_or_else(|| stats.min().unwrap());
        let max = max.unwrap_or_else(|| stats.max().unwrap());
        let width = (max - min).abs();

        let count = count.unwrap_or_else(|| self.optimal_bin_count(width, &stats));
        let mut bins: Vec<Bin> = Vec::with_capacity(count);

        let cell_width = width / count as f64;

        let mut lower_bound = min;

        for _ in 0..count {
            let upper_bound = f64::min(lower_bound + cell_width, max);

            bins.push(Bin {
                lower_bound,
                upper_bound,
                count: 0,
            });

            lower_bound = upper_bound;
        }

        for n in self.numbers.iter() {
            let mut bin_index = ((n - min) / cell_width).floor() as usize;

            // Exception to include max in last bin
            if bin_index == bins.len() {
                bin_index -= 1;
            }

            bins[bin_index].count += 1;
        }

        Some(bins)
    }
}
