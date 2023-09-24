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
    flag_select: SelectColumns,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_output: Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = conf.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let headers = rdr.byte_headers()?;
    let sel = conf.selection(&headers)?;

    let mut all_series: Vec<Series> = sel.iter().map(|i| Series::new(*i)).collect();

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        for (cell, series) in sel.select(&record).zip(all_series.iter_mut()) {
            let cell = std::str::from_utf8(cell).unwrap();
            series.add(cell);
        }
    }

    println!("{:?}", all_series);

    Ok(wtr.flush()?)
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
}
