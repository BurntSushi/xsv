use std::collections::BTreeMap;

use colored::Colorize;
use csv;
use numfmt::Formatter;
use termsize;
use unicode_width::UnicodeWidthStr;

use config::{Config, Delimiter};
use util;
use CliResult;

// TODO: log scales etc.

static USAGE: &str = "
Print a horizontal histogram for the given CSV file with each line
representing a bar in the resulting graph.

This command is very useful when used in conjunction with the `frequency` or `bins`
command.

Usage:
    xsv hist [options] [<input>]
    xsv hist --help

hist options:
    --field <name>           Name of the field column. I.e. the one containing
                             the represented value (remember this command can
                             print several histograms). [default: field].
    --label <name>           Name of the label column. I.e. the one containing the
                             label for a single bar of the histogram. [default: value].
    --value <name>           Name of the count column. I.e. the one containing the value
                             for each bar. [default: count].
    --cols <num>             Width of the graph in terminal columns, i.e. characters.
                             Defaults to using all your terminal's width or 80 if
                             terminal's size cannot be found (i.e. when piping to file).
    -m, --domain-max <type>  If \"max\" max bar length will be scaled to the
                             max bar value. If \"sum\", max bar length will be scaled to
                             the sum of bar values (i.e. sum of bar lengths will be 100%).
                             [default: max]

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the file will be considered as having no
                           headers.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

fn find_column_index(headers: &csv::ByteRecord, name: &str) -> Result<usize, String> {
    let index = headers
        .iter()
        .enumerate()
        .find(|(_, h)| *h == name.as_bytes())
        .map(|r| r.0);

    match index {
        None => Err(format!("could not find column \"{}\"", name)),
        Some(i) => Ok(i),
    }
}

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_field: String,
    flag_label: String,
    flag_value: String,
    flag_cols: Option<usize>,
    flag_domain_max: String,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let conf = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = conf.reader()?;
    let headers = rdr.byte_headers()?;

    let field_pos = find_column_index(headers, &args.flag_field)?;
    let label_pos = find_column_index(headers, &args.flag_label)?;
    let value_pos = find_column_index(headers, &args.flag_value)?;

    let mut histograms = Histograms::new();

    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {
        let field = record[field_pos].to_string();
        let label = record[label_pos].to_string();
        let value = record[value_pos]
            .parse::<f64>()
            .map_err(|_| "could not parse value")?;

        histograms.add(field, label, value);
    }

    let mut formatter = util::acquire_number_formatter();

    let cols: usize = match args.flag_cols {
        None => match termsize::get() {
            None => 80,
            Some(size) => size.cols as usize,
        },
        Some(c) => c,
    };

    for histogram in histograms.iter() {
        let sum = histogram.sum();

        if histogram.len() == 0 {
            continue;
        }

        let domain_max = match args.flag_domain_max.as_str() {
            "max" => histogram.max().unwrap(),
            "sum" => histogram.sum(),
            _ => return fail!("unknown --domain-max. Should be one of \"sum\", \"max\"."),
        };

        println!(
            "\nHistogram for field {} (counting {}, max: {}):\n",
            histogram.field.green(),
            util::pretty_print_float(&mut formatter, sum).cyan(),
            util::pretty_print_float(&mut formatter, histogram.max().unwrap()).cyan()
        );

        let pct_cols: usize = 8;

        if cols < 30 {
            return fail!("You did not provide enough --cols to print anything!");
        }

        let remaining_cols = cols - pct_cols;
        let count_cols = usize::min(
            (remaining_cols as f64 * 0.2).floor() as usize,
            histogram.value_max_width(&mut formatter).unwrap(),
        );
        let label_cols = usize::min(
            (remaining_cols as f64 * 0.3).floor() as usize,
            histogram.label_max_width().unwrap(),
        );
        let bar_cols = remaining_cols - count_cols - label_cols - 4;

        let mut odd = false;

        for bar in histogram.bars() {
            let bar_width =
                from_domain_to_range(bar.value, (0.0, domain_max), (0.0, bar_cols as f64));

            let mut bar_as_chars =
                util::unicode_aware_rpad(&create_bar(bar_width), bar_cols, " ").clear();

            if odd {
                bar_as_chars = bar_as_chars.dimmed();
                odd = false;
            } else {
                odd = true;
            }

            let label = util::unicode_aware_rpad_with_ellipsis(&bar.label, label_cols, " ");
            let label = match bar.label.as_str() {
                "<REST>" | "<NULL>" | "<NaN>" => label.dimmed(),
                _ => label.normal(),
            };

            println!(
                "{} |{} {}|{}|",
                label,
                util::unicode_aware_rpad_with_ellipsis(
                    &util::pretty_print_float(&mut formatter, bar.value),
                    count_cols,
                    " "
                )
                .cyan(),
                format!("{:>6.2}%", bar.value / sum * 100.0).purple(),
                bar_as_chars
            );
        }
    }

    Ok(())
}

fn from_domain_to_range(x: f64, domain: (f64, f64), range: (f64, f64)) -> f64 {
    let domain_width = (domain.1 - domain.0).abs();
    let pct = (x - domain.0) / domain_width;

    let range_widht = (range.1 - range.0).abs();

    pct * range_widht + range.0
}

fn create_bar(width: f64) -> String {
    let chars = ["▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"];
    // let chars = ["╸", "╾", "━"];
    let f = width.fract();

    if f < f64::EPSILON {
        chars[chars.len() - 1].repeat(width as usize)
    } else {
        let mut string = chars[chars.len() - 1].repeat(width.floor() as usize);

        let padding = chars[((chars.len() - 1) as f64 * f).floor() as usize];
        string.push_str(padding);

        string
    }
}

#[derive(Debug)]
struct Bar {
    label: String,
    value: f64,
}

#[derive(Debug)]
struct Histogram {
    field: String,
    bars: Vec<Bar>,
}

impl Histogram {
    fn len(&self) -> usize {
        self.bars.len()
    }

    fn max(&self) -> Option<f64> {
        let mut max: Option<f64> = None;

        for bar in self.bars.iter() {
            let n = bar.value;

            max = match max {
                None => Some(n),
                Some(m) => Some(f64::max(n, m)),
            };
        }

        max
    }

    fn sum(&self) -> f64 {
        self.bars.iter().map(|bar| bar.value).sum()
    }

    fn bars(&self) -> impl Iterator<Item = &Bar> {
        self.bars.iter()
    }

    fn label_max_width(&self) -> Option<usize> {
        self.bars.iter().map(|bar| bar.label.width_cjk()).max()
    }

    fn value_max_width(&self, fmt: &mut Formatter) -> Option<usize> {
        self.bars
            .iter()
            .map(|bar| util::pretty_print_float(fmt, bar.value).len())
            .max()
    }
}

#[derive(Debug)]
struct Histograms {
    histograms: BTreeMap<String, Histogram>,
}

impl Histograms {
    pub fn new() -> Self {
        Histograms {
            histograms: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, field: String, label: String, value: f64) {
        self.histograms
            .entry(field.clone())
            .and_modify(|h| {
                h.bars.push(Bar {
                    label: label.clone(),
                    value,
                })
            })
            .or_insert_with(|| Histogram {
                field: field,
                bars: vec![Bar { label, value }],
            });
    }

    pub fn iter(&self) -> impl Iterator<Item = &Histogram> {
        self.histograms.values()
    }
}
