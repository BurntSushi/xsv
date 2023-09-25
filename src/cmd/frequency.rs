use std::cmp;
use std::fs;
use std::io;

use channel;
use colored::Colorize;
use csv;
use stats::{merge_all, Frequencies};
use threadpool::ThreadPool;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

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
                           items. Set to '0' to disable a limit.
                           [default: 10]
    -a, --asc              Sort the frequency tables in ascending order by
                           count. The default is descending order.
    --no-nulls             Don't include NULLs in the frequency table.
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
    --domain-max <arg>     The maximum value for a bar in the histogram. If set to 'max',
                           the maximum possible size for a bar will be the maximum cardinality
                           of all bars in the histogram. If set to 'total', the maximum
                           possible size for a bar will be the sum of the cardinalities
                           of the bars (default).

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be included
                           in the frequency table. Additionally, the 'field'
                           column will be 1-based indices instead of header
                           names.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Clone, Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_limit: usize,
    flag_asc: bool,
    flag_no_nulls: bool,
    flag_jobs: usize,
    flag_pretty: bool,
    flag_screen_size: Option<usize>,
    flag_domain_max: Option<DomainMax>,
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

    if !args.flag_pretty && (args.flag_domain_max.is_some() || args.flag_screen_size.is_some()) {
        return fail!("`--bar-max` and `--screen-size` can only be used with `--histogram`");
    }

    let args_clone = args.clone();

    let mut wtr = Config::new(&args.flag_output).writer()?;
    let (headers, tables, lines_total) = match args.rconfig().indexed()? {
        Some(ref mut idx) if args.njobs() > 1 => args.parallel_ftables(idx),
        _ => args.sequential_ftables(),
    }?;

    if args.flag_pretty {
        let screen_size = args.flag_screen_size.unwrap_or(0);
        let domain_max = args.flag_domain_max.unwrap_or(DomainMax::Total);

        let mut bar = Bar {
            header: String::new(),
            screen_size,
            lines_total,
            lines_total_str: String::new(),
            legend_str_len: 0,
            size_bar_cols: 0,
            size_labels: 0,
            longest_bar: 0,
        };

        match bar.update_sizes() {
            Ok(_) => {}
            Err(e) => return fail!(e),
        };

        if args.flag_output.is_some() {
            wtr.write_record(vec!["field", "value", "count"])?;
        }

        let head_ftables = headers.into_iter().zip(tables);
        for (i, (header, ftab)) in head_ftables.enumerate() {
            let vec_ftables = args_clone.counts(&ftab);
            if vec_ftables.is_empty() {
                let error_message = format!(
                    "The histogram for the column \"{}\" is empty.",
                    String::from_utf8(header.to_vec()).unwrap()
                );
                println!("{}\n", error_message.yellow().bold());
                continue;
            }

            let header_file = if rconfig.no_headers {
                (i + 1).to_string().into_bytes()
            } else {
                header.to_vec()
            };
            bar.header = cut_properly(
                String::from_utf8(header_file.clone()).unwrap(),
                bar.size_labels,
            );
            bar.print_title();

            bar.longest_bar = if domain_max == DomainMax::Max {
                if args.flag_asc {
                    vec_ftables[vec_ftables.len() - 1].1 as usize
                } else {
                    vec_ftables[0].1 as usize
                }
            } else {
                lines_total as usize
            };

            let mut lines_done = 0;

            for (j, (value, count)) in vec_ftables.into_iter().enumerate() {
                bar.print_bar(
                    cut_properly(String::from_utf8(value.clone()).unwrap(), bar.size_labels),
                    count,
                    j,
                );
                lines_done += count;

                if args.flag_output.is_some() {
                    let count_file = count.to_string();
                    let row = vec![&*header_file, &*value, count_file.as_bytes()];
                    wtr.write_record(row)?;
                }
            }

            let nb_categories_total = ftab.cardinality();
            let nb_categories_done = if args.flag_limit != 0 {
                cmp::min(args.flag_limit as u64, nb_categories_total)
            } else {
                nb_categories_total
            };
            let resume = " ".repeat(bar.size_labels + 1)
                + &"Histogram for "
                + &format_number(lines_done)
                + "/"
                + &bar.lines_total_str
                + " lines and "
                + &format_number(nb_categories_done)
                + "/"
                + &format_number(nb_categories_total)
                + " categories.";
            println!("{}\n", resume.yellow().bold());
        }
    } else {
        wtr.write_record(vec!["field", "value", "count"])?;
        let head_ftables = headers.into_iter().zip(tables);
        for (i, (header, ftab)) in head_ftables.enumerate() {
            let header = if rconfig.no_headers {
                (i + 1).to_string().into_bytes()
            } else {
                header.to_vec()
            };
            for (value, count) in args_clone.counts(&ftab).into_iter() {
                let count = count.to_string();
                let row = vec![&*header, &*value, count.as_bytes()];
                wtr.write_record(row)?;
            }
        }
    }
    Ok(())
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
        counts
            .into_iter()
            .map(|(bs, c)| {
                if b"" == &**bs {
                    (b"<NULL>"[..].to_vec(), c)
                } else {
                    (bs.clone(), c)
                }
            })
            .collect()
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
                } else if !self.flag_no_nulls {
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

fn cut_properly(value: String, size_labels: usize) -> String {
    let mut value = value.replace('\n', " ");
    value = value.replace('\r', " ");
    value = value.replace('\t', " ");
    value = value.replace('\u{200F}', "");
    value = value.replace('\u{200E}', "");
    let mut value_str_len = UnicodeWidthStr::width(&value[..]);
    if value_str_len >= size_labels {
        let moved_value = value.clone();
        let value_chars =
            UnicodeSegmentation::graphemes(&moved_value[..], true).collect::<Vec<&str>>();
        let mut it = cmp::min(size_labels - 1, value_chars.len());
        while value_str_len >= size_labels {
            value = value_chars[0..it].join("");
            value_str_len = UnicodeWidthStr::width(&value[..]);
            it -= 1;
        }
        value += "…";
    }

    value
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
    fn update_sizes(&mut self) -> CliResult<()> {
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

        Ok(())
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

        let value =
            " ".repeat(self.size_labels - UnicodeWidthStr::width(&value[..])) + &value.to_string();
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
            "{}\u{200E} {}{} {} | {}",
            value,
            &colored_bar_str,
            &empty,
            &count_str,
            &format!("{:.2}", (count as f64 * 100.0 / self.lines_total as f64))
        );
    }
}
