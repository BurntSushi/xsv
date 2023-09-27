use colored::{self, Colorize};
use csv;

use config::{Config, Delimiter};
use unicode_width::UnicodeWidthStr;
use util;
use CliResult;

const TRAILING_COLS: usize = 4;
const PER_CELL_PADDING_COLS: usize = 3;

static USAGE: &str = "
Preview CSV data in the terminal in a human-friendly way with aligned columns,
shiny colors & all.

When using the -e/--expand flag, pipe into \"less -SR\" if you need to page the
result, and use -C/--force-colors not to lose the colors:

    $ xsv view -eC file.csv | less -SR

Usage:
    xsv view [options] [<input>]
    xsv view --help

view options:
    --cols <num>           Width of the graph in terminal columns, i.e. characters.
                           Defaults to using all your terminal's width or 80 if
                           terminal's size cannot be found (i.e. when piping to file).
    -C, --force-colors     Force colors even if output is not supposed to be able to
                           handle them.
    -l, --limit <number>   Maximum of lines of files to read into memory. Set
                           to <=0 to disable a limit. [default: 100].
    -R, --rainbow          Alternating colors for columns, rather than color by value type.
    -e, --expand           Expand the table so that in can be easily piped to
                           a pager such as \"less\", with no with constraints.

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not considered as being
                           the file header.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_cols: Option<usize>,
    flag_delimiter: Option<Delimiter>,
    flag_no_headers: bool,
    flag_force_colors: bool,
    flag_limit: isize,
    flag_rainbow: bool,
    flag_expand: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    if args.flag_force_colors {
        colored::control::set_override(true);
    }

    let cols = util::acquire_term_cols(&args.flag_cols);

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;

    let potential_headers = prepend(rdr.headers()?, "⁂");
    let mut headers: Vec<String> = Vec::new();

    for (i, header) in potential_headers.iter().enumerate() {
        let header = match rconfig.no_headers {
            true => i.to_string(),
            false => header.to_string(),
        };
        headers.push(header);
    }

    let mut all_records_buffered = false;

    let records = if args.flag_limit > 0 {
        let limit = args.flag_limit as usize;

        let mut r_iter = rdr.into_records().enumerate();

        let mut records: Vec<csv::StringRecord> = Vec::new();

        loop {
            match r_iter.next() {
                None => break,
                Some((i, record)) => {
                    records.push(prepend(&record?, &i.to_string()));

                    if records.len() == limit {
                        break;
                    }
                }
            };
        }

        if r_iter.next().is_none() {
            all_records_buffered = true;
        }

        records
    } else {
        all_records_buffered = true;
        rdr.into_records().collect::<Result<Vec<_>, _>>()?
    };

    let max_column_widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            usize::max(
                h.width(),
                records
                    .iter()
                    .map(|c| match c[i].width() {
                        0 => 7, // NOTE: taking <empty> into account
                        v => v,
                    })
                    .max()
                    .unwrap_or(0),
            )
        })
        .collect();

    let (columns_fitting_in_budget, column_widths) =
        find_best_column_widths(cols, &max_column_widths, args.flag_expand);

    let all_columns_shown = columns_fitting_in_budget == column_widths.len();

    let mut formatter = util::acquire_number_formatter();

    let mut print_info = || {
        let pretty_records_len = util::pretty_print_float(&mut formatter, records.len() as f64);
        let pretty_headers_len =
            util::pretty_print_float(&mut formatter, (headers.len() - 1) as f64);
        let pretty_displayed_headers_len =
            util::pretty_print_float(&mut formatter, (columns_fitting_in_budget - 1) as f64);

        println!(
            "Displaying {} col{} from {} of {}",
            if all_columns_shown {
                format!("{}", pretty_headers_len.cyan())
            } else {
                format!(
                    "{}/{}",
                    pretty_displayed_headers_len.cyan(),
                    pretty_headers_len.cyan(),
                )
            },
            if headers.len() > 2 { "s" } else { "" },
            if all_records_buffered {
                format!("{} rows", pretty_records_len.cyan())
            } else {
                format!("{} first rows", pretty_records_len.cyan())
            },
            match &args.arg_input {
                Some(filename) => filename,
                None => "<stdin>",
            }
            .dimmed()
        )
    };

    let hr_cols: usize = if args.flag_expand {
        column_widths
            .iter()
            .take(columns_fitting_in_budget)
            .sum::<usize>()
            + (columns_fitting_in_budget - 1) * PER_CELL_PADDING_COLS
            + if all_columns_shown { 0 } else { TRAILING_COLS }
    } else {
        cols
    };

    let print_horizontal_ruler = || {
        println!("{}", "-".repeat(hr_cols));
    };

    let print_row = |row: Vec<colored::ColoredString>| {
        for (i, cell) in row.iter().enumerate() {
            if i != 0 {
                print!(" | ");
            }

            print!("{}", cell);
        }

        if !all_columns_shown {
            print!(" | …");
        }

        print!("\n");
    };

    let print_headers = || {
        print_horizontal_ruler();

        let headers_row: Vec<colored::ColoredString> = headers
            .iter()
            .take(columns_fitting_in_budget)
            .enumerate()
            .map(|(i, h)| {
                let cell = util::unicode_aware_rpad_with_ellipsis(h, column_widths[i], " ");

                if i == 0 {
                    cell.dimmed()
                } else {
                    cell.bold()
                }
            })
            .collect();

        print_row(headers_row);
        print_horizontal_ruler();
    };

    println!();
    print_info();
    print_headers();

    for record in records.iter() {
        let row: Vec<colored::ColoredString> = record
            .iter()
            .take(columns_fitting_in_budget)
            .enumerate()
            .map(|(i, cell)| {
                let cell = match cell.trim() {
                    "" => "<empty>",
                    _ => cell,
                };

                let allowed_width = column_widths[i];

                let colorizer = if args.flag_rainbow {
                    util::colorizer_by_rainbow(i)
                } else {
                    util::colorizer_by_type(cell)
                };

                let cell = util::unicode_aware_rpad_with_ellipsis(cell, allowed_width, " ");

                if i == 0 {
                    cell.dimmed()
                } else {
                    util::colorize(&colorizer, &cell)
                }
            })
            .collect();

        print_row(row);
    }

    if !all_records_buffered {
        let row: Vec<colored::ColoredString> = headers
            .iter()
            .take(columns_fitting_in_budget)
            .enumerate()
            .map(|(i, _)| {
                let allowed_width = column_widths[i];
                util::unicode_aware_rpad_with_ellipsis("…", allowed_width, " ").normal()
            })
            .collect();

        print_row(row);
    }

    print_headers();
    print_info();
    println!();

    Ok(())
}

fn prepend(record: &csv::StringRecord, item: &str) -> csv::StringRecord {
    let mut new_record = csv::StringRecord::new();
    new_record.push_field(item);
    new_record.extend(record);

    new_record
}

fn adjust_column_widths(widths: &Vec<usize>, max_width: usize) -> Vec<usize> {
    widths.iter().map(|m| usize::min(*m, max_width)).collect()
}

// NOTE: greedy way to find best ratio for columns
// We basically test a range of dividers based on the number of columns in the
// CSV file and we try to find the organization optimizing the number of columns
// fitting perfectly, then the number of columns displayed.
fn find_best_column_widths(
    cols: usize,
    max_column_widths: &Vec<usize>,
    expand: bool,
) -> (usize, Vec<usize>) {
    if expand {
        // NOTE: we keep max column size to 3/4 of current screen
        return (
            max_column_widths.len(),
            adjust_column_widths(max_column_widths, ((cols as f64) * 0.75) as usize),
        );
    }

    let mut attempts: Vec<(usize, usize, Vec<usize>)> = Vec::new();

    // TODO: this code can be greatly optimized and early break
    for divider in 1..=max_column_widths.len() {
        let mut widths = adjust_column_widths(max_column_widths, cols / divider);

        let mut col_budget = cols - TRAILING_COLS;
        let mut columns_fitting_in_budget: usize = 0;

        for column_width in widths.iter_mut() {
            if col_budget == 0 {
                break;
            }

            if *column_width + PER_CELL_PADDING_COLS > col_budget {
                *column_width = col_budget;
                columns_fitting_in_budget += 1;
                break;
            }
            col_budget -= *column_width + PER_CELL_PADDING_COLS;
            columns_fitting_in_budget += 1;
        }

        let columns_fitting_perfectly = widths
            .iter()
            .zip(max_column_widths.iter())
            .take(columns_fitting_in_budget)
            .filter(|(a, b)| a == b)
            .count();

        attempts.push((columns_fitting_perfectly, columns_fitting_in_budget, widths));
    }

    let best_attempt = attempts.into_iter().max().unwrap();

    (best_attempt.1, best_attempt.2)
}
