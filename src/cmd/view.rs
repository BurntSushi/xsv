use std::io::{self, Write};

use colored::{self, Colorize};
use csv;

use config::{Config, Delimiter};
use unicode_width::UnicodeWidthStr;
use util;
use CliResult;

const TRAILING_COLS: usize = 8;
const PER_CELL_PADDING_COLS: usize = 3;
const HEADERS_ROWS: usize = 8;

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

    let output = io::stdout();

    let cols = util::acquire_term_cols(&args.flag_cols);
    let rows = util::acquire_term_rows();

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;

    let potential_headers = prepend(rdr.headers()?, "-");
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
        rdr.into_records()
            .enumerate()
            .map(|(i, r)| r.map(|record| prepend(&record, &i.to_string())))
            .collect::<Result<Vec<_>, _>>()?
    };

    let need_to_repeat_headers = match rows {
        None => true,
        Some(r) => records.len() + HEADERS_ROWS > r,
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

    let displayed_columns = infer_best_column_display(cols, &max_column_widths, args.flag_expand);
    let all_columns_shown = displayed_columns.len() == headers.len();

    let mut formatter = util::acquire_number_formatter();

    let mut write_info = || -> Result<(), io::Error> {
        let pretty_records_len = util::pretty_print_float(&mut formatter, records.len());
        let pretty_headers_len = util::pretty_print_float(&mut formatter, headers.len() - 1);
        let pretty_displayed_headers_len =
            util::pretty_print_float(&mut formatter, displayed_columns.len() - 1);

        writeln!(
            &output,
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
        )?;

        Ok(())
    };

    enum HRPosition {
        Top,
        Middle,
        Bottom,
    }

    let write_horizontal_ruler = |pos: HRPosition| -> Result<(), io::Error> {
        let mut s = String::new();

        s.push(match pos {
            HRPosition::Bottom => '┌',
            HRPosition::Top => '└',
            HRPosition::Middle => '├',
        });

        displayed_columns.iter().enumerate().for_each(|(i, col)| {
            s.push_str(&"─".repeat(col.allowed_width + 2));

            if i == displayed_columns.len() - 1 {
                return;
            }

            if !all_columns_shown && Some(i) == displayed_columns.split_point() {
                s.push(match pos {
                    HRPosition::Bottom => '┬',
                    HRPosition::Top => '┴',
                    HRPosition::Middle => '┼',
                });

                s.push_str(&"─".repeat(3));
            }

            s.push(match pos {
                HRPosition::Bottom => '┬',
                HRPosition::Top => '┴',
                HRPosition::Middle => '┼',
            });
        });

        s.push(match pos {
            HRPosition::Bottom => '┐',
            HRPosition::Top => '┘',
            HRPosition::Middle => '┤',
        });

        writeln!(&output, "{}", s.dimmed())?;

        Ok(())
    };

    let write_row = |row: Vec<colored::ColoredString>| -> Result<(), io::Error> {
        write!(&output, "{}", "│ ".dimmed())?;

        for (i, cell) in row.iter().enumerate() {
            if i != 0 {
                write!(&output, "{}", " │ ".dimmed())?;
            }

            write!(&output, "{}", cell)?;

            if !all_columns_shown && Some(i) == displayed_columns.split_point() {
                write!(&output, "{}", " │ …".dimmed())?;
            }
        }

        write!(&output, "{}", " │".dimmed())?;
        write!(&output, "\n")?;

        Ok(())
    };

    let write_headers = |above: bool| -> Result<(), io::Error> {
        write_horizontal_ruler(if above {
            HRPosition::Bottom
        } else {
            HRPosition::Middle
        })?;

        let headers_row: Vec<colored::ColoredString> = displayed_columns
            .iter()
            .map(|col| (col, &headers[col.index]))
            .enumerate()
            .map(|(i, (col, h))| {
                let cell = util::unicode_aware_rpad_with_ellipsis(h, col.allowed_width, " ");

                if i == 0 {
                    cell.dimmed()
                } else {
                    cell.bold()
                }
            })
            .collect();

        write_row(headers_row)?;
        write_horizontal_ruler(if above {
            HRPosition::Middle
        } else {
            HRPosition::Top
        })?;

        Ok(())
    };

    writeln!(&output)?;
    write_info()?;
    write_headers(true)?;

    for record in records.iter() {
        let row: Vec<colored::ColoredString> = displayed_columns
            .iter()
            .map(|col| (col, &record[col.index]))
            .enumerate()
            .map(|(i, (col, cell))| {
                let cell = match cell.trim() {
                    "" => "<empty>",
                    _ => cell,
                };

                let colorizer = if args.flag_rainbow {
                    util::colorizer_by_rainbow(i, cell)
                } else {
                    util::colorizer_by_type(cell)
                };

                let cell = util::unicode_aware_rpad_with_ellipsis(cell, col.allowed_width, " ");

                if i == 0 {
                    cell.dimmed()
                } else {
                    util::colorize(&colorizer, &cell)
                }
            })
            .collect();

        write_row(row)?;
    }

    if !all_records_buffered {
        let row: Vec<colored::ColoredString> = displayed_columns
            .iter()
            .map(|col| util::unicode_aware_rpad_with_ellipsis("…", col.allowed_width, " ").dimmed())
            .collect();

        write_row(row)?;
    }

    if need_to_repeat_headers {
        write_headers(false)?;
        write_info()?;
        writeln!(&output)?;
    } else {
        write_horizontal_ruler(HRPosition::Top)?;
        writeln!(&output)?;
    }

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

#[derive(Debug)]
struct DisplayedColumn {
    index: usize,
    allowed_width: usize,
    max_width: usize,
}

#[derive(Debug)]
struct DisplayedColumns {
    left: Vec<DisplayedColumn>,
    // NOTE: columns are inserted into right in reversed order
    right: Vec<DisplayedColumn>,
}

impl DisplayedColumns {
    fn new() -> Self {
        DisplayedColumns {
            left: Vec::new(),
            right: Vec::new(),
        }
    }

    fn split_point(&self) -> Option<usize> {
        self.left.last().map(|col| col.index)
    }

    fn from_widths(widths: Vec<usize>) -> Self {
        let left = widths
            .iter()
            .copied()
            .enumerate()
            .map(|(i, w)| DisplayedColumn {
                index: i,
                allowed_width: w,
                max_width: w,
            })
            .collect::<Vec<_>>();

        DisplayedColumns {
            left,
            right: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.left.len() + self.right.len()
    }

    fn fitting_count(&self) -> usize {
        self.iter()
            .filter(|col| col.allowed_width == col.max_width)
            .count()
    }

    fn push(&mut self, left: bool, index: usize, allowed_width: usize, max_width: usize) {
        let col = DisplayedColumn {
            index,
            allowed_width,
            max_width,
        };

        if left {
            self.left.push(col);
        } else {
            self.right.push(col);
        }
    }

    fn iter(&self) -> DisplayedColumnsIter {
        DisplayedColumnsIter {
            iter_left: self.left.iter(),
            iter_right: self.right.iter(),
        }
    }
}

struct DisplayedColumnsIter<'a> {
    iter_left: std::slice::Iter<'a, DisplayedColumn>,
    iter_right: std::slice::Iter<'a, DisplayedColumn>,
}

impl<'a> Iterator for DisplayedColumnsIter<'a> {
    type Item = &'a DisplayedColumn;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter_left
            .next()
            .or_else(|| self.iter_right.next_back())
    }
}

// NOTE: greedy way to find best ratio for columns
// We basically test a range of dividers based on the number of columns in the
// CSV file and we try to find the organization optimizing the number of columns
// fitting perfectly, then the number of columns displayed.
fn infer_best_column_display(
    cols: usize,
    max_column_widths: &Vec<usize>,
    expand: bool,
) -> DisplayedColumns {
    if expand {
        // NOTE: we keep max column size to 3/4 of current screen
        return DisplayedColumns::from_widths(adjust_column_widths(
            max_column_widths,
            ((cols as f64) * 0.75) as usize,
        ));
    }

    let mut attempts: Vec<DisplayedColumns> = Vec::new();

    // TODO: this code can be greatly optimized and early break
    // NOTE: here we iteratively test for a range of max width being a division
    // of the term width. But we could also test for an increasing number of
    // columns, all while respecting the width proportion of each column compared
    // to the other selected ones.
    for divider in 1..=max_column_widths.len() {
        let mut attempt = DisplayedColumns::new();

        // If we don't have reasonable space we break
        if cols / divider <= 3 {
            break;
        }

        let widths = adjust_column_widths(max_column_widths, cols / divider);

        let mut col_budget = cols - TRAILING_COLS;
        let mut widths_iter = widths.iter().enumerate();
        let mut toggle = true;
        let mut first = true;

        loop {
            let value = if toggle {
                widths_iter
                    .next()
                    .map(|step| (step, true))
                    .or_else(|| widths_iter.next_back().map(|step| (step, false)))
            } else {
                widths_iter
                    .next_back()
                    .map(|step| (step, false))
                    .or_else(|| widths_iter.next().map(|step| (step, true)))
            };

            if let Some(((i, column_width), left)) = value {
                // NOTE: we favor left-leaning because of the index column
                if first {
                    first = false;
                } else {
                    toggle = !toggle;
                }

                if col_budget == 0 {
                    break;
                }

                if *column_width + PER_CELL_PADDING_COLS > col_budget {
                    if col_budget > 7 {
                        attempt.push(left, i, col_budget, max_column_widths[i]);
                    }
                    break;
                }

                col_budget -= column_width + PER_CELL_PADDING_COLS;
                attempt.push(left, i, *column_width, max_column_widths[i]);
            } else {
                break;
            }
        }

        attempts.push(attempt);
    }

    // NOTE: we sort by number of columns fitting perfectly, then number of
    // columns we can display
    let best_attempt = attempts
        .into_iter()
        .max_by_key(|a| (a.fitting_count(), a.len()))
        .unwrap();

    best_attempt
}
