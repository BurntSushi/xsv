use std::io::{self, Write};

use colored::{self, Colorize};
use csv;

use config::{Config, Delimiter};
use unicode_width::UnicodeWidthStr;
use util::{self, ImmutableRecordHelpers};
use CliResult;

const TRAILING_COLS: usize = 8;
const PER_CELL_PADDING_COLS: usize = 3;
const HEADERS_ROWS: usize = 8;

static USAGE: &str = "
Preview CSV data in the terminal in a human-friendly way with aligned columns,
shiny colors & all.

The command will by default try to display as many columns as possible but
will truncate cells/columns to avoid overflowing available terminal screen.

If you want to display all the columns using a pager, prefer using
the -p/--pager flag that internally rely on the ubiquitous \"less\"
command.

If you still want to use a pager manually, don't forget to use
the -e/--expand and -C/--force-colors flags before piping like so:

    $ xsv view -eC file.csv | less -SR

Usage:
    xsv view [options] [<input>]
    xsv view --help

view options:
    -p, --pager            Automatically use the \"less\" command to page the results.
    -l, --limit <number>   Maximum of lines of files to read into memory. Set
                           to <=0 to disable a limit. [default: 100].
    -R, --rainbow          Alternating colors for columns, rather than color by value type.
    --cols <num>           Width of the graph in terminal columns, i.e. characters.
                           Defaults to using all your terminal's width or 80 if
                           terminal's size cannot be found (i.e. when piping to file).
    -C, --force-colors     Force colors even if output is not supposed to be able to
                           handle them.
    -e, --expand           Expand the table so that in can be easily piped to
                           a pager such as \"less\", with no with constraints.
    -E, --sanitize-emojis  Replace emojis by their shortcode to avoid formatting issues.
    -I, --hide-index       Hide the row index on the left.

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
    flag_pager: bool,
    flag_cols: Option<usize>,
    flag_delimiter: Option<Delimiter>,
    flag_no_headers: bool,
    flag_force_colors: bool,
    flag_limit: isize,
    flag_rainbow: bool,
    flag_expand: bool,
    flag_sanitize_emojis: bool,
    flag_hide_index: bool,
}

impl Args {
    fn infer_expand(&self) -> bool {
        self.flag_pager || self.flag_expand
    }

    fn infer_force_colors(&self) -> bool {
        self.flag_pager || self.flag_force_colors
    }
}

// TODO: no-headers, file empty with -I panic
pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    if args.flag_pager {
        pager::Pager::with_pager("less -SR").setup();
    }

    if args.infer_force_colors() {
        colored::control::set_override(true);
    }

    let emoji_sanitizer = util::EmojiSanitizer::new();

    let output = io::stdout();

    let cols = util::acquire_term_cols(&args.flag_cols);
    let rows = util::acquire_term_rows();

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;

    let mut potential_headers = rdr.headers()?.clone();

    if !args.flag_hide_index {
        potential_headers = potential_headers.prepend("-");
    }

    let mut headers: Vec<String> = Vec::new();

    for (i, header) in potential_headers.iter().enumerate() {
        let header = match rconfig.no_headers {
            true => i.to_string(),
            false => header.to_string(),
        };
        headers.push(header);
    }

    let mut all_records_buffered = false;

    let records = {
        let limit = args.flag_limit as usize;

        let mut r_iter = rdr.into_records().enumerate();

        let mut records: Vec<csv::StringRecord> = Vec::new();

        loop {
            match r_iter.next() {
                None => break,
                Some((i, record)) => {
                    let mut record = record?;

                    if args.flag_sanitize_emojis {
                        record = sanitize_emojis(&emoji_sanitizer, &record);
                    }

                    if !args.flag_hide_index {
                        record = record.prepend(&i.to_string());
                    }

                    records.push(record);

                    if limit > 0 && records.len() == limit {
                        break;
                    }
                }
            };
        }

        if r_iter.next().is_none() {
            all_records_buffered = true;
        }

        records
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

    let displayed_columns = infer_best_column_display(
        cols,
        &max_column_widths,
        args.infer_expand(),
        if args.flag_hide_index { 0 } else { 1 },
    );

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

            if !all_columns_shown && Some(i) == displayed_columns.split_point() {
                s.push(match pos {
                    HRPosition::Bottom => '┬',
                    HRPosition::Top => '┴',
                    HRPosition::Middle => '┼',
                });

                s.push_str(&"─".repeat(3));
            }

            if i == displayed_columns.len() - 1 {
                return;
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

                if !args.flag_hide_index && i == 0 {
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

                if !args.flag_hide_index && i == 0 {
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

fn sanitize_emojis(
    sanitizer: &util::EmojiSanitizer,
    record: &csv::StringRecord,
) -> csv::StringRecord {
    record.iter().map(|cell| sanitizer.sanitize(cell)).collect()
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
    left_advantage: usize,
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
        let mut left_leaning = left_advantage;

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
                // NOTE: we favor left-leaning columns because of
                // the index column or just for aesthetical reasons
                if left_leaning > 0 {
                    left_leaning -= 1;
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
