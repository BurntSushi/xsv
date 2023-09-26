use colored::{self, Colorize};
use csv;

use config::{Config, Delimiter};
use unicode_width::UnicodeWidthStr;
use util;
use CliResult;

static USAGE: &str = "
Preview CSV data in the terminal in a human-friendly way with aligned columns,
shiny colors & all.

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

    let width_max = if args.flag_expand { 120 } else { cols / 2 };

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

    let column_widths: Vec<usize> = max_column_widths
        .iter()
        .map(|m| usize::min(*m, width_max))
        .collect();

    let mut col_budget = cols - 4;
    let mut columns_fitting_in_budget: usize = 0;

    let additional_chars_per_cell = 5; // NOTE: taking into account pipes, etc. for the frames

    for column_width in column_widths.iter() {
        if column_width + additional_chars_per_cell > col_budget {
            break;
        }
        col_budget -= column_width + additional_chars_per_cell;
        columns_fitting_in_budget += 1;
    }

    let all_columns_shown = columns_fitting_in_budget == column_widths.len();

    // TODO: expand
    // TODO: deal better when everything can be shown on screen
    // TODO: print some useful info on top & bottom regarding columns, rows etc.

    let print_horizontal_ruler = || {
        println!("{}", "-".repeat(cols));
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

    print_headers();

    for record in records.into_iter() {
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

                let colorizer = util::colorizer_by_type(cell);
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

    Ok(())
}

fn prepend(record: &csv::StringRecord, item: &str) -> csv::StringRecord {
    let mut new_record = csv::StringRecord::new();
    new_record.push_field(item);
    new_record.extend(record);

    new_record
}
