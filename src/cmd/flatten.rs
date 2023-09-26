use colored;
use colored::Colorize;
use config::{Config, Delimiter};
use unicode_width::UnicodeWidthStr;
use util;
use CliResult;

static USAGE: &str = "
Prints flattened records such that fields are labeled separated by a new line.
This mode is particularly useful for viewing one record at a time. Each
record is separated by a special '#' character (on a line by itself), which
can be changed with the --separator flag.

There is also a condensed view (-c or --condense) that will shorten the
contents of each field to provide a summary view.

Pipe into \"less -r\" if you need to page the result.

Usage:
    xsv flatten [options] [<input>]

flatten options:
    -c, --condense         Don't wrap cell values on new lines but truncate them
                           with ellipsis instead.
    -w, --wrap             Wrap cell values all while minding the header's indent.
    --cols <num>           Width of the graph in terminal columns, i.e. characters.
                           Defaults to using all your terminal's width or 80 if
                           terminal's size cannot be found (i.e. when piping to file).
    -R, --rainbow          Alternating colors for cells, rather than color by value type.
    -C, --force-colors     Force colors even if output is not supposed to be able to
                           handle them.

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. When set, the name of each field
                           will be its index.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_condense: bool,
    flag_wrap: bool,
    flag_cols: Option<usize>,
    flag_rainbow: bool,
    flag_force_colors: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let mut rdr = rconfig.reader()?;

    if args.flag_force_colors {
        colored::control::set_override(true);
    }

    let cols = util::acquire_term_cols(&args.flag_cols);

    let potential_headers = rdr.headers()?.clone();
    let mut headers: Vec<String> = Vec::new();

    for (i, header) in potential_headers.iter().enumerate() {
        let header = match rconfig.no_headers {
            true => i.to_string(),
            false => header.to_string(),
        };
        headers.push(header);
    }

    let max_header_width = headers
        .iter()
        .map(|h| h.width())
        .max()
        .ok_or("file is empty")?;

    if cols < max_header_width + 2 {
        return fail!("not enough cols provided to safely print data!");
    }

    let mut record = csv::StringRecord::new();
    let mut record_index: usize = 0;

    let max_value_width = cols - max_header_width - 1;

    while rdr.read_record(&mut record)? {
        println!("{}", "-".repeat(cols));
        println!("{}", format!("Row n°{}", record_index).bold());
        println!("{}", "-".repeat(cols));

        for (i, (header, cell)) in headers.iter().zip(record.iter()).enumerate() {
            let cell = match cell.trim() {
                "" => "<empty>",
                _ => cell,
            };

            let cell_colorizer = if args.flag_rainbow {
                util::colorizer_by_rainbow(i)
            } else {
                util::colorizer_by_type(cell)
            };

            let cell = if args.flag_condense {
                util::unicode_aware_rpad_with_ellipsis(&cell, max_value_width, " ")
            } else if args.flag_wrap {
                util::unicode_aware_wrap(&cell, max_value_width, max_header_width + 1)
            } else {
                cell.to_string()
            };

            let cell = util::colorize(&cell_colorizer, &cell);

            println!(
                "{}{}",
                util::unicode_aware_rpad(header, max_header_width + 1, " "),
                cell
            );
        }

        record_index += 1;
    }

    Ok(())
}
