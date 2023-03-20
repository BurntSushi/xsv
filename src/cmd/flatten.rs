use std::borrow::Cow;
use std::io::{self, Write};

use colored::Colorize;
use tabwriter::TabWriter;
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use config::{Config, Delimiter};
use util;
use CliResult;

static USAGE: &'static str = "
Prints flattened records such that fields are labeled separated by a new line.
This mode is particularly useful for viewing one record at a time. Each
record is separated by a special '#' character (on a line by itself), which
can be changed with the --separator flag.

There is also a condensed view (-c or --condense) that will shorten the
contents of each field to provide a summary view.

Usage:
    xsv flatten [options] [<input>]

flatten options:
    -c, --condense <arg>  Limits the length of each field to the value
                           specified. If the field is UTF-8 encoded, then
                           <arg> refers to the number of code points.
                           Otherwise, it refers to the number of bytes.
    -s, --separator <arg>  A string of characters to write after each record.
                           When non-empty, a new line is automatically
                           appended to the separator.
    --pretty               Human-friendly output.

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. When set, the name of each field
                           will be its index.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_condense: Option<usize>,
    flag_separator: Option<String>,
    flag_pretty: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let colors = vec![
        vec![31, 119, 180],
        vec![255, 127, 14],
        vec![44, 160, 44],
        vec![214, 39, 40],
        vec![148, 103, 189],
        vec![140, 86, 75],
        vec![227, 119, 194],
        vec![127, 127, 127],
        vec![188, 189, 34],
        vec![23, 190, 207],
    ];

    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);
    let mut rdr = rconfig.reader()?;
    let init_headers = rdr.headers()?.clone();

    let mut headers: Vec<String> = Vec::new();
    let mut max_header_size = 0;
    for (i, header) in init_headers.iter().enumerate() {
        let header = match rconfig.no_headers {
            true => i.to_string(),
            false => header.to_string(),
        };
        headers.push(header.clone());
        if UnicodeWidthStr::width(&header[..]) > max_header_size {
            max_header_size = UnicodeWidthStr::width(&header[..]);
        }
    }
    max_header_size += 1;
    let mut align = "\n".to_string();
    align += &" ".repeat(max_header_size);

    let separator = args.flag_separator.unwrap_or(" ".to_string());

    let screen_size = match termsize::get() {
        Some(size) => size.cols as usize,
        None => 80,
    };

    let mut wtr = TabWriter::new(io::stdout());
    let mut count = 0;
    for r in rdr.byte_records() {
        if count != 0 && !separator.is_empty() {
            writeln!(&mut wtr, "{}", separator)?;
        }
        if args.flag_pretty {
            let title = format!("Row n°{}", count);
            writeln!(&mut wtr, "{}", title.white().bold())?;
        }
        count += 1;
        let r = r?;
        for (i, (header, field)) in headers.iter().zip(&r).enumerate() {
            let remainder = i % 10;
            let size = header.chars().count();
            if args.flag_pretty {
                write!(
                    &mut wtr,
                    "{}{}",
                    header
                        .truecolor(
                            colors[remainder][0],
                            colors[remainder][1],
                            colors[remainder][2]
                        )
                        .bold(),
                    " ".repeat(max_header_size - size)
                )?;
            } else {
                write!(&mut wtr, "{}{}", header, " ".repeat(max_header_size - size))?
            }

            let field = String::from_utf8(
                (&*util::condense(Cow::Borrowed(&*field), args.flag_condense)).to_vec(),
            )
            .unwrap();
            let mut final_field: String = field.clone();
            if screen_size > max_header_size {
                final_field = String::new();
                let field_chars =
                    UnicodeSegmentation::graphemes(&field[..], true).collect::<Vec<&str>>();
                let mut i = 0;
                while i < field_chars.len() {
                    if field_chars[i].to_string() == "\n" {
                        final_field += &align;
                        i += 1;
                        continue;
                    }
                    if i != 0 {
                        final_field += &align;
                    }
                    let mut temp_field = field_chars[i].to_string();
                    i += 1;
                    while UnicodeWidthStr::width(&temp_field[..]) < (screen_size - max_header_size)
                    {
                        if i >= field_chars.len() {
                            break;
                        }
                        if field_chars[i].to_string() == "\n" {
                            i += 1;
                            break;
                        }
                        temp_field += &field_chars[i].to_string();
                        i += 1;
                    }
                    final_field += &temp_field;
                }
            }

            if args.flag_pretty {
                write!(
                    &mut wtr,
                    "{}",
                    final_field.truecolor(
                        colors[remainder][0],
                        colors[remainder][1],
                        colors[remainder][2]
                    )
                )?;
            } else {
                write!(&mut wtr, "{}", final_field)?;
            }
            wtr.write_all(b"\n")?;
        }
    }
    wtr.flush()?;
    Ok(())
}
