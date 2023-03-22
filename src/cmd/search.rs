use csv;
use regex::bytes::RegexBuilder;

use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &'static str = "
Filters CSV data by whether the given regex matches a row.

The regex is applied to each field in each row, and if any field matches,
then the row is written to the output. The columns to search can be limited
with the '--select' flag (but the full row is still written to the output if
there is a match).

Usage:
    xsv search [options] <regex> [<input>]
    xsv search --help

search options:
    -e, --exact            Perform an exact match rather than using a
                           regular expression.
    -i, --ignore-case      Case insensitive search. This is equivalent to
                           prefixing the regex with '(?i)'.
    -s, --select <arg>     Select the columns to search. See 'xsv select -h'
                           for the full syntax.
    -v, --invert-match     Select only rows that did not match

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
    -f, --flag <column>    If given, the command will not filter rows
                           but will instead flag the found rows in a new
                           column named <column>.
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_regex: String,
    flag_select: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_invert_match: bool,
    flag_ignore_case: bool,
    flag_exact: bool,
    flag_flag: Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let pattern = RegexBuilder::new(&*args.arg_regex)
        .case_insensitive(args.flag_ignore_case)
        .build()?;
    let exact_pattern = args.arg_regex.as_bytes();
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    if let Some(column_name) = args.flag_flag.clone() {
        headers.push_field(column_name.as_bytes());
    }

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {

        // Thanks to LLVM loop unswitch, no guerilla optimization seem to be
        // needed here.
        // Ref: https://llvm.org/docs/Passes.html#loop-unswitch-unswitch-loops
        let mut m = if args.flag_exact {
            sel.select(&record).any(|cell| cell == exact_pattern)
        } else {
            sel.select(&record).any(|cell| pattern.is_match(cell))
        };

        if args.flag_invert_match {
            m = !m;
        }

        if let Some(_) = args.flag_flag {
            record.push_field(if m { b"1" } else { b"0" });
            wtr.write_byte_record(&record)?;
        } else if m {
            wtr.write_byte_record(&record)?;
        }
    }
    Ok(wtr.flush()?)
}
