use std::cmp;

use config::{Config, Delimiter};
use csv;
use select::SelectColumns;
use std::str::from_utf8;
use util;
use CliResult;

use self::Number::{Float, Int};

static USAGE: &'static str = "
Sorts CSV data lexicographically.

Note that this requires reading all of the CSV data into memory.

Usage:
    xsv sort [options] [<input>]

sort options:
    -s, --select <arg>     Select a subset of columns to sort.
                           See 'xsv select --help' for the format details.
    -N, --numeric          Compare according to string numerical value
    -R, --reverse          Reverse order
    -c, --count <name>     Number of times the line was consecutively duplicated.
                           Needs a column name. Can only be used with '--uniq'.
    -u, --uniq             When set, identical consecutive lines will be dropped
                           to keep only one line per sorted value.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be sorted with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_numeric: bool,
    flag_reverse: bool,
    flag_count: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_uniq: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let numeric = args.flag_numeric;
    let reverse = args.flag_reverse;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let count = &args.flag_count;

    if !count.is_none() && !args.flag_uniq {
        return fail!("--count can only be used with --uniq");
    };

    let mut rdr = rconfig.reader()?;

    let mut headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
    match (numeric, reverse) {
        (false, false) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp(a, b)
        }),
        (true, false) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(a, b)
        }),
        (false, true) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp(b, a)
        }),
        (true, true) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(b, a)
        }),
    }

    let mut wtr = Config::new(&args.flag_output).writer()?;

    if !rconfig.no_headers {
        if let Some(count_name) = count {
            headers.push_field(count_name.as_bytes());
        }
        if !headers.is_empty() {
            wtr.write_record(&headers)?;
        }
    }

    let mut prev: Option<csv::ByteRecord> = None;
    let mut counter: u64 = 1;
    let mut line_buffer: Option<csv::ByteRecord> = None;

    for r in all.into_iter() {
        if args.flag_uniq {
            match prev {
                Some(other_r) => match iter_cmp(sel.select(&r), sel.select(&other_r)) {
                    cmp::Ordering::Equal => {
                        if !count.is_none() {
                            counter += 1;
                        }
                    }
                    _ => {
                        if let Some(mut to_flush) = line_buffer {
                            to_flush.push_field(&counter.to_string().as_bytes());
                            wtr.write_byte_record(&to_flush)?;
                            line_buffer = Some(r.clone());
                            counter = 1;
                        } else {
                            wtr.write_byte_record(&r)?;
                        }
                    }
                },
                None => {
                    if !count.is_none() {
                        line_buffer = Some(r.clone());
                    } else {
                        wtr.write_byte_record(&r)?;
                    }
                }
            }

            prev = Some(r);
        } else {
            wtr.write_byte_record(&r)?;
        }
    }
    if let Some(mut to_flush) = line_buffer {
        to_flush.push_field(&counter.to_string().as_bytes());
        wtr.write_byte_record(&to_flush)?;
    }
    Ok(wtr.flush()?)
}

/// Order `a` and `b` lexicographically using `Ord`
pub fn iter_cmp<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    A: Ord,
    L: Iterator<Item = A>,
    R: Iterator<Item = A>,
{
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

/// Try parsing `a` and `b` as numbers when ordering
pub fn iter_cmp_num<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    L: Iterator<Item = &'a [u8]>,
    R: Iterator<Item = &'a [u8]>,
{
    loop {
        match (next_num(&mut a), next_num(&mut b)) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match compare_num(x, y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Number {
    Int(i64),
    Float(f64),
}

fn compare_num(n1: Number, n2: Number) -> cmp::Ordering {
    match (n1, n2) {
        (Int(i1), Int(i2)) => i1.cmp(&i2),
        (Int(i1), Float(f2)) => compare_float(i1 as f64, f2),
        (Float(f1), Int(i2)) => compare_float(f1, i2 as f64),
        (Float(f1), Float(f2)) => compare_float(f1, f2),
    }
}

fn compare_float(f1: f64, f2: f64) -> cmp::Ordering {
    f1.partial_cmp(&f2).unwrap_or(cmp::Ordering::Equal)
}

fn next_num<'a, X>(xs: &mut X) -> Option<Number>
where
    X: Iterator<Item = &'a [u8]>,
{
    xs.next()
        .and_then(|bytes| from_utf8(bytes).ok())
        .and_then(|s| {
            if let Ok(i) = s.parse::<i64>() {
                Some(Number::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Some(Number::Float(f))
            } else {
                None
            }
        })
}
