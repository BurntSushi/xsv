use std::cmp;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use std::str::from_utf8;

static USAGE: &'static str = "
Sorts CSV data lexicographically.

Note that this requires reading all of the CSV data into memory.

Usage:
    xsv sort [options] [<input>]

sort options:
    -s, --select <arg>     Select a subset of columns to sort.
                           See 'xsv select --help' for the format details.
    -N, --numeric          Compare according to string numerical value

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

#[derive(RustcDecodable)]
struct Args {
    arg_input: Option<String>,
    flag_select: SelectColumns,
    flag_numeric: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let numeric = args.flag_numeric;
    let rconfig = Config::new(&args.arg_input)
                         .delimiter(args.flag_delimiter)
                         .no_headers(args.flag_no_headers)
                         .select(args.flag_select);

    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(Config::new(&args.flag_output).writer());

    let headers = try!(rdr.byte_headers());
    let sel = try!(rconfig.selection(&*headers));

    let mut all = try!(rdr.byte_records().collect::<Result<Vec<_>, _>>());
    all.sort_by(|r1, r2| {
        let a = sel.select(r1.as_slice());
        let b = sel.select(r2.as_slice());
        if !numeric {
            iter_cmp(a, b)
        } else {
            iter_cmp_num(a, b)
        }
    });

    try!(rconfig.write_headers(&mut rdr, &mut wtr));
    for r in all.into_iter() {
        try!(wtr.write(r.into_iter()));
    }
    Ok(try!(wtr.flush()))
}

/// Order `a` and `b` lexicographically using `Ord`
pub fn iter_cmp<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
        where A: Ord, L: Iterator<Item=A>, R: Iterator<Item=A> {
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _   ) => return cmp::Ordering::Less,
            (_   , None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

/// Try parsing `a` and `b` as numbers when ordering
pub fn iter_cmp_num<'a, L, R>(mut a: L, mut b: R) -> cmp::Ordering
        where L: Iterator<Item=&'a [u8]>, R: Iterator<Item=&'a [u8]> {
    loop {
        match (next_num(&mut a), next_num(&mut b)) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _   ) => return cmp::Ordering::Less,
            (_   , None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

fn next_num<'a, X>(xs: &mut X) -> Option<i64>
        where X: Iterator<Item=&'a [u8]> {
    xs.next()
        .and_then(|bytes| from_utf8(bytes).ok())
        .and_then(|s| s.parse::<i64>().ok())
}
