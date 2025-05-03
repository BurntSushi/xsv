use std::cmp;

use CliResult;
use byteorder::{ByteOrder, LittleEndian};
use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use rand::{thread_rng, Rng, SeedableRng, StdRng};
use std::str::from_utf8;

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
    --random               Random order
    --seed <number>        RNG seed

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
    flag_random: bool,
    flag_seed: Option<usize>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let numeric = args.flag_numeric;
    let reverse = args.flag_reverse;
    let random = args.flag_random;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    // Seeding rng
    let seed = args.flag_seed;
    let mut rng: StdRng = match seed {
        None => {
            StdRng::from_rng(thread_rng()).unwrap()
        }
        Some(seed) => {
            let mut buf = [0u8; 32];
            LittleEndian::write_u64(&mut buf, seed as u64);
            SeedableRng::from_seed(buf)
        }
    };

    let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
    match (numeric, reverse, random) {
        (_, _, true) =>
            rng.shuffle(&mut all),
        (false, false, false) =>
            all.sort_by(|r1, r2| {
                let a = sel.select(r1);
                let b = sel.select(r2);
                iter_cmp(a, b)
            }),
        (true, false, false) =>
            all.sort_by(|r1, r2| {
                let a = sel.select(r1);
                let b = sel.select(r2);
                iter_cmp_num(a, b)
            }),
        (false, true, false) =>
            all.sort_by(|r1, r2| {
                let a = sel.select(r1);
                let b = sel.select(r2);
                iter_cmp(b, a)
            }),
        (true, true, false) =>
            all.sort_by(|r1, r2| {
                let a = sel.select(r1);
                let b = sel.select(r2);
                iter_cmp_num(b, a)
            }),
    }

    let mut wtr = Config::new(&args.flag_output).writer()?;
    rconfig.write_headers(&mut rdr, &mut wtr)?;
    for r in all.into_iter() {
        wtr.write_byte_record(&r)?;
    }
    Ok(wtr.flush()?)
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

fn compare_num(n1: Number, n2: Number) -> cmp::Ordering{
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
        where X: Iterator<Item=&'a [u8]> {
    xs.next()
        .and_then(|bytes| from_utf8(bytes).ok())
        .and_then(|s| {
            if let Ok(i) = s.parse::<i64>() { Some(Number::Int(i)) }
            else if let Ok(f) = s.parse::<f64>() { Some(Number::Float(f)) }
            else { None }
        })
}
