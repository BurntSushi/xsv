use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use cmd::sort::{ComparableByteRecord, NumericallyComparableByteRecord};
use config::{Config, Delimiter};
use select::SelectColumns;
use util;
use CliResult;

static USAGE: &str = "
Perform k-way merge of multiple already sorted CSV files. Those files MUST:

1. have the same columns (they don't have to be in the same order, though)
2. have the same row order wrt -s/--select, -R/--reverse & -N/--numeric

If those conditions are not met, the result will be in some arbitrary order.

This command consumes memory proportional to one CSV row per file.

Usage:
    xsv kway [options] [<input>...]
    xsv kway --help

kway options:
    -s, --select <arg>     Select a subset of columns to sort.
                           See 'xsv select --help' for the format details.
    -N, --numeric          Compare according to string numerical value
    -R, --reverse          Reverse order
    -u, --uniq             When set, identical consecutive lines will be dropped
                           to keep only one line per sorted value.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Note that this has no effect when
                           concatenating columns.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
";

#[derive(PartialEq, PartialOrd, Ord, Eq)]
struct Forward<T>(T);

macro_rules! kway {
    ($wtr:ident, $iters:ident, $sels:ident, $wrapper:ident, $record:ident, $unique:expr) => {
        let mut heap: BinaryHeap<($wrapper<$record>, usize)> =
            BinaryHeap::with_capacity($iters.len());

        for (i, (iter, sel)) in $iters.iter_mut().zip($sels.iter()).enumerate() {
            match iter.next() {
                None => continue,
                Some(record) => {
                    let record = $wrapper($record::new(record?, sel));
                    heap.push((record, i));
                }
            }
        }

        let mut last_record: Option<$wrapper<$record>> = None;

        while !heap.is_empty() {
            match heap.pop() {
                None => break,
                Some(entry) => {
                    let (comparable_record, i) = entry;

                    if $unique {
                        match last_record {
                            None => {
                                $wtr.write_byte_record(comparable_record.0.as_byte_record())?;
                                last_record = Some(comparable_record);
                            }
                            Some(ref r) => match r.cmp(&comparable_record) {
                                Ordering::Equal => (),
                                _ => {
                                    $wtr.write_byte_record(comparable_record.0.as_byte_record())?;
                                    last_record = Some(comparable_record);
                                }
                            },
                        }
                    } else {
                        $wtr.write_byte_record(comparable_record.0.as_byte_record())?;
                    }

                    match $iters[i].next() {
                        None => continue,
                        Some(record) => {
                            let record = $wrapper($record::new(record?, &$sels[i]));
                            heap.push((record, i));
                        }
                    }
                }
            }
        }
    };
}

#[derive(Deserialize)]
struct Args {
    arg_input: Vec<String>,
    flag_select: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_numeric: bool,
    flag_reverse: bool,
    flag_uniq: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let mut wtr = Config::new(&args.flag_output).writer()?;

    let confs = args.configs()?.into_iter().collect::<Vec<Config>>();

    let mut readers = confs
        .iter()
        .map(|conf| conf.reader())
        .collect::<Result<Vec<_>, _>>()?;

    let headers = readers
        .iter_mut()
        .map(|rdr| rdr.byte_headers())
        .collect::<Result<Vec<_>, _>>()?;

    if !args.flag_no_headers {
        if !headers.iter().skip(1).all(|h| *h == headers[0]) {
            return fail!("all given files should have identical headers!");
        }

        wtr.write_byte_record(&headers[0])?;
    }

    let selections = confs
        .iter()
        .zip(headers.iter())
        .map(|(c, h)| c.selection(h))
        .collect::<Result<Vec<_>, _>>()?;

    let mut record_iterators = readers
        .into_iter()
        .map(|rdr| rdr.into_byte_records())
        .collect::<Vec<_>>();

    match (args.flag_numeric, args.flag_reverse) {
        (false, false) => {
            kway!(
                wtr,
                record_iterators,
                selections,
                Reverse,
                ComparableByteRecord,
                args.flag_uniq
            );
        }
        (true, false) => {
            kway!(
                wtr,
                record_iterators,
                selections,
                Reverse,
                NumericallyComparableByteRecord,
                args.flag_uniq
            );
        }
        (false, true) => {
            kway!(
                wtr,
                record_iterators,
                selections,
                Forward,
                ComparableByteRecord,
                args.flag_uniq
            );
        }
        (true, true) => {
            kway!(
                wtr,
                record_iterators,
                selections,
                Forward,
                NumericallyComparableByteRecord,
                args.flag_uniq
            );
        }
    };

    Ok(wtr.flush()?)
}

impl Args {
    fn configs(&self) -> CliResult<Vec<Config>> {
        util::many_configs(
            &self.arg_input,
            self.flag_delimiter,
            self.flag_no_headers,
            Some(&self.flag_select),
        )
        .map_err(From::from)
    }
}
