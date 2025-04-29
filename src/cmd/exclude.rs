use std::collections::hash_map::{Entry, HashMap};
use std::fmt;
use std::fs;
use std::io;
use std::str;

use byteorder::{BigEndian, WriteBytesExt};
use csv;

use config::{Config, Delimiter};
use index::Indexed;
use select::{SelectColumns, Selection};
use util;
use CliResult;

static USAGE: &'static str = "
Removes a set of CSV data from another set based on the specified columns.

Also can compute the intersection of two CSV sets with the -v flag.

Matching is always done by ignoring leading and trailing whitespace. By default,
matching is done case sensitively, but this can be disabled with the --no-case
flag.

The columns arguments specify the columns to match for each input. Columns can
be referenced by name or index, starting at 1. Specify multiple columns by
separating them with a comma. Specify a range of columns with `-`. Both
columns1 and columns2 must specify exactly the same number of columns.
(See 'xsv select --help' for the full syntax.)

Usage:
    xsv exclude [options] <columns1> <input1> <columns2> <input2>
    xsv exclude --help

join options:
    --no-case              When set, matching is done case insensitively.
    -v                     When set, matching rows will be the only ones included,
                           forming set intersection, instead of the ones discarded.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

type ByteString = Vec<u8>;

#[derive(Deserialize)]
struct Args {
    arg_columns1: SelectColumns,
    arg_input1: String,
    arg_columns2: SelectColumns,
    arg_input2: String,
    flag_v: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_no_case: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut state = args.new_io_state()?;
    state.write_headers()?;
    state.exclude(args.flag_v)
}

struct IoState<R, W: io::Write> {
    wtr: csv::Writer<W>,
    rdr1: csv::Reader<R>,
    sel1: Selection,
    rdr2: csv::Reader<R>,
    sel2: Selection,
    no_headers: bool,
    casei: bool,
}

impl<R: io::Read + io::Seek, W: io::Write> IoState<R, W> {
    fn write_headers(&mut self) -> CliResult<()> {
        if !self.no_headers {
            let mut headers = self.rdr1.byte_headers()?.clone();
            self.wtr.write_record(&headers)?;
        }
        Ok(())
    }

    fn exclude(mut self, invert: bool) -> CliResult<()> {
        let mut scratch = csv::ByteRecord::new();
        let mut validx = ValueIndex::new(self.rdr2, &self.sel2, self.casei)?;
        for row in self.rdr1.byte_records() {
            let row = row?;
            let key = get_row_key(&self.sel1, &row, self.casei);
            match validx.values.get(&key) {
                None => {
                    if !invert {
                        self.wtr.write_record(row.iter())?;
                    } else {
                        continue;
                    }
                }
                Some(rows) => {
                    if invert {
                        self.wtr.write_record(row.iter())?;
                    } else {
                        continue;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Args {
    fn new_io_state(&self) -> CliResult<IoState<fs::File, Box<io::Write + 'static>>> {
        let rconf1 = Config::new(&Some(self.arg_input1.clone()))
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.arg_columns1.clone());
        let rconf2 = Config::new(&Some(self.arg_input2.clone()))
            .delimiter(self.flag_delimiter)
            .no_headers(self.flag_no_headers)
            .select(self.arg_columns2.clone());

        let mut rdr1 = rconf1.reader_file()?;
        let mut rdr2 = rconf2.reader_file()?;
        let (sel1, sel2) = self.get_selections(&rconf1, &mut rdr1, &rconf2, &mut rdr2)?;
        Ok(IoState {
            wtr: Config::new(&self.flag_output).writer()?,
            rdr1: rdr1,
            sel1: sel1,
            rdr2: rdr2,
            sel2: sel2,
            no_headers: rconf1.no_headers,
            casei: self.flag_no_case,
        })
    }

    fn get_selections<R: io::Read>(
        &self,
        rconf1: &Config,
        rdr1: &mut csv::Reader<R>,
        rconf2: &Config,
        rdr2: &mut csv::Reader<R>,
    ) -> CliResult<(Selection, Selection)> {
        let headers1 = rdr1.byte_headers()?;
        let headers2 = rdr2.byte_headers()?;
        let select1 = rconf1.selection(&*headers1)?;
        let select2 = rconf2.selection(&*headers2)?;
        if select1.len() != select2.len() {
            return fail!(format!(
                "Column selections must have the same number of columns, \
                 but found column selections with {} and {} columns.",
                select1.len(),
                select2.len()
            ));
        }
        Ok((select1, select2))
    }
}

struct ValueIndex<R> {
    // This maps tuples of values to corresponding rows.
    values: HashMap<Vec<ByteString>, Vec<usize>>,
    idx: Indexed<R, io::Cursor<Vec<u8>>>,
    num_rows: usize,
}

impl<R: io::Read + io::Seek> ValueIndex<R> {
    fn new(mut rdr: csv::Reader<R>, sel: &Selection, casei: bool) -> CliResult<ValueIndex<R>> {
        let mut val_idx = HashMap::with_capacity(10000);
        let mut row_idx = io::Cursor::new(Vec::with_capacity(8 * 10000));
        let (mut rowi, mut count) = (0usize, 0usize);

        // This logic is kind of tricky. Basically, we want to include
        // the header row in the line index (because that's what csv::index
        // does), but we don't want to include header values in the ValueIndex.
        if !rdr.has_headers() {
            // ... so if there are no headers, we seek to the beginning and
            // index everything.
            let mut pos = csv::Position::new();
            pos.set_byte(0);
            rdr.seek(pos)?;
        } else {
            // ... and if there are headers, we make sure that we've parsed
            // them, and write the offset of the header row to the index.
            rdr.byte_headers()?;
            row_idx.write_u64::<BigEndian>(0)?;
            count += 1;
        }

        let mut row = csv::ByteRecord::new();
        while rdr.read_byte_record(&mut row)? {
            // This is a bit hokey. We're doing this manually instead of using
            // the `csv-index` crate directly so that we can create both
            // indexes in one pass.
            row_idx.write_u64::<BigEndian>(row.position().unwrap().byte())?;

            let fields: Vec<_> = sel.select(&row).map(|v| transform(v, casei)).collect();
            if !fields.iter().any(|f| f.is_empty()) {
                match val_idx.entry(fields) {
                    Entry::Vacant(v) => {
                        let mut rows = Vec::with_capacity(4);
                        rows.push(rowi);
                        v.insert(rows);
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(rowi);
                    }
                }
            }
            rowi += 1;
            count += 1;
        }

        row_idx.write_u64::<BigEndian>(count as u64)?;
        let idx = Indexed::open(rdr, io::Cursor::new(row_idx.into_inner()))?;
        Ok(ValueIndex {
            values: val_idx,
            idx: idx,
            num_rows: rowi,
        })
    }
}

impl<R> fmt::Debug for ValueIndex<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Sort the values by order of first appearance.
        let mut kvs = self.values.iter().collect::<Vec<_>>();
        kvs.sort_by(|&(_, v1), &(_, v2)| v1[0].cmp(&v2[0]));
        for (keys, rows) in kvs.into_iter() {
            // This is just for debugging, so assume Unicode for now.
            let keys = keys
                .iter()
                .map(|k| String::from_utf8(k.to_vec()).unwrap())
                .collect::<Vec<_>>();
            writeln!(f, "({}) => {:?}", keys.join(", "), rows)?
        }
        Ok(())
    }
}

fn get_row_key(sel: &Selection, row: &csv::ByteRecord, casei: bool) -> Vec<ByteString> {
    sel.select(row).map(|v| transform(&v, casei)).collect()
}

fn transform(bs: &[u8], casei: bool) -> ByteString {
    match str::from_utf8(bs) {
        Err(_) => bs.to_vec(),
        Ok(s) => {
            if !casei {
                s.trim().as_bytes().to_vec()
            } else {
                let norm: String = s
                    .trim()
                    .chars()
                    .map(|c| c.to_lowercase().next().unwrap())
                    .collect();
                norm.into_bytes()
            }
        }
    }
}
