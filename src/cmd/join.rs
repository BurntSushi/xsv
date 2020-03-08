use std::collections::hash_map::{HashMap, Entry};
use std::fmt;
use std::fs;
use std::io;
use std::iter::repeat;
use std::str;

use byteorder::{WriteBytesExt, BigEndian};
use csv;

use CliResult;
use config::{Config, Delimiter};
use index::Indexed;
use select::{SelectColumns, Selection};
use util;

static USAGE: &'static str = "
Joins two sets of CSV data on the specified columns.

The default join operation is an 'inner' join. This corresponds to the
intersection of rows on the keys specified.

Joins are always done by ignoring leading and trailing whitespace. By default,
joins are done case sensitively, but this can be disabled with the --no-case
flag.

The columns arguments specify the columns to join for each input. Columns can
be referenced by name or index, starting at 1. Specify multiple columns by
separating them with a comma. Specify a range of columns with `-`. Both
columns1 and columns2 must specify exactly the same number of columns.
(See 'xsv select --help' for the full syntax.)

Usage:
    xsv join [options] <columns1> <input1> <columns2> <input2>
    xsv join --help

join options:
    --no-case              When set, joins are done case insensitively.
    --left                 Do a 'left outer' join. This returns all rows in
                           first CSV data set, including rows with no
                           corresponding row in the second data set. When no
                           corresponding row exists, it is padded out with
                           empty fields.
    --right                Do a 'right outer' join. This returns all rows in
                           second CSV data set, including rows with no
                           corresponding row in the first data set. When no
                           corresponding row exists, it is padded out with
                           empty fields. (This is the reverse of 'outer left'.)
    --full                 Do a 'full outer' join. This returns all rows in
                           both data sets with matching records joined. If
                           there is no match, the missing side will be padded
                           out with empty fields. (This is the combination of
                           'outer left' and 'outer right'.)
    --cross                USE WITH CAUTION.
                           This returns the cartesian product of the CSV
                           data sets given. The number of rows return is
                           equal to N * M, where N and M correspond to the
                           number of rows in the given data sets, respectively.
    --nulls                When set, joins will work on empty fields.
                           Otherwise, empty fields are completely ignored.
                           (In fact, any row that has an empty field in the
                           key specified is ignored.)

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
    flag_left: bool,
    flag_right: bool,
    flag_full: bool,
    flag_cross: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_no_case: bool,
    flag_nulls: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let mut state = args.new_io_state()?;
    match (
        args.flag_left,
        args.flag_right,
        args.flag_full,
        args.flag_cross,
    ) {
        (true, false, false, false) => {
            state.write_headers()?;
            state.outer_join(false)
        }
        (false, true, false, false) => {
            state.write_headers()?;
            state.outer_join(true)
        }
        (false, false, true, false) => {
            state.write_headers()?;
            state.full_outer_join()
        }
        (false, false, false, true) => {
            state.write_headers()?;
            state.cross_join()
        }
        (false, false, false, false) => {
            state.write_headers()?;
            state.inner_join()
        }
        _ => fail!("Please pick exactly one join operation.")
    }
}

struct IoState<R, W: io::Write> {
    wtr: csv::Writer<W>,
    rdr1: csv::Reader<R>,
    sel1: Selection,
    rdr2: csv::Reader<R>,
    sel2: Selection,
    no_headers: bool,
    casei: bool,
    nulls: bool,
}

impl<R: io::Read + io::Seek, W: io::Write> IoState<R, W> {
    fn write_headers(&mut self) -> CliResult<()> {
        if !self.no_headers {
            let mut headers = self.rdr1.byte_headers()?.clone();
            headers.extend(self.rdr2.byte_headers()?.iter());
            self.wtr.write_record(&headers)?;
        }
        Ok(())
    }

    fn inner_join(mut self) -> CliResult<()> {
        let mut scratch = csv::ByteRecord::new();
        let mut validx = ValueIndex::new(
            self.rdr2, &self.sel2, self.casei, self.nulls)?;
        for row in self.rdr1.byte_records() {
            let row = row?;
            let key = get_row_key(&self.sel1, &row, self.casei);
            match validx.values.get(&key) {
                None => continue,
                Some(rows) => {
                    for &rowi in rows.iter() {
                        validx.idx.seek(rowi as u64)?;

                        validx.idx.read_byte_record(&mut scratch)?;
                        let combined = row.iter().chain(scratch.iter());
                        self.wtr.write_record(combined)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn outer_join(mut self, right: bool) -> CliResult<()> {
        if right {
            ::std::mem::swap(&mut self.rdr1, &mut self.rdr2);
            ::std::mem::swap(&mut self.sel1, &mut self.sel2);
        }

        let mut scratch = csv::ByteRecord::new();
        let (_, pad2) = self.get_padding()?;
        let mut validx = ValueIndex::new(
            self.rdr2, &self.sel2, self.casei, self.nulls)?;
        for row in self.rdr1.byte_records() {
            let row = row?;
            let key = get_row_key(&self.sel1, &row, self.casei);
            match validx.values.get(&key) {
                None => {
                    if right {
                        self.wtr.write_record(pad2.iter().chain(&row))?;
                    } else {
                        self.wtr.write_record(row.iter().chain(&pad2))?;
                    }
                }
                Some(rows) => {
                    for &rowi in rows.iter() {
                        validx.idx.seek(rowi as u64)?;
                        let row1 = row.iter();
                        validx.idx.read_byte_record(&mut scratch)?;
                        if right {
                            self.wtr.write_record(scratch.iter().chain(row1))?;
                        } else {
                            self.wtr.write_record(row1.chain(&scratch))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn full_outer_join(mut self) -> CliResult<()> {
        let mut scratch = csv::ByteRecord::new();
        let (pad1, pad2) = self.get_padding()?;
        let mut validx = ValueIndex::new(
            self.rdr2, &self.sel2, self.casei, self.nulls)?;

        // Keep track of which rows we've written from rdr2.
        let mut rdr2_written: Vec<_> =
            repeat(false).take(validx.num_rows).collect();
        for row1 in self.rdr1.byte_records() {
            let row1 = row1?;
            let key = get_row_key(&self.sel1, &row1, self.casei);
            match validx.values.get(&key) {
                None => {
                    self.wtr.write_record(row1.iter().chain(&pad2))?;
                }
                Some(rows) => {
                    for &rowi in rows.iter() {
                        rdr2_written[rowi] = true;

                        validx.idx.seek(rowi as u64)?;
                        validx.idx.read_byte_record(&mut scratch)?;
                        self.wtr.write_record(row1.iter().chain(&scratch))?;
                    }
                }
            }
        }

        // OK, now write any row from rdr2 that didn't get joined with a row
        // from rdr1.
        for (i, &written) in rdr2_written.iter().enumerate() {
            if !written {
                validx.idx.seek(i as u64)?;
                validx.idx.read_byte_record(&mut scratch)?;
                self.wtr.write_record(pad1.iter().chain(&scratch))?;
            }
        }
        Ok(())
    }

    fn cross_join(mut self) -> CliResult<()> {
        let mut pos = csv::Position::new();
        pos.set_byte(0);
        let mut row2 = csv::ByteRecord::new();
        for row1 in self.rdr1.byte_records() {
            let row1 = row1?;
            self.rdr2.seek(pos.clone())?;
            if self.rdr2.has_headers() {
                // Read and skip the header row, since CSV readers disable
                // the header skipping logic after being seeked.
                self.rdr2.read_byte_record(&mut row2)?;
            }
            while self.rdr2.read_byte_record(&mut row2)? {
                self.wtr.write_record(row1.iter().chain(&row2))?;
            }
        }
        Ok(())
    }

    fn get_padding(
        &mut self,
    ) -> CliResult<(csv::ByteRecord, csv::ByteRecord)> {
        let len1 = self.rdr1.byte_headers()?.len();
        let len2 = self.rdr2.byte_headers()?.len();
        Ok((
            repeat(b"").take(len1).collect(),
            repeat(b"").take(len2).collect(),
        ))
    }
}

impl Args {
    fn new_io_state(&self)
        -> CliResult<IoState<fs::File, Box<dyn io::Write+'static>>> {
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
        let (sel1, sel2) = self.get_selections(
            &rconf1, &mut rdr1, &rconf2, &mut rdr2)?;
        Ok(IoState {
            wtr: Config::new(&self.flag_output).writer()?,
            rdr1: rdr1,
            sel1: sel1,
            rdr2: rdr2,
            sel2: sel2,
            no_headers: rconf1.no_headers,
            casei: self.flag_no_case,
            nulls: self.flag_nulls,
        })
    }

    fn get_selections<R: io::Read>(
        &self,
        rconf1: &Config, rdr1: &mut csv::Reader<R>,
        rconf2: &Config, rdr2: &mut csv::Reader<R>,
    ) -> CliResult<(Selection, Selection)> {
        let headers1 = rdr1.byte_headers()?;
        let headers2 = rdr2.byte_headers()?;
        let select1 = rconf1.selection(&*headers1)?;
        let select2 = rconf2.selection(&*headers2)?;
        if select1.len() != select2.len() {
            return fail!(format!(
                "Column selections must have the same number of columns, \
                 but found column selections with {} and {} columns.",
                select1.len(), select2.len()));
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
    fn new(
        mut rdr: csv::Reader<R>,
        sel: &Selection,
        casei: bool,
        nulls: bool,
    ) -> CliResult<ValueIndex<R>> {
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

            let fields: Vec<_> = sel
                .select(&row)
                .map(|v| transform(v, casei))
                .collect();
            if nulls || !fields.iter().any(|f| f.is_empty()) {
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
            let keys = keys.iter()
                           .map(|k| String::from_utf8(k.to_vec()).unwrap())
                           .collect::<Vec<_>>();
            writeln!(f, "({}) => {:?}", keys.join(", "), rows)?
        }
        Ok(())
    }
}

fn get_row_key(
    sel: &Selection,
    row: &csv::ByteRecord,
    casei: bool,
) -> Vec<ByteString> {
    sel.select(row).map(|v| transform(&v, casei)).collect()
}

fn transform(bs: &[u8], casei: bool) -> ByteString {
    match str::from_utf8(bs) {
        Err(_) => bs.to_vec(),
        Ok(s) => {
            if !casei {
                s.trim().as_bytes().to_vec()
            } else {
                let norm: String =
                    s.trim().chars()
                     .map(|c| c.to_lowercase().next().unwrap()).collect();
                norm.into_bytes()
            }
        }
    }
}
