use std::collections::hashmap::{HashMap, Vacant, Occupied};
use std::fmt;
use std::io;

use csv::{mod, ByteString};
use csv::index::Indexed;
use docopt;

use types::{
    CliError, CsvConfig, Delimiter, NormalSelection, Selection, SelectColumns
};
use util;

docopt!(Args, "
Joins two sets of CSV data on the specified columns.

The columns arguments specify the columns to join for each input. Columns can
be referenced by name or index, starting at 1. Specify multiple columns by
separating them with a comma. Specify a range of columns with `-`. Both
columns1 and columns2 must specify exactly the same number of columns.

Usage:
    xcsv join [options] <columns1> <input1> <columns2> <input2>
    xcsv join --help

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_columns1: SelectColumns, arg_input1: String,
   arg_columns2: SelectColumns, arg_input2: String,
   flag_output: Option<String>, flag_delimiter: Delimiter)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());

    let rconf1 = CsvConfig::new(Some(args.arg_input1.clone()))
                           .delimiter(args.flag_delimiter)
                           .no_headers(args.flag_no_headers);
    let rconf2 = CsvConfig::new(Some(args.arg_input2.clone()))
                           .delimiter(args.flag_delimiter)
                           .no_headers(args.flag_no_headers);

    let mut rdr1 = try!(io| rconf1.reader_file());
    let mut rdr2 = try!(io| rconf2.reader_file());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output.clone()).writer());

    let (sel1, sel2) = try!(args.get_selections(&rconf1, &mut rdr1,
                                                &rconf2, &mut rdr2));
    let (nsel1, nsel2) = (sel1.normalized(), sel2.normalized());
    try!(args.write_headers(&mut rdr1, &mut rdr2, &mut wtr));

    let mut validx = try!(ValueIndex::new(rdr2, &nsel2));
    for row in rdr1.byte_records() {
        let row = try!(csv| row);
        let val = sel1.select(row[])
                      .map(ByteString::from_bytes)
                      .collect::<Vec<ByteString>>();
        match validx.values.find_equiv(&val[]) {
            None => continue,
            Some(rows) => {
                for &rowi in rows.iter() {
                    try!(csv| validx.idx.seek(rowi));
                    let row1 = row.iter().map(|f| f.as_slice());

                    // The use of unwrap here is somewhat justified.
                    // In particular, `row2` is from the indexed CSV data,
                    // **which has already been parsed successfully** in its
                    // entirety. If `unwrap` fails, then one of two things
                    // must have happened: 1) there is a bug in my code
                    // somewhere (likely in the indexing) or 2) the file
                    // changed on disk (a risk we take).
                    //
                    // Actually, I suspect there is a more principled way.
                    // I think `write_bytes` might be able to handle the error
                    // somehow, but I'm not quite sure how to do it. (Without
                    // expanding the API of csv::Writer.)
                    let row2 = validx.idx.csv().by_ref().map(|f| f.unwrap());
                    try!(csv| wtr.write_bytes(row1.chain(row2)));
                }
            }
        }
    }
    Ok(())
}

impl Args {
    fn get_selections<R: Reader>
                     (&self,
                      rconf1: &CsvConfig, rdr1: &mut csv::Reader<R>,
                      rconf2: &CsvConfig, rdr2: &mut csv::Reader<R>)
                     -> Result<(Selection, Selection), CliError> {
        let headers1 = try!(csv| rdr1.byte_headers());
        let headers2 = try!(csv| rdr2.byte_headers());
        let select1 =
            try!(str| self.arg_columns1.selection(rconf1, headers1[]));
        let select2 =
            try!(str| self.arg_columns2.selection(rconf2, headers2[]));
        if select1.len() != select2.len() {
            return Err(CliError::from_str(format!(
                "Column selections must have the same number of columns, \
                 but found column selections with {} and {} columns.",
                select1.len(), select2.len())));
        }
        Ok((select1, select2))
    }

    fn write_headers<R: Reader, W: Writer>
                     (&self,
                      rdr1: &mut csv::Reader<R>,
                      rdr2: &mut csv::Reader<R>,
                      wtr: &mut csv::Writer<W>)
                     -> Result<(), CliError> {
        let headers1 = try!(csv| rdr1.byte_headers());
        let headers2 = try!(csv| rdr2.byte_headers());
        if !self.flag_no_headers {
            let mut headers = headers1.clone();
            headers.push_all(headers2[]);
            try!(csv| wtr.write_bytes(headers.into_iter()));
        }
        Ok(())
    }
}

struct ValueIndex<R> {
    // This maps tuples of values to corresponding rows.
    values: HashMap<Vec<ByteString>, Vec<u64>>,
    idx: Indexed<R, io::MemReader>,
}

impl<R: Reader + Seek> ValueIndex<R> {
    fn new(mut rdr: csv::Reader<R>, nsel: &NormalSelection)
          -> Result<ValueIndex<R>, CliError> {
        let mut val_idx = HashMap::with_capacity(10000);
        let mut rows = io::MemWriter::with_capacity(8 * 10000);
        let mut rowi = 0u64;
        try!(io| rows.write_be_u64(0)); // offset to the first row, which
                                        // has already been read as a header.
        while !rdr.done() {
            // This is a bit hokey. We're doing this manually instead of
            // calling `csv::index::create` so we can create both indexes
            // in one pass.
            try!(io| rows.write_be_u64(rdr.byte_offset()));

            let fields = try!(csv| nsel.select(rdr.by_ref())
                                       .map(|v| v.map(ByteString::from_bytes))
                                       .collect::<Result<Vec<_>, _>>());
            match val_idx.entry(fields) {
                Vacant(v) => {
                    let mut rows = Vec::with_capacity(10);
                    rows.push(rowi);
                    v.set(rows);
                }
                Occupied(mut v) => { v.get_mut().push(rowi); }
            }
            rowi += 1;
        }
        Ok(ValueIndex {
            values: val_idx,
            idx: Indexed::new(rdr, io::MemReader::new(rows.unwrap())),
        })
    }
}

impl<R> fmt::Show for ValueIndex<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Sort the values by order of first appearance.
        let mut kvs = self.values.iter().collect::<Vec<_>>();
        kvs.sort_by(|&(_, v1), &(_, v2)| v1[0].cmp(&v2[0]));
        for (keys, rows) in kvs.into_iter() {
            // This is just for debugging, so assume Unicode for now.
            let keys = keys.iter()
                           .map(|k| String::from_utf8(k[].to_vec()).unwrap())
                           .collect::<Vec<_>>();
            try!(writeln!(f, "({}) => {}", keys.connect(", "), rows))
        }
        Ok(())
    }
}
