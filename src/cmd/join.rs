use std::collections::hashmap::{HashMap, Vacant, Occupied};

use csv::{mod, ByteString};
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

    let mut rdr1 = try!(io| rconf1.reader());
    let mut rdr2 = try!(io| rconf2.reader());
    let mut wtr = try!(io| CsvConfig::new(args.flag_output.clone()).writer());

    let (sel1, sel2) = try!(args.get_selections(&rconf1, &mut rdr1,
                                                &rconf2, &mut rdr2));
    let (nsel1, nsel2) = (sel1.normalized(), sel2.normalized());
    try!(args.write_headers(&mut rdr1, &mut rdr2, &mut wtr));

    let validx = try!(new_value_index(&mut rdr1, &nsel1));
    println!("{}", validx);
    Ok(())
}

fn new_value_index<R: Reader>
                  (rdr: &mut csv::Reader<R>, nsel: &NormalSelection)
                  -> Result<HashMap<Vec<ByteString>, Vec<uint>>, CliError> {
    let mut validx = HashMap::new();
    let mut rowi = 0u;
    while !rdr.done() {
        let fields = try!(csv| nsel.select(rdr.by_ref())
                                   .map(|v| v.map(ByteString::from_bytes))
                                   .collect::<Result<Vec<_>, _>>());
        match validx.entry(fields) {
            Vacant(v) => { v.set(vec![rowi]); }
            Occupied(mut v) => { v.get_mut().push(rowi); }
        }
        rowi += 1;
    }
    Ok(validx)
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
