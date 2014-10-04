use std::io;
use std::io::fs::mkdir_recursive;

use csv;
use docopt;

use types::{CliError, CsvConfig, Delimiter};
use util;

docopt!(Args, "
Splits the given CSV data into chunks.

The files are written to the directory given with the name '{start}-{end}.csv',
where {start} and {end} is the half-open interval corresponding to the records
in the chunk.

Usage:
    xcsv split [options] <outdir> [<input>]

split options:
    -s, --size <arg>       The number of records to write into each chunk.
                           [default: 2]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will NOT be interpreted
                           as column names. Note that this has no effect when
                           concatenating columns.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: Option<String>, arg_outdir: String, flag_output: Option<String>,
   flag_delimiter: Delimiter, flag_size: u64)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());
    try!(io| mkdir_recursive(&Path::new(args.arg_outdir[]),
                             io::AllPermissions));

    let rconfig = CsvConfig::new(args.arg_input.clone())
                            .delimiter(args.flag_delimiter)
                            .no_headers(args.flag_no_headers);
    let mut rdr = try!(io| rconfig.reader());
    let headers = try!(csv| rdr.byte_headers());

    let mut wtr = try!(args.new_writer(headers[], 0));
    for (i, row) in rdr.byte_records().enumerate() {
        if i > 0 && i as u64 % args.flag_size == 0 {
            try!(csv| wtr.flush());
            wtr = try!(args.new_writer(headers[], i as u64));
        }
        let row = try!(csv| row);
        try!(csv| wtr.write_bytes(row.into_iter()));
    }
    try!(csv| wtr.flush());
    Ok(())
}

impl Args {
    fn new_writer(&self, headers: &[csv::ByteString], start: u64)
                 -> Result<csv::Writer<Box<io::Writer+'static>>, CliError> {
        let dir = Path::new(self.arg_outdir.clone());
        let path = dir.join(format!("{}.csv", start));
        let spath = Some(path.display().to_string());
        let mut wtr = try!(io| CsvConfig::new(spath).writer());
        if !self.flag_no_headers {
            try!(csv| wtr.write_bytes(headers.iter().map(|f| f[])));
        }
        Ok(wtr)
    }
}
