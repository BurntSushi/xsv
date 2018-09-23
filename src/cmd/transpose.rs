use config::{Config, Delimiter};
use csv::ByteRecord;
use std::str;
use util;
use CliResult;

static USAGE: &'static str = "
Transpose the rows/columns of CSV data.

Note that by default this reads all of the CSV data into memory,
unless --multipass is given.

Usage:
    xsv transpose [options] [<input>]

transpose options:
    -m, --multipass        Process the transpose by making multiple
                           passes over the dataset. Useful for really 
                           big datasets. Consumes memory relative to
                           the number of rows.
                           Note that in general it is faster to
                           process the transpose in memory.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
    flag_multipass: bool,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let input_is_stdin = match args.arg_input {
        Some(ref s) if s == "-" => true,
        None => true,
        _ => false,
    };

  if args.flag_multipass && !input_is_stdin {
        args.multipass_transpose()
    } else {
        args.in_memory_transpose()
    }
}

impl Args {
    fn in_memory_transpose(&self) -> CliResult<()> {
        let mut rdr = self.rconfig().reader()?;
        let mut wtr = self.wconfig().writer()?;
        let nrows = rdr.byte_headers()?.len();
        
        let all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
        for i in 0..nrows {
            let mut record = ByteRecord::new();
            
            for row in all.iter() {
                record.push_field(&row[i]);
            }
            wtr.write_byte_record(&record)?;
        }
        Ok(wtr.flush()?)
    }

    fn multipass_transpose(&self) -> CliResult<()> {
        let mut wtr = self.wconfig().writer()?;
        let nrows = self.rconfig().reader()?.byte_headers()?.len();

        for i in 0..nrows {
            let mut rdr = self.rconfig().reader()?;

            let mut record = ByteRecord::new();
            for row in rdr.byte_records() {
                record.push_field(&row?[i]);
            }
            wtr.write_byte_record(&record)?;
        }
      Ok(wtr.flush()?)
    }

    fn wconfig(&self) -> Config {
        Config::new(&self.flag_output)
    }

    fn rconfig(&self) -> Config {
        Config::new(&self.arg_input)
            .delimiter(self.flag_delimiter)
            .no_headers(true)
    }
}
