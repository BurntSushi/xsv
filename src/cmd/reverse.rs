use config::{Config, Delimiter};
use util;
use CliResult;

static USAGE: &'static str = "
Reverses rows of CSV data.

Useful for cases when there is no column that can be used for sorting in reverse order,
or when keys are not unique and order of rows with the same key needs to be preserved.

This function is memory efficient.

Usage:
    xsv reverse [options] [<input>]

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be reversed with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
    -m, --in-memory        Load the csv content in memory before reversing it. 
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_in_memory: bool
}


pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(true);

    let mut config_csv_reader= rconfig.reader()?;

    let headers = config_csv_reader.byte_headers()?.clone();

    let headers_size = if args.flag_no_headers {
        0
    } else {
        let position = config_csv_reader.position();
        position.clone().byte()
    };
    
    let reverse_reader = rconfig.io_reader_for_reverse_reading(headers_size as u64);

    match reverse_reader {
        Ok(reader) => {
            let mut wtr = Config::new(&args.flag_output).writer()?;
            
            let mut reverse_csv_reader = rconfig.from_reader(reader);

            if !args.flag_no_headers { wtr.write_byte_record(&headers)?; }

            for r in reverse_csv_reader.byte_records() {
                match r {
                    Ok(record) => {
                        let mut new_record = csv::ByteRecord::new();

                        for b in record.iter().rev() {
                            let mut rec = Vec::<u8>::with_capacity(b.len());
                            for c in b.iter().rev() { rec.push(*c); }
                            new_record.push_field(rec.as_slice())
                        }

                        wtr.write_record(new_record.iter())?;
                    },
                    Err(_) => {}
                }
            }
            
            Ok(wtr.flush()?)
        },
        Err(e) => {
            if !args.flag_in_memory { 
                Err(crate::CliError::Io(e)) 
            }
            else {
                let dconfig = Config::new(&args.arg_input)
                    .delimiter(args.flag_delimiter)
                    .no_headers(args.flag_no_headers);

                let mut reader = dconfig.reader()?;
                let mut all = reader.byte_records().collect::<Result<Vec<_>, _>>()?;
                all.reverse();

                let mut wtr = Config::new(&args.flag_output).writer()?;
                rconfig.write_headers(&mut reader, &mut wtr)?;

                for r in all.into_iter() {
                    wtr.write_byte_record(&r)?;
                }

                Ok(wtr.flush()?)
            }
        }
    }
}
