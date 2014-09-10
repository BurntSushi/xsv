use csv;
use docopt;

use types::{
    CliError, Delimiter, InputReader, OutputWriter,
    SelectColumns, Selection,
};
use util;

docopt!(Args, "
Usage:
    xcsv select [options] [<input>]

select options:
    -s, --select <arg>  Column selection. Each column can be referenced
                        by its column name or index, starting at 1.
                        Specify multiple columns by separating them with
                        a comma. Specify a range of columns with `-`.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. [default: ,]
", arg_input: InputReader, flag_output: OutputWriter,
   flag_delimiter: Delimiter,
   flag_select: SelectColumns)

pub fn main() -> Result<(), CliError> {
    let args: Args = try!(util::get_args());

    let mut rdr = csv_reader!(args);
    let mut wtr = csv::Encoder::to_writer(args.flag_output);
    let selection = ctry!(Selection::new(&mut rdr, &args.flag_select,
                                         args.flag_no_headers));

    let write_row = |wtr: &mut csv::Encoder<_>, row: Vec<_>| {
        let selected = selection.select(row.as_slice());
        wtr.record_bytes(selected.move_iter().map(|r| r.as_slice()))
    };
    if !args.flag_no_headers {
        ctry!(write_row(&mut wtr, ctry!(rdr.headers_bytes())));
    }
    for (i, r) in rdr.iter_bytes().enumerate() {
        ctry!(ignore_pipe write_row(&mut wtr, ctry!(r)));
    }
    ctry!(wtr.flush());
    Ok(())
}
