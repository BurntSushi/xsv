use std::io;

use tabwriter::TabWriter;

use CliResult;
use config::Delimiter;
use util;

static USAGE: &'static str = "
Prints the fields of the first row in the CSV data.

These names can be used in commands like 'select' to refer to columns in the
CSV data.

Note that multiple CSV files may be given to this command. This is useful with
the --intersect flag.

Usage:
    xsv headers [options] [<input>...]

headers options:
    -j, --just-names       Only show the header names (hide column index).
                           This is automatically enabled if more than one
                           input is given.
    --intersect            Shows the intersection of all headers in all of
                           the inputs given.

Common options:
    -h, --help             Display this message
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(RustcDecodable)]
struct Args {
    arg_input: Vec<String>,
    flag_just_names: bool,
    flag_intersect: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));
    let configs = try!(util::many_configs(args.arg_input.as_slice(),
                                          args.flag_delimiter, true));

    let num_inputs = configs.len();
    let mut headers = vec!();
    for conf in configs.into_iter() {
        let mut rdr = try!(conf.reader());
        for header in try!(rdr.byte_headers()).into_iter() {
            if !args.flag_intersect || !headers.contains(&header) {
                headers.push(header);
            }
        }
    }

    let mut wtr: Box<Writer> =
        if args.flag_just_names {
            Box::new(io::stdout()) as Box<Writer>
        } else {
            Box::new(TabWriter::new(io::stdout())) as Box<Writer>
        };
    for (i, header) in headers.into_iter().enumerate() {
        if num_inputs == 1 && !args.flag_just_names {
            try!(wtr.write_str((i + 1).to_string().as_slice()));
            try!(wtr.write_u8(b'\t'));
        }
        try!(wtr.write(header.as_slice()));
        try!(wtr.write_u8(b'\n'));
    }
    try!(wtr.flush());
    Ok(())
}
