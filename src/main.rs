#![feature(macro_rules, phase)]

#![allow(non_snake_case)]
#![allow(dead_code)]

extern crate regex;
#[phase(plugin)] extern crate regex_macros;
extern crate serialize;

extern crate csv;
extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;

use std::io;
use std::os;

macro_rules! ctry(
    ($e:expr) => (
        match $e {
            Ok(e) => e,
            Err(e) => return Err(::types::CliError::from_str(e)),
        }
    )
)

macro_rules! csv_reader(
    ($args:expr) => ({
        let d = ::csv::Decoder::from_reader($args.arg_input)
                               .separator($args.flag_delimiter.to_byte());
        if $args.flag_no_headers { d.no_headers() } else { d }
    })
)

macro_rules! csv_write_headers(
    ($args:expr, $rdr:expr, $wtr:expr) => (
        if !$args.flag_no_headers {
            ctry!($wtr.record_bytes(ctry!($rdr.headers_bytes()).move_iter()));
        }
    )
)

docopt!(Args, "
Usage:
    xcsv <command> [<args>...]
    xcsv [options]

Options:
    -h, --help    Display this message
    --version     Print version info and exit

Commands:
    count    Count records
    fmt      Format CSV output (change field delimiter)
    select   Select columns from CSV
", arg_command: Command)

fn main() {
    let mut conf = util::arg_config();
    conf.options_first = true;
    let args: Args = docopt::FlagParser::parse_conf(conf)
                                        .unwrap_or_else(|e| e.exit());
    let result = match args.arg_command {
        Count => count::main(),
        Fmt => fmt::main(),
        Select => select::main(),
    };
    match result {
        Ok(()) => os::set_exit_status(0),
        Err(types::ErrOther(msg)) => {
            os::set_exit_status(1);
            let _ = write!(io::stderr(), "{}\n", msg);
        }
        Err(types::ErrFlag(err)) => err.exit(),
    }
}

#[deriving(Decodable, Show)]
enum Command {
    Count,
    Fmt,
    Select,
}

mod types;
mod util;

mod count;
mod fmt;
mod select;
