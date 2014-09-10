#![feature(macro_rules, phase)]

#![allow(non_snake_case)]
#![allow(dead_code)]

extern crate regex;
#[phase(plugin)] extern crate regex_macros;
extern crate serialize;

extern crate csv;
extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;
extern crate tabwriter;

use std::io;
use std::os;

macro_rules! ctry(
    (ignore_pipe $e:expr) => ({
        use std::io::{IoError, BrokenPipe};
        match $e {
            Ok(e) => e,
            Err(csv::ErrIo(IoError { kind: BrokenPipe, .. })) =>
                return Err(::types::ErrBrokenPipe),
            Err(e) => return Err(::types::CliError::from_str(e)),
        }
    });
    ($e:expr) => (
        match $e {
            Ok(e) => e,
            Err(e) => return Err(::types::CliError::from_str(e)),
        }
    );
)

macro_rules! csv_reader(
    ($args:expr) => ({
        csv_reader!($args, $args.arg_input)
    });
    ($args:expr, $rdr:expr) => ({
        let d = ::csv::Decoder::from_reader($rdr)
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

macro_rules! command_list(
    () => (
"
    cat         Concatenate by row or column
    count       Count records
    fixlengths  Makes all records have same length
    fmt         Format CSV output (change field delimiter)
    headers     Show header names
    select      Select columns from CSV
    slice       Slice records from CSV
    table       Align CSV data into columns
"
    )
)

docopt!(Args, concat!("
Usage:
    xcsv <command> [<args>...]
    xcsv [options]

Options:
    -h, --help    Display this message
    --version     Print version info and exit

Commands:", command_list!()),
arg_command: Option<Command>)

fn main() {
    let mut conf = util::arg_config();
    conf.options_first = true;
    let args: Args = docopt::FlagParser::parse_conf(conf)
                                        .unwrap_or_else(|e| e.exit());
    match args.arg_command {
        None => {
            os::set_exit_status(1);
            let msg = concat!(
                "Please choose one of the following commands:",
                command_list!());
            {write!(io::stderr(), "{}", msg)}.unwrap();
        }
        Some(cmd) => {
            match cmd.run() {
                Ok(()) => os::set_exit_status(0),
                Err(types::ErrBrokenPipe) => {
                    os::set_exit_status(0);
                }
                Err(types::ErrOther(msg)) => {
                    os::set_exit_status(1);
                    let _ = write!(io::stderr(), "{}\n", msg);
                }
                Err(types::ErrFlag(err)) => err.exit(),
            }
        }
    }
}

#[deriving(Decodable, Show)]
enum Command {
    Cat,
    Count,
    FixLengths,
    Fmt,
    Headers,
    Select,
    Slice,
    Table,
}

impl Command {
    fn run(self) -> Result<(), types::CliError> {
        match self {
            Cat => cmd::cat::main(),
            Count => cmd::count::main(),
            FixLengths => cmd::fixlengths::main(),
            Fmt => cmd::fmt::main(),
            Headers => cmd::headers::main(),
            Select => cmd::select::main(),
            Slice => cmd::slice::main(),
            Table => cmd::table::main(),
        }
    }
}

mod cmd;
mod types;
mod util;
