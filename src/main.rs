#![feature(macro_rules, phase)]

/*!
These are some docs.
*/

extern crate regex;
extern crate serialize;

extern crate csv;
extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;
extern crate tabwriter;

use std::io;
use std::os;

macro_rules! try(
    (csv| $e:expr) => (try!($e.map_err(::types::CliError::from_csv)));
    (io| $e:expr) => (try!($e.map_err(::types::CliError::from_io)));
    (str| $e:expr) => (try!($e.map_err(::types::CliError::from_str)));
    ($e:expr) => (
        match $e {
            Ok(e) => e,
            Err(e) => return Err(e)
        }
    );
)

macro_rules! werr(
    ($($arg:tt)*) => (
        match ::std::io::stderr().write_str(format!($($arg)*).as_slice()) {
            Ok(_) => (),
            Err(err) => fail!("{}", err),
        }
    )
)

macro_rules! command_list(
    () => (
"
    cat         Concatenate by row or column
    count       Count records
    fixlengths  Makes all records have same length
    flatten     Show one field per line
    fmt         Format CSV output (change field delimiter)
    headers     Show header names
    index       Create CSV index for faster access
    join        Join CSV files
    search      Search CSV data with regexes
    select      Select columns from CSV
    slice       Slice records from CSV
    split       Split CSV data into many files
    table       Align CSV data into columns
"
    )
)

docopt!(Args, concat!("
Usage:
    xsv <command> [<args>...]
    xsv [options]

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
            os::set_exit_status(0);
            let msg = concat!(
                "xsv is a suite of CSV command line utilities.

Please choose one of the following commands:",
                command_list!());
            {write!(io::stderr(), "{}", msg)}.unwrap();
        }
        Some(cmd) => {
            match cmd.run() {
                Ok(()) => os::set_exit_status(0),
                Err(types::ErrFlag(err)) => err.exit(),
                Err(types::ErrCsv(err)) => {
                    os::set_exit_status(1);
                    let _ = writeln!(io::stderr(), "{}", err.to_string());
                }
                Err(types::ErrIo(io::IoError { kind: io::BrokenPipe, .. })) => {
                    os::set_exit_status(0);
                }
                Err(types::ErrIo(err)) => {
                    os::set_exit_status(1);
                    let _ = writeln!(io::stderr(), "{}", err.to_string());
                }
                Err(types::ErrOther(msg)) => {
                    os::set_exit_status(1);
                    let _ = writeln!(io::stderr(), "{}", msg);
                }
            }
        }
    }
}

#[deriving(Decodable, Show)]
enum Command {
    Cat,
    Count,
    FixLengths,
    Flatten,
    Fmt,
    Headers,
    Index,
    Join,
    Search,
    Select,
    Slice,
    Split,
    Table,
}

impl Command {
    fn run(self) -> Result<(), types::CliError> {
        match self {
            Cat => cmd::cat::main(),
            Count => cmd::count::main(),
            FixLengths => cmd::fixlengths::main(),
            Flatten => cmd::flatten::main(),
            Fmt => cmd::fmt::main(),
            Headers => cmd::headers::main(),
            Index => cmd::index::main(),
            Join => cmd::join::main(),
            Search => cmd::search::main(),
            Select => cmd::select::main(),
            Slice => cmd::slice::main(),
            Split => cmd::split::main(),
            Table => cmd::table::main(),
        }
    }
}

mod cmd;
mod types;
mod util;
