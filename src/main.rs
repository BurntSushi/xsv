#![feature(if_let, macro_rules, phase, slicing_syntax, tuple_indexing)]

/*!
These are some docs.
*/

extern crate regex;
extern crate serialize;

extern crate csv;
extern crate docopt;
extern crate stats;
extern crate tabwriter;

use std::io;
use std::os;

use docopt::Docopt;

macro_rules! try(
    (csv| $e:expr) => (try!($e.map_err(::CliError::from_csv)));
    (io| $e:expr) => (try!($e.map_err(::CliError::from_io)));
    (str| $e:expr) => (try!($e.map_err(::CliError::from_str)));
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
    frequency   Show frequency tables
    headers     Show header names
    index       Create CSV index for faster access
    join        Join CSV files
    search      Search CSV data with regexes
    select      Select columns from CSV
    slice       Slice records from CSV
    sort        Sort CSV data
    split       Split CSV data into many files
    stats       Compute basic statistics
    table       Align CSV data into columns
"
    )
)

static USAGE: &'static str = concat!("
Usage:
    xsv <command> [<args>...]
    xsv [options]

Options:
    -h, --help    Display this message
    --version     Print version info and exit

Commands:", command_list!());

#[deriving(Decodable)]
struct Args {
    arg_command: Option<Command>,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.options_first(true)
                                           .version(Some(util::version()))
                                           .decode())
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
                Err(ErrFlag(err)) => err.exit(),
                Err(ErrCsv(err)) => {
                    os::set_exit_status(1);
                    let _ = writeln!(io::stderr(), "{}", err.to_string());
                }
                Err(ErrIo(io::IoError { kind: io::BrokenPipe, .. })) => {
                    os::set_exit_status(0);
                }
                Err(ErrIo(err)) => {
                    os::set_exit_status(1);
                    let _ = writeln!(io::stderr(), "{}", err.to_string());
                }
                Err(ErrOther(msg)) => {
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
    Frequency,
    Headers,
    Index,
    Join,
    Search,
    Select,
    Slice,
    Sort,
    Split,
    Stats,
    Table,
}

impl Command {
    fn run(self) -> CliResult<()> {
        let argv = os::args();
        let argv: Vec<_> = argv.iter().map(|s| s[]).collect();
        let argv = argv[];
        match self {
            Cat => cmd::cat::run(argv),
            Count => cmd::count::run(argv),
            FixLengths => cmd::fixlengths::run(argv),
            Flatten => cmd::flatten::run(argv),
            Fmt => cmd::fmt::run(argv),
            Frequency => cmd::frequency::run(argv),
            Headers => cmd::headers::run(argv),
            Index => cmd::index::run(argv),
            Join => cmd::join::run(argv),
            Search => cmd::search::run(argv),
            Select => cmd::select::run(argv),
            Slice => cmd::slice::run(argv),
            Sort => cmd::sort::run(argv),
            Split => cmd::split::run(argv),
            Stats => cmd::stats::run(argv),
            Table => cmd::table::run(argv),
        }
    }
}

pub type CliResult<T> = Result<T, CliError>;

#[deriving(Show)]
pub enum CliError {
    ErrFlag(docopt::Error),
    ErrCsv(csv::Error),
    ErrIo(io::IoError),
    ErrOther(String),
}

impl CliError {
    pub fn from_flags(v: docopt::Error) -> CliError {
        ErrFlag(v)
    }
    pub fn from_csv(v: csv::Error) -> CliError {
        match v {
            csv::ErrIo(v) => CliError::from_io(v),
            v => ErrCsv(v),
        }
    }
    pub fn from_io(v: io::IoError) -> CliError {
        ErrIo(v)
    }
    pub fn from_str<T: ToString>(v: T) -> CliError {
        ErrOther(v.to_string())
    }
}

mod cmd;
mod config;
mod select;
mod util;
