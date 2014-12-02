#![feature(if_let, macro_rules, slicing_syntax, tuple_indexing)]

/*!
These are some docs.
*/

extern crate regex;
extern crate serialize;

extern crate csv;
extern crate docopt;
extern crate stats;
extern crate tabwriter;

use std::error::FromError;
use std::io;
use std::os;

use docopt::Docopt;

macro_rules! werr(
    ($($arg:tt)*) => (
        match ::std::io::stderr().write_str(format!($($arg)*).as_slice()) {
            Ok(_) => (),
            Err(err) => fail!("{}", err),
        }
    )
)

macro_rules! fail(
    ($e:expr) => (Err(::std::error::FromError::from_error($e)));
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
    sample      Randomly sample CSV data
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
            io::stderr().write_str(msg).unwrap();
        }
        Some(cmd) => {
            match cmd.run() {
                Ok(()) => os::set_exit_status(0),
                Err(CliError::Flag(err)) => err.exit(),
                Err(CliError::Csv(err)) => {
                    os::set_exit_status(1);
                    io::stderr()
                       .write_str(format!("{}\n", err.to_string()).as_slice())
                       .unwrap();
                }
                Err(CliError::Io(
                        io::IoError { kind: io::BrokenPipe, .. })) => {
                    os::set_exit_status(0);
                }
                Err(CliError::Io(err)) => {
                    os::set_exit_status(1);
                    io::stderr()
                       .write_str(format!("{}\n", err.to_string()).as_slice())
                       .unwrap();
                }
                Err(CliError::Other(msg)) => {
                    os::set_exit_status(1);
                    io::stderr()
                       .write_str(format!("{}\n", msg).as_slice())
                       .unwrap();
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
    Sample,
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
            Command::Cat => cmd::cat::run(argv),
            Command::Count => cmd::count::run(argv),
            Command::FixLengths => cmd::fixlengths::run(argv),
            Command::Flatten => cmd::flatten::run(argv),
            Command::Fmt => cmd::fmt::run(argv),
            Command::Frequency => cmd::frequency::run(argv),
            Command::Headers => cmd::headers::run(argv),
            Command::Index => cmd::index::run(argv),
            Command::Join => cmd::join::run(argv),
            Command::Sample => cmd::sample::run(argv),
            Command::Search => cmd::search::run(argv),
            Command::Select => cmd::select::run(argv),
            Command::Slice => cmd::slice::run(argv),
            Command::Sort => cmd::sort::run(argv),
            Command::Split => cmd::split::run(argv),
            Command::Stats => cmd::stats::run(argv),
            Command::Table => cmd::table::run(argv),
        }
    }
}

type CliResult<T> = Result<T, CliError>;

#[deriving(Show)]
enum CliError {
    Flag(docopt::Error),
    Csv(csv::Error),
    Io(io::IoError),
    Other(String),
}

impl FromError<docopt::Error> for CliError {
    fn from_error(err: docopt::Error) -> CliError {
        CliError::Flag(err)
    }
}

impl FromError<csv::Error> for CliError {
    fn from_error(err: csv::Error) -> CliError {
        match err {
            csv::Error::Io(v) => FromError::from_error(v),
            v => CliError::Csv(v),
        }
    }
}

impl FromError<io::IoError> for CliError {
    fn from_error(err: io::IoError) -> CliError {
        CliError::Io(err)
    }
}

impl<T: StrAllocating> FromError<T> for CliError {
    fn from_error(err: T) -> CliError {
        CliError::Other(err.into_string())
    }
}

impl FromError<regex::Error> for CliError {
    fn from_error(err: regex::Error) -> CliError {
        CliError::Other(err.to_string())
    }
}

mod cmd;
mod config;
mod select;
mod util;
