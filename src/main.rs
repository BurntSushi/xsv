extern crate byteorder;
extern crate crossbeam_channel as channel;
extern crate csv;
extern crate csv_index;
extern crate docopt;
extern crate filetime;
extern crate num_cpus;
extern crate rand;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate stats;
extern crate tabwriter;
extern crate threadpool;

use std::borrow::ToOwned;
use std::env;
use std::fmt;
use std::io;
use std::process;

use docopt::Docopt;

macro_rules! wout {
    ($($arg:tt)*) => ({
        use std::io::Write;
        (writeln!(&mut ::std::io::stdout(), $($arg)*)).unwrap();
    });
}

macro_rules! werr {
    ($($arg:tt)*) => ({
        use std::io::Write;
        (writeln!(&mut ::std::io::stderr(), $($arg)*)).unwrap();
    });
}

macro_rules! fail {
    ($e:expr) => (Err(::std::convert::From::from($e)));
}

macro_rules! command_list {
    () => (
"
    behead      Drop header from CSV file
    cat         Concatenate by row or column
    count       Count records
    fixlengths  Makes all records have same length
    flatten     Show one field per line
    fmt         Format CSV output (change field delimiter)
    frequency   Show frequency tables
    headers     Show header names
    help        Show this usage message.
    index       Create CSV index for faster access
    input       Read CSV data with special quoting rules
    join        Join CSV files
    partition   Partition CSV data based on a column value
    sample      Randomly sample CSV data
    reverse     Reverse rows of CSV data
    search      Search CSV data with regexes
    select      Select columns from CSV
    slice       Slice records from CSV
    sort        Sort CSV data
    split       Split CSV data into many files
    stats       Compute basic statistics
    table       Align CSV data into columns
"
    )
}

mod cmd;
mod config;
mod index;
mod select;
mod util;

static USAGE: &'static str = concat!("
Usage:
    xsv <command> [<args>...]
    xsv [options]

Options:
    --list        List all commands available.
    -h, --help    Display this message
    <command> -h  Display the command help message
    --version     Print version info and exit

Commands:", command_list!());

#[derive(Deserialize)]
struct Args {
    arg_command: Option<Command>,
    flag_list: bool,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                            .and_then(|d| d.options_first(true)
                                           .version(Some(util::version()))
                                           .deserialize())
                            .unwrap_or_else(|e| e.exit());
    if args.flag_list {
        wout!(concat!("Installed commands:", command_list!()));
        return;
    }
    match args.arg_command {
        None => {
            werr!(concat!(
                "xsv is a suite of CSV command line utilities.

Please choose one of the following commands:",
                command_list!()));
            process::exit(0);
        }
        Some(cmd) => {
            match cmd.run() {
                Ok(()) => process::exit(0),
                Err(CliError::Flag(err)) => err.exit(),
                Err(CliError::Csv(err)) => {
                    werr!("{}", err);
                    process::exit(1);
                }
                Err(CliError::Io(ref err))
                        if err.kind() == io::ErrorKind::BrokenPipe => {
                    process::exit(0);
                }
                Err(CliError::Io(err)) => {
                    werr!("{}", err);
                    process::exit(1);
                }
                Err(CliError::Other(msg)) => {
                    werr!("{}", msg);
                    process::exit(1);
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Command {
    Behead,
    Cat,
    Count,
    FixLengths,
    Flatten,
    Fmt,
    Frequency,
    Headers,
    Help,
    Index,
    Input,
    Join,
    Partition,
    Reverse,
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
        let argv: Vec<_> = env::args().map(|v| v.to_owned()).collect();
        let argv: Vec<_> = argv.iter().map(|s| &**s).collect();
        let argv = &*argv;

        if !argv[1].chars().all(char::is_lowercase) {
            return Err(CliError::Other(format!(
                "xsv expects commands in lowercase. Did you mean '{}'?",
                argv[1].to_lowercase()).to_string()));
        }
        match self {
            Command::Behead => cmd::behead::run(argv),
            Command::Cat => cmd::cat::run(argv),
            Command::Count => cmd::count::run(argv),
            Command::FixLengths => cmd::fixlengths::run(argv),
            Command::Flatten => cmd::flatten::run(argv),
            Command::Fmt => cmd::fmt::run(argv),
            Command::Frequency => cmd::frequency::run(argv),
            Command::Headers => cmd::headers::run(argv),
            Command::Help => { wout!("{}", USAGE); Ok(()) }
            Command::Index => cmd::index::run(argv),
            Command::Input => cmd::input::run(argv),
            Command::Join => cmd::join::run(argv),
            Command::Partition => cmd::partition::run(argv),
            Command::Reverse => cmd::reverse::run(argv),
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

pub type CliResult<T> = Result<T, CliError>;

#[derive(Debug)]
pub enum CliError {
    Flag(docopt::Error),
    Csv(csv::Error),
    Io(io::Error),
    Other(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CliError::Flag(ref e) => { e.fmt(f) }
            CliError::Csv(ref e) => { e.fmt(f) }
            CliError::Io(ref e) => { e.fmt(f) }
            CliError::Other(ref s) => { f.write_str(&**s) }
        }
    }
}

impl From<docopt::Error> for CliError {
    fn from(err: docopt::Error) -> CliError {
        CliError::Flag(err)
    }
}

impl From<csv::Error> for CliError {
    fn from(err: csv::Error) -> CliError {
        if !err.is_io_error() {
            return CliError::Csv(err);
        }
        match err.into_kind() {
            csv::ErrorKind::Io(v) => From::from(v),
            _ => unreachable!(),
        }
    }
}

impl From<io::Error> for CliError {
    fn from(err: io::Error) -> CliError {
        CliError::Io(err)
    }
}

impl From<String> for CliError {
    fn from(err: String) -> CliError {
        CliError::Other(err)
    }
}

impl<'a> From<&'a str> for CliError {
    fn from(err: &'a str) -> CliError {
        CliError::Other(err.to_owned())
    }
}

impl From<regex::Error> for CliError {
    fn from(err: regex::Error) -> CliError {
        CliError::Other(format!("{:?}", err))
    }
}
