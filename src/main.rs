extern crate atty;
extern crate byteorder;
extern crate bytesize;
extern crate calamine;
extern crate chrono;
extern crate chrono_tz;
extern crate colored;
extern crate crossbeam_channel as channel;
extern crate csv;
extern crate csv_index;
extern crate dateparser;
extern crate docopt;
extern crate emojis;
extern crate encoding;
extern crate ext_sort;
extern crate filetime;
extern crate flate2;
extern crate glob;
#[cfg(feature = "lang")]
extern crate lingua;
extern crate nom;
extern crate num_cpus;
extern crate numfmt;
extern crate pager;
extern crate pariter;
extern crate rand;
extern crate rayon;
extern crate regex;
extern crate serde;
extern crate thread_local;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate stats;
extern crate tabwriter;
extern crate termsize;
extern crate textwrap;
extern crate threadpool;
extern crate unicode_bidi;
extern crate unicode_segmentation;
extern crate unicode_width;
extern crate unidecode;
extern crate uuid;

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
    ($e:expr) => {
        Err(::std::convert::From::from($e))
    };
}

macro_rules! command_list {
    () => {
        "
    behead      Drop header from CSV file
    bins        Dispatch numeric columns into bins
    cat         Concatenate by row or column
    count       Count records
    datefmt     Format a recognized date column to a specified format and timezone
    enum        Enumerate CSV file by preprending an index column
    explode     Explode rows based on some column separator
    filter      Only keep some CSV rows based on an evaluated expression
    fixlengths  Makes all records have same length
    flatmap     Emit one row per value yielded by an expression evaluated for each CSV row
    flatten     Show one field per line
    fmt         Format CSV output (change field delimiter)
    foreach     Loop over a CSV file to execute bash commands
    frequency   Show frequency tables
    glob        Create a CSV file with paths matching a glob pattern
    headers     Show header names
    help        Show this usage message.
    hist        Print a histogram with rows of CSV file as bars
    implode     Collapse consecutive identical rows based on a diverging column
    index       Create CSV index for faster access
    input       Read CSV data with special quoting rules
    join        Join CSV files
    jsonl       Convert newline-delimited JSON files to CSV
    kway        Merge multiple similar already sorted CSV files
    lang        Add a column with the language detected in a given CSV column
    map         Create a new column by evaluating an expression on each CSV row
    partition   Partition CSV data based on a column value
    pseudo      Pseudonymise the values of a column
    sample      Randomly sample CSV data
    transform   Transform a column by evaluating an expression on each CSV row
    replace     Replace patterns in CSV data
    reverse     Reverse rows of CSV data
    search      Search CSV data with regexes
    select      Select columns from CSV
    shuffle     Shuffle CSV data
    slice       Slice records from CSV
    sort        Sort CSV data
    split       Split CSV data into many files
    stats       Compute basic statistics
    view        Preview a CSV file in a human-friendly way
    xls         Convert Excel/OpenOffice spreadsheets to CSV
"
    };
}

mod cmd;
mod config;
mod index;
mod select;
mod util;
mod xan;

static USAGE: &str = concat!(
    "
Usage:
    xsv <command> [<args>...]
    xsv [options]

Options:
    --list        List all commands available.
    -h, --help    Display this message
    <command> -h  Display the command help message
    --version     Print version info and exit

Commands:",
    command_list!()
);

#[derive(Deserialize)]
struct Args {
    arg_command: Option<Command>,
    flag_list: bool,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| {
            d.options_first(true)
                .version(Some(util::version()))
                .deserialize()
        })
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
                command_list!()
            ));
            process::exit(0);
        }
        Some(cmd) => match cmd.run() {
            Ok(()) => process::exit(0),
            Err(CliError::Flag(err)) => err.exit(),
            Err(CliError::Csv(err)) => {
                werr!("{}", err);
                process::exit(1);
            }
            Err(CliError::Io(ref err)) if err.kind() == io::ErrorKind::BrokenPipe => {
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
        },
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Command {
    Behead,
    Bins,
    Cat,
    Count,
    Datefmt,
    Enum,
    Explode,
    ForEach,
    Filter,
    FixLengths,
    Flatmap,
    Flatten,
    Fmt,
    Frequency,
    Glob,
    Headers,
    Help,
    Hist,
    Implode,
    Index,
    Input,
    Join,
    Jsonl,
    Kway,
    Lang,
    Map,
    Partition,
    Pseudo,
    Replace,
    Reverse,
    Sample,
    Search,
    Select,
    Shuffle,
    Slice,
    Sort,
    Split,
    Stats,
    Transform,
    View,
    Xls,
}

impl Command {
    fn run(self) -> CliResult<()> {
        let argv: Vec<_> = env::args().collect();
        let argv: Vec<_> = argv.iter().map(|s| &**s).collect();
        let argv = &*argv;

        if !argv[1].chars().all(char::is_lowercase) {
            return Err(CliError::Other(format!(
                "xsv expects commands in lowercase. Did you mean '{}'?",
                argv[1].to_lowercase()
            )));
        }
        match self {
            Command::Behead => cmd::behead::run(argv),
            Command::Bins => cmd::bins::run(argv),
            Command::Cat => cmd::cat::run(argv),
            Command::Count => cmd::count::run(argv),
            Command::Datefmt => cmd::datefmt::run(argv),
            Command::Enum => cmd::enumerate::run(argv),
            Command::Explode => cmd::explode::run(argv),
            Command::ForEach => cmd::foreach::run(argv),
            Command::Filter => cmd::filter::run(argv),
            Command::FixLengths => cmd::fixlengths::run(argv),
            Command::Flatmap => cmd::flatmap::run(argv),
            Command::Flatten => cmd::flatten::run(argv),
            Command::Fmt => cmd::fmt::run(argv),
            Command::Frequency => cmd::frequency::run(argv),
            Command::Glob => cmd::glob::run(argv),
            Command::Headers => cmd::headers::run(argv),
            Command::Help => {
                wout!("{}", USAGE);
                Ok(())
            }
            Command::Hist => cmd::hist::run(argv),
            Command::Implode => cmd::implode::run(argv),
            Command::Index => cmd::index::run(argv),
            Command::Input => cmd::input::run(argv),
            Command::Join => cmd::join::run(argv),
            Command::Jsonl => cmd::jsonl::run(argv),
            Command::Kway => cmd::kway::run(argv),
            #[cfg(feature = "lang")]
            Command::Lang => cmd::lang::run(argv),
            #[cfg(not(feature = "lang"))]
            Command::Lang => Ok(println!(
                "This version of XSV was not compiled with the \"lang\" feature."
            )),
            Command::Map => cmd::map::run(argv),
            Command::Partition => cmd::partition::run(argv),
            Command::Pseudo => cmd::pseudo::run(argv),
            Command::Replace => cmd::replace::run(argv),
            Command::Reverse => cmd::reverse::run(argv),
            Command::Sample => cmd::sample::run(argv),
            Command::Search => cmd::search::run(argv),
            Command::Select => cmd::select::run(argv),
            Command::Shuffle => cmd::shuffle::run(argv),
            Command::Slice => cmd::slice::run(argv),
            Command::Sort => cmd::sort::run(argv),
            Command::Split => cmd::split::run(argv),
            Command::Stats => cmd::stats::run(argv),
            Command::Transform => cmd::transform::run(argv),
            Command::View => cmd::view::run(argv),
            Command::Xls => cmd::xls::run(argv),
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
            CliError::Flag(ref e) => e.fmt(f),
            CliError::Csv(ref e) => e.fmt(f),
            CliError::Io(ref e) => e.fmt(f),
            CliError::Other(ref s) => f.write_str(s),
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

impl From<xan::PrepareError> for CliError {
    fn from(err: xan::PrepareError) -> CliError {
        CliError::Other(err.to_string())
    }
}

impl From<xan::EvaluationError> for CliError {
    fn from(err: xan::EvaluationError) -> CliError {
        CliError::Other(err.to_string())
    }
}

impl From<glob::GlobError> for CliError {
    fn from(err: glob::GlobError) -> Self {
        CliError::Other(err.to_string())
    }
}

impl From<glob::PatternError> for CliError {
    fn from(err: glob::PatternError) -> Self {
        CliError::Other(err.to_string())
    }
}

impl From<()> for CliError {
    fn from(_: ()) -> CliError {
        CliError::Other("unknown error".to_string())
    }
}
