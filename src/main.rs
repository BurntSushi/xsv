#![feature(macro_rules, phase)]

#![allow(non_snake_case)]
#![allow(dead_code)]

extern crate serialize;

extern crate csv;
extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;

use std::io;
use std::os;

macro_rules! ctry(
    ($e:expr) => (
        match $e { Ok(e) => e, Err(e) => return Err(CliError::from_str(e)) }
    )
)

macro_rules! csv_reader(
    ($args:expr) => (
        util::csv_reader($args.arg_input,
                         $args.flag_no_headers,
                         $args.flag_delimiter.to_byte(),
                         $args.flag_flexible)
    )
)

macro_rules! csv_writer(
    ($args:expr) => (
        util::csv_writer($args.flag_output,
                         $args.flag_flexible,
                         $args.flag_crlf)
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
    fmt    Format CSV output (change field delimiter)
", arg_command: Command)

#[deriving(Decodable, Show)]
enum Command {
    Fmt,
}

enum CliError {
    ErrFlag(docopt::Error),
    ErrOther(String),
}

impl CliError {
    fn from_str<T: ToString>(v: T) -> CliError {
        ErrOther(v.to_string())
    }
    fn from_flags(v: docopt::Error) -> CliError {
        ErrFlag(v)
    }
}

fn version() -> String {
    let (maj, min, pat) = (
        option_env!("CARGO_PKG_VERSION_MAJOR"),
        option_env!("CARGO_PKG_VERSION_MINOR"),
        option_env!("CARGO_PKG_VERSION_PATCH"),
    );
    match (maj, min, pat) {
        (Some(maj), Some(min), Some(pat)) => format!("{}.{}.{}", maj, min, pat),
        _ => "".to_string(),
    }
}

fn arg_config() -> docopt::Config {
    docopt::Config {
        options_first: false,
        help: true,
        version: Some(version()),
    }
}

fn get_args<D: docopt::FlagParser>() -> Result<D, CliError> {
    docopt::FlagParser::parse_conf(arg_config()).map_err(CliError::from_flags)
}

fn main() {
    let mut conf = arg_config();
    conf.options_first = true;
    let args: Args = docopt::FlagParser::parse_conf(conf)
                                        .unwrap_or_else(|e| e.exit());
    let result = match args.arg_command {
        Fmt => fmt::main(),
    };
    match result {
        Ok(()) => os::set_exit_status(0),
        Err(ErrOther(msg)) => {
            os::set_exit_status(1);
            let _ = write!(io::stderr(), "{}\n", msg);
        }
        Err(ErrFlag(err)) => err.exit(),
    }
}

mod types;
mod util;

mod fmt;
