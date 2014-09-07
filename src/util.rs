use docopt;

use types::CliError;

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

pub fn arg_config() -> docopt::Config {
    docopt::Config {
        options_first: false,
        help: true,
        version: Some(version()),
    }
}

pub fn get_args<D: docopt::FlagParser>() -> Result<D, CliError> {
    docopt::FlagParser::parse_conf(arg_config()).map_err(CliError::from_flags)
}
