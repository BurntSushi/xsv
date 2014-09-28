use std::path::BytesContainer;

use csv;
use docopt;

use types::{CliError, CsvConfig, Delimiter};

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

pub fn many_configs(inps: &[String], delim: Delimiter, no_headers: bool)
                   -> Result<Vec<CsvConfig>, String> {
    let mut inps = inps.to_vec();
    if inps.is_empty() {
        inps.push("-".to_string()); // stdin
    }
    let confs = inps.into_iter()
                    .map(|p| CsvConfig::new(Some(p))
                                       .delimiter(delim)
                                       .no_headers(no_headers))
                    .collect::<Vec<_>>();
    try!(errif_greater_one_stdin(confs.as_slice()));
    Ok(confs)
}

pub fn errif_greater_one_stdin(inps: &[CsvConfig]) -> Result<(), String> {
    let nstd = inps.iter().filter(|inp| inp.is_std()).count();
    if nstd > 1 {
        return Err("At most one <stdin> input is allowed.".to_string());
    }
    Ok(())
}

pub fn empty_field() -> csv::ByteString {
    csv::ByteString::from_bytes::<&[u8]>([])
}

pub fn idx_path(csv_path: &Path) -> Path {
    let mut p = csv_path.container_into_owned_bytes();
    p.push_all(".idx".as_bytes());
    Path::new(p)
}
