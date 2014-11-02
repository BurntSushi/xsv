use std::path::BytesContainer;

use csv;
use docopt::Docopt;

use {CliError, CliResult};
use config::{Config, Delimiter};

pub fn version() -> String {
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

pub fn get_args<T>(usage: &str, argv: &[&str]) -> CliResult<T>
       where T: ::serialize::Decodable<::docopt::Decoder, ::docopt::Error> {
    Docopt::new(usage)
           .and_then(|d| d.argv(argv.iter().map(|&x| x))
                          .version(Some(version()))
                          .decode())
           .map_err(CliError::from_flags)
}

pub fn many_configs(inps: &[String], delim: Delimiter, no_headers: bool)
                   -> Result<Vec<Config>, String> {
    let mut inps = inps.to_vec();
    if inps.is_empty() {
        inps.push("-".to_string()); // stdin
    }
    let confs = inps.into_iter()
                    .map(|p| Config::new(Some(p))
                                    .delimiter(delim)
                                    .no_headers(no_headers))
                    .collect::<Vec<_>>();
    try!(errif_greater_one_stdin(confs.as_slice()));
    Ok(confs)
}

pub fn errif_greater_one_stdin(inps: &[Config]) -> Result<(), String> {
    let nstd = inps.iter().filter(|inp| inp.is_std()).count();
    if nstd > 1 {
        return Err("At most one <stdin> input is allowed.".to_string());
    }
    Ok(())
}

pub fn empty_field() -> csv::ByteString {
    csv::ByteString::from_bytes::<&[u8]>([])
}

pub fn num_of_chunks(nitems: uint, chunk_size: uint) -> uint {
    let mut n = nitems / chunk_size;
    if nitems % chunk_size != 0 {
        n += 1;
    }
    n
}

pub fn idx_path(csv_path: &Path) -> Path {
    let mut p = csv_path.container_into_owned_bytes();
    p.push_all(".idx".as_bytes());
    Path::new(p)
}

type Idx = Option<uint>;

pub fn range(start: Idx, end: Idx, len: Idx, index: Idx)
            -> Result<(uint, uint), String> {
    let (s, e) =
        match index {
            Some(i) => {
                let exists = |i: Idx| i.is_some();
                if exists(start) || exists(end) || exists(len) {
                    return Err("--index cannot be used with \
                                --start, --end or --len".to_string());
                }
                (i, i+1)
            }
            None => {
                let s = start.unwrap_or(0);
                let e = match (&end, &len) {
                    (&Some(_), &Some(_)) =>
                        return Err("--end and --len cannot be used
                                    at the same time.".to_string()),
                    (&None, &None) => ::std::uint::MAX,
                    (&Some(e), &None) => e,
                    (&None, &Some(l)) => s + l,
                };
                (s, e)
            }
        };
    if s > e {
        return Err(format!(
            "The end of the range ({:u}) must be greater than or\n\
             equal to the start of the range ({:u}).", e, s));
    }
    Ok((s, e))
}
