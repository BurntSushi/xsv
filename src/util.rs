use std::borrow::{Cow, IntoCow, ToOwned};
use std::error::FromError;
use std::ops::Deref;
use std::path::BytesContainer;
use std::str;

use csv;
use docopt::Docopt;
use rustc_serialize::Decodable;

use CliResult;
use config::{Config, Delimiter};

pub fn version() -> String {
    let (maj, min, pat) = (
        option_env!("CARGO_PKG_VERSION_MAJOR"),
        option_env!("CARGO_PKG_VERSION_MINOR"),
        option_env!("CARGO_PKG_VERSION_PATCH"),
    );
    match (maj, min, pat) {
        (Some(maj), Some(min), Some(pat)) =>
            format!("{}.{}.{}", maj, min, pat),
        _ => "".to_string(),
    }
}

pub fn get_args<T>(usage: &str, argv: &[&str]) -> CliResult<T>
        where T: Decodable {
    Docopt::new(usage)
           .and_then(|d| d.argv(argv.iter().map(|&x| x))
                          .version(Some(version()))
                          .decode())
           .map_err(FromError::from_error)
}

pub fn many_configs(inps: &[String], delim: Option<Delimiter>,
                    no_headers: bool) -> Result<Vec<Config>, String> {
    let mut inps = inps.to_vec();
    if inps.is_empty() {
        inps.push("-".to_string()); // stdin
    }
    let confs = inps.into_iter()
                    .map(|p| Config::new(&Some(p))
                                    .delimiter(delim)
                                    .no_headers(no_headers))
                    .collect::<Vec<_>>();
    try!(errif_greater_one_stdin(&*confs));
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
    csv::ByteString::from_bytes::<&[u8]>(&[])
}

pub fn chunk_size(nitems: usize, njobs: usize) -> usize {
    if nitems < njobs {
        nitems
    } else {
        nitems / njobs
    }
}

pub fn num_of_chunks(nitems: usize, chunk_size: usize) -> usize {
    if chunk_size == 0 {
        return nitems;
    }
    let mut n = nitems / chunk_size;
    if nitems % chunk_size != 0 {
        n += 1;
    }
    n
}

pub fn condense<'a, V>(val: V, n: Option<usize>) -> Cow<'a, Vec<u8>, [u8]>
        where V: Deref<Target=[u8]> + IntoCow<'a, Vec<u8>, [u8]> {
    match n {
        None => val.into_cow(),
        Some(n) => {
            // It would be much nicer to just use a `match` here, but the
            // borrow checker won't allow it. ---AG
            //
            // (We could circumvent it by allocating a new Unicode string,
            // but that seems excessive.)
            let mut is_short_utf8 = false;
            if let Ok(s) = str::from_utf8(&*val) {
                if n >= s.chars().count() {
                    is_short_utf8 = true;
                } else {
                    let mut s = s.slice_chars(0, n).to_owned();
                    s.push_str("...");
                    return s.into_bytes().into_cow();
                }
            }
            if is_short_utf8 || n >= (*val).len() { // already short enough
                val.into_cow()
            } else {
                // This is a non-Unicode string, so we just trim on bytes.
                let mut s = val[0..n].to_vec();
                s.push_all(b"...");
                s.into_cow()
            }
        }
    }
}

pub fn idx_path(csv_path: &Path) -> Path {
    let mut p = csv_path.container_as_bytes().to_vec();
    p.push_all(".idx".as_bytes());
    Path::new(p)
}

type Idx = Option<usize>;

pub fn range(start: Idx, end: Idx, len: Idx, index: Idx)
            -> Result<(usize, usize), String> {
    let (s, e) =
        match index {
            Some(i) => {
                let exists = |&: i: Idx| i.is_some();
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
                    (&None, &None) => ::std::usize::MAX,
                    (&Some(e), &None) => e,
                    (&None, &Some(l)) => s + l,
                };
                (s, e)
            }
        };
    if s > e {
        return Err(format!(
            "The end of the range ({}) must be greater than or\n\
             equal to the start of the range ({}).", e, s));
    }
    Ok((s, e))
}
