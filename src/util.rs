use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};

use crate::app::{App, Args};

/// An error that indicates that a sub-command was seen that was not
/// recognized.
///
/// This is a sentinel error that is always converted to a panic via
/// run_subcommand. Namely, not handling a defined sub-command is a programmer
/// error.
#[derive(Debug)]
pub struct UnrecognizedCommandError;

impl std::error::Error for UnrecognizedCommandError {}

impl std::fmt::Display for UnrecognizedCommandError {
    fn fmt(&self, _: &mut std::fmt::Formatter) -> std::fmt::Result {
        unreachable!()
    }
}

/// Choose the sub-command of 'args' to run with 'run'. If the sub-command
/// wasn't recognized or is unknown, then an error is returned.
pub fn run_subcommand(
    args: &Args,
    app: impl FnOnce() -> App,
    run: impl FnOnce(&str, &Args) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let (name, args) = args.subcommand();
    if name.is_empty() || args.is_none() {
        app().print_help()?;
        writeln!(io::stdout(), "")?;
        return Ok(());
    }
    let err = match run(name, args.unwrap()) {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };
    if err.is::<UnrecognizedCommandError>() {
        // The programmer should handle all defined sub-commands,
        unreachable!("unrecognized command: {}", name);
    }
    Err(err)
}

/// Time an arbitrary operation.
pub fn timeit<T>(run: impl FnOnce() -> T) -> (T, std::time::Duration) {
    let start = std::time::Instant::now();
    let t = run();
    (t, start.elapsed())
}

/// Convenient time an operation that returns a result by packing the duration
/// into the `Ok` variant.
pub fn timeitr<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> Result<(T, std::time::Duration), E> {
    let (result, time) = timeit(run);
    let t = result?;
    Ok((t, time))
}

/// Interpret the given value as a single byte. If it is otherwise (empty or
/// longer), then return an error. Note that this permits the given value to
/// be an escape string. e.g., `\t` or `\x09` are both interpreted as a single
/// byte.
pub fn get_one_byte(val: &OsStr) -> anyhow::Result<u8> {
    let val = grep_cli::unescape_os(val);
    if val.is_empty() {
        anyhow::bail!("empty argument");
    }
    if val.len() > 1 {
        anyhow::bail!(
            "argument must be exactly one byte, but got {} bytes: '{}'",
            val.len(),
            grep_cli::escape(&val),
        );
    }
    Ok(val[0])
}

/// Return the last modified Unix timestamp for the given file metadata.
pub fn last_modified(md: &fs::Metadata) -> i64 {
    filetime::FileTime::from_last_modification_time(md).unix_seconds()
}

/// Returns true if and only if the given environment variable is set to the
/// value of '1'.
pub fn is_env_true<E: AsRef<OsStr>>(name: E) -> bool {
    env::var(name).map(|v| v == "1").unwrap_or(false)
}
