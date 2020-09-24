#![allow(dead_code, unused_imports, unused_variables)]

mod app;
mod cmd;
mod config;
mod index;
mod util;

fn main() -> anyhow::Result<()> {
    let args = app::root().get_matches();
    util::run_subcommand(&args, app::root, |cmd, args| match cmd {
        "count" => cmd::count::run(args),
        _ => Err(util::UnrecognizedCommandError.into()),
    })
}
