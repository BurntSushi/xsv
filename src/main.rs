#![allow(non_snake_case)]
#![feature(phase)]

extern crate docopt;
#[phase(plugin)] extern crate docopt_macros;
extern crate serialize;

docopt!(Args, "
Usage: cp FILE... DIR
")

fn main() {
    let args: Args = docopt::FlagParser::parse().unwrap_or_else(|e| e.exit());
    println!("Args: {}", args);
}
