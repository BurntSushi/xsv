use csv;
use regex::bytes::{Regex, NoExpand};
use std::process::{Command, Stdio};
use std::io::{BufReader, BufRead};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use CliResult;
use config::{Delimiter, Config};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Execute a bash command once per line in given CSV file.

Usage:
    xsv foreach [options] <column> <command> [<input>]

foreach options:
    -u, --unify            If the output of execute command is CSV, will
                           unify the result by skipping headers on each
                           subsequent command.
    -c, --new-column       If --unify is set, add a new column with given name
                           and copying the value of the current input file line.

Common options:
    -h, --help             Display this message
    -n, --no-headers       When set, the file will be considered to have no
                           headers.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_column: SelectColumns,
    arg_command: String,
    arg_input: Option<String>,
    flag_unify: bool,
    flag_new_column: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let mut rdr = rconfig.reader()?;

    let template_pattern = Regex::new(r"\{\}")?;
    let splitter_pattern = Regex::new(r#"(?:\w+|"[^"]*"|'[^']*'|`[^`]*`)"#)?;
    let cleaner_pattern = Regex::new(r#"(?:^["'`]|["'`]$)"#)?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let column_index = *sel.iter().next().unwrap();

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        let templated_command = template_pattern
            .replace_all(&args.arg_command.as_bytes(), &record[column_index])
            .to_vec();

        let mut command_pieces = splitter_pattern.find_iter(&templated_command);

        let prog = OsStr::from_bytes(command_pieces.next().unwrap().as_bytes());

        let args: Vec<String> = command_pieces.map(|piece| {
            let clean_piece = cleaner_pattern.replace_all(&piece.as_bytes(), NoExpand(b""));

            return String::from_utf8(clean_piece.into_owned()).expect("encoding error");
        }).collect();

        let mut cmd = Command::new(prog)
            .args(args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();

        // {
        //     let stdout = cmd.stdout.as_mut().unwrap();
        //     let stdout_reader = BufReader::new(stdout);
        //     let stdout_lines = stdout_reader.lines();

        //     for line in stdout_lines {
        //         println!("{}", line?);
        //     }
        // }

        cmd.wait().unwrap();
    }

    Ok(())
}
