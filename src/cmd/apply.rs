use CliError;
use CliResult;
use config::{Config, Delimiter};
use select::{SelectColumns, Selection};
use util;

use os_type;
use std::collections::VecDeque;
use std::io::{Write, BufReader, BufRead};
use std::process;
use std::sync::mpsc;
use std::thread;

static USAGE: &'static str = "
Runs an external program over specified columns of a csv file.

Usage:
    xsv apply [options] <executable> <column-name> [<input>]
    xsv apply --help

Given a program which reads lines from stdin and writes lines to stdout, the apply command will
transform columns of the input file by sending each entry for each row (for each column) to the
external program. The entires are replaced with the values printed by the external program.

For example, with the input file:
Column1,Column2
Entry1,Entry2
Entry3,Entry4

We could run the following command (on linux, where sed is available)
$ xsv apply \"sed -e 's/^/new_prefix_/'\" Column1 input.csv
Column1,Column2
new_prefix_Entry1,Entry2
new_prefix_Entry3,Entry4

Note: The sed invocation just prepends \"new_prefix_\" to each line in its input.

On linux, the program will be started in a subshell.
On Windows, the program will be started directly.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

/// Struct representing an external program from which we will read and write lines of data
struct ChildProgram {
    prog: process::Child,
    rx: mpsc::Receiver<String>,
    reader: Option<thread::JoinHandle<()>>,
}

impl ChildProgram {
    /// Creates a child process running the command given
    /// On unix systems, this will start a subshell
    /// On windows, we start the process directly
    /// This method also creates and starts a thread which will read
    /// from the child process's stdin whenever lines are available.
    /// These lines are cached until read using `for_all_avail_lines`
    pub fn new(exe: &str) -> CliResult<Self> {
        let mut cmd = match os_type::current_platform() {
            os_type::OSType::Windows => {
                let s = exe.split_whitespace().collect::<Vec<&str>>();
                process::Command::new(s[0])
            }
            _ => process::Command::new("sh"),
        };

        match os_type::current_platform() {
            os_type::OSType::Windows => {
                let s = exe.split_whitespace().collect::<Vec<&str>>();
                for arg in s[1..].iter() {
                    cmd.arg(arg);
                }
            }

            _ => {
                cmd.arg("-c").arg(exe);
                ()
            }
        };

        let prog = cmd.stdout(process::Stdio::piped())
            .stdin(process::Stdio::piped())
            .spawn();

        if prog.is_err() {
            let error = prog.err().unwrap();
            return Err(CliError::Other(format!("Failed to start child process because: {}",
                                               error)));
        }

        let mut prog = prog.unwrap();
        let progout = prog.stdout.take().expect("Failed to setup stdout for child");
        let progout = BufReader::new(progout);

        // create a thread to keep up with the external program's output The thread lets us perform
        // simple blocking reads, then deliver lines to the main thread as they become available
        // via the channel
        let (tx, rx) = mpsc::channel();
        let reader = thread::Builder::new()
            .name("reader".into())
            .spawn(move || {
                let lines = progout.lines();
                for line in lines {
                    match tx.send(line.unwrap()) {
                        Ok( () ) => (),
                        Err(_) => break,
                    }
                }
            })
            .unwrap();

        Ok(ChildProgram {
            prog: prog,
            rx: rx,
            reader: Some(reader),
        })
    }

    /// Calls a callback for all lines cached from the child
    pub fn for_all_avail_lines<F>(&mut self, mut f: F)
        where F: FnMut(String) -> ()
    {
        loop {
            match self.rx.try_recv() {
                Ok(l) => f(l),
                Err(_) => break,
            }
        }
    }

    /// Writes a line to the child
    /// Do not include a trailing \n
    pub fn write_line(&mut self, line: &[u8]) -> CliResult<()> {
        let sref = self.prog.stdin.as_mut().unwrap();
        try!(sref.write(line));
        try!(sref.write("\n".as_bytes()));
        Ok(())
    }

    /// Shutdown the child and wait for it to exit
    pub fn wait(&mut self) -> CliResult<process::ExitStatus> {
        // if this is not the case, we have already joined
        assert!(self.reader.is_some());

        let res = match self.prog.wait() {
            Ok(s) => Ok(s),
            Err(e) => Err(CliError::Io(e)),
        };

        match self.reader.take().unwrap().join() {
            Ok(_) => res,
            Err(e) => Err(CliError::Other(format!("Reader thread died with error: {:?}", e))),
        }
    }
}

impl Drop for ChildProgram {
    fn drop(&mut self) {
        // if the program is still running, shut it down gracefully
        // calling wait will close the child's stdin
        if self.reader.is_some() {
            self.wait().unwrap();
        }
    }
}

#[derive(RustcDecodable)]
struct Args {
    arg_column_name: SelectColumns,
    arg_executable: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_delimiter: Option<Delimiter>,
}

// replaces each selection with the values at the front of new_values
// modifies the row in place
pub fn replace_in_row(selection: &Selection,
                      new_values: &mut VecDeque<String>,
                      row: &mut Vec<Vec<u8>>) {
    for &i in selection.iter() {
        let l = new_values.pop_front().unwrap();
        row[i] = l.into_bytes();
    }

}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = try!(util::get_args(USAGE, argv));

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .select(args.arg_column_name);

    let mut rdr = try!(rconfig.reader());
    let mut wtr = try!(Config::new(&args.flag_output).writer());

    let headers = try!(rdr.byte_headers());
    let sel = try!(rconfig.selection(&*headers));
    try!(rconfig.write_headers(&mut rdr, &mut wtr));

    let mut prog = try!(ChildProgram::new(&args.arg_executable));

    // we will hold a small number of CSV rows in memory while waiting for the external program to
    // generate results Once the program generates some new lines, we will begin writing rows of
    // output CSV (and removing the rows from memory)
    let mut rows = VecDeque::new();
    let mut lines = VecDeque::new();

    for row in rdr.byte_records() {
        let row = try!(row);

        for i in sel.iter() {
            try!(prog.write_line(&*row[*i]))
        }

        rows.push_back(row);

        // for each line we have read from the output of the external program so far,
        // find the row, do substitutions, then write the row
        // use integer division so we never get a row into some partially substituted state
        // We should always have at least as many rows as lines.len()/sel.len() so it will be
        // safe to unwrap
        prog.for_all_avail_lines(|line| lines.push_back(line));
        for _i in 0..(lines.len() / sel.len()) {
            let mut row = rows.pop_front().unwrap();
            replace_in_row(&sel, &mut lines, &mut row);
            try!(wtr.write(row.into_iter()));
        }
    }

    try!(prog.wait());

    // push out the rest of the lines
    prog.for_all_avail_lines(|line| lines.push_back(line));
    for mut row in rows {
        replace_in_row(&sel, &mut lines, &mut row);
        try!(wtr.write(row.into_iter()));
    }

    try!(wtr.flush());
    Ok(())
}
