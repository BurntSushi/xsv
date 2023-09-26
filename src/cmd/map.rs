use std::convert::TryFrom;

use cmd::xan::{run_xan_cmd, XanCmdArgs, XanErrorPolicy, XanMode};
use config::Delimiter;
use util;
use CliResult;

static USAGE: &str = r#"
The map command evaluates an expression for each row of the given CSV file and
output the row with an added column containing the result of beforementioned
expression.

For instance, given the following CSV file:

a,b
1,4
5,2

The following command:

    $ xsv map 'add(a, b)' c

Will produce the following result:

a,b,c
1,4,5
5,2,7

For a quick review of the capabilities of the script language, use
the --cheatsheet flag.

If you want to list available functions, use the --functions flag.

Usage:
    xsv map [options] <expression> <column> [<input>]
    xsv map --cheatsheet
    xsv map --functions
    xsv map --help

map options:
    -t, --threads <threads>    Number of threads to use in order to run the
                               computations in parallel. Only useful if you
                               perform heavy stuff such as reading files etc.
    -e, --errors <policy>      What to do with evaluation errors. One of:
                                 - "panic": exit on first error
                                 - "report": add a column containing error
                                 - "ignore": coerce result for row to null
                                 - "log": print error to stderr
                               [default: panic].
    -E, --error-column <name>  Name of the column containing errors if
                               "-e/--errors" is set to "report".
                               [default: xsv_error].

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be evaled
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. [default: ,]
"#;

#[derive(Deserialize)]
struct Args {
    arg_column: String,
    arg_expression: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_functions: bool,
    flag_cheatsheet: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_threads: Option<usize>,
    flag_errors: String,
    flag_error_column: String,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let xan_args = XanCmdArgs {
        print_cheatsheet: args.flag_cheatsheet,
        print_functions: args.flag_functions,
        new_column: Some(args.arg_column),
        map_expr: args.arg_expression,
        input: args.arg_input,
        output: args.flag_output,
        no_headers: args.flag_no_headers,
        delimiter: args.flag_delimiter,
        threads: args.flag_threads,
        error_policy: XanErrorPolicy::try_from(args.flag_errors)?,
        error_column_name: Some(args.flag_error_column),
        mode: XanMode::Map,
    };

    run_xan_cmd(xan_args)
}
