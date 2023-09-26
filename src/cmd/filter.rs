use cmd::xan::{run_xan_cmd, XanCmdArgs, XanErrorPolicy, XanMode};
use config::Delimiter;
use util;
use CliResult;

static USAGE: &str = r#"
The filter command evaluates an expression for each row of the given CSV file and
only output the row with if the result of beforementioned expression is truthy.

For instance, given the following CSV file:

a
1
2
3

The following command:

    $ xsv filter 'lt(a, 1)'

Will produce the following result:

a
2
3

For a quick review of the capabilities of the script language, use
the --cheatsheet flag.

If you want to list available functions, use the --functions flag.

Usage:
    xsv filter [options] <expression> [<input>]
    xsv filter --cheatsheet
    xsv filter --functions
    xsv filter --help

filter options:
    -t, --threads <threads>    Number of threads to use in order to run the
                               computations in parallel. Only useful if you
                               perform heavy stuff such as reading files etc.
    -e, --errors <policy>      What to do with evaluation errors. One of:
                                 - "panic": exit on first error
                                 - "ignore": coerce result for row to null
                                 - "log": print error to stderr
                               [default: panic].

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
    arg_expression: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_cheatsheet: bool,
    flag_functions: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_threads: Option<usize>,
    flag_errors: String,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let xan_args = XanCmdArgs {
        print_cheatsheet: args.flag_cheatsheet,
        print_functions: args.flag_functions,
        new_column: None,
        map_expr: args.arg_expression,
        input: args.arg_input,
        output: args.flag_output,
        no_headers: args.flag_no_headers,
        delimiter: args.flag_delimiter,
        threads: args.flag_threads,
        error_policy: XanErrorPolicy::from_restricted(&args.flag_errors)?,
        error_column_name: None,
        mode: XanMode::Filter,
    };

    run_xan_cmd(xan_args)
}
