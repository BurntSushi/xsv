use cmd::xan::{run_xan_cmd, XanCmdArgs, XanErrorPolicy, XanMode};
use config::Delimiter;
use util;
use CliResult;

static USAGE: &str = r#"
The flatmap command evaluates an expression for each row of the given CSV file.
This expression is expected to return a potentially iterable value (e.g. a list).

If said value is falsey, then no row will be written in the output of the input
row.

Then, for each nested value yielded by the expression, one row of CSV will be
written to the output.

This row will have the same columns as the input with an additional one
containing the nested value or replacing the value of a column of your choice,
using the -r/--replace flag.

For instance, given the following CSV file:

name,colors
John,blue
Mary,yellow|red

The following command:

    $ xsv flatmap 'split(colors, "|")' color -r colors

Will produce the following result:

name,color
John,blue
Mary,yellow
Mary,red

Note that if the expression returns an empty list or a falsey value, no row will
be written in the output for the current input row. This means one can use the
flatmap command as a sort of combined map and filter in a single pass over the CSV file.

For instance, given the following CSV file:

name,age
John Mayer,34
Mary Sue,45

The following command:

    $ xsv flatmap 'if(gte(age, 40), last(split(name, " ")))' surname

Will produce the following result:

name,age,surname
Mary Sue,45,Sue

For a quick review of the capabilities of the script language, use
the --cheatsheet flag.

If you want to list available functions, use the --functions flag.

Usage:
    xsv flatmap [options] <expression> <column> [<input>]
    xsv flatmap --cheatsheet
    xsv flatmap --functions
    xsv flatmap --help

flatmap options:
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
    arg_column: String,
    arg_expression: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_cheatsheet: bool,
    flag_functions: bool,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_threads: Option<usize>,
    flag_errors: String,
    flag_replace: Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let xan_args = XanCmdArgs {
        print_cheatsheet: args.flag_cheatsheet,
        print_functions: args.flag_functions,
        target_column: Some(args.arg_column),
        rename_column: args.flag_replace,
        map_expr: args.arg_expression,
        input: args.arg_input,
        output: args.flag_output,
        no_headers: args.flag_no_headers,
        delimiter: args.flag_delimiter,
        threads: args.flag_threads,
        error_policy: XanErrorPolicy::from_restricted(&args.flag_errors)?,
        error_column_name: None,
        mode: XanMode::Flatmap,
    };

    run_xan_cmd(xan_args)
}
