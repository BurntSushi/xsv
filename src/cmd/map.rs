use cmd::xan::{run_xan_cmd, XanCmdArgs};
use config::Delimiter;
use util;
use CliResult;

static USAGE: &str = "
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

Usage:
    xsv map [options] <operations> <column> [<input>]
    xsv map --help

map options:
    -t, --threads <threads>  Number of threads to use in order to run the
                             computations in parallel. Only useful if you
                             perform heavy stuff such as reading files etc.

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be evaled
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_column: String,
    arg_operations: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_threads: Option<usize>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let xan_args = XanCmdArgs {
        column: args.arg_column,
        map_expr: args.arg_operations,
        input: args.arg_input,
        output: args.flag_output,
        no_headers: args.flag_no_headers,
        delimiter: args.flag_delimiter,
        threads: args.flag_threads,
    };

    run_xan_cmd(xan_args)
}
