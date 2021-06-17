use csv;

use std::io::prelude::*;
use std::fs::File;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use CliResult;
use CliError;
use config::{Config, Delimiter};
use util;

const HELPERS: &str = r#"
def cast_as_string(value):
    if isinstance(value, str):
        return value
    return str(value)

def cast_as_bool(value):
    return bool(value)

class XSVRow(object):
    def __init__(self, headers):
        self.__data = None
        self.__headers = headers
        self.__mapping = {h: i for i, h in enumerate(headers)}

    def _update_underlying_data(self, row_data):
        self.__data = row_data

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.__data[key]

        return self.__data[self.__mapping[key]]

    def __getattr__(self, key):
        return self.__data[self.__mapping[key]]
"#;

fn template_execution(statements: &str) -> String {
    format!("def __run__():\n{}\n__return_value__ = __run__()", textwrap::indent(statements, "  "))
}

// TODO: options for boolean return coercion

static USAGE: &'static str = r#"
Create a new column, filter rows or compute aggregations by executing a Lua
script of every line of a CSV file.

The executed Lua has 3 ways to reference row columns (as strings):
  1. Directly by using column name (e.g. Amount), can be disabled with -g
  2. Indexing col variable by column name: col.Amount or col["Total Balance"]
  3. Indexing col variable by column 1-based index: col[1], col[2], etc.

Of course, if your input has no headers, then 3. will be the only available
option.

Some usage examples:

  Sum numeric columns 'a' and 'b' and call new column 'c'
  $ xsv py map c "a + b"
  $ xsv py map c "col.a + col['b']"
  $ xsv py map c "col[1] + col[2]"

  There is some magic in the previous example as 'a' and 'b' are passed in
  as strings (not numbers), but Lua still manages to add them up.
  A more explicit way of doing it, is by using tonumber
  $ xsv py map c "tonumber(a) + tonumber(b)"

  Add running total column for Amount
  $ xsv py map Total -x "tot = (tot or 0) + Amount; return tot"

  Add running total column for Amount when previous balance was 900
  $ xsv py map Total -x "tot = (tot or 900) + Amount; return tot"

  Convert Amount to always-positive AbsAmount and Type (debit/credit) columns
  $ xsv py map Type -x \
        "if tonumber(Amount) < 0 then return 'debit' else return 'credit' end" | \
    xsv py map AbsAmount "math.abs(tonumber(Amount))"

  Typing long scripts at command line gets tiresome rather quickly,
  so -f should be used for non-trivial scripts to read them from a file
  $ xsv py map Type -x -f debitcredit.py

Usage:
    xsv py map [options] -n <script> [<input>]
    xsv py map [options] <new-column> <script> [<input>]
    xsv py filter [options] <script> [<input>]
    xsv py map --help
    xsv py filter --help
    xsv py --help

py options:
    -x, --exec         exec[ute] Lua script, instead of the default eval[uate].
                       eval (default) expects just a single Lua expression,
                       while exec expects one or more statements, allowing
                       to create Lua programs.
    -f, --script-file  <script> is a file name containing Lua script.
                       By default (no -f) <script> is the script text.
    -g, --no-globals   Don't create Lua global variables for each column, only col.
                       Useful when some column names mask standard Lua globals.
                       Note: access to Lua globals thru _G remains even without -g.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. Namely, it will be sorted with the rest
                           of the rows. Otherwise, the first row will always
                           appear as the header row in the output.
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
"#;

#[derive(Deserialize)]
struct Args {
    cmd_map: bool,
    cmd_filter: bool,
    arg_new_column: Option<String>,
    arg_script: String,
    arg_input: Option<String>,
    flag_exec: bool,
    flag_script_file: bool,
    flag_no_globals: bool,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

impl From<PyErr> for CliError {
    fn from(err: PyErr) -> CliError {
        CliError::Other(err.to_string())
    }
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let gil = Python::acquire_gil();
    let py = gil.python();

    let helpers = PyModule::from_code(py, HELPERS, "xsv_helpers.py", "xsv_helpers")?;
    let globals = PyDict::new(py);
    let locals = PyDict::new(py);

    // Global imports
    let builtins = PyModule::import(py, "builtins")?;
    let math_module = PyModule::import(py, "math")?;

    globals.set_item("__builtins__", builtins)?;
    globals.set_item("math", math_module)?;

    let mut headers = rdr.headers()?.clone();
    let headers_len = headers.len();

    let py_row = helpers.call1("XSVRow", (headers.iter().collect::<Vec<&str>>(),))?;
    locals.set_item("row", py_row)?;

    if !rconfig.no_headers {

        if !args.cmd_filter {
            let new_column = args.arg_new_column.as_ref().ok_or("Specify new column name")?;
            headers.push_field(new_column);
        }

        wtr.write_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {

        // Initializing locals
        let mut row_data: Vec<&str> = Vec::with_capacity(headers_len);

        for (i, h) in headers.iter().take(headers_len).enumerate() {
            let cell_value = record.get(i).unwrap();
            locals.set_item(h, cell_value)?;
            row_data.push(cell_value);
        }

        py_row.call_method1("_update_underlying_data", (row_data,))?;

        let result = py.eval(&args.arg_script, Some(&globals), Some(&locals)).map_err(|e| {
            e.print_and_set_sys_last_vars(py);
            "Evaluation of given expression failed with the above error!"
        })?;

        if args.cmd_map {
            let result = helpers.call1("cast_as_string", (result,))?;
            let value: String = result.extract()?;

            record.push_field(&value);
            wtr.write_record(&record)?;
        }
    }

    Ok(wtr.flush()?)
}
