#[cfg(feature = "py")] use {
    csv,
    pyo3::prelude::*,
    pyo3::types::PyDict,
    config::{Config, Delimiter},
    CliError,
    util,
};

use CliResult;

#[cfg(feature = "py")]
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

// fn template_execution(statements: &str) -> String {
//     format!("def __run__():\n{}\n__return_value__ = __run__()", textwrap::indent(statements, "  "))
// }

// TODO: options for boolean return coercion
#[cfg(feature = "py")]
static USAGE: &'static str = r#"
Create a new column, filter rows or compute aggregations by evaluating a python
expression on every row of a CSV file.

The executed Python has 4 ways to reference cell values (as strings):
  1. Directly by using column name (e.g. amount) as a local variable
  2. Indexing cell value by column name as an attribute: row.amount
  3. Indexing cell value by column name as a key: row["amount"]
  4. Indexing cell value by column position: row[0]

Of course, if your input has no headers, then 4. will be the only available
option.

Some usage examples:

  Sum numeric columns 'a' and 'b' and call new column 'c'
  $ xsv py map c "int(a) + int(b)"
  $ xsv py map c "int(col.a) + int(col['b'])"
  $ xsv py map c "int(col[0]) + int(col[1])"

  Strip and prefix cell values
  $ xsv py map prefixed "'clean_' + a.strip()"

  Filter some lines based on numerical filtering
  $ xsv py filter "int(a) > 45"

Usage:
    xsv py map [options] -n <script> [<input>]
    xsv py map [options] <new-column> <script> [<input>]
    xsv py filter [options] <script> [<input>]
    xsv py map --help
    xsv py filter --help
    xsv py --help

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

#[cfg(feature = "py")]
#[derive(Deserialize)]
struct Args {
    cmd_map: bool,
    cmd_filter: bool,
    arg_new_column: Option<String>,
    arg_script: String,
    arg_input: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

#[cfg(feature = "py")]
impl From<PyErr> for CliError {
    fn from(err: PyErr) -> CliError {
        CliError::Other(err.to_string())
    }
}

#[cfg(not(feature = "py"))]
pub fn run(_argv: &[&str]) -> CliResult<()> {
    Ok(println!("This version of XSV was not compiled with the \"py\" feature."))
}

#[cfg(feature = "py")]
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
    else {
        headers = csv::StringRecord::new();

        for i in 0..headers_len {
            headers.push_field(&i.to_string());
        }
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
        else if args.cmd_filter {
            let result = helpers.call1("cast_as_bool", (result,))?;
            let value: bool = result.extract()?;

            if value {
                wtr.write_record(&record)?;
            }
        }
    }

    Ok(wtr.flush()?)
}
