use csv;

use std::io::prelude::*;
use std::fs::File;

use CliResult;
use CliError;
use config::{Config, Delimiter};
use rlua::{Lua, Error as LuaError, Table};
use util;

static USAGE: &'static str = r#"
Append a new column by executing a Lua script on every row.

This command lets you add a new column calculated by a Lua script.
The Lua script gets called once per non-header row and must return a string
with new column value for this row.

Script has 3 ways* to reference row columns (as strings):
  1. Directly by using column name (e.g. Amount), can be disabled with -g
  2. Indexing col variable by column name: col.Amount or col["Total Balance"]
  3. Indexing col variable by column 1-based index: col[1], col[2], etc.

* Only 3rd way can be used if input has no headers.

Some usage examples:

  Sum numeric columns 'a' and 'b' and call new column 'c'
  $ xsv script newcolumn c "a + b"
  $ xsv script newcolumn c "col.a + col['b']"
  $ xsv script newcolumn c "col[1] + col[2]"

  There is some magic in the previous example as 'a' and 'b' are passed in
  as strings (not numbers), but Lua still manages to add them up.
  A more explicit way of doing it, is by using tonumber
  $ xsv script newcolumn c "tonumber(a) + tonumber(b)"

  Add running total column for Amount
  $ xsv script newcolumn Total -x "tot = (tot or 0) + Amount; return tot"

  Add running total column for Amount when previous balance was 900
  $ xsv script newcolumn Total -x "tot = (tot or 900) + Amount; return tot"

  Convert Amount to always-positive AbsAmount and Type (debit/credit) columns
  $ xsv script newcolumn Type -x \
        "if tonumber(Amount) < 0 then return 'debit' else return 'credit' end" | \
    xsv script newcolumn AbsAmount "math.abs(tonumber(Amount))"

  Typing long scripts at command line gets tiresome rather quickly,
  so -f should be used for non-trivial scripts to read them from a file
  $ xsv script newcolumn Type -x -f debitcredit.lua

Usage:
    xsv script newcolumn [options] -n <script> [<input>]
    xsv script newcolumn [options] <new-column> <script> [<input>]
    xsv script --help

script newcolumn options:
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

impl From<LuaError> for CliError {
    fn from(err: LuaError) -> CliError {
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

    let mut headers = rdr.headers()?.clone();

    if !rconfig.no_headers {
        let new_column = args.arg_new_column.as_ref().ok_or("Specify new column name")?;
        headers.push_field(new_column);
        wtr.write_record(&headers)?;
    }

    let lua = Lua::new();

    lua.context(|lua_ctx| -> Result<(), CliError> {
        let globals = lua_ctx.globals();
        let col = lua_ctx.create_table()?;

        globals.set("col", col)?;

        let mut script_text =
            if args.flag_no_globals {
                String::new()
            }
            else {
                lua_ctx.load("setmetatable(col, {__index=_G})").exec()?;
                String::from("_ENV = col\n")
            };

        if !args.flag_exec {
            script_text.push_str("return ");
        }

        if args.flag_script_file {
            let mut file = File::open(&args.arg_script)?;
            file.read_to_string(&mut script_text)?;
            &args.arg_script;
        }
        else {
            script_text.push_str(&args.arg_script);
        }

        let col: Table = globals.get("col")?;

        let mut record = csv::StringRecord::new();
        while rdr.read_record(&mut record)? {
            for (i, v) in record.iter().enumerate() {
                col.set(i + 1, v)?;
            }
            if !rconfig.no_headers {
                for (h, v) in headers.iter().zip(record.iter()) {
                    col.set(h, v)?;
                }
            }

            // TODO: would be nice not to need loading the script each time
            let computed_value = lua_ctx.load(&script_text).eval::<String>()?;

            record.push_field(&computed_value);
            wtr.write_record(&record)?;
        }

        Ok(())
    })?;

    Ok(wtr.flush()?)
}
