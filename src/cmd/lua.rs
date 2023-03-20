use csv;

use std::fs::File;
use std::io::prelude::*;

use config::{Config, Delimiter};
use hlua::{AnyLuaValue, Lua, LuaError, LuaTable};
use util;
use CliError;
use CliResult;

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
  $ xsv lua map c "a + b"
  $ xsv lua map c "col.a + col['b']"
  $ xsv lua map c "col[1] + col[2]"

  There is some magic in the previous example as 'a' and 'b' are passed in
  as strings (not numbers), but Lua still manages to add them up.
  A more explicit way of doing it, is by using tonumber
  $ xsv lua map c "tonumber(a) + tonumber(b)"

  Add running total column for Amount
  $ xsv lua map Total -x "tot = (tot or 0) + Amount; return tot"

  Add running total column for Amount when previous balance was 900
  $ xsv lua map Total -x "tot = (tot or 900) + Amount; return tot"

  Convert Amount to always-positive AbsAmount and Type (debit/credit) columns
  $ xsv lua map Type -x \
        "if tonumber(Amount) < 0 then return 'debit' else return 'credit' end" | \
    xsv lua map AbsAmount "math.abs(tonumber(Amount))"

  Typing long scripts at command line gets tiresome rather quickly,
  so -f should be used for non-trivial scripts to read them from a file
  $ xsv lua map Type -x -f debitcredit.lua

Usage:
    xsv lua map [options] -n <script> [<input>]
    xsv lua map [options] <new-column> <script> [<input>]
    xsv lua filter [options] <script> [<input>]
    xsv lua map --help
    xsv lua filter --help
    xsv lua --help

lua options:
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
        if !args.cmd_filter {
            let new_column = args
                .arg_new_column
                .as_ref()
                .ok_or("Specify new column name")?;
            headers.push_field(new_column);
        }

        wtr.write_record(&headers)?;
    }

    let mut lua = Lua::new();
    lua.openlibs();
    lua.execute("col = {}")?;

    let lua_script = if args.flag_script_file {
        let mut file = File::open(&args.arg_script)?;

        let mut script_text = String::new();
        file.read_to_string(&mut script_text)?;
        script_text
    } else {
        args.arg_script
    };

    let mut lua_program = if args.flag_exec {
        String::new()
    } else {
        String::from("return ")
    };

    lua_program.push_str(&lua_script);

    let mut record = csv::StringRecord::new();

    while rdr.read_record(&mut record)? {
        // Updating col
        {
            let mut col: LuaTable<_> = lua.get("col").unwrap();

            for (i, v) in record.iter().enumerate() {
                // TODO: drop this `as`
                col.set((i as u16) + 1, v);
            }
            if !rconfig.no_headers {
                for (h, v) in headers.iter().zip(record.iter()) {
                    col.set(h, v);
                }
            }
        }

        // Updating global
        if !args.flag_no_globals {
            let mut globals = lua.globals_table();

            if !rconfig.no_headers {
                for (h, v) in headers.iter().zip(record.iter()) {
                    globals.set(h, v);
                }
            }
        }

        let computed_value: AnyLuaValue = lua.execute(&lua_program)?;

        if args.cmd_map {
            match computed_value {
                AnyLuaValue::LuaString(string) => {
                    record.push_field(&string);
                }
                AnyLuaValue::LuaNumber(number) => {
                    record.push_field(&number.to_string());
                }
                AnyLuaValue::LuaBoolean(boolean) => {
                    record.push_field(if boolean { "true" } else { "false" });
                }
                AnyLuaValue::LuaNil => {
                    record.push_field("");
                }
                _ => {
                    return fail!("Unexpected value type returned by provided Lua expression.");
                }
            }

            wtr.write_record(&record)?;
        } else if args.cmd_filter {
            let must_keep_line = match computed_value {
                AnyLuaValue::LuaString(string) => !string.is_empty(),
                AnyLuaValue::LuaNumber(_) => true,
                AnyLuaValue::LuaBoolean(boolean) => boolean,
                AnyLuaValue::LuaNil => false,
                _ => true,
            };

            if must_keep_line {
                wtr.write_record(&record)?;
            }
        }
    }

    Ok(wtr.flush()?)
}
