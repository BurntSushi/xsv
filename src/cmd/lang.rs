#[cfg(feature = "lang")] use {
    csv,
    lingua::{Language, LanguageDetectorBuilder},
    config::{Delimiter, Config},
    select::SelectColumns,
    util
};

use CliResult;

#[cfg(feature = "lang")]
static USAGE: &'static str = r#"
Add a column with the language detected in a given CSV column

Usage:
    xsv lang [options] <column> [<input>]
    xsv lang --help

lang options:
    -c, --new-column <name>  Name of the column to create.
                             Will default to "lang".

Common options:
    -h, --help               Display this message
    -o, --output <file>      Write output to <file> instead of stdout.
    -n, --no-headers         When set, the first row will not be interpreted
                             as headers.
    -d, --delimiter <arg>    The field delimiter for reading CSV data.
                             Must be a single character. (default: ,)
"#;

#[cfg(feature = "lang")]
#[derive(Deserialize)]
struct Args {
    arg_column: SelectColumns,
    arg_input: Option<String>,
    flag_new_column: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

#[cfg(not(feature = "lang"))]
pub fn run(_argv: &[&str]) -> CliResult<()> {
    Ok(println!("This version of XSV was not compiled with the \"lang\" feature."))
}

#[cfg(feature = "lang")]
pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_column);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let mut headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
    let column_index = *sel.iter().next().unwrap();

    if !rconfig.no_headers {
        headers.push_field(args.flag_new_column.map_or("lang".to_string(), |name| name).as_bytes());
        wtr.write_byte_record(&headers)?;
    }

    let mut record = csv::StringRecord::new();

    let detector = LanguageDetectorBuilder::from_all_spoken_languages().build();

    while rdr.read_record(&mut record)? {
        let cell = record[column_index].to_owned();

        let mut language = String::new();

        let detected_language = detector.detect_language_of(&cell);

        if detected_language != None {
            language = Language::to_string(&detected_language.unwrap());
        }

        record.push_field(&language);
        wtr.write_record(&record)?;
    }

    Ok(wtr.flush()?)
}
