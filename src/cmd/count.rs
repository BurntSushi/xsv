use std::io::{self, Write};

use crate::{
    app::{self, App, Args},
    config,
};

const ABOUT: &'static str = "\
Counts the number of records in CSV data.
";

pub fn define() -> App {
    let mut cmd = app::leaf("count")
        .about("Count the number of records in CSV data.")
        .before_help(ABOUT);
    cmd = config::Input::define(cmd, "A CSV file.", "file");
    cmd = config::CsvRead::define(cmd);
    cmd
}

pub fn run(args: &Args) -> anyhow::Result<()> {
    let inp = config::Input::get_required(args, "file");
    let csv_config = config::CsvRead::get(args)?;

    let count = match csv_config.indexed_csv_reader(&inp)? {
        Some(idx) => idx.count(),
        None => {
            let mut rdr = csv_config.csv_reader(&inp)?;
            let mut count = 0u64;
            let mut record = csv::ByteRecord::new();
            while rdr.read_byte_record(&mut record)? {
                count += 1;
            }
            count
        }
    };
    writeln!(io::stdout(), "{}", count)?;
    Ok(())
}
