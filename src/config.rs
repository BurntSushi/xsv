#[allow(deprecated, unused_imports)]
use std::ascii::AsciiExt;
use std::borrow::ToOwned;
use std::env;
use std::fs;
use std::io::{self, Read};
use std::ops::Deref;
use std::path::PathBuf;

use csv;
use index::Indexed;
use serde::de::{Deserializer, Deserialize, Error};

use CliResult;
use select::{SelectColumns, Selection};
use util;


#[derive(Clone, Copy, Debug)]
pub struct Delimiter(pub u8);

/// Delimiter represents values that can be passed from the command line that
/// can be used as a field delimiter in CSV data.
///
/// Its purpose is to ensure that the Unicode character given decodes to a
/// valid ASCII character as required by the CSV parser.
impl Delimiter {
    pub fn as_byte(self) -> u8 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Delimiter {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Delimiter, D::Error> {
        let c = String::deserialize(d)?;
        match &*c {
            r"\t" => Ok(Delimiter(b'\t')),
            s => {
                if s.len() != 1 {
                    let msg = format!("Could not convert '{}' to a single \
                                       ASCII character.", s);
                    return Err(D::Error::custom(msg));
                }
                let c = s.chars().next().unwrap();
                if c.is_ascii() {
                    Ok(Delimiter(c as u8))
                } else {
                    let msg = format!("Could not convert '{}' \
                                       to ASCII delimiter.", c);
                    Err(D::Error::custom(msg))
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Config {
    path: Option<PathBuf>, // None implies <stdin>
    idx_path: Option<PathBuf>,
    select_columns: Option<SelectColumns>,
    delimiter: u8,
    pub no_headers: bool,
    flexible: bool,
    terminator: csv::Terminator,
    quote: u8,
    quote_style: csv::QuoteStyle,
    double_quote: bool,
    escape: Option<u8>,
    quoting: bool,
}

impl Config {
    pub fn new(path: &Option<String>) -> Config {
        let (path, delim) = match *path {
            None => (None, b','),
            Some(ref s) if s.deref() == "-" => (None, b','),
            Some(ref s) => {
                let path = PathBuf::from(s);
                let delim =
                    if path.extension().map_or(false, |v| v == "tsv" || v == "tab") {
                        b'\t'
                    } else {
                        b','
                    };
                (Some(path), delim)
            }
        };
        Config {
            path: path,
            idx_path: None,
            select_columns: None,
            delimiter: delim,
            no_headers: false,
            flexible: false,
            terminator: csv::Terminator::Any(b'\n'),
            quote: b'"',
            quote_style: csv::QuoteStyle::Necessary,
            double_quote: true,
            escape: None,
            quoting: true,
        }
    }

    pub fn delimiter(mut self, d: Option<Delimiter>) -> Config {
        if let Some(d) = d {
            self.delimiter = d.as_byte();
        }
        self
    }

    pub fn no_headers(mut self, mut yes: bool) -> Config {
        if env::var("XSV_TOGGLE_HEADERS").unwrap_or("0".to_owned()) == "1" {
            yes = !yes;
        }
        self.no_headers = yes;
        self
    }

    pub fn flexible(mut self, yes: bool) -> Config {
        self.flexible = yes;
        self
    }

    pub fn crlf(mut self, yes: bool) -> Config {
        if yes {
            self.terminator = csv::Terminator::CRLF;
        } else {
            self.terminator = csv::Terminator::Any(b'\n');
        }
        self
    }

    pub fn terminator(mut self, term: csv::Terminator) -> Config {
        self.terminator = term;
        self
    }

    pub fn quote(mut self, quote: u8) -> Config {
        self.quote = quote;
        self
    }

    pub fn quote_style(mut self, style: csv::QuoteStyle) -> Config {
        self.quote_style = style;
        self
    }

    pub fn double_quote(mut self, yes: bool) -> Config {
        self.double_quote = yes;
        self
    }

    pub fn escape(mut self, escape: Option<u8>) -> Config {
        self.escape = escape;
        self
    }

    pub fn quoting(mut self, yes: bool) -> Config {
        self.quoting = yes;
        self
    }

    pub fn select(mut self, sel_cols: SelectColumns) -> Config {
        self.select_columns = Some(sel_cols);
        self
    }

    pub fn is_std(&self) -> bool {
        self.path.is_none()
    }

    pub fn selection(
        &self,
        first_record: &csv::ByteRecord,
    ) -> Result<Selection, String> {
        match self.select_columns {
            None => Err("Config has no 'SelectColums'. Did you call \
                         Config::select?".to_owned()),
            Some(ref sel) => sel.selection(first_record, !self.no_headers),
        }
    }

    pub fn write_headers<R: io::Read, W: io::Write>
                        (&self, r: &mut csv::Reader<R>, w: &mut csv::Writer<W>)
                        -> csv::Result<()> {
        if !self.no_headers {
            let r = r.byte_headers()?;
            if !r.is_empty() {
                w.write_record(r)?;
            }
        }
        Ok(())
    }

    pub fn writer(&self)
                 -> io::Result<csv::Writer<Box<dyn io::Write+'static>>> {
        Ok(self.from_writer(self.io_writer()?))
    }

    pub fn reader(&self)
                 -> io::Result<csv::Reader<Box<dyn io::Read+'static>>> {
        Ok(self.from_reader(self.io_reader()?))
    }

    pub fn reader_file(&self) -> io::Result<csv::Reader<fs::File>> {
        match self.path {
            None => Err(io::Error::new(
                io::ErrorKind::Other, "Cannot use <stdin> here",
            )),
            Some(ref p) => fs::File::open(p).map(|f| self.from_reader(f)),
        }
    }

    pub fn index_files(&self)
           -> io::Result<Option<(csv::Reader<fs::File>, fs::File)>> {
        let (csv_file, idx_file) = match (&self.path, &self.idx_path) {
            (&None, &None) => return Ok(None),
            (&None, &Some(_)) => return Err(io::Error::new(
                io::ErrorKind::Other,
                "Cannot use <stdin> with indexes",
                // Some(format!("index file: {}", p.display()))
            )),
            (&Some(ref p), &None) => {
                // We generally don't want to report an error here, since we're
                // passively trying to find an index.
                let idx_file = match fs::File::open(&util::idx_path(p)) {
                    // TODO: Maybe we should report an error if the file exists
                    // but is not readable.
                    Err(_) => return Ok(None),
                    Ok(f) => f,
                };
                (fs::File::open(p)?, idx_file)
            }
            (&Some(ref p), &Some(ref ip)) => {
                (fs::File::open(p)?, fs::File::open(ip)?)
            }
        };
        // If the CSV data was last modified after the index file was last
        // modified, then return an error and demand the user regenerate the
        // index.
        let data_modified = util::last_modified(&csv_file.metadata()?);
        let idx_modified = util::last_modified(&idx_file.metadata()?);
        if data_modified > idx_modified {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "The CSV file was modified after the index file. \
                 Please re-create the index.",
            ));
        }
        let csv_rdr = self.from_reader(csv_file);
        Ok(Some((csv_rdr, idx_file)))
    }

    pub fn indexed(&self)
                  -> CliResult<Option<Indexed<fs::File, fs::File>>> {
        match self.index_files()? {
            None => Ok(None),
            Some((r, i)) => Ok(Some(Indexed::open(r, i)?)),
        }
    }

    pub fn io_reader(&self) -> io::Result<Box<dyn io::Read+'static>> {
        Ok(match self.path {
                None => Box::new(io::stdin()),
                Some(ref p) => {
                    match fs::File::open(p){
                        Ok(x) => Box::new(x),
                        Err(err) => {
                            let msg = format!(
                                "failed to open {}: {}", p.display(), err);
                            return Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                msg,
                            ));
                        }
                    }
                },
            })
    }

    pub fn from_reader<R: Read>(&self, rdr: R) -> csv::Reader<R> {
        csv::ReaderBuilder::new()
            .flexible(self.flexible)
            .delimiter(self.delimiter)
            .has_headers(!self.no_headers)
            .quote(self.quote)
            .quoting(self.quoting)
            .escape(self.escape)
            .from_reader(rdr)
    }

    pub fn io_writer(&self) -> io::Result<Box<dyn io::Write+'static>> {
        Ok(match self.path {
            None => Box::new(io::stdout()),
            Some(ref p) => Box::new(fs::File::create(p)?),
        })
    }

    pub fn from_writer<W: io::Write>(&self, wtr: W) -> csv::Writer<W> {
        csv::WriterBuilder::new()
            .flexible(self.flexible)
            .delimiter(self.delimiter)
            .terminator(self.terminator)
            .quote(self.quote)
            .quote_style(self.quote_style)
            .double_quote(self.double_quote)
            .escape(self.escape.unwrap_or(b'\\'))
            .buffer_capacity(32 * (1<<10))
            .from_writer(wtr)
    }
}
