use std::ascii::AsciiExt;
use std::borrow::ToOwned;
use std::env;
use std::old_io as io;

use csv;
use csv::index::Indexed;
use rustc_serialize::{Decodable, Decoder};

use CliResult;
use select::{SelectColumns, Selection, NormalSelection};
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
        let Delimiter(b) = self;
        b
    }
}

impl Decodable for Delimiter {
    fn decode<D: Decoder>(d: &mut D) -> Result<Delimiter, D::Error> {
        let c = try!(d.read_str());
        match &*c {
            r"\t" => Ok(Delimiter(b'\t')),
            s => {
                if s.len() != 1 {
                    let msg = format!("Could not convert '{}' to a single \
                                       ASCII character.", s);
                    return Err(d.error(&*msg));
                }
                let c = s.char_at(0);
                if c.is_ascii() {
                    Ok(Delimiter(c as u8))
                } else {
                    let msg = format!("Could not convert '{}' \
                                       to ASCII delimiter.", c);
                    Err(d.error(&*msg))
                }
            }
        }
    }
}

pub struct Config {
    path: Option<Path>, // None implies <stdin>
    idx_path: Option<Path>,
    select_columns: Option<SelectColumns>,
    delimiter: u8,
    pub no_headers: bool,
    flexible: bool,
    crlf: bool,
}

impl Config {
    pub fn new(path: &Option<String>) -> Config {
        let path =
            path.clone()
                .map(|p| Path::new(p))
                .and_then(|p| if p.as_vec() == b"-" { None } else { Some(p) });
        let ext = path.as_ref()
                      .and_then(|p| p.extension())
                      .unwrap_or(b"")
                      .to_vec();
        Config {
            path: path,
            idx_path: None,
            select_columns: None,
            delimiter: if ext == b"tsv" { b'\t' } else { b',' },
            no_headers: false,
            flexible: false,
            crlf: false,
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
        self.crlf = yes;
        self
    }

    pub fn select(mut self, sel_cols: SelectColumns) -> Config {
        self.select_columns = Some(sel_cols);
        self
    }

    pub fn is_std(&self) -> bool {
        self.path.is_none()
    }

    pub fn selection(&self, first_record: &[csv::ByteString])
                    -> Result<Selection, String> {
        match self.select_columns {
            None => Err("Config has no 'SelectColums'. Did you call \
                         Config::select?".to_string()),
            Some(ref sel) => sel.selection(first_record, !self.no_headers),
        }
    }

    pub fn normal_selection(&self, first_record: &[csv::ByteString])
                    -> Result<NormalSelection, String> {
        self.selection(first_record).map(|sel| sel.normal())
    }

    pub fn write_headers<R: io::Reader, W: io::Writer>
                        (&self, r: &mut csv::Reader<R>, w: &mut csv::Writer<W>)
                        -> csv::CsvResult<()> {
        if !self.no_headers {
            let r = try!(r.byte_headers());
            if !r.is_empty() {
                try!(w.write(r.into_iter()));
            }
        }
        Ok(())
    }

    pub fn writer(&self)
                 -> io::IoResult<csv::Writer<Box<io::Writer+'static>>> {
        Ok(self.from_writer(try!(self.io_writer())))
    }

    pub fn reader(&self)
                 -> io::IoResult<csv::Reader<Box<io::Reader+'static>>> {
        Ok(self.from_reader(try!(self.io_reader())))
    }

    pub fn reader_file(&self) -> io::IoResult<csv::Reader<io::File>> {
        match self.path {
            None => Err(io::IoError {
                kind: io::OtherIoError,
                desc: "Cannot use <stdin> here",
                detail: None,
            }),
            Some(ref p) => io::File::open(p).map(|f| self.from_reader(f)),
        }
    }

    pub fn index_files(&self)
           -> io::IoResult<Option<(csv::Reader<io::File>, io::File)>> {
        let (csv_file, idx_file) = match (&self.path, &self.idx_path) {
            (&None, &None) => return Ok(None),
            (&None, &Some(ref p)) => return Err(io::IoError {
                kind: io::OtherIoError,
                desc: "Cannot use <stdin> with indexes",
                detail: Some(format!("index file: {}", p.display())),
            }),
            (&Some(ref p), &None) => {
                // We generally don't want to report an error here, since we're
                // passively trying to find an index.
                let idx_file = match io::File::open(&util::idx_path(p)) {
                    // TODO: Maybe we should report an error if the file exists
                    // but is not readable.
                    Err(_) => return Ok(None),
                    Ok(f) => f,
                };
                (try!(io::File::open(p)), idx_file)
            }
            (&Some(ref p), &Some(ref ip)) => {
                (try!(io::File::open(p)), try!(io::File::open(ip)))
            }
        };
        // If the CSV data was last modified after the index file was last
        // modified, then return an error and demand the user regenerate the
        // index.
        let data_modified = try!(csv_file.stat()).modified;
        let idx_modified = try!(idx_file.stat()).modified;
        if data_modified > idx_modified {
            return Err(io::IoError {
                kind: io::OtherIoError,
                desc: "The CSV file was modified after the index file. \
                       Please re-create the index.",
                detail: Some(format!("CSV file: {}, index file: {}",
                                     csv_file.path().display(),
                                     idx_file.path().display())),
            });
        }
        let csv_rdr = self.from_reader(csv_file);
        Ok(Some((csv_rdr, idx_file)))
    }

    pub fn indexed(&self)
                  -> CliResult<Option<Indexed<io::File, io::File>>> {
        match try!(self.index_files()) {
            None => Ok(None),
            Some((r, i)) => Ok(Some(try!(Indexed::new(r, i)))),
        }
    }

    pub fn io_reader(&self) -> io::IoResult<Box<io::Reader+'static>> {
        Ok(match self.path {
            None => Box::new(io::stdin()) as Box<io::Reader>,
            Some(ref p) => {
                let f = try!(io::File::open(p));
                Box::new(f) as Box<io::Reader>
            }
        })
    }

    pub fn from_reader<R: Reader>(&self, rdr: R) -> csv::Reader<R> {
        csv::Reader::from_reader(rdr)
                    .flexible(self.flexible)
                    .delimiter(self.delimiter)
                    .has_headers(!self.no_headers)
    }

    pub fn io_writer(&self) -> io::IoResult<Box<io::Writer+'static>> {
        Ok(match self.path {
            None => Box::new(io::stdout()) as Box<io::Writer>,
            Some(ref p) => {
                let f = try!(io::File::create(p));
                Box::new(f) as Box<io::Writer>
            }
        })
    }

    pub fn from_writer<W: Writer>(&self, wtr: W) -> csv::Writer<W> {
        let term = if self.crlf { csv::RecordTerminator::CRLF }
                   else { csv::RecordTerminator::Any(b'\n') };
        csv::Writer::from_writer(wtr)
                    .flexible(self.flexible)
                    .delimiter(self.delimiter)
                    .record_terminator(term)
    }
}
