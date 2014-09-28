use std::fmt;
use std::from_str::FromStr;
use std::io;
use std::iter;
use std::slice;

use serialize::{Decodable, Decoder};

use csv;
use csv::index::Indexed;
use docopt;

use util;

pub enum CliError {
    ErrFlag(docopt::Error),
    ErrCsv(csv::Error),
    ErrIo(io::IoError),
    ErrOther(String),
}

impl CliError {
    pub fn from_flags(v: docopt::Error) -> CliError {
        ErrFlag(v)
    }
    pub fn from_csv(v: csv::Error) -> CliError {
        match v {
            csv::ErrIo(v) => CliError::from_io(v),
            v => ErrCsv(v),
        }
    }
    pub fn from_io(v: io::IoError) -> CliError {
        ErrIo(v)
    }
    pub fn from_str<T: ToString>(v: T) -> CliError {
        ErrOther(v.to_string())
    }
}

#[deriving(Clone, Show)]
pub struct Delimiter(pub u8);

/// Delimiter represents values that can be passed from the command line that
/// can be used as a field delimiter in CSV data.
///
/// Its purpose is to ensure that the Unicode character given decodes to a
/// valid ASCII character as required by the CSV parser.
impl Delimiter {
    pub fn to_byte(self) -> u8 {
        let Delimiter(b) = self;
        b
    }

    pub fn as_byte(self) -> u8 {
        let Delimiter(b) = self;
        b
    }
}

impl<E, D: Decoder<E>> Decodable<D, E> for Delimiter {
    fn decode(d: &mut D) -> Result<Delimiter, E> {
        let c = try!(d.read_char());
        match c.to_ascii_opt() {
            Some(ascii) => Ok(Delimiter(ascii.to_byte())),
            None => {
                let msg = format!("Could not convert '{}' \
                                   to ASCII delimiter.", c);
                Err(d.error(msg.as_slice()))
            }
        }
    }
}

pub struct CsvConfig {
    path: Option<Path>, // None implies <stdin>
    idx_path: Option<Path>,
    delimiter: u8,
    no_headers: bool,
    flexible: bool,
    crlf: bool,
}

impl CsvConfig {
    pub fn new(mut path: Option<String>) -> CsvConfig {
        if path.as_ref().map(|p| p.equiv(&"-")) == Some(true) {
            // If the path explicitly wants stdin/stdout, then give it to them.
            path = None;
        }
        CsvConfig {
            path: path.map(|p| Path::new(p)),
            idx_path: None,
            delimiter: b',',
            no_headers: false,
            flexible: false,
            crlf: false,
        }
    }

    pub fn delimiter(mut self, d: Delimiter) -> CsvConfig {
        self.delimiter = d.as_byte();
        self
    }

    pub fn no_headers(mut self, yes: bool) -> CsvConfig {
        self.no_headers = yes;
        self
    }

    pub fn flexible(mut self, yes: bool) -> CsvConfig {
        self.flexible = yes;
        self
    }

    pub fn crlf(mut self, yes: bool) -> CsvConfig {
        self.crlf = yes;
        self
    }

    pub fn is_std(&self) -> bool {
        self.path.is_none()
    }

    pub fn idx_path(mut self, idx_path: Option<String>) -> CsvConfig {
        self.idx_path = idx_path.map(|p| Path::new(p));
        self
    }

    pub fn write_headers<R: io::Reader, W: io::Writer>
                        (&self, r: &mut csv::Reader<R>, w: &mut csv::Writer<W>)
                        -> csv::CsvResult<()> {
        if !self.no_headers {
            try!(w.write_bytes(try!(r.byte_headers()).into_iter()));
        }
        Ok(())
    }

    pub fn writer(&self) -> io::IoResult<csv::Writer<Box<io::Writer+'static>>> {
        Ok(self.from_writer(try!(self.io_writer())))
    }

    pub fn reader(&self) -> io::IoResult<csv::Reader<Box<io::Reader+'static>>> {
        Ok(self.from_reader(try!(self.io_reader())))
    }

    pub fn index_files(&self)
           -> io::IoResult<Option<(csv::Reader<io::File>, io::File)>> {
        let (mut csv_file, mut idx_file) = match (&self.path, &self.idx_path) {
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

    pub fn indexed(&self) -> io::IoResult<Option<Indexed<io::File, io::File>>> {
        Ok({ try!(self.index_files()) }.map(|(r, i)| Indexed::new(r, i)))
    }

    pub fn io_reader(&self) -> io::IoResult<Box<io::Reader+'static>> {
        Ok(match self.path {
            None => box io::stdin() as Box<io::Reader+'static>,
            Some(ref p) =>
                box try!(io::File::open(p)) as Box<io::Reader+'static>,
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
            None => box io::stdout() as Box<io::Writer+'static>,
            Some(ref p) =>
                box try!(io::File::create(p)) as Box<io::Writer+'static>,
        })
    }

    pub fn from_writer<W: Writer>(&self, wtr: W) -> csv::Writer<W> {
        csv::Writer::from_writer(wtr)
                    .flexible(self.flexible)
                    .delimiter(self.delimiter)
                    .crlf(self.crlf)
    }
}

pub struct SelectColumns(Vec<Selector>);

// This parser is super basic at the moment. Field names cannot contain [-,].
impl SelectColumns {
    pub fn selection(&self, conf: &CsvConfig, headers: &[csv::ByteString])
                    -> Result<Selection, String> {
        let mut map = vec![];
        for sel in self.selectors().iter() {
            let idxs = sel.indices(conf, headers);
            map.extend(try!(idxs).into_iter());
        }
        Ok(Selection(map))
    }

    fn selectors<'a>(&'a self) -> &'a [Selector] {
        let &SelectColumns(ref sels) = self;
        sels.as_slice()
    }

    fn parse(s: &str) -> Result<SelectColumns, String> {
        let mut sels = vec!();
        if s.is_empty() {
            return Ok(SelectColumns(sels));
        }
        for sel in s.split(',') {
            sels.push(try!(SelectColumns::parse_selector(sel)));
        }
        Ok(SelectColumns(sels))
    }

    fn parse_selector(sel: &str) -> Result<Selector, String> {
        if sel.contains_char('-') {
            let pieces: Vec<&str> = sel.splitn(1, '-').collect();
            let start = 
                if pieces[0].is_empty() {
                    SelStart
                } else {
                    try!(SelectColumns::parse_one_selector(pieces[0]))
                };
            let end =
                if pieces[1].is_empty() {
                    SelEnd
                } else {
                    try!(SelectColumns::parse_one_selector(pieces[1]))
                };
            Ok(SelRange(box start, box end))
        } else {
            SelectColumns::parse_one_selector(sel)
        }
    }

    fn parse_one_selector(sel: &str) -> Result<Selector, String> {
        if sel.contains_char('-') {
            return Err(format!("Illegal '-' in selector '{}'.", sel))
        }
        let idx: Option<uint> = FromStr::from_str(sel);
        Ok(match idx {
            None => SelName(sel.to_string()),
            Some(idx) => SelIndex(idx),
        })
    }
}

impl fmt::Show for SelectColumns {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.selectors().is_empty() {
            return write!(f, "<All>");
        }
        let strs: Vec<String> = self.selectors().iter()
                                .map(|sel| sel.to_string())
                                .collect();
        write!(f, "{}", strs.connect(", "))
    }
}

impl <E, D: Decoder<E>> Decodable<D, E> for SelectColumns {
    fn decode(d: &mut D) -> Result<SelectColumns, E> {
        SelectColumns::parse(try!(d.read_str()).as_slice())
                      .map_err(|e| d.error(e.as_slice()))
    }
}

enum Selector {
    SelStart,
    SelEnd,
    SelIndex(uint),
    SelName(String),
    // invariant: selectors MUST NOT be ranges
    SelRange(Box<Selector>, Box<Selector>),
}

impl Selector {
    fn is_range(&self) -> bool {
        match self {
            &SelRange(_, _) => true,
            _ => false,
        }
    }

    fn indices(&self, conf: &CsvConfig, headers: &[csv::ByteString])
              -> Result<Vec<uint>, String> {
        match self {
            &SelStart => Ok(vec!(0)),
            &SelEnd => Ok(vec!(headers.len())),
            &SelIndex(i) => {
                if i < 1 || i > headers.len() {
                    Err(format!("Selector index {} is out of \
                                 bounds. Index must be >= 1 \
                                 and <= {}.", i, headers.len()))
                } else {
                    // Indices given by user are 1-offset. Convert them here!
                    Ok(vec!(i-1))
                }
            }
            &SelName(ref s) => {
                if conf.no_headers {
                    return Err(format!("Cannot use names ('{}') in selection \
                                        with --no-headers set.", s));
                }
                match headers.iter().position(|h| h.equiv(s)) {
                    None => Err(format!("Selector name '{}' does not exist \
                                         as a named header in the given CSV \
                                         data.", s)),
                    Some(i) => Ok(vec!(i)),
                }
            }
            &SelRange(box ref sel1, box ref sel2) => {
                assert!(!sel1.is_range());
                assert!(!sel2.is_range());
                let is1 = try!(sel1.indices(conf, headers));
                let is2 = try!(sel2.indices(conf, headers));
                let i1 = { assert!(is1.len() == 1); is1[0] };
                let i2 = { assert!(is2.len() == 1); is2[0] };
                Ok(match i1.cmp(&i2) {
                    Equal => vec!(i1),
                    Less => iter::range_inclusive(i1, i2).collect(),
                    Greater =>
                        iter::range_step_inclusive(i1 as int, i2 as int, -1)
                             .map(|i| i as uint).collect(),
                })
            }
        }
    }
}

impl fmt::Show for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SelStart => write!(f, "Start"),
            &SelEnd => write!(f, "End"),
            &SelName(ref s) => write!(f, "Name({})", s),
            &SelIndex(idx) => write!(f, "Index({:u})", idx),
            &SelRange(ref s, ref e) => write!(f, "Range({}, {})", s, e),
        }
    }
}

#[deriving(Show)]
pub struct Selection(Vec<uint>);

impl Selection {
    pub fn select<'a, 'b>(&'a self, row: &'b [csv::ByteString])
                 -> iter::Scan<&'a uint,
                               &'b [u8],
                               slice::Items<'a, uint>,
                               &'b [csv::ByteString]> {
        // This is horrifying.
        // Help me closure reform, you're my only hope.
        self.as_slice().iter().scan(row, |row, &idx| Some(row[idx].as_slice()))
    }

    pub fn as_slice<'a>(&'a self) -> &'a [uint] {
        let &Selection(ref inds) = self;
        inds.as_slice()
    }
}
