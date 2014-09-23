use std::fmt;
use std::from_str::FromStr;
use std::io;
use std::iter;

use serialize::{Decodable, Decoder};

use csv;
use docopt;

pub enum CliError {
    ErrFlag(docopt::Error),
    ErrOther(String),
    ErrBrokenPipe,
}

impl CliError {
    pub fn from_str<T: ToString>(v: T) -> CliError {
        ErrOther(v.to_string())
    }
    pub fn from_flags(v: docopt::Error) -> CliError {
        ErrFlag(v)
    }
}

#[deriving(Clone, Show)]
pub struct Delimiter(u8);

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

/// InputReader corresponds to a single source of input of CSV data. It
/// abstracts over whether the data is coming from a file or stdin.
pub struct InputReader {
    rdr: InputType,
    name: String, // <stdin> or file path given
}

enum InputType {
    InputStdin(io::BufferedReader<io::stdio::StdReader>),
    InputFile(io::File),
}

impl InputReader {
    pub fn new(fpath: Option<&Path>) -> io::IoResult<InputReader> {
        Ok(match fpath {
            None => {
                InputReader {
                    rdr: InputStdin(io::stdin()),
                    name: "<stdin>".to_string(),
                }
            }
            Some(p) => {
                InputReader {
                    rdr: InputFile(try!(io::File::open(p))),
                    name: p.display().to_string(),
                }
            }
        })
    }

    pub fn file_ref<'a>(&'a mut self) -> Result<&'a mut io::File, String> {
        match self.rdr {
            InputStdin(_) =>
                Err("Cannot get file ref from stdin reader.".to_string()),
            InputFile(ref mut f) => Ok(f),
        }
    }

    pub fn is_seekable(&self) -> bool {
        match self.rdr {
            InputFile(_) => true,
            _ => false,
        }
    }

    pub fn is_stdin(&self) -> bool {
        match self.rdr {
            InputStdin(_) => true,
            _ => false,
        }
    }
}

impl fmt::Show for InputReader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Reader for InputReader {
    fn read(&mut self, buf: &mut [u8]) -> io::IoResult<uint> {
        match self.rdr {
            InputStdin(ref mut rdr) => rdr.read(buf),
            InputFile(ref mut rdr) => rdr.read(buf),
        }
    }
}

impl<E, D: Decoder<E>> Decodable<D, E> for InputReader {
    fn decode(d: &mut D) -> Result<InputReader, E> {
        decode_file_arg(d, "<stdin>", InputReader::new)
    }
}

/// OutputWriter corresponds to a single destination of CSV data. It
/// abstracts over whether the data is going to a file or stdout.
pub struct OutputWriter {
    wtr: Box<Writer+'static>,
    name: String, // <stdout> or file path given
}

impl OutputWriter {
    pub fn new(fpath: Option<&Path>) -> io::IoResult<OutputWriter> {
        Ok(match fpath {
            None => OutputWriter::from_writer("<stdout>", io::stdout()),
            Some(p) => {
                let f = try!(io::File::create(p));
                OutputWriter::from_writer(p.display().to_string(), f)
            }
        })
    }

    fn from_writer<S: StrAllocating, W: Writer+'static>
                  (name: S, wtr: W) -> OutputWriter {
        OutputWriter {
            wtr: box wtr as Box<Writer+'static>,
            name: name.into_string(),
        }
    }
}

impl fmt::Show for OutputWriter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl Writer for OutputWriter {
    fn write(&mut self, buf: &[u8]) -> io::IoResult<()> {
        self.wtr.write(buf)
    }
}

impl<E, D: Decoder<E>> Decodable<D, E> for OutputWriter {
    fn decode(d: &mut D) -> Result<OutputWriter, E> {
        decode_file_arg(d, "<stdout>", OutputWriter::new)
    }
}

fn decode_file_arg<T, E, D: Decoder<E>>
                  (d: &mut D, stdname: &str,
                   mk: |Option<&Path>| -> io::IoResult<T>) -> Result<T, E> {
    let s = try!(d.read_str());
    let p =
        if s.len() == 0 || s.as_slice() == "-" {
            None
        } else {
            Some(Path::new(s))
        };
    mk(p.as_ref()).map_err(|e| {
        let p = match p {
            None => stdname.to_string(),
            Some(ref p) => p.display().to_string(),
        };
        let msg = format!("Error opening {}: {}", p, e.to_string());
        d.error(msg.as_slice())
    })
}

pub struct SelectColumns(Vec<Selector>);

enum Selector {
    SelStart,
    SelEnd,
    SelIndex(uint),
    SelName(String),
    // invariant: selectors MUST NOT be ranges
    SelRange(Box<Selector>, Box<Selector>),
}

// This parser is super basic at the moment. Field names cannot contain [-,].
impl SelectColumns {
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

impl Selector {
    fn is_range(&self) -> bool {
        match self {
            &SelRange(_, _) => true,
            _ => false,
        }
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

impl <E, D: Decoder<E>> Decodable<D, E> for SelectColumns {
    fn decode(d: &mut D) -> Result<SelectColumns, E> {
        SelectColumns::parse(try!(d.read_str()).as_slice())
        .map_err(|e| d.error(e.as_slice()))
    }
}

#[deriving(Show)]
pub struct Selection(Vec<uint>);

impl Selection {
    pub fn new<R: Reader>(rdr: &mut csv::Reader<R>, scols: &SelectColumns,
                          no_headers: bool)
                         -> Result<Selection, String> {
        let headers = try!(rdr.byte_headers().map_err(|e| e.to_string()));
        let mut map = vec!();
        for sel in scols.selectors().iter() {
            let idxs = Selection::indices(sel, headers.as_slice(), no_headers);
            map.push_all_move(try!(idxs));
        }
        Ok(Selection(map))
    }

    pub fn select<'a>(&self, row: &'a [csv::ByteString])
                     -> Vec<&'a csv::ByteString> {
        if self.as_slice().is_empty() {
            return row.iter().collect();
        }
        let mut new = Vec::with_capacity(self.as_slice().len());
        for &idx in self.as_slice().iter() {
            new.push(&row[idx]);
        }
        new
    }

    fn as_slice<'a>(&'a self) -> &'a [uint] {
        let &Selection(ref inds) = self;
        inds.as_slice()
    }

    fn indices(sel: &Selector, headers: &[csv::ByteString], no_headers: bool)
              -> Result<Vec<uint>, String> {
        match sel {
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
                if no_headers {
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
                let is1 = try!(Selection::indices(sel1, headers, no_headers));
                let is2 = try!(Selection::indices(sel2, headers, no_headers));
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
