use std::fmt;
use std::io;

use serialize::{Decodable, Decoder};

#[deriving(Show)]
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
    rdr: Box<Reader+'static>,
    name: String, // <stdin> or file path given
}

impl InputReader {
    pub fn new(fpath: Option<&Path>) -> io::IoResult<InputReader> {
        Ok(match fpath {
            None => InputReader::from_reader("<stdin>", io::stdin()),
            Some(p) => {
                let f = try!(io::File::open(p));
                InputReader::from_reader(p.display().to_string(), f)
            }
        })
    }

    fn from_reader<S: StrAllocating, R: Reader+'static>
                  (name: S, rdr: R) -> InputReader {
        InputReader {
            rdr: box rdr as Box<Reader+'static>,
            name: name.into_string(),
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
        self.rdr.read(buf)
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
