use csv;

use types::{InputReader, OutputWriter};

pub fn csv_reader(i: InputReader,
                  no_headers: bool, delimiter: u8, flexible: bool)
             -> csv::Decoder<InputReader> {
    let d = csv::Decoder::from_reader(i)
            .separator(delimiter)
            .enforce_same_length(!flexible);
    if no_headers { d.no_headers() } else { d }
}

pub fn csv_writer(o: OutputWriter, flexible: bool, crlf: bool)
             -> csv::Encoder<OutputWriter> {
    csv::Encoder::to_writer(o)
    .enforce_same_length(!flexible)
    .crlf(crlf)
}
