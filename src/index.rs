use std::io;
use std::ops;

use csv;
use csv_index::RandomAccessSimple;

use CliResult;

/// Indexed composes a CSV reader with a simple random access index.
pub struct Indexed<R, I> {
    csv_rdr: csv::Reader<R>,
    idx: RandomAccessSimple<I>,
}

impl<R, I> ops::Deref for Indexed<R, I> {
    type Target = csv::Reader<R>;
    fn deref(&self) -> &csv::Reader<R> { &self.csv_rdr }
}

impl<R, I> ops::DerefMut for Indexed<R, I> {
    fn deref_mut(&mut self) -> &mut csv::Reader<R> { &mut self.csv_rdr }
}

impl<R: io::Read + io::Seek, I: io::Read + io::Seek> Indexed<R, I> {
    /// Opens an index.
    pub fn open(
        csv_rdr: csv::Reader<R>,
        idx_rdr: I,
    ) -> CliResult<Indexed<R, I>> {
        Ok(Indexed {
            csv_rdr,
            idx: RandomAccessSimple::open(idx_rdr)?,
        })
    }

    /// Return the number of records (not including the header record) in this
    /// index.
    pub fn count(&self) -> u64 {
        if self.csv_rdr.has_headers() && !self.idx.is_empty() {
            self.idx.len() - 1
        } else {
            self.idx.len()
        }
    }

    /// Seek to the starting position of record `i`.
    pub fn seek(&mut self, mut i: u64) -> CliResult<()> {
        if i >= self.count() {
            let msg = format!(
                "invalid record index {} (there are {} records)",
                i, self.count());
            return fail!(io::Error::new(io::ErrorKind::Other, msg));
        }
        if self.csv_rdr.has_headers() {
            i += 1;
        }
        let pos = self.idx.get(i)?;
        self.csv_rdr.seek(pos)?;
        Ok(())
    }
}
