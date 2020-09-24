use std::io;
use std::ops;

use csv;
use csv_index::RandomAccessSimple;

/// Indexed composes a CSV reader with a simple random access index.
///
/// This provides a convenient way to deal with a CSV reader that has an index.
pub struct Indexed<R, I> {
    csv_rdr: csv::Reader<R>,
    idx: RandomAccessSimple<I>,
}

impl<R: io::Read + io::Seek, I: io::Read + io::Seek> Indexed<R, I> {
    /// Opens an index.
    pub fn open(
        csv_rdr: csv::Reader<R>,
        idx_rdr: I,
    ) -> anyhow::Result<Indexed<R, I>> {
        Ok(Indexed {
            csv_rdr: csv_rdr,
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
    ///
    /// If the given record index doesn't exist in the CSV data, then an error
    /// is returned.
    ///
    /// Note that this routine accounts for whether the CSV reader interprets
    /// the first record as a header. That is, if you want to seek to the first
    /// non-header record row, then an index of `0` will accomplish that. Thus,
    /// seeking will never position a CSV reader at the beginning of a header
    /// record. (If one needs the header record, ue the `headers` routine on
    /// `csv::Reader`.)
    pub fn seek(&mut self, mut i: u64) -> anyhow::Result<()> {
        if i >= self.count() {
            anyhow::bail!(
                "invalid record index {} (there are {} records)",
                i,
                self.count()
            );
        }
        if self.csv_reader().has_headers() {
            i += 1;
        }
        let pos = self.idx.get(i)?;
        self.csv_reader_mut().seek(pos)?;
        Ok(())
    }

    /// Returns a reference to the underlying CSV reader.
    pub fn csv_reader(&self) -> &csv::Reader<R> {
        &self.csv_rdr
    }

    /// Returns a mutable reference to the underlying CSV reader.
    pub fn csv_reader_mut(&mut self) -> &mut csv::Reader<R> {
        &mut self.csv_rdr
    }
}
