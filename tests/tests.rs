#![allow(dead_code)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate csv;
extern crate filetime;
extern crate quickcheck;
extern crate rand;
extern crate stats;

use std::fmt;
use std::mem::transmute;
use std::ops;

use quickcheck::{Arbitrary, Gen, QuickCheck, StdGen, Testable};
use rand::{Rng, thread_rng};

macro_rules! svec[
    ($($x:expr),*) => (
        vec![$($x),*].into_iter()
                     .map(|s: &'static str| s.to_string())
                     .collect::<Vec<String>>()
    );
    ($($x:expr,)*) => (svec![$($x),*]);
];

macro_rules! rassert_eq {
    ($given:expr, $expected:expr) => ({assert_eq!($given, $expected); true});
}

mod workdir;

mod test_cat;
mod test_count;
mod test_fixlengths;
mod test_flatten;
mod test_fmt;
mod test_frequency;
mod test_headers;
mod test_index;
mod test_join;
mod test_partition;
mod test_reverse;
mod test_search;
mod test_select;
mod test_slice;
mod test_sort;
mod test_split;
mod test_stats;
mod test_table;

fn qcheck<T: Testable>(p: T) {
    QuickCheck::new().gen(StdGen::new(thread_rng(), 5)).quickcheck(p);
}

fn qcheck_sized<T: Testable>(p: T, size: usize) {
    QuickCheck::new().gen(StdGen::new(thread_rng(), size)).quickcheck(p);
}

pub type CsvVecs = Vec<Vec<String>>;

pub trait Csv {
    fn to_vecs(self) -> CsvVecs;
    fn from_vecs(CsvVecs) -> Self;
}

impl Csv for CsvVecs {
    fn to_vecs(self) -> CsvVecs { self }
    fn from_vecs(vecs: CsvVecs) -> CsvVecs { vecs }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct CsvRecord(Vec<String>);

impl CsvRecord {
    fn unwrap(self) -> Vec<String> {
        let CsvRecord(v) = self;
        v
    }
}

impl ops::Deref for CsvRecord {
    type Target = [String];
    fn deref<'a>(&'a self) -> &'a [String] { &*self.0 }
}

impl fmt::Debug for CsvRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bytes: Vec<_> = self.iter()
                                .map(|s| s.as_bytes())
                                .collect();
        write!(f, "{:?}", bytes)
    }
}

impl Arbitrary for CsvRecord {
    fn arbitrary<G: Gen>(g: &mut G) -> CsvRecord {
        let size = { let s = g.size(); g.gen_range(1, s) };
        CsvRecord((0..size).map(|_| Arbitrary::arbitrary(g)).collect())
    }

    fn shrink(&self) -> Box<dyn Iterator<Item=CsvRecord>+'static> {
        Box::new(self.clone().unwrap()
                     .shrink().filter(|r| r.len() > 0).map(CsvRecord))
    }
}

impl Csv for Vec<CsvRecord> {
    fn to_vecs(self) -> CsvVecs {
        unsafe { transmute(self) }
    }
    fn from_vecs(vecs: CsvVecs) -> Vec<CsvRecord> {
        unsafe { transmute(vecs) }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialOrd)]
struct CsvData {
    data: Vec<CsvRecord>,
}

impl CsvData {
    fn unwrap(self) -> Vec<CsvRecord> { self.data }

    fn len(&self) -> usize { (&**self).len() }

    fn is_empty(&self) -> bool { self.len() == 0 }
}

impl ops::Deref for CsvData {
    type Target = [CsvRecord];
    fn deref<'a>(&'a self) -> &'a [CsvRecord] { &*self.data }
}

impl Arbitrary for CsvData {
    fn arbitrary<G: Gen>(g: &mut G) -> CsvData {
        let record_len = { let s = g.size(); g.gen_range(1, s) };
        let num_records: usize = g.gen_range(0, 100);
        CsvData{
            data: (0..num_records).map(|_| {
                CsvRecord((0..record_len)
                          .map(|_| Arbitrary::arbitrary(g))
                          .collect())
            }).collect(),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item=CsvData>+'static> {
        let len = if self.is_empty() { 0 } else { self[0].len() };
        let mut rows: Vec<CsvData> =
            self.clone()
                .unwrap()
                .shrink()
                .filter(|rows| rows.iter().all(|r| r.len() == len))
                .map(|rows| CsvData { data: rows })
                .collect();
        // We should also introduce CSV data with fewer columns...
        if len > 1 {
            rows.extend(
                self.clone()
                    .unwrap()
                    .shrink()
                    .filter(|rows|
                        rows.iter().all(|r| r.len() == len - 1))
                    .map(|rows| CsvData { data: rows }));
        }
        Box::new(rows.into_iter())
    }
}

impl Csv for CsvData {
    fn to_vecs(self) -> CsvVecs { unsafe { transmute(self.data) } }
    fn from_vecs(vecs: CsvVecs) -> CsvData {
        CsvData {
            data: unsafe { transmute(vecs) },
        }
    }
}

impl PartialEq for CsvData {
    fn eq(&self, other: &CsvData) -> bool {
        (self.data.is_empty() && other.data.is_empty())
        || self.data == other.data
    }
}
