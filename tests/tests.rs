#![feature(macro_rules, phase)]

#![allow(dead_code)]

#[phase(plugin, link)] extern crate log;

extern crate csv;
extern crate quickcheck;

use std::fmt;
use std::mem::transmute;
use std::rand::{Rng, task_rng};

use quickcheck::{Arbitrary, Gen, QuickCheck, Shrinker, StdGen, Testable};

macro_rules! svec[
    ($($x:expr),*) => (
        vec![$($x),*].into_iter().map(|s| s.to_string()).collect::<Vec<_>>()
    );
    ($($x:expr,)*) => (svec![$($x),*]);
]

mod workdir;

mod test_cat;

fn qcheck<T: Testable>(p: T) {
    QuickCheck::new().gen(StdGen::new(task_rng(), 5)).quickcheck(p);
}

type CsvVecs = Vec<Vec<String>>;

trait Csv {
    fn to_vecs(self) -> CsvVecs;
    fn from_vecs(CsvVecs) -> Self;
}

impl Csv for CsvVecs {
    fn to_vecs(self) -> CsvVecs { self }
    fn from_vecs(vecs: CsvVecs) -> CsvVecs { vecs }
}

#[deriving(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct CsvRecord(Vec<String>);

impl CsvRecord {
    fn unwrap(self) -> Vec<String> {
        let CsvRecord(v) = self;
        v
    }

    fn as_slice(&self) -> &[String] {
        let CsvRecord(ref v) = *self;
        v.as_slice()
    }
}

impl fmt::Show for CsvRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let bytes: Vec<_> = self.as_slice()
                                .iter()
                                .map(|s| s.as_bytes())
                                .collect();
        write!(f, "{}", bytes)
    }
}

impl Arbitrary for CsvRecord {
    fn arbitrary<G: Gen>(g: &mut G) -> CsvRecord {
        let size = { let s = g.size(); g.gen_range(1, s) };
        CsvRecord(Vec::from_fn(size, |_| Arbitrary::arbitrary(g)))
    }

    fn shrink(&self) -> Box<Shrinker<CsvRecord>+'static> {
        box self.clone().unwrap()
                .shrink().filter(|r| r.len() > 0).map(CsvRecord)
    }
}

impl Csv for Vec<CsvRecord> {
    fn to_vecs(self) -> CsvVecs { unsafe { transmute(self) } }
    fn from_vecs(vecs: CsvVecs) -> Vec<CsvRecord> { unsafe { transmute(vecs) } }
}

#[deriving(Clone, Eq, Ord, PartialEq, PartialOrd, Show)]
struct CsvData(Vec<CsvRecord>);

impl CsvData {
    fn unwrap(self) -> Vec<CsvRecord> {
        let CsvData(v) = self;
        v
    }

    fn as_slice(&self) -> &[CsvRecord] {
        let CsvData(ref v) = *self;
        v.as_slice()
    }

    fn len(&self) -> uint { self.as_slice().len() }
    fn is_empty(&self) -> bool { self.len() == 0 }
}

impl Arbitrary for CsvData {
    fn arbitrary<G: Gen>(g: &mut G) -> CsvData {
        let record_len = { let s = g.size(); g.gen_range(1, s) };
        let num_records = g.gen_range(0, 100);
        CsvData(Vec::from_fn(num_records, |_| {
            CsvRecord(Vec::from_fn(record_len, |_| Arbitrary::arbitrary(g)))
        }))
    }

    fn shrink(&self) -> Box<Shrinker<CsvData>+'static> {
        box self.clone().unwrap().shrink().map(CsvData)
    }
}

impl Csv for CsvData {
    fn to_vecs(self) -> CsvVecs { unsafe { transmute(self) } }
    fn from_vecs(vecs: CsvVecs) -> CsvData { unsafe { transmute(vecs) } }
}
