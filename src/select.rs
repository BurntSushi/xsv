use std::fmt;
use std::from_str::FromStr;
use std::iter;
use std::slice;

use serialize::{Decodable, Decoder};

use csv;

#[deriving(Clone)]
pub struct SelectColumns {
    selectors: Vec<Selector>,
    invert: bool,
}

// This parser is super basic at the moment. Field names cannot contain [-,].
impl SelectColumns {
    pub fn selection(&self, first_record: &[csv::ByteString], use_names: bool)
                    -> Result<Selection, String> {
        if self.selectors.is_empty() {
            return Ok(Selection(Vec::from_fn(first_record.len(), |i| i)));
        }

        let mut map = vec![];
        for sel in self.selectors.iter() {
            let idxs = sel.indices(first_record, use_names);
            map.extend(try!(idxs).into_iter());
        }
        Ok(Selection(map))
    }

    fn parse(s: &str) -> Result<SelectColumns, String> {
        let mut scols = SelectColumns { selectors: vec![], invert: false };
        if s.is_empty() { return Ok(scols); }
        for sel in s.split(',') {
            scols.selectors.push(try!(SelectColumns::parse_selector(sel)));
        }
        Ok(scols)
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
            Ok(SelRange(start, end))
        } else {
            SelectColumns::parse_one_selector(sel).map(SelOne)
        }
    }

    fn parse_one_selector(sel: &str) -> Result<OneSelector, String> {
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
        if self.selectors.is_empty() {
            write!(f, "<All>")
        } else {
            let strs: Vec<_> =
                self.selectors.iter().map(|sel| sel.to_string()).collect();
            write!(f, "{}", strs.connect(", "))
        }
    }
}

impl <E, D: Decoder<E>> Decodable<D, E> for SelectColumns {
    fn decode(d: &mut D) -> Result<SelectColumns, E> {
        SelectColumns::parse(try!(d.read_str()).as_slice())
                      .map_err(|e| d.error(e.as_slice()))
    }
}

#[deriving(Clone)]
enum Selector {
    SelOne(OneSelector),
    SelRange(OneSelector, OneSelector),
}

#[deriving(Clone)]
enum OneSelector {
    SelStart,
    SelEnd,
    SelIndex(uint),
    SelName(String),
    SelIndexedName(String, uint),
}

impl Selector {
    fn indices(&self, first_record: &[csv::ByteString], use_names: bool)
              -> Result<Vec<uint>, String> {
        match self {
            &SelOne(ref sel) => {
                sel.index(first_record, use_names).map(|i| vec![i])
            }
            &SelRange(ref sel1, ref sel2) => {
                let i1 = try!(sel1.index(first_record, use_names));
                let i2 = try!(sel2.index(first_record, use_names));
                Ok(match i1.cmp(&i2) {
                    Equal => vec!(i1),
                    Less => iter::range_inclusive(i1, i2).collect(),
                    Greater => {
                        iter::range_step_inclusive(i1 as int, i2 as int, -1)
                             .map(|i| i as uint).collect()
                    }
                })
            }
        }
    }
}

impl OneSelector {
    fn index(&self, first_record: &[csv::ByteString], use_names: bool)
            -> Result<uint, String> {
        match self {
            &SelStart => Ok(0),
            &SelEnd => Ok(first_record.len()),
            &SelIndex(i) => {
                if i < 1 || i > first_record.len() {
                    Err(format!("Selector index {} is out of \
                                 bounds. Index must be >= 1 \
                                 and <= {}.", i, first_record.len()))
                } else {
                    // Indices given by user are 1-offset. Convert them here!
                    Ok(i-1)
                }
            }
            &SelName(ref s) => {
                if !use_names {
                    return Err(format!("Cannot use names ('{}') in selection \
                                        with --no-headers set.", s));
                }
                match first_record.iter().position(|h| h.equiv(s)) {
                    None => Err(format!("Selector name '{}' does not exist \
                                         as a named header in the given CSV \
                                         data.", s)),
                    Some(i) => Ok(i),
                }
            }
            &SelIndexedName(ref s, sidx) => {
                if !use_names {
                    return Err(format!("Cannot use names ('{}') in selection \
                                        with --no-headers set.", s));
                }
                let mut num_found = 0;
                for (i, field) in first_record.iter().enumerate() {
                    if field.equiv(s) {
                        if num_found == sidx {
                            return Ok(i);
                        }
                        num_found += 1;
                    }
                }
                if num_found == 0 {
                    return Err(format!("Selector name '{}' does not exist \
                                        as a named header in the given CSV \
                                        data.", s));
                }
                Err(format!("Selector index '{}' for name '{}' is \
                             out of bounds. Must be >= 0 and <= {}.",
                             sidx, s, num_found - 1))
            }
        }
    }
}

impl fmt::Show for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SelOne(ref sel) => sel.fmt(f),
            &SelRange(ref s, ref e) => write!(f, "Range({}, {})", s, e),
        }
    }
}

impl fmt::Show for OneSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &SelStart => write!(f, "Start"),
            &SelEnd => write!(f, "End"),
            &SelIndex(idx) => write!(f, "Index({:u})", idx),
            &SelName(ref s) => write!(f, "Name({})", s),
            &SelIndexedName(ref s, idx) => write!(f, "IndexedName({}[{}])",
                                                  s, idx),
        }
    }
}

#[deriving(Clone, Show)]
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

    pub fn normal(&self) -> NormalSelection {
        let &Selection(ref inds) = self;
        let mut normal = inds.clone();
        normal.sort();
        normal.dedup();
        let mut set = Vec::from_elem(normal[normal.len()-1] + 1, false);
        for i in normal.into_iter() {
            *set.get_mut(i) = true;
        }
        NormalSelection(set)
    }
}

impl AsSlice<uint> for Selection {
    fn as_slice(&self) -> &[uint] { self.0[] }
}

impl Collection for Selection {
    fn len(&self) -> uint {
        self.0.len()
    }
}

#[deriving(Clone, Show)]
pub struct NormalSelection(Vec<bool>);

impl NormalSelection {
    pub fn select<'a, 'b, T, I: Iterator<T>>(&'a self, row: I)
                 -> iter::FilterMap<Option<T>, T,
                                    iter::Scan<(uint, T),
                                               Option<T>,
                                               iter::Enumerate<I>,
                                               &'a [bool]>> {
        let set = self.as_slice();
        row.enumerate().scan(set, |set, (i, v)| {
            if i < set.len() && set[i] { Some(Some(v)) } else { Some(None) }
        }).filter_map(|v| v)
    }
}

impl AsSlice<bool> for NormalSelection {
    fn as_slice(&self) -> &[bool] { self.0[] }
}

impl Collection for NormalSelection {
    fn len(&self) -> uint {
        self.as_slice().iter().filter(|b| **b).count()
    }
}
