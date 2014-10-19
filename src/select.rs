use std::fmt;
use std::from_str::FromStr;
use std::iter;
use std::slice;

use serialize::{Decodable, Decoder};

use csv;

#[deriving(Clone)]
pub struct SelectColumns(Vec<Selector>);

// This parser is super basic at the moment. Field names cannot contain [-,].
impl SelectColumns {
    pub fn selection(&self, first_record: &[csv::ByteString], use_names: bool)
                    -> Result<Selection, String> {
        if self.selectors().is_empty() {
            return Ok(Selection(Vec::from_fn(first_record.len(), |i| i)));
        }

        let mut map = vec![];
        for sel in self.selectors().iter() {
            let idxs = sel.indices(first_record, use_names);
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

#[deriving(Clone)]
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

    fn indices(&self, first_record: &[csv::ByteString], use_names: bool)
              -> Result<Vec<uint>, String> {
        match self {
            &SelStart => Ok(vec!(0)),
            &SelEnd => Ok(vec!(first_record.len())),
            &SelIndex(i) => {
                if i < 1 || i > first_record.len() {
                    Err(format!("Selector index {} is out of \
                                 bounds. Index must be >= 1 \
                                 and <= {}.", i, first_record.len()))
                } else {
                    // Indices given by user are 1-offset. Convert them here!
                    Ok(vec!(i-1))
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
                    Some(i) => Ok(vec!(i)),
                }
            }
            &SelRange(box ref sel1, box ref sel2) => {
                assert!(!sel1.is_range());
                assert!(!sel2.is_range());
                let is1 = try!(sel1.indices(first_record, use_names));
                let is2 = try!(sel2.indices(first_record, use_names));
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

    pub fn as_slice<'a>(&'a self) -> &'a [uint] {
        let &Selection(ref inds) = self;
        inds.as_slice()
    }
}

impl Collection for Selection {
    fn len(&self) -> uint {
        let &Selection(ref inds) = self;
        inds.len()
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

    pub fn as_slice<'a>(&'a self) -> &'a [bool] {
        let &NormalSelection(ref inds) = self;
        inds.as_slice()
    }
}

impl Collection for NormalSelection {
    fn len(&self) -> uint {
        self.as_slice().iter().filter(|b| **b).count()
    }
}
