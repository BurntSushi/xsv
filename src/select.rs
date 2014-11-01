use std::collections::HashSet;
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

impl SelectColumns {
    fn parse(mut s: &str) -> Result<SelectColumns, String> {
        let invert =
            if !s.is_empty() && s.as_bytes()[0] == b'!' {
                s = s[1..];
                true
            } else {
                false
            };
        Ok(SelectColumns {
            selectors: try!(SelectorParser::new(s).parse()),
            invert: invert,
        })
    }

    pub fn selection(&self, first_record: &[csv::ByteString], use_names: bool)
                    -> Result<Selection, String> {
        if self.selectors.is_empty() {
            return Ok(Selection(if self.invert {
                // Inverting everything means we get nothing.
                vec![]
            } else {
                Vec::from_fn(first_record.len(), |i| i)
            }));
        }

        let mut map = vec![];
        for sel in self.selectors.iter() {
            let idxs = sel.indices(first_record, use_names);
            map.extend(try!(idxs).into_iter());
        }
        if self.invert {
            let set: HashSet<_> = map.into_iter().collect();
            let mut map = vec![];
            for i in range(0, first_record.len()) {
                if !set.contains(&i) {
                    map.push(i);
                }
            }
            return Ok(Selection(map));
        }
        Ok(Selection(map))
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

struct SelectorParser {
    chars: Vec<char>,
    pos: uint,
}

impl SelectorParser {
    fn new(s: &str) -> SelectorParser {
        SelectorParser { chars: s.chars().collect(), pos: 0 }
    }

    fn parse(&mut self) -> Result<Vec<Selector>, String> {
        let mut sels = vec![];
        loop {
            if self.cur().is_none() {
                break;
            }
            let f1: OneSelector =
                if self.cur() == Some('-') {
                    self.bump();
                    SelStart
                } else {
                    try!(self.parse_one())
                };
            let f2: Option<OneSelector> =
                if self.cur() == Some('-') {
                    self.bump();
                    Some(if self.is_end_of_selector() {
                        SelEnd
                    } else {
                        try!(self.parse_one())
                    })
                } else {
                    None
                };
            if !self.is_end_of_selector() {
                return Err(format!(
                    "Expected end of field but got '{}' instead.",
                    self.cur().unwrap()));
            }
            sels.push(match f2 {
                Some(end) => SelRange(f1, end),
                None => SelOne(f1),
            });
            self.bump();
        }
        Ok(sels)
    }

    fn parse_one(&mut self) -> Result<OneSelector, String> {
        let name =
            if self.cur() == Some('"') {
                self.bump();
                try!(self.parse_quoted_name())
            } else {
                try!(self.parse_name())
            };
        Ok(if self.cur() == Some('[') {
            let idx = try!(self.parse_index());
            SelIndexedName(name, idx)
        } else {
            match FromStr::from_str(name[]) {
                None => SelIndexedName(name, 0),
                Some(idx) => SelIndex(idx),
            }
        })
    }

    fn parse_name(&mut self) -> Result<String, String> {
        let mut name = String::new();
        loop {
            if self.is_end_of_field() || self.cur() == Some('[') {
                break;
            }
            name.push(self.cur().unwrap());
            self.bump();
        }
        Ok(name)
    }

    fn parse_quoted_name(&mut self) -> Result<String, String> {
        let mut name = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err("Unclosed quote, missing closing \"."
                               .to_string());
                }
                Some('"') => {
                    self.bump();
                    if self.cur() == Some('"') {
                        self.bump();
                        name.push('"'); name.push('"');
                        continue;
                    }
                    break
                }
                Some(c) => { name.push(c); self.bump(); }
            }
        }
        Ok(name)
    }

    fn parse_index(&mut self) -> Result<uint, String> {
        assert_eq!(self.cur().unwrap(), '[');
        self.bump();

        let mut idx = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err("Unclosed index bracket, missing closing ]."
                               .to_string());
                }
                Some(']') => { self.bump(); break; }
                Some(c) => { idx.push(c); self.bump(); }
            }
        }
        match FromStr::from_str(idx[]) {
            None => Err(format!("Could not convert '{}' to an integer.", idx)),
            Some(idx) => Ok(idx),
        }
    }

    fn cur(&self) -> Option<char> {
        self.chars[].get(self.pos).map(|c| *c)
    }

    fn is_end_of_field(&self) -> bool {
        self.cur().map(|c| c == ',' || c == '-').unwrap_or(true)
    }

    fn is_end_of_selector(&self) -> bool {
        self.cur().map(|c| c == ',').unwrap_or(true)
    }

    fn bump(&mut self) {
        if self.pos < self.chars.len() { self.pos += 1; }
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
