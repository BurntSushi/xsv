use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::iter::{self, repeat};
use std::ops;
use std::slice;
use std::str::FromStr;

use rustc_serialize::{Decodable, Decoder};

use csv;

#[derive(Clone)]
pub struct SelectColumns {
    selectors: Vec<Selector>,
    invert: bool,
}

impl SelectColumns {
    fn parse(mut s: &str) -> Result<SelectColumns, String> {
        let invert =
            if !s.is_empty() && s.as_bytes()[0] == b'!' {
                s = &s[1..];
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
                (0..first_record.len()).collect()
            }));
        }

        let mut map = vec![];
        for sel in &self.selectors {
            let idxs = sel.indices(first_record, use_names);
            map.extend(try!(idxs).into_iter());
        }
        if self.invert {
            let set: HashSet<_> = map.into_iter().collect();
            let mut map = vec![];
            for i in 0..first_record.len() {
                if !set.contains(&i) {
                    map.push(i);
                }
            }
            return Ok(Selection(map));
        }
        Ok(Selection(map))
    }
}

impl fmt::Debug for SelectColumns {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.selectors.is_empty() {
            write!(f, "<All>")
        } else {
            let strs: Vec<_> =
                self.selectors
                    .iter().map(|sel| format!("{:?}", sel)).collect();
            write!(f, "{}", strs.connect(", "))
        }
    }
}

impl Decodable for SelectColumns {
    fn decode<D: Decoder>(d: &mut D) -> Result<SelectColumns, D::Error> {
        SelectColumns::parse(&*try!(d.read_str()))
                      .map_err(|e| d.error(&e))
    }
}

struct SelectorParser {
    chars: Vec<char>,
    pos: usize,
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
                    OneSelector::Start
                } else {
                    try!(self.parse_one())
                };
            let f2: Option<OneSelector> =
                if self.cur() == Some('-') {
                    self.bump();
                    Some(if self.is_end_of_selector() {
                        OneSelector::End
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
                Some(end) => Selector::Range(f1, end),
                None => Selector::One(f1),
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
            OneSelector::IndexedName(name, idx)
        } else {
            match FromStr::from_str(&name) {
                Err(_) => OneSelector::IndexedName(name, 0),
                Ok(idx) => OneSelector::Index(idx),
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
                               .to_owned());
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

    fn parse_index(&mut self) -> Result<usize, String> {
        assert_eq!(self.cur().unwrap(), '[');
        self.bump();

        let mut idx = String::new();
        loop {
            match self.cur() {
                None => {
                    return Err("Unclosed index bracket, missing closing ]."
                               .to_owned());
                }
                Some(']') => { self.bump(); break; }
                Some(c) => { idx.push(c); self.bump(); }
            }
        }
        FromStr::from_str(&idx).map_err(|err| {
            format!("Could not convert '{}' to an integer: {}", idx, err)
        })
    }

    fn cur(&self) -> Option<char> {
        self.chars.get(self.pos).cloned()
    }

    fn is_end_of_field(&self) -> bool {
        self.cur().map_or(true, |c| c == ',' || c == '-')
    }

    fn is_end_of_selector(&self) -> bool {
        self.cur().map_or(true, |c| c == ',')
    }

    fn bump(&mut self) {
        if self.pos < self.chars.len() { self.pos += 1; }
    }
}

#[derive(Clone)]
enum Selector {
    One(OneSelector),
    Range(OneSelector, OneSelector),
}

#[derive(Clone)]
enum OneSelector {
    Start,
    End,
    Index(usize),
    IndexedName(String, usize),
}

impl Selector {
    fn indices(&self, first_record: &[csv::ByteString], use_names: bool)
              -> Result<Vec<usize>, String> {
        match *self {
            Selector::One(ref sel) => {
                sel.index(first_record, use_names).map(|i| vec![i])
            }
            Selector::Range(ref sel1, ref sel2) => {
                let i1 = try!(sel1.index(first_record, use_names));
                let i2 = try!(sel2.index(first_record, use_names));
                Ok(match i1.cmp(&i2) {
                    Ordering::Equal => vec!(i1),
                    Ordering::Less => (i1..(i2 + 1)).collect(),
                    Ordering::Greater => {
                        let mut inds = vec![];
                        let mut i = i1 + 1;
                        while i > i2 {
                            i -= 1;
                            inds.push(i);
                        }
                        inds
                    }
                })
            }
        }
    }
}

impl OneSelector {
    fn index(&self, first_record: &[csv::ByteString], use_names: bool)
            -> Result<usize, String> {
        match *self {
            OneSelector::Start => Ok(0),
            OneSelector::End => Ok(
                if first_record.len() == 0 {
                    0
                } else {
                    first_record.len() - 1
                }
            ),
            OneSelector::Index(i) => {
                if i < 1 || i > first_record.len() {
                    Err(format!("Selector index {} is out of \
                                 bounds. Index must be >= 1 \
                                 and <= {}.", i, first_record.len()))
                } else {
                    // Indices given by user are 1-offset. Convert them here!
                    Ok(i-1)
                }
            }
            OneSelector::IndexedName(ref s, sidx) => {
                if !use_names {
                    return Err(format!("Cannot use names ('{}') in selection \
                                        with --no-headers set.", s));
                }
                let mut num_found = 0;
                for (i, field) in first_record.iter().enumerate() {
                    if *field == s.as_bytes() {
                        if num_found == sidx {
                            return Ok(i);
                        }
                        num_found += 1;
                    }
                }
                if num_found == 0 {
                    Err(format!("Selector name '{}' does not exist \
                                 as a named header in the given CSV \
                                 data.", s))
                } else {
                    Err(format!("Selector index '{}' for name '{}' is \
                                 out of bounds. Must be >= 0 and <= {}.",
                                 sidx, s, num_found - 1))
                }
            }
        }
    }
}

impl fmt::Debug for Selector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Selector::One(ref sel) => sel.fmt(f),
            Selector::Range(ref s, ref e) =>
                write!(f, "Range({:?}, {:?})", s, e),
        }
    }
}

impl fmt::Debug for OneSelector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OneSelector::Start => write!(f, "Start"),
            OneSelector::End => write!(f, "End"),
            OneSelector::Index(idx) => write!(f, "Index({})", idx),
            OneSelector::IndexedName(ref s, idx) =>
                write!(f, "IndexedName({}[{}])", s, idx),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Selection(Vec<usize>);

pub type _GetField = for <'c> fn(&mut &'c [csv::ByteString], &usize)
                            -> Option<&'c [u8]>;

impl Selection {
    pub fn select<'a, 'b>(&'a self, row: &'b [csv::ByteString])
                 -> iter::Scan<
                        slice::Iter<'a, usize>,
                        &'b [csv::ByteString],
                        _GetField,
                    > {
        // This is horrifying.
        fn get_field<'c>(row: &mut &'c [csv::ByteString], idx: &usize)
                        -> Option<&'c [u8]> {
            Some(&row[*idx])
        }
        let get_field: _GetField = get_field;
        self.iter().scan(row, get_field)
    }

    pub fn normal(&self) -> NormalSelection {
        let &Selection(ref inds) = self;
        if inds.is_empty() {
            return NormalSelection(vec![]);
        }

        let mut normal = inds.clone();
        normal.sort();
        normal.dedup();
        let mut set: Vec<_> =
            repeat(false).take(normal[normal.len()-1] + 1).collect();
        for i in normal.into_iter() {
            set[i] = true;
        }
        NormalSelection(set)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl ops::Deref for Selection {
    type Target = [usize];

    fn deref(&self) -> &[usize] {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct NormalSelection(Vec<bool>);

pub type _NormalScan<'a, T, I> = iter::Scan<
    iter::Enumerate<I>,
    &'a [bool],
    _NormalGetField<T>,
>;

pub type _NormalFilterMap<'a, T, I> = iter::FilterMap<
    _NormalScan<'a, T, I>,
    fn(Option<T>) -> Option<T>
>;

pub type _NormalGetField<T> = fn(&mut &[bool], (usize, T)) -> Option<Option<T>>;

impl NormalSelection {
    pub fn select<'a, T, I>(&'a self, row: I) -> _NormalFilterMap<'a, T, I>
             where I: Iterator<Item=T> {
        fn filmap<T>(v: Option<T>) -> Option<T> { v }
        fn get_field<T>(set: &mut &[bool], t: (usize, T))
                       -> Option<Option<T>> {
            let (i, v) = t;
            if i < set.len() && set[i] { Some(Some(v)) } else { Some(None) }
        }
        let get_field: _NormalGetField<T> = get_field;
        let filmap: fn(Option<T>) -> Option<T> = filmap;
        row.enumerate().scan(&**self, get_field).filter_map(filmap)
    }

    pub fn len(&self) -> usize {
        self.iter().filter(|b| **b).count()
    }
}

impl ops::Deref for NormalSelection {
    type Target = [bool];

    fn deref(&self) -> &[bool] {
        &self.0
    }
}
