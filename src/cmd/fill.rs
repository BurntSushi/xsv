use std::collections::hash_map::HashMap;
use std::io;
use std::iter;
use std::ops;

use csv;

use CliResult;
use config::{Config, Delimiter};
use select::{SelectColumns, Selection};
use util;

static USAGE: &'static str = "
Fill empty fields in selected columns of a CSV.

This command fills empty fields in the selected column
using the last seen non-empty field in the CSV. This is
useful to forward-fill values which may only be included
the first time they are encountered.

The option `--default <value>` fills all empty values
in the selected columns with the provided default value.

The option `--first` fills empty values using the first
seen non-empty value in that column, instead of the most
recent non-empty value in that column.

The option `--backfill` fills empty values at the start of
the CSV with the first valid value in that column. This
requires buffering rows with empty values in the target
column which appear before the first valid value.

The option `--groupby` groups the rows by the specified
columns before filling in the empty values. Using this
option, empty values are only filled with values which
belong to the same group of rows, as determined by the
columns selected in the `--groupby` option.

When both `--groupby` and `--backfill` are specified, and the
CSV is not sorted by the `--groupby` columns, rows may be
re-ordered during output due to the buffering of rows
collected before the first valid value.

Usage:
    xsv fill [options] [--] <selection> [<input>]
    xsv fill --help

fill options:
    -g --groupby <keys>    Group by specified columns.
    -f --first             Fill using the first valid value of a column, instead of the latest.
    -b --backfill          Fill initial empty values with the first valid value.
    -v --default <value>   Fill using this default value.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

type ByteString = Vec<u8>;

type BoxedWriter = csv::Writer<Box<io::Write + 'static>>;
type BoxedReader = csv::Reader<Box<io::Read + 'static>>;

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_groupby: Option<SelectColumns>,
    flag_first: bool,
    flag_backfill: bool,
    flag_default: Option<String>,
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_selection);

    let wconfig = Config::new(&args.flag_output);

    let mut rdr = rconfig.reader()?;
    let mut wtr = wconfig.writer()?;

    let headers = rdr.byte_headers()?.clone();
    let select = rconfig.selection(&headers)?;
    let groupby = match args.flag_groupby {
        Some(value) => Some(value.selection(&headers, !rconfig.no_headers)?),
        None => None,
    };

    if !rconfig.no_headers {
        rconfig.write_headers(&mut rdr, &mut wtr)?;
    }

    let filler = Filler::new(groupby, select)
        .use_first_value(args.flag_first)
        .backfill_empty_values(args.flag_backfill)
        .use_default_value(args.flag_default);
    filler.fill(&mut rdr, &mut wtr)
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct ByteRecord(Vec<ByteString>);

impl<'a> From<&'a csv::ByteRecord> for ByteRecord {
    fn from(record: &'a csv::ByteRecord) -> Self {
        ByteRecord(record.iter().map(|f| f.to_vec()).collect())
    }
}

impl iter::FromIterator<ByteString> for ByteRecord {
    fn from_iter<T: IntoIterator<Item = ByteString>>(iter: T) -> Self {
        ByteRecord(Vec::from_iter(iter))
    }
}

impl iter::IntoIterator for ByteRecord {
    type Item = ByteString;
    type IntoIter = ::std::vec::IntoIter<ByteString>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ops::Deref for ByteRecord {
    type Target = [ByteString];
    fn deref(&self) -> &[ByteString] {
        &self.0
    }
}

type GroupKey = Option<ByteRecord>;
type GroupBuffer = HashMap<GroupKey, Vec<ByteRecord>>;
type Grouper = HashMap<GroupKey, GroupValues>;
type GroupKeySelection = Option<Selection>;

trait GroupKeyConstructor {
    fn key(&self, record: &csv::ByteRecord) -> Result<GroupKey, String>;
}

impl GroupKeyConstructor for GroupKeySelection {
    fn key(&self, record: &csv::ByteRecord) -> Result<GroupKey, String> {
        match *self {
            Some(ref value) => Ok(Some(value.iter().map(|&i| record[i].to_vec()).collect())),
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
struct GroupValues {
    map: HashMap<usize, ByteString>,
    default: Option<ByteString>,
}

impl GroupValues {
    fn new(default: Option<ByteString>) -> Self {
        Self {
            map: HashMap::new(),
            default: default,
        }
    }
}

trait GroupMemorizer {
    fn fill(&self, selection: &Selection, record: ByteRecord) -> ByteRecord;
    fn memorize(&mut self, selection: &Selection, record: &csv::ByteRecord);
    fn memorize_first(&mut self, selection: &Selection, record: &csv::ByteRecord);
}

impl GroupMemorizer for GroupValues {
    fn memorize(&mut self, selection: &Selection, record: &csv::ByteRecord) {
        for &col in selection.iter().filter(|&col| !record[*col].is_empty()) {
            self.map.insert(col, record[col].to_vec());
        }
    }

    fn memorize_first(&mut self, selection: &Selection, record: &csv::ByteRecord) {
        for &col in selection.iter().filter(|&col| !record[*col].is_empty()) {
            self.map.entry(col).or_insert(record[col].to_vec());
        }
    }

    fn fill(&self, selection: &Selection, record: ByteRecord) -> ByteRecord {
        record
            .into_iter()
            .enumerate()
            .map_selected(selection, |(col, field)| {
                (
                    col,
                    if field.is_empty() {
                        self.default
                            .clone()
                            .or_else(|| self.map.get(&col).cloned())
                            .unwrap_or_else(|| field.to_vec())
                    } else {
                        field
                    },
                )
            })
            .map(|(_, field)| field)
            .collect()
    }
}

struct Filler {
    grouper: Grouper,
    groupby: GroupKeySelection,
    select: Selection,
    buffer: GroupBuffer,
    first: bool,
    backfill: bool,
    default_value: Option<ByteString>,
}

impl Filler {
    fn new(groupby: GroupKeySelection, select: Selection) -> Self {
        Self {
            grouper: Grouper::new(),
            groupby: groupby,
            select: select,
            buffer: GroupBuffer::new(),
            first: false,
            backfill: false,
            default_value: None,
        }
    }

    fn use_first_value(mut self, first: bool) -> Self {
        self.first = first;
        self
    }

    fn backfill_empty_values(mut self, backfill: bool) -> Self {
        self.backfill = backfill;
        self
    }

    fn use_default_value(mut self, value: Option<String>) -> Self {
        self.default_value = value.map(|v| v.as_bytes().to_vec());
        self
    }

    fn fill(mut self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()> {
        let mut record = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut record)? {
            // Precompute groupby key
            let key = self.groupby.key(&record)?;

            // Record valid fields, and fill empty fields
            let default_value = self.default_value.clone();
            let group = self.grouper
                .entry(key.clone())
                .or_insert_with(|| GroupValues::new(default_value));

            match (self.default_value.is_some(), self.first) {
                (true, _) => {}
                (false, true) => group.memorize_first(&self.select, &record),
                (false, false) => group.memorize(&self.select, &record),
            };

            let row = group.fill(&self.select, ByteRecord::from(&record));

            // Handle buffering rows which still have nulls.
            if self.backfill && (self.select.iter().any(|&i| row[i] == b"")) {
                self.buffer
                    .entry(key.clone())
                    .or_insert_with(Vec::new)
                    .push(row);
            } else {
                if let Some(rows) = self.buffer.remove(&key) {
                    for buffered_row in rows {
                        wtr.write_record(group.fill(&self.select, buffered_row).iter())?;
                    }
                }
                wtr.write_record(row.iter())?;
            }
        }

        // Ensure any remaining buffers are dumped at the end.
        for (key, rows) in self.buffer {
            let group = self.grouper.get(&key).unwrap();
            for buffered_row in rows {
                wtr.write_record(group.fill(&self.select, buffered_row).iter())?;
            }
        }

        wtr.flush()?;
        Ok(())
    }
}

struct MapSelected<I, F> {
    selection: Vec<usize>,
    selection_index: usize,
    index: usize,
    iterator: I,
    predicate: F,
}

impl<I: iter::Iterator, F> iter::Iterator for MapSelected<I, F>
where
    F: FnMut(I::Item) -> I::Item,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = match self.iterator.next() {
            Some(item) => item,
            None => return None
        };
        let result = match self.selection_index {
            ref mut sidx if (self.selection.get(*sidx) == Some(&self.index)) => {
                *sidx += 1;
                Some((self.predicate)(item))
            }
            _ => Some(item),
        };
        self.index += 1;
        result
    }
}

trait Selectable<B>
where
    Self: iter::Iterator<Item = B> + Sized,
{
    fn map_selected<F>(self, selector: &Selection, predicate: F) -> MapSelected<Self, F>
    where
        F: FnMut(B) -> B;
}

impl<B, C> Selectable<B> for C
where
    C: iter::Iterator<Item = B> + Sized,
{
    fn map_selected<F>(self, selector: &Selection, predicate: F) -> MapSelected<Self, F>
    where
        F: FnMut(B) -> B,
    {
        MapSelected {
            selection: selector.iter().map(|&x| x).collect(),
            selection_index: 0,
            index: 0,
            iterator: self,
            predicate: predicate,
        }
    }
}
