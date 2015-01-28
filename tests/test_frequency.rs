use std::borrow::ToOwned;
use std::collections::hash_map::{HashMap, Hasher, Entry};
use std::old_io::process;

use csv;
use stats::Frequencies;

use {Csv, CsvData, qcheck_sized};
use workdir::Workdir;

fn setup(name: &str) -> (Workdir, process::Command) {
    let rows = vec![
        svec!["h1", "h2"],
        svec!["a", "z"],
        svec!["a", "y"],
        svec!["a", "y"],
        svec!["b", "z"],
        svec!["", "z"],
        svec!["(NULL)", "x"],
    ];

    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("frequency");
    cmd.arg("in.csv");

    (wrk, cmd)
}

#[test]
fn frequency_no_headers() {
    let (wrk, mut cmd) = setup("frequency_no_headers");
    cmd.args(&["--limit", "0"]).args(&["--select", "1"]).arg("--no-headers");

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got = got.into_iter().skip(1).collect();
    got.sort();
    let expected = vec![
        svec!["1", "(NULL)", "1"],
        svec!["1", "(NULL)", "1"],
        svec!["1", "a", "3"],
        svec!["1", "b", "1"],
        svec!["1", "h1", "1"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn frequency_no_nulls() {
    let (wrk, mut cmd) = setup("frequency_no_nulls");
    cmd.arg("--no-nulls").args(&["--limit", "0"]).args(&["--select", "h1"]);

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "a", "3"],
        svec!["h1", "b", "1"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn frequency_nulls() {
    let (wrk, mut cmd) = setup("frequency_nulls");
    cmd.args(&["--limit", "0"]).args(&["--select", "h1"]);

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "(NULL)", "1"],
        svec!["h1", "a", "3"],
        svec!["h1", "b", "1"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn frequency_limit() {
    let (wrk, mut cmd) = setup("frequency_limit");
    cmd.args(&["--limit", "1"]);

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h1", "a", "3"],
        svec!["h2", "z", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn frequency_asc() {
    let (wrk, mut cmd) = setup("frequency_asc");
    cmd.args(&["--limit", "1"]).args(&["--select", "h2"]).arg("--asc");

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h2", "x", "1"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn frequency_select() {
    let (wrk, mut cmd) = setup("frequency_select");
    cmd.args(&["--limit", "0"]).args(&["--select", "h2"]);

    let mut got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    got.sort();
    let expected = vec![
        svec!["field", "value", "count"],
        svec!["h2", "x", "1"],
        svec!["h2", "y", "2"],
        svec!["h2", "z", "3"],
    ];
    assert_eq!(got, expected);
}

// This tests that a frequency table computed by `xsv` is always the same
// as the frequency table computed in memory.
#[test]
fn prop_frequency() {
    fn p(rows: CsvData) -> bool {
        param_prop_frequency("prop_frequency", rows, false)
    }
    // Run on really small values because we are incredibly careless
    // with allocation.
    qcheck_sized(p as fn(CsvData) -> bool, 2);
}

// This tests that a frequency table computed by `xsv` (with an index) is
// always the same as the frequency table computed in memory.
#[test]
fn prop_frequency_indexed() {
    fn p(rows: CsvData) -> bool {
        param_prop_frequency("prop_frequency_indxed", rows, true)
    }
    // Run on really small values because we are incredibly careless
    // with allocation.
    qcheck_sized(p as fn(CsvData) -> bool, 2);
}

fn param_prop_frequency(name: &str, rows: CsvData, idx: bool) -> bool {
    let wrk = Workdir::new(name);
    if idx {
        wrk.create_indexed("in.csv", rows.clone());
    } else {
        wrk.create("in.csv", rows.clone());
    }

    let mut cmd = wrk.command("frequency");
    cmd.arg("in.csv").args(&["-j", "4"]).args(&["--limit", "0"]);

    let got_ftables = ftables_from_csv_string(wrk.stdout::<String>(&cmd));
    let expected_ftables = ftables_from_rows(rows);
    assert_eq_ftables(&got_ftables, &expected_ftables)
}

type FTables = HashMap<String, Frequencies<String>>;

#[derive(RustcDecodable)]
struct FRow {
    field: String,
    value: String,
    count: usize,
}

fn ftables_from_rows<T: Csv>(rows: T) -> FTables {
    let mut rows = rows.to_vecs();
    if rows.len() <= 1 {
        return HashMap::new();
    }

    let header = rows.remove(0);
    let mut ftables = HashMap::new();
    for field in header.iter() {
        ftables.insert(field.clone(), Frequencies::new());
    }
    for row in rows.into_iter() {
        for (i, mut field) in row.into_iter().enumerate() {
            if field.is_empty() {
                field = "(NULL)".to_owned();
            }
            ftables.get_mut(&header[i]).unwrap().add(field);
        }
    }
    ftables
}

fn ftables_from_csv_string(data: String) -> FTables {
    let mut rdr = csv::Reader::from_string(data);
    let mut ftables = HashMap::new();
    for frow in rdr.decode() {
        let frow: FRow = frow.unwrap();
        match ftables.entry(frow.field) {
            Entry::Vacant(v) => {
                let mut ftable = Frequencies::new();
                for _ in range(0, frow.count) {
                    ftable.add(frow.value.clone());
                }
                v.insert(ftable);
            }
            Entry::Occupied(mut v) => {
                for _ in range(0, frow.count) {
                    v.get_mut().add(frow.value.clone());
                }
            }
        }
    }
    ftables
}

fn freq_data<T>(ftable: &Frequencies<T>) -> Vec<(&T, u64)>
        where T: ::std::hash::Hash<Hasher> + Ord + Clone {
    let mut freqs = ftable.most_frequent();
    freqs.sort();
    freqs
}

fn assert_eq_ftables(got: &FTables, expected: &FTables) -> bool {
    for (k, v) in got.iter() {
        assert_eq!(freq_data(v), freq_data(expected.get(k).unwrap()));
    }
    for (k, v) in expected.iter() {
        assert_eq!(freq_data(got.get(k).unwrap()), freq_data(v));
    }
    true
}
