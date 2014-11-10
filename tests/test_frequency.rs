use std::collections::hash_map::{HashMap, Occupied, Vacant};

use csv;
use stats::Frequencies;

use {Csv, CsvData, qcheck_sized};
use workdir::Workdir;

#[deriving(Decodable)]
struct FRow {
    field: String,
    value: String,
    count: uint,
}

fn ftables<T: Csv>(rows: T) -> HashMap<String, Frequencies<String>> {
    let mut rows = rows.to_vecs();
    if rows.len() <= 1 {
        return HashMap::new();
    }

    let header = rows.remove(0).unwrap();
    let mut ftables = HashMap::new();
    for field in header.iter() { 
        ftables.insert(field.clone(), Frequencies::new());
    }
    for row in rows.into_iter() {
        for (i, mut field) in row.into_iter().enumerate() {
            if field.is_empty() {
                field = "NULL".to_string();
            }
            ftables.get_mut(&header[i]).unwrap().add(field);
        }
    }
    ftables
}

fn freq_data<T>(ftable: &Frequencies<T>) -> Vec<(&T, u64)>
            where T: ::std::hash::Hash + Ord {
    let mut freqs = ftable.most_frequent();
    freqs.sort();
    freqs
}

fn param_prop_frequency(name: &str, rows: CsvData, idx: bool) -> bool {
    let wrk = Workdir::new(name);
    if idx {
        wrk.create_indexed("in.csv", rows.clone());
    } else {
        wrk.create("in.csv", rows.clone());
    }

    let expected_ftables = ftables(rows);

    let mut cmd = wrk.command("frequency");
    cmd.arg("in.csv").args(["--limit", "0"]);

    let mut rdr = csv::Reader::from_string(wrk.stdout::<String>(&cmd));
    let mut got_ftables = HashMap::new();
    for frow in rdr.decode() {
        let frow: FRow = frow.unwrap();
        match got_ftables.entry(frow.field) {
            Vacant(v) => {
                let mut ftable = Frequencies::new();
                for _ in range(0, frow.count) {
                    ftable.add(frow.value.clone());
                }
                v.set(ftable);
            }
            Occupied(mut v) => {
                for _ in range(0, frow.count) {
                    v.get_mut().add(frow.value.clone());
                }
            }
        }
    }
    for (k, v) in got_ftables.iter() {
        assert_eq!(freq_data(v),
                   freq_data(expected_ftables.get(k).unwrap()));
    }
    for (k, v) in expected_ftables.iter() {
        assert_eq!(freq_data(got_ftables.get(k).unwrap()),
                   freq_data(v));
    }
    true
}

#[test]
fn prop_frequency() {
    fn p(rows: CsvData) -> bool {
        param_prop_frequency("prop_frequency", rows, false)
    }
    // Run on really small values because we are incredibly careless
    // with allocation.
    qcheck_sized(p, 2);
}

#[test]
fn prop_frequency_indexed() {
    fn p(rows: CsvData) -> bool {
        param_prop_frequency("prop_frequency_indxed", rows, true)
    }
    // Run on really small values because we are incredibly careless
    // with allocation.
    qcheck_sized(p, 2);
}
