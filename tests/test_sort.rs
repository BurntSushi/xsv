use std::iter::order;

use workdir::Workdir;

use {Csv, CsvData, qcheck};

fn prop_sort(name: &str, rows: CsvData, headers: bool) -> bool {
    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows.clone());

    let mut cmd = wrk.command("sort");
    cmd.arg("in.csv");
    if !headers { cmd.arg("--no-headers"); }

    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let mut expected = rows.to_vecs();
    let headers = if headers && !expected.is_empty() {
        expected.remove(0)
    } else {
        vec![]
    };
    expected.sort_by(|r1, r2| order::cmp(r1.iter(), r2.iter()));
    if !headers.is_empty() { expected.insert(0, headers); }
    rassert_eq!(got, expected)
}

#[test]
fn prop_sort_headers() {
    fn p(rows: CsvData) -> bool {
        prop_sort("prop_sort_headers", rows, true)
    }
    qcheck(p as fn(CsvData) -> bool);
}

#[test]
fn prop_sort_no_headers() {
    fn p(rows: CsvData) -> bool {
        prop_sort("prop_sort_no_headers", rows, false)
    }
    qcheck(p as fn(CsvData) -> bool);
}

#[test]
fn sort_select() {
    let wrk = Workdir::new("sort_select");
    wrk.create("in.csv", vec![svec!["1", "b"], svec!["2", "a"]]);

    let mut cmd = wrk.command("sort");
    cmd.arg("--no-headers").args(&["--select", "2"]).arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![svec!["2", "a"], svec!["1", "b"]];
    assert_eq!(got, expected);
}
