use std::cmp;

use workdir::Workdir;

use {Csv, CsvData, qcheck};

fn prop_sort(name: &str, rows: CsvData, headers: bool) -> bool {
    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows.clone());

    let mut cmd = wrk.command("sort");
    cmd.arg("in.csv");
    if !headers { cmd.arg("--no-headers"); }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let mut expected = rows.to_vecs();
    let headers = if headers && !expected.is_empty() {
        expected.remove(0)
    } else {
        vec![]
    };
    expected.sort_by(|r1, r2| iter_cmp(r1.iter(), r2.iter()));
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

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["2", "a"], svec!["1", "b"]];
    assert_eq!(got, expected);
}

#[test]
fn sort_numeric() {
    let wrk = Workdir::new("sort_numeric");
    wrk.create("in.csv", vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["2", "c"],
        svec!["1", "b"],
    ]);

    let mut cmd = wrk.command("sort");
    cmd.arg("-N").arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["N", "S"],
        svec!["1", "b"],
        svec!["2", "c"],
        svec!["10", "a"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn sort_reverse() {
    let wrk = Workdir::new("sort_reverse");
    wrk.create("in.csv", vec![
        svec!["R", "S"],
        svec!["1", "b"],
        svec!["2", "a"],
    ]);

    let mut cmd = wrk.command("sort");
    cmd.arg("-R").arg("--no-headers").arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["R", "S"],
        svec!["2", "a"],
        svec!["1", "b"],
    ];
    assert_eq!(got, expected);
}

/// Order `a` and `b` lexicographically using `Ord`
pub fn iter_cmp<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
        where A: Ord, L: Iterator<Item=A>, R: Iterator<Item=A> {
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _   ) => return cmp::Ordering::Less,
            (_   , None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}
