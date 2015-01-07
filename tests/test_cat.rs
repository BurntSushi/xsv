use std::io::process;

use {Csv, CsvData, qcheck};
use workdir::Workdir;

fn no_headers(cmd: &mut process::Command) {
    cmd.arg("--no-headers");
}

fn pad(cmd: &mut process::Command) {
    cmd.arg("--pad");
}

fn run_cat<X, Y, Z, F>(test_name: &str, which: &str, rows1: X, rows2: Y,
                       modify_cmd: F) -> Z
          where X: Csv, Y: Csv, Z: Csv, F: FnOnce(&mut process::Command) {
    let wrk = Workdir::new(test_name);
    wrk.create("in1.csv", rows1);
    wrk.create("in2.csv", rows2);

    let mut cmd = wrk.command("cat");
    modify_cmd(cmd.arg(which).arg("in1.csv").arg("in2.csv"));
    wrk.read_stdout(&cmd)
}

#[test]
fn prop_cat_rows() {
    fn p(rows: CsvData) -> bool {
        let expected = rows.clone();
        let (rows1, rows2) =
            if rows.is_empty() {
                (vec![], vec![])
            } else {
                let (rows1, rows2) = rows.as_slice().split_at(rows.len() / 2);
                (rows1.to_vec(), rows2.to_vec())
            };
        let got: CsvData = run_cat("cat_rows", "rows",
                                   rows1, rows2, no_headers);
        rassert_eq!(got, expected)
    }
    qcheck(p as fn(CsvData) -> bool);
}

#[test]
fn cat_rows_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h1", "h2"], svec!["y", "z"]];

    let mut expected = rows1.clone();
    expected.extend(rows2.clone().into_iter().skip(1));

    let got: Vec<Vec<String>> = run_cat("cat_rows_headers", "rows",
                                        rows1, rows2, |_| ());
    assert_eq!(got, expected);
}

#[test]
fn prop_cat_cols() {
    fn p(rows1: CsvData, rows2: CsvData) -> bool {
        let got: Vec<Vec<String>> = run_cat(
            "cat_cols", "columns", rows1.clone(), rows2.clone(), no_headers);

        let mut expected: Vec<Vec<String>> = vec![];
        let (rows1, rows2) = (rows1.to_vecs().into_iter(),
                              rows2.to_vecs().into_iter());
        for (mut r1, r2) in rows1.zip(rows2) {
            r1.extend(r2.into_iter());
            expected.push(r1);
        }
        rassert_eq!(got, expected)
    }
    qcheck(p as fn(CsvData, CsvData) -> bool);
}

#[test]
fn cat_cols_headers() {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h3", "h4"], svec!["y", "z"]];

    let expected = vec![
        svec!["h1", "h2", "h3", "h4"],
        svec!["a", "b", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat_cols_headers", "columns",
                                        rows1, rows2, |_| ());
    assert_eq!(got, expected);
}

#[test]
fn cat_cols_no_pad() {
    let rows1 = vec![svec!["a", "b"]];
    let rows2 = vec![svec!["y", "z"], svec!["y", "z"]];

    let expected = vec![
        svec!["a", "b", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat_cols_headers", "columns",
                                        rows1, rows2, no_headers);
    assert_eq!(got, expected);
}

#[test]
fn cat_cols_pad() {
    let rows1 = vec![svec!["a", "b"]];
    let rows2 = vec![svec!["y", "z"], svec!["y", "z"]];

    let expected = vec![
        svec!["a", "b", "y", "z"],
        svec!["", "", "y", "z"],
    ];
    let got: Vec<Vec<String>> = run_cat("cat_cols_headers", "columns",
                                        rows1, rows2, pad);
    assert_eq!(got, expected);
}
