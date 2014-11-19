use {CsvData, qcheck};
use workdir::Workdir;

/// This tests whether `xsv count` gets the right answer.
///
/// It does some simple case analysis to handle whether we want to test counts
/// in the presence of headers and/or indexes.
fn prop_count_len(name: &str, rows: CsvData,
                  headers: bool, idx: bool) -> bool {
    let mut expected_count = rows.as_slice().len();
    if headers && expected_count > 0 {
        expected_count -= 1;
    }

    let wrk = Workdir::new(name);
    if idx {
        wrk.create_indexed("in.csv", rows);
    } else {
        wrk.create("in.csv", rows);
    }

    let mut cmd = wrk.command("count");
    if !headers {
        cmd.arg("--no-headers");
    }
    cmd.arg("in.csv");

    let got_count: uint = wrk.stdout(&cmd);
    rassert_eq!(got_count, expected_count)
}

#[test]
fn prop_count() {
    fn p(rows: CsvData) -> bool {
        prop_count_len("prop_count", rows, false, false)
    }
    qcheck(p);
}

#[test]
fn prop_count_headers() {
    fn p(rows: CsvData) -> bool {
        prop_count_len("prop_count_headers", rows, true, false)
    }
    qcheck(p);
}

#[test]
fn prop_count_indexed() {
    fn p(rows: CsvData) -> bool {
        prop_count_len("prop_count_indexed", rows, false, true)
    }
    qcheck(p);
}

#[test]
fn prop_count_indexed_headers() {
    fn p(rows: CsvData) -> bool {
        prop_count_len("prop_count_indexed_headers", rows, true, true)
    }
    qcheck(p);
}
