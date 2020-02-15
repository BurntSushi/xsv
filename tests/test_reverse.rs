use workdir::Workdir;

use {Csv, CsvData, qcheck};

fn prop_reverse(name: &str, rows: CsvData, headers: bool) -> bool {
    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows.clone());

    let mut cmd = wrk.command("reverse");
    cmd.arg("in.csv");
    if !headers { cmd.arg("--no-headers"); }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let mut expected = rows.to_vecs();
    let headers = if headers && !expected.is_empty() {
        expected.remove(0)
    } else {
        vec![]
    };
    expected.reverse();
    if !headers.is_empty() { expected.insert(0, headers); }
    rassert_eq!(got, expected)
}

#[test]
fn prop_reverse_headers() {
    fn p(rows: CsvData) -> bool {
        prop_reverse("prop_reverse_headers", rows, true)
    }
    qcheck(p as fn(CsvData) -> bool);
}

#[test]
fn prop_reverse_no_headers() {
    fn p(rows: CsvData) -> bool {
        prop_reverse("prop_reverse_no_headers", rows, false)
    }
    qcheck(p as fn(CsvData) -> bool);
}
