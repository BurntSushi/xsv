use workdir::Workdir;

use {CsvData, qcheck};

fn prop_transpose(name: &str, rows: CsvData, streaming: bool) -> bool {
    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows.clone());

    let mut cmd = wrk.command("transpose");
    cmd.arg("in.csv");
    if streaming { cmd.arg("--multipass"); }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);

    let mut expected = vec![];

    let nrows = rows.len();
    let ncols = if !rows.is_empty() {rows[0].len() } else {0};

    for i in 0..ncols {
        let mut expected_row = vec![];
        for j in 0..nrows {
            expected_row.push(rows[j][i].to_owned());
        }
        expected.push(expected_row);
    }
    rassert_eq!(got, expected)
}

#[test]
fn prop_transpose_in_memory() {
    fn p(rows: CsvData) -> bool {
        prop_transpose("prop_transpose_in_memory", rows, false)
    }
    qcheck(p as fn(CsvData) -> bool);
}

#[test]
fn prop_transpose_multipass() {
    fn p(rows: CsvData) -> bool {
        prop_transpose("prop_transpose_multipass", rows, true)
    }
    qcheck(p as fn(CsvData) -> bool);
}
