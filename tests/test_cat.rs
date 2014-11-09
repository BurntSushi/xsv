use {CsvData, qcheck};
use workdir::Workdir;

#[test]
fn cat_rows() {
    fn p(rows: CsvData) -> bool {
        let expected = rows.clone();
        let (rows1, rows2) =
            if rows.is_empty() {
                (vec![], vec![])
            } else {
                let (rows1, rows2) = rows.as_slice().split_at(rows.len() / 2);
                (rows1.to_vec(), rows2.to_vec())
            };
        let wrk = Workdir::new("cat_rows");
        wrk.create("in1.csv", rows1.clone());
        wrk.create("in2.csv", rows2.clone());

        let mut cmd = wrk.command("cat");
        cmd.arg("rows")
           .arg("-n")
           .arg("in1.csv").arg("in2.csv")
           .arg("-o").arg("out.csv");
        wrk.run(&mut cmd);
        let got: CsvData = wrk.read("out.csv");
        assert_eq!(got, expected);
        true
    }
    qcheck(p);
}

#[test]
fn cat_rows_headers() {
    let wrk = Workdir::new("cat_rows_headers");
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h1", "h2"], svec!["y", "z"]];
    wrk.create("in1.csv", rows1.clone());
    wrk.create("in2.csv", rows2.clone());

    let mut cmd = wrk.command("cat");
    cmd.arg("rows")
       .arg("in1.csv").arg("in2.csv")
       .arg("-o").arg("out.csv");
    wrk.run(&mut cmd);
    let got: Vec<Vec<String>> = wrk.read("out.csv");

    let mut expected = rows1.clone();
    expected.extend(rows2.clone().into_iter().skip(1));
    assert_eq!(got, expected);
}
