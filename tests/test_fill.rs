use CsvRecord;
use workdir::Workdir;

fn compare_column(got: &[CsvRecord], expected: &[String], column: usize, skip_header: bool) {
    for (value, value_expected) in got.iter()
        .skip(if skip_header { 1 } else { 0 })
        .map(|row| &row[column])
        .zip(expected.iter())
    {
        assert_eq!(value, value_expected)
    }
}

fn example() -> Vec<Vec<String>> {
    vec![
        svec!["h1", "h2", "h3"],
        svec!["", "baz", "egg"],
        svec!["", "foo", ""],
        svec!["abc", "baz", "foo"],
        svec!["", "baz", "egg"],
        svec!["zap", "baz", "foo"],
        svec!["bar", "foo", ""],
        svec!["bongo", "foo", ""],
        svec!["", "foo", "jar"],
        svec!["", "baz", "jar"],
        svec!["", "foo", "jar"],
    ]
}

#[test]
fn fill_forward() {
    let wrk = Workdir::new("fill_forward");
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);

    // Filled target column
    let expected = svec![
        "", "", "abc", "abc", "zap", "bar", "bongo", "bongo", "bongo", "bongo"
    ];
    compare_column(&got, &expected, 0, true);

    // Left non-target column alone
    let expected = svec!["egg", "", "foo", "egg", "foo", "", "", "jar", "jar", "jar"];
    compare_column(&got, &expected, 2, true);
}

#[test]
fn fill_forward_both() {
    let wrk = Workdir::new("fill_forward");
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--").arg("1,3").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);

    // Filled target column
    let expected = svec![
        "", "", "abc", "abc", "zap", "bar", "bongo", "bongo", "bongo", "bongo"
    ];
    compare_column(&got, &expected, 0, true);

    let expected = svec![
        "egg", "egg", "foo", "egg", "foo", "foo", "foo", "jar", "jar", "jar"
    ];
    compare_column(&got, &expected, 2, true);
}

#[test]
fn fill_forward_groupby() {
    let wrk = Workdir::new("fill_forward_groupby").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.args(&vec!["-g", "2"]).arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "", "", "abc", "abc", "zap", "bar", "bongo", "bongo", "zap", "bongo"
    ];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_first_groupby() {
    let wrk = Workdir::new("fill_first_groupby").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.args(&vec!["-g", "2"])
        .arg("--first")
        .arg("--")
        .arg("1")
        .arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "", "", "abc", "abc", "zap", "bar", "bongo", "bar", "abc", "bar"
    ];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_first() {
    let wrk = Workdir::new("fill_first").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--first").arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "", "", "abc", "abc", "zap", "bar", "bongo", "abc", "abc", "abc"
    ];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_backfill() {
    let wrk = Workdir::new("fill_backfill").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--backfill").arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "abc", "abc", "abc", "abc", "zap", "bar", "bongo", "bongo", "bongo", "bongo"
    ];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_backfill_first() {
    let wrk = Workdir::new("fill_backfill").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--backfill")
        .arg("--first")
        .arg("--")
        .arg("1")
        .arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "abc", "abc", "abc", "abc", "zap", "bar", "bongo", "abc", "abc", "abc"
    ];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_default() {
    let wrk = Workdir::new("fill_default").flexible(true);
    wrk.create("in.csv", example());

    let mut cmd = wrk.command("fill");
    cmd.arg("--default")
        .arg("dat")
        .arg("--")
        .arg("1")
        .arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec![
        "dat", "dat", "abc", "dat", "zap", "bar", "bongo", "dat", "dat", "dat"
    ];
    compare_column(&got, &expected, 0, true);
}
