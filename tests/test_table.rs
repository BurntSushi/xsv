use workdir::Workdir;

static EXPECTED_TABLE: &'static str = "\
h1       h2   h3
abcdefg  a    a
a        abc  z\
";

fn data() -> Vec<Vec<String>> {
    vec![
        svec!["h1", "h2", "h3"],
        svec!["abcdefg", "a", "a"],
        svec!["a", "abc", "z"],
    ]
}

#[test]
fn table() {
    let wrk = Workdir::new("table");
    wrk.create("in.csv", data());

    let mut cmd = wrk.command("table");
    cmd.env("XSV_DEFAULT_DELIMITER", "\t");
    cmd.arg("in.csv");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, EXPECTED_TABLE)
}

#[test]
fn table_tsv() {
    let wrk = Workdir::new("table");
    wrk.create_with_delim("in.tsv", data(), b'\t');

    let mut cmd = wrk.command("table");
    cmd.env("XSV_DEFAULT_DELIMITER", "\t");
    cmd.arg("in.tsv");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, EXPECTED_TABLE)
}

#[test]
fn table_default() {
    let wrk = Workdir::new("table");
    wrk.create_with_delim("in.bin", data(), b'\t');

    let mut cmd = wrk.command("table");
    cmd.env("XSV_DEFAULT_DELIMITER", "\t");
    cmd.arg("in.bin");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, EXPECTED_TABLE)
}
