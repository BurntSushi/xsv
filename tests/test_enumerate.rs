use workdir::Workdir;

#[test]
fn enumerate() {
    let wrk = Workdir::new("enum");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("enum");
    cmd.arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "index"],
        svec!["a", "13", "0"],
        svec!["b", "24", "1"],
        svec!["c", "72", "2"],
        svec!["d", "7", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_column_name() {
    let wrk = Workdir::new("enum");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("enum");
    cmd.arg("-c").arg("row").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "row"],
        svec!["a", "13", "0"],
        svec!["b", "24", "1"],
        svec!["c", "72", "2"],
        svec!["d", "7", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_constant() {
    let wrk = Workdir::new("enum");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("enum");
    cmd.arg("--constant").arg("test").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "constant"],
        svec!["a", "13", "test"],
        svec!["b", "24", "test"],
        svec!["c", "72", "test"],
        svec!["d", "7", "test"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_copy() {
    let wrk = Workdir::new("enum");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("enum");
    cmd.arg("--copy").arg("number").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "number_copy"],
        svec!["a", "13", "13"],
        svec!["b", "24", "24"],
        svec!["c", "72", "72"],
        svec!["d", "7", "7"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_copy_name() {
    let wrk = Workdir::new("enum");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("enum");
    cmd.arg("--copy").arg("number").arg("-c").arg("chiffre").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "chiffre"],
        svec!["a", "13", "13"],
        svec!["b", "24", "24"],
        svec!["c", "72", "72"],
        svec!["d", "7", "7"],
    ];
    assert_eq!(got, expected);
}
