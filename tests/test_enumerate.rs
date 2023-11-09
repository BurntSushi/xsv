use workdir::Workdir;

#[test]
fn enumerate() {
    let wrk = Workdir::new("enumerate");
    wrk.create(
        "data.csv",
        vec![svec!["a"], svec!["1",], svec!["2"], svec!["3"]],
    );
    let mut cmd = wrk.command("enum");
    cmd.arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["index", "a"],
        svec!["0", "1"],
        svec!["1", "2"],
        svec!["2", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_no_headers() {
    let wrk = Workdir::new("enumerate_no_headers");
    wrk.create("data.csv", vec![svec!["1"], svec!["2"], svec!["3"]]);
    let mut cmd = wrk.command("enum");
    cmd.arg("data.csv").arg("-n");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["0", "1"], svec!["1", "2"], svec!["2", "3"]];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_start() {
    let wrk = Workdir::new("enumerate_start");
    wrk.create(
        "data.csv",
        vec![svec!["a"], svec!["1",], svec!["2"], svec!["3"]],
    );
    let mut cmd = wrk.command("enum");
    cmd.arg("data.csv").args(&["-S", "10"]);

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["index", "a"],
        svec!["10", "1"],
        svec!["11", "2"],
        svec!["12", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn enumerate_column_name() {
    let wrk = Workdir::new("enumerate_column_name");
    wrk.create(
        "data.csv",
        vec![svec!["a"], svec!["1",], svec!["2"], svec!["3"]],
    );
    let mut cmd = wrk.command("enum");
    cmd.arg("data.csv").args(&["-c", "i"]);

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["i", "a"],
        svec!["0", "1"],
        svec!["1", "2"],
        svec!["2", "3"],
    ];
    assert_eq!(got, expected);
}
