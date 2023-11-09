use workdir::Workdir;

#[test]
fn transform() {
    let wrk = Workdir::new("transform");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "2"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)").arg("b").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["a", "b"], svec!["1", "3"], svec!["2", "5"]];
    assert_eq!(got, expected);
}

#[test]
fn transform_rename() {
    let wrk = Workdir::new("transform_rename");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "2"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)")
        .arg("b")
        .args(&["-r", "c"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["a", "c"], svec!["1", "3"], svec!["2", "5"]];
    assert_eq!(got, expected);
}

#[test]
fn transform_errors_panic() {
    let wrk = Workdir::new("transform_errors_panic");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)").arg("b").arg("data.csv");

    wrk.assert_err(&mut cmd);
}

#[test]
fn transform_errors_report() {
    let wrk = Workdir::new("transform_errors_report");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)")
        .arg("b")
        .args(&["-e", "report"])
        .args(&["-E", "error"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "error"],
        svec!["1", "", "error when calling function \"add\": cannot safely cast from type \"string\" to type \"number\""],
        svec!["2", "5", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn transform_errors_ignore() {
    let wrk = Workdir::new("transform_errors_ignore");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)")
        .arg("b")
        .args(&["-e", "ignore"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["a", "b"], svec!["1", ""], svec!["2", "5"]];
    assert_eq!(got, expected);
}

#[test]
fn transform_errors_log() {
    let wrk = Workdir::new("transform_errors_log");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("transform");
    cmd.arg("add(a, b)")
        .arg("b")
        .args(&["-e", "log"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["a", "b",], svec!["1", "",], svec!["2", "5",]];
    assert_eq!(got, expected);
}

// TODO: test implicit
