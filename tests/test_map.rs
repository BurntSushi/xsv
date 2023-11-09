use workdir::Workdir;

#[test]
fn map() {
    let wrk = Workdir::new("map");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "2"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("map");
    cmd.arg("add(a, b)").arg("c").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "c"],
        svec!["1", "2", "3"],
        svec!["2", "3", "5"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn map_errors_panic() {
    let wrk = Workdir::new("map_errors_panic");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("map");
    cmd.arg("add(a, b)").arg("c").arg("data.csv");

    wrk.assert_err(&mut cmd);
}

#[test]
fn map_errors_report() {
    let wrk = Workdir::new("map_errors_report");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("map");
    cmd.arg("add(a, b)")
        .arg("c")
        .args(&["-e", "report"])
        .args(&["-E", "error"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "c", "error"],
        svec!["1", "test", "", "error when calling function \"add\": cannot safely cast from type \"string\" to type \"number\""],
        svec!["2", "3", "5", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn map_errors_ignore() {
    let wrk = Workdir::new("map_errors_ignore");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("map");
    cmd.arg("add(a, b)")
        .arg("c")
        .args(&["-e", "ignore"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "c"],
        svec!["1", "test", ""],
        svec!["2", "3", "5"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn map_errors_log() {
    let wrk = Workdir::new("map_errors_log");
    wrk.create(
        "data.csv",
        vec![svec!["a", "b"], svec!["1", "test"], svec!["2", "3"]],
    );
    let mut cmd = wrk.command("map");
    cmd.arg("add(a, b)")
        .arg("c")
        .args(&["-e", "log"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "c"],
        svec!["1", "test", ""],
        svec!["2", "3", "5"],
    ];
    assert_eq!(got, expected);
}
