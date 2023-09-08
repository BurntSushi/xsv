use workdir::Workdir;

#[test]
fn shuffle() {
    let wrk = Workdir::new("shuffle");
    wrk.create(
        "data.csv",
        vec![
            svec!["number"],
            svec!["1"],
            svec!["2"],
            svec!["3"],
            svec!["4"],
        ],
    );
    let mut cmd = wrk.command("shuffle");
    cmd.arg("data.csv").args(&["--seed", "123"]);

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["number"],
        svec!["2"],
        svec!["3"],
        svec!["1"],
        svec!["4"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn shuffle_in_memory() {
    let wrk = Workdir::new("shuffle");
    wrk.create(
        "data.csv",
        vec![
            svec!["number"],
            svec!["1"],
            svec!["2"],
            svec!["3"],
            svec!["4"],
        ],
    );
    let mut cmd = wrk.command("shuffle");
    cmd.arg("data.csv")
        .args(&["--seed", "123"])
        .arg("--in-memory");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["number"],
        svec!["2"],
        svec!["3"],
        svec!["1"],
        svec!["4"],
    ];
    assert_eq!(got, expected);
}
// no headers, in memory matrix

#[test]
fn shuffle_no_headers() {
    let wrk = Workdir::new("shuffle");
    wrk.create(
        "data.csv",
        vec![svec!["1"], svec!["2"], svec!["3"], svec!["4"]],
    );
    let mut cmd = wrk.command("shuffle");
    cmd.arg("data.csv")
        .args(&["--seed", "123"])
        .arg("--no-headers");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["2"], svec!["3"], svec!["1"], svec!["4"]];
    assert_eq!(got, expected);
}

#[test]
fn shuffle_in_memory_no_headers() {
    let wrk = Workdir::new("shuffle");
    wrk.create(
        "data.csv",
        vec![svec!["1"], svec!["2"], svec!["3"], svec!["4"]],
    );
    let mut cmd = wrk.command("shuffle");
    cmd.arg("data.csv")
        .args(&["--seed", "123"])
        .arg("--no-headers")
        .arg("--in-memory");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![svec!["2"], svec!["3"], svec!["1"], svec!["4"]];
    assert_eq!(got, expected);
}
