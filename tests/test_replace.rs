use workdir::Workdir;

#[test]
fn replace() {
    let wrk = Workdir::new("replace");
    wrk.create(
        "data.csv",
        vec![
            svec!["identifier", "color"],
            svec!["164.0", "yellow"],
            svec!["165.0", "yellow"],
            svec!["166.0", "yellow"],
            svec!["167.0", "yellow.0"],
        ],
    );
    let mut cmd = wrk.command("replace");
    cmd.arg("\\.0$").arg("").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["identifier", "color"],
        svec!["164", "yellow"],
        svec!["165", "yellow"],
        svec!["166", "yellow"],
        svec!["167", "yellow"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn replace_no_headers() {
    let wrk = Workdir::new("replace");
    wrk.create(
        "data.csv",
        vec![
            svec!["164.0", "yellow"],
            svec!["165.0", "yellow"],
            svec!["166.0", "yellow"],
            svec!["167.0", "yellow.0"],
        ],
    );
    let mut cmd = wrk.command("replace");
    cmd.arg("\\.0$")
        .arg("")
        .arg("--no-headers")
        .arg("--select")
        .arg("1")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["164", "yellow"],
        svec!["165", "yellow"],
        svec!["166", "yellow"],
        svec!["167", "yellow.0"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn replace_select() {
    let wrk = Workdir::new("replace");
    wrk.create(
        "data.csv",
        vec![
            svec!["identifier", "color"],
            svec!["164.0", "yellow"],
            svec!["165.0", "yellow"],
            svec!["166.0", "yellow"],
            svec!["167.0", "yellow.0"],
        ],
    );
    let mut cmd = wrk.command("replace");
    cmd.arg("\\.0$")
        .arg("")
        .arg("--select")
        .arg("identifier")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["identifier", "color"],
        svec!["164", "yellow"],
        svec!["165", "yellow"],
        svec!["166", "yellow"],
        svec!["167", "yellow.0"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn replace_groups() {
    let wrk = Workdir::new("replace");
    wrk.create(
        "data.csv",
        vec![
            svec!["identifier", "color"],
            svec!["164.0", "yellow"],
            svec!["165.0", "yellow"],
            svec!["166.0", "yellow"],
            svec!["167.0", "yellow.0"],
        ],
    );
    let mut cmd = wrk.command("replace");
    cmd.arg("\\d+(\\d)\\.0$")
        .arg("$1")
        .arg("--select")
        .arg("identifier")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["identifier", "color"],
        svec!["4", "yellow"],
        svec!["5", "yellow"],
        svec!["6", "yellow"],
        svec!["7", "yellow.0"],
    ];
    assert_eq!(got, expected);
}
