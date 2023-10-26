use workdir::Workdir;

#[test]
fn implode() {
    let wrk = Workdir::new("implode");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["Mary", "yellow"],
            svec!["John", "blue"],
            svec!["John", "orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("implode");
    cmd.arg("colors").arg("|").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "colors"],
        svec!["Mary", "yellow"],
        svec!["John", "blue|orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn implode_rename() {
    let wrk = Workdir::new("implode");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "color"],
            svec!["Mary", "yellow"],
            svec!["John", "blue"],
            svec!["John", "orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("implode");
    cmd.arg("color")
        .args(["--rename", "colors"])
        .arg("|")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "colors"],
        svec!["Mary", "yellow"],
        svec!["John", "blue|orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn implode_no_headers() {
    let wrk = Workdir::new("implode");
    wrk.create(
        "data.csv",
        vec![
            svec!["Mary", "yellow"],
            svec!["John", "blue"],
            svec!["John", "orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("implode");
    cmd.arg("2").arg("|").arg("--no-headers").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["Mary", "yellow"],
        svec!["John", "blue|orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}
