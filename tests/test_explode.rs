use workdir::Workdir;

#[test]
fn explode() {
    let wrk = Workdir::new("explode");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["Mary", "yellow"],
            svec!["John", "blue|orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("explode");
    cmd.arg("colors").arg("|").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "colors"],
        svec!["Mary", "yellow"],
        svec!["John", "blue"],
        svec!["John", "orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn explode_rename() {
    let wrk = Workdir::new("explode");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["Mary", "yellow"],
            svec!["John", "blue|orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("explode");
    cmd.arg("colors")
        .args(["--rename", "color"])
        .arg("|")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "color"],
        svec!["Mary", "yellow"],
        svec!["John", "blue"],
        svec!["John", "orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}

#[test]
fn explode_no_headers() {
    let wrk = Workdir::new("explode");
    wrk.create(
        "data.csv",
        vec![
            svec!["Mary", "yellow"],
            svec!["John", "blue|orange"],
            svec!["Jack", ""],
        ],
    );
    let mut cmd = wrk.command("explode");
    cmd.arg("2").arg("|").arg("--no-headers").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["Mary", "yellow"],
        svec!["John", "blue"],
        svec!["John", "orange"],
        svec!["Jack", ""],
    ];
    assert_eq!(got, expected);
}
