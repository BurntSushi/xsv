use workdir::Workdir;

#[test]
fn pseudo() {
    let wrk = Workdir::new("pseudo");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["Mary", "yellow"],
            svec!["John", "blue"],
            svec!["Mary", "purple"],
            svec!["Sue", "orange"],
            svec!["John", "magenta"],
            svec!["Mary", "cyan"],
        ],
    );
    let mut cmd = wrk.command("pseudo");
    cmd.arg("name").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "colors"],
        svec!["0", "yellow"],
        svec!["1", "blue"],
        svec!["0", "purple"],
        svec!["2", "orange"],
        svec!["1", "magenta"],
        svec!["0", "cyan"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn pseudo_no_headers() {
    let wrk = Workdir::new("pseudo");
    wrk.create(
        "data.csv",
        vec![
            svec!["Mary", "yellow"],
            svec!["John", "blue"],
            svec!["Mary", "purple"],
            svec!["Sue", "orange"],
            svec!["John", "magenta"],
            svec!["Mary", "cyan"],
        ],
    );
    let mut cmd = wrk.command("pseudo");
    cmd.arg("1").arg("--no-headers").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["0", "yellow"],
        svec!["1", "blue"],
        svec!["0", "purple"],
        svec!["2", "orange"],
        svec!["1", "magenta"],
        svec!["0", "cyan"],
    ];
    assert_eq!(got, expected);
}
