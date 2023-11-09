use workdir::Workdir;

#[test]
fn flatmap() {
    let wrk = Workdir::new("flatmap");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["john", "yellow|red"],
            svec!["mary", "red"],
            svec!["jordan", ""],
        ],
    );
    let mut cmd = wrk.command("flatmap");
    cmd.arg("split(colors, '|') | compact")
        .arg("color")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "colors", "color"],
        svec!["john", "yellow|red", "yellow"],
        svec!["john", "yellow|red", "red"],
        svec!["mary", "red", "red"],
    ];
    assert_eq!(got, expected);
}

// TODO: replace, filtermap example
