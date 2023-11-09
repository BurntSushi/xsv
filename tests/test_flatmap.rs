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

#[test]
fn flatmap_replace() {
    let wrk = Workdir::new("flatmap_replace");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "colors"],
            svec!["john", "yellow|red"],
            svec!["mary", "red"],
        ],
    );
    let mut cmd = wrk.command("flatmap");
    cmd.arg("split(colors, '|')")
        .arg("color")
        .args(&["-r", "colors"])
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "color"],
        svec!["john", "yellow"],
        svec!["john", "red"],
        svec!["mary", "red"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn flatmap_filtermap() {
    let wrk = Workdir::new("flatmap_filtermap");
    wrk.create(
        "data.csv",
        vec![
            svec!["name", "age"],
            svec!["John Mayer", "34"],
            svec!["Mary Sue", "45"],
        ],
    );
    let mut cmd = wrk.command("flatmap");
    cmd.arg("if(gte(age, 40), last(split(name, ' ')))")
        .arg("surname")
        .arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["name", "age", "surname"],
        svec!["Mary Sue", "45", "Sue"],
    ];
    assert_eq!(got, expected);
}
