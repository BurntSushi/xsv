use workdir::Workdir;

#[test]
fn py_map() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("inc").arg("int(number) + 1").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "inc"],
        svec!["a", "13", "14"],
        svec!["b", "24", "25"],
        svec!["c", "72", "73"],
        svec!["d", "7", "8"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_builtins() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("sum").arg("sum([int(number), 2, 23])").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "sum"],
        svec!["a", "13", "38"],
        svec!["b", "24", "49"],
        svec!["c", "72", "97"],
        svec!["d", "7", "32"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_math() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("div").arg("math.floor(int(number) / 2)").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "div"],
        svec!["a", "13", "6"],
        svec!["b", "24", "12"],
        svec!["c", "72", "36"],
        svec!["d", "7", "3"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_row_positional() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("inc").arg("int(row[1]) + 1").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "inc"],
        svec!["a", "13", "14"],
        svec!["b", "24", "25"],
        svec!["c", "72", "73"],
        svec!["d", "7", "8"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_row_by_key() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("inc").arg("int(row['number']) + 1").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "inc"],
        svec!["a", "13", "14"],
        svec!["b", "24", "25"],
        svec!["c", "72", "73"],
        svec!["d", "7", "8"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_row_by_attr() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("inc").arg("int(row.number) + 1").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "inc"],
        svec!["a", "13", "14"],
        svec!["b", "24", "25"],
        svec!["c", "72", "73"],
        svec!["d", "7", "8"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_no_headers() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("int(row[1]) + 1").arg("--no-headers").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "13", "14"],
        svec!["b", "24", "25"],
        svec!["c", "72", "73"],
        svec!["d", "7", "8"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_map_boolean() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("map").arg("test").arg("int(number) > 14").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number", "test"],
        svec!["a", "13", "False"],
        svec!["b", "24", "True"],
        svec!["c", "72", "True"],
        svec!["d", "7", "False"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn py_filter() {
    let wrk = Workdir::new("py");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("py");
    cmd.arg("filter").arg("int(number) > 14").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "number"],
        svec!["b", "24"],
        svec!["c", "72"],
    ];
    assert_eq!(got, expected);
}
