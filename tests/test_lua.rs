use workdir::Workdir;

#[test]
fn lua_map() {
    let wrk = Workdir::new("lua");
    wrk.create("data.csv", vec![
        svec!["letter", "number"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("lua");
    cmd.arg("map").arg("inc").arg("number + 1").arg("data.csv");

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
fn lua_map_no_headers() {
    let wrk = Workdir::new("lua");
    wrk.create("data.csv", vec![
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("lua");
    cmd.arg("map").arg("col[2] + 1").arg("--no-headers").arg("data.csv");

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
fn lua_map_exec() {
    let wrk = Workdir::new("lua");
    wrk.create("data.csv", vec![
        svec!["letter", "x"],
        svec!["a", "13"],
        svec!["b", "24"],
        svec!["c", "72"],
        svec!["d", "7"],
    ]);
    let mut cmd = wrk.command("lua");
    cmd.arg("map").arg("running_total").arg("-x").arg("tot = (tot or 0) + x; return tot").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["letter", "x", "running_total"],
        svec!["a", "13", "13"],
        svec!["b", "24", "37"],
        svec!["c", "72", "109"],
        svec!["d", "7", "116"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn lua_map_no_globals() {
    let wrk = Workdir::new("lua");
    wrk.create("data.csv", vec![
        svec!["y", "x"],
        svec!["1", "13"],
        svec!["2", "24"],
        svec!["3", "72"],
        svec!["4", "7"],
    ]);
    let mut cmd = wrk.command("lua");
    cmd.arg("map").arg("z").arg("-g").arg("(x or col[1]) + 1").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["y", "x", "z"],
        svec!["1", "13", "2"],
        svec!["2", "24", "3"],
        svec!["3", "72", "4"],
        svec!["4", "7", "5"],
    ];
    assert_eq!(got, expected);
}
