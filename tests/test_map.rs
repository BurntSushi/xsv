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
