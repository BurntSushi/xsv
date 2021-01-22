use workdir::Workdir;

#[test]
fn foreach() {
    let wrk = Workdir::new("apply");
    wrk.create("data.csv", vec![
        svec!["name"],
        svec!["John"],
        svec!["Mary"],
    ]);
    let mut cmd = wrk.command("foreach");
    cmd.arg("name").arg("echo 'NAME = {}'").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["NAME = John"],
        svec!["NAME = Mary"],
    ];
    assert_eq!(got, expected);
}
