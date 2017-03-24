use workdir::Workdir;

fn data() -> Vec<Vec<String>> {
    vec![svec!["Time", "C1", "C2"],
         svec!["1480004979", "Val1", "Val2"],
         svec!["1480004989", "Val3", "Val4"]]
}

#[cfg(windows)]
#[test]
fn win_apply_test() {
    // this test is rather unrealisitic, but windows has
    // a fairly limited set of built in, always available tools to test with
    let wrk = Workdir::new("apply");
    wrk.create("sortin.csv", data());

    let mut cmd = wrk.command("apply");
    cmd.arg("sort /r");
    cmd.arg("Time");
    cmd.arg("sortin.csv");

    // the first and second Time values should have traded places
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got,
               "\
Time,C1,C2
1480004989,Val1,Val2
1480004979,Val3,Val4")
}

#[cfg(unix)]
#[test]
fn nix_apply_test() {
    // this test is rather unrealisitic, but windows has
    // a fairly limited set of built in, always available tools to test with
    let wrk = Workdir::new("apply");
    wrk.create("sortin.csv", data());

    let mut cmd = wrk.command("apply");
    cmd.arg("sed -e 's/^/new_prefix_/'");
    cmd.arg("C1");
    cmd.arg("sortin.csv");

    // the first and second Time values should have traded places
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got,
               "\
Time,C1,C2
1480004979,new_prefix_Val1,Val2
1480004989,new_prefix_Val3,Val4")
}
