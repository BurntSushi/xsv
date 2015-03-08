use workdir::Workdir;

fn data() -> Vec<Vec<String>> {
    vec![
        svec!["h1", "h2", "h3"],
        svec!["abcdefg", "a", "a"],
        svec!["a", "abc", "z"],
    ]
}

#[test]
fn table() {
    let wrk = Workdir::new("table");
    wrk.create("in.csv", data());

    let mut cmd = wrk.command("table");
    cmd.arg("in.csv");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, "\
h1       h2   h3
abcdefg  a    a
a        abc  z\
")
}
