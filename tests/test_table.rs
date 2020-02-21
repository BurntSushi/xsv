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

#[test]
fn table_right_align() {
    let wrk = Workdir::new("table");
    wrk.create("in.csv", data());

    let mut cmd = wrk.command("table");
    cmd.arg("--align");
    cmd.arg("right");
    cmd.arg("in.csv");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, concat!(
"     h1   h2  h3\n",
"abcdefg    a  a\n",
"      a  abc  z",
    ));
}

#[test]
fn table_center_align() {
    let wrk = Workdir::new("table");
    wrk.create("in.csv", data());

    let mut cmd = wrk.command("table");
    cmd.arg("-a");
    cmd.arg("center");
    cmd.arg("in.csv");

    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(&*got, concat!(
"  h1     h2   h3\n",
"abcdefg   a   a\n",
"   a     abc  z",
    ));
}
