use std::process;

use workdir::Workdir;

fn setup(name: &str) -> (Workdir, process::Command) {
    let rows1 = vec![svec!["h1", "h2"], svec!["a", "b"]];
    let rows2 = vec![svec!["h2", "h3"], svec!["y", "z"]];

    let wrk = Workdir::new(name);
    wrk.create("in1.csv", rows1);
    wrk.create("in2.csv", rows2);

    let mut cmd = wrk.command("headers");
    cmd.arg("in1.csv");

    (wrk, cmd)
}

#[test]
fn headers_basic() {
    let (wrk, mut cmd) = setup("headers_basic");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
1   h1
2   h2";
    assert_eq!(got, expected.to_string());
}

#[test]
fn headers_just_names() {
    let (wrk, mut cmd) = setup("headers_just_names");
    cmd.arg("--just-names");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
h1
h2";
    assert_eq!(got, expected.to_string());
}

#[test]
fn headers_multiple() {
    let (wrk, mut cmd) = setup("headers_multiple");
    cmd.arg("in2.csv");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
h1
h2
h2
h3";
    assert_eq!(got, expected.to_string());
}

#[test]
fn headers_intersect() {
    let (wrk, mut cmd) = setup("headers_intersect");
    cmd.arg("in2.csv").arg("--intersect");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
h1
h2
h3";
    assert_eq!(got, expected.to_string());
}
