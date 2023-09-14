use std::process;

use workdir::Workdir;

fn setup(name: &str) -> (Workdir, process::Command) {
    let rows = vec![
        svec!["h1", "h2"],
        svec!["abcdef", "ghijkl"],
        svec!["mnopqr", "stuvwx"],
    ];

    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fmt");
    cmd.arg("in.csv");

    (wrk, cmd)
}

#[test]
fn fmt_delimiter() {
    let (wrk, mut cmd) = setup("fmt_delimiter");
    cmd.args(["--out-delimiter", "\t"]);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
h1\th2
abcdef\tghijkl
mnopqr\tstuvwx";
    assert_eq!(got, expected.to_string());
}

#[test]
fn fmt_weird_delimiter() {
    let (wrk, mut cmd) = setup("fmt_weird_delimiter");
    cmd.args(["--out-delimiter", "h"]);

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
\"h1\"h\"h2\"
abcdefh\"ghijkl\"
mnopqrhstuvwx";
    assert_eq!(got, expected.to_string());
}

#[test]
fn fmt_crlf() {
    let (wrk, mut cmd) = setup("fmt_crlf");
    cmd.arg("--crlf");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
h1,h2\r
abcdef,ghijkl\r
mnopqr,stuvwx";
    assert_eq!(got, expected.to_string());
}

#[test]
fn fmt_quote_always() {
    let (wrk, mut cmd) = setup("fmt_quote_always");
    cmd.arg("--quote-always");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
\"h1\",\"h2\"
\"abcdef\",\"ghijkl\"
\"mnopqr\",\"stuvwx\"";
    assert_eq!(got, expected.to_string());
}
