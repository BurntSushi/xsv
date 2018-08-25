use std::process;

use workdir::Workdir;

fn setup(name: &str, script: &str) -> (Workdir, process::Command) {
    let rows = vec![
        svec![ "a", "b"],
        svec!["-1", "2"],
        svec![ "3", "4"],
    ];

    let wrk = Workdir::new(name);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("script");
    cmd.arg("newcolumn");
    cmd.arg("c");
    cmd.arg(script);
    cmd.arg("in.csv");

    (wrk, cmd)
}

#[test]
fn add() {
    let expected = "\
a,b,c
-1,2,1.0
3,4,7.0";

    let (wrk, mut cmd) = setup("add", r#"a + b"#);
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(got, expected.to_string());

    let (wrk, mut cmd) = setup("add", r#"col.a + col['b']"#);
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(got, expected.to_string());

    let (wrk, mut cmd) = setup("add", r#"col[1] + col[2]"#);
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(got, expected.to_string());
}

#[test]
fn add_exec() {
    let expected = "\
a,b,c
-1,2,9
3,4,12";

    let (wrk, mut cmd) = setup("add_exec", r#"tot = (tot or 10) + tonumber(a); return tot"#);
    cmd.arg("--exec");
    let got: String = wrk.stdout(&mut cmd);
    assert_eq!(got, expected.to_string());
}

#[test]
fn add_no_headers() {
    let (wrk, mut cmd) = setup("add_no_headers", r#"col[1] .. col[2]"#);
    cmd.arg("--no-headers");

    let got: String = wrk.stdout(&mut cmd);
    let expected = "\
a,b,ab
-1,2,-12
3,4,34";
    assert_eq!(got, expected.to_string());
}
