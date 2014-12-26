use std::borrow::ToOwned;

use workdir::Workdir;

macro_rules! split_eq {
    ($wrk:expr, $path:expr, $expected:expr) => (
        assert_eq!($wrk.from_str::<String>(&$wrk.path($path)),
                   $expected.to_owned());
    );
}

fn data(headers: bool) -> Vec<Vec<String>> {
    let mut rows = vec![
        svec!["a", "b"], svec!["c", "d"],
        svec!["e", "f"], svec!["g", "h"],
        svec!["i", "j"], svec!["k", "l"],
    ];
    if headers { rows.insert(0, svec!["h1", "h2"]); }
    rows
}

#[test]
fn split_zero() {
    let wrk = Workdir::new("split_zero");
    wrk.create("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "0"]).arg(wrk.path(".")).arg("in.csv");
    wrk.assert_err(&cmd);
}

#[test]
fn split() {
    let wrk = Workdir::new("split");
    wrk.create("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "2"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
c,d
");
    split_eq!(wrk, "2.csv", "\
h1,h2
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
k,l
");
}

#[test]
fn split_idx() {
    let wrk = Workdir::new("split_idx");
    wrk.create_indexed("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "2"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
c,d
");
    split_eq!(wrk, "2.csv", "\
h1,h2
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
k,l
");
}

#[test]
fn split_no_headers() {
    let wrk = Workdir::new("split_no_headers");
    wrk.create("in.csv", data(false));

    let mut cmd = wrk.command("split");
    cmd.args(&["--no-headers", "--size", "2"])
       .arg(wrk.path("."))
       .arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
a,b
c,d
");
    split_eq!(wrk, "2.csv", "\
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
i,j
k,l
");
}

#[test]
fn split_no_headers_idx() {
    let wrk = Workdir::new("split_no_headers_idx");
    wrk.create_indexed("in.csv", data(false));

    let mut cmd = wrk.command("split");
    cmd.args(&["--no-headers", "--size", "2"])
       .arg(wrk.path("."))
       .arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
a,b
c,d
");
    split_eq!(wrk, "2.csv", "\
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
i,j
k,l
");
}

#[test]
fn split_one() {
    let wrk = Workdir::new("split_one");
    wrk.create("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "1"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
");
    split_eq!(wrk, "1.csv", "\
h1,h2
c,d
");
    split_eq!(wrk, "2.csv", "\
h1,h2
e,f
");
    split_eq!(wrk, "3.csv", "\
h1,h2
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
");
    split_eq!(wrk, "5.csv", "\
h1,h2
k,l
");
}

#[test]
fn split_one_idx() {
    let wrk = Workdir::new("split_one_idx");
    wrk.create_indexed("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "1"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
");
    split_eq!(wrk, "1.csv", "\
h1,h2
c,d
");
    split_eq!(wrk, "2.csv", "\
h1,h2
e,f
");
    split_eq!(wrk, "3.csv", "\
h1,h2
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
");
    split_eq!(wrk, "5.csv", "\
h1,h2
k,l
");
}

#[test]
fn split_uneven() {
    let wrk = Workdir::new("split_uneven");
    wrk.create("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "4"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
c,d
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
k,l
");
}

#[test]
fn split_uneven_idx() {
    let wrk = Workdir::new("split_uneven_idx");
    wrk.create_indexed("in.csv", data(true));

    let mut cmd = wrk.command("split");
    cmd.args(&["--size", "4"]).arg(wrk.path(".")).arg("in.csv");
    wrk.run(&cmd);

    split_eq!(wrk, "0.csv", "\
h1,h2
a,b
c,d
e,f
g,h
");
    split_eq!(wrk, "4.csv", "\
h1,h2
i,j
k,l
");
}
