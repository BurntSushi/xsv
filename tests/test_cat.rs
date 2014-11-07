use {assert_csv_eq, to_strings};
use workdir::Workdir;

#[test]
fn abc() {
    let wrk = Workdir::new("abc");
    let rows = vec![
        vec!["name", "value"],
        vec!["andrew", "human"],
        vec!["cauchy", "cat"],
    ];
    wrk.create("in.csv", rows.clone());
    let got = wrk.read("in.csv");

    assert_csv_eq(rows, got);
}

#[test]
fn xyz() {
    let wrk = Workdir::new("xyz");
    let rows = vec![
        vec!["andrew", "human"],
        vec!["cauchy", "cat"],
    ];
    wrk.create("in.csv", rows.clone());

    let mut cmd = wrk.command("cat");
    let status = cmd.arg("rows").arg("-n")
                    .arg(wrk.path("in.csv")).arg(wrk.path("in.csv"))
                    .arg("-o").arg(wrk.path("out.csv"))
                    .status().unwrap();
    assert!(status.success());
    let got = wrk.read("out.csv");

    let mut expected = to_strings(rows.clone());
    expected.extend(to_strings(rows).into_iter());
    assert_csv_eq(expected, got);
}
