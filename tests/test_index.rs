use std::io::fs;

use workdir::Workdir;

#[test]
fn index_outdated() {
    let wrk = Workdir::new("index_outdated");
    wrk.create_indexed("in.csv", vec![svec![""]]);

    let s = fs::stat(&wrk.path("in.csv.idx")).unwrap();
    fs::change_file_times(&wrk.path("in.csv"),
                          s.accessed + 10000, s.modified + 10000).unwrap();

    let mut cmd = wrk.command("count");
    cmd.arg("--no-headers").arg("in.csv");
    wrk.assert_err(&cmd);
}
