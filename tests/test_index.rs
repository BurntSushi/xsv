use std::fs;

use filetime::{FileTime, set_file_times};

use crate::workdir::Workdir;

#[test]
fn index_outdated() {
    let wrk = Workdir::new("index_outdated");
    wrk.create_indexed("in.csv", vec![svec![""]]);

    let md = fs::metadata(&wrk.path("in.csv.idx")).unwrap();
    set_file_times(
        &wrk.path("in.csv"),
        future_time(FileTime::from_last_modification_time(&md)),
        future_time(FileTime::from_last_access_time(&md)),
    ).unwrap();

    let mut cmd = wrk.command("count");
    cmd.arg("--no-headers").arg("in.csv");
    wrk.assert_err(&mut cmd);
}

fn future_time(ft: FileTime) -> FileTime {
    let secs = ft.unix_seconds();
    FileTime::from_unix_time(secs + 10_000, 0)
}
