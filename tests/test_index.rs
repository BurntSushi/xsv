use std::fs;

use filetime::{set_file_times, FileTime};

use workdir::Workdir;

#[test]
fn index_outdated() {
    let wrk = Workdir::new("index_outdated");
    wrk.create_indexed("in.csv", vec![svec![""]]);

    let md = fs::metadata(wrk.path("in.csv.idx")).unwrap();
    set_file_times(
        wrk.path("in.csv"),
        future_time(FileTime::from_last_modification_time(&md)),
        future_time(FileTime::from_last_access_time(&md)),
    )
    .unwrap();

    let mut cmd = wrk.command("count");
    cmd.arg("--no-headers").arg("in.csv");
    wrk.assert_err(&mut cmd);
}

fn future_time(ft: FileTime) -> FileTime {
    let secs = ft.seconds_relative_to_1970();
    FileTime::from_seconds_since_1970(secs + 10_000, 0)
}
