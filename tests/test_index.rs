use std::fs;

use workdir::Workdir;

#[test]
fn index_outdated() {
    let wrk = Workdir::new("index_outdated");
    wrk.create_indexed("in.csv", vec![svec![""]]);

    let s = fs::metadata(&wrk.path("in.csv.idx")).unwrap();
    let new_access = 10000 + (last_access(&s) * 1000);
    let new_modified = 10000 + (last_modified(&s) * 1000);
    fs::set_file_times(&wrk.path("in.csv"), new_access, new_modified).unwrap();

    let mut cmd = wrk.command("count");
    cmd.arg("--no-headers").arg("in.csv");
    wrk.assert_err(&mut cmd);
}

pub fn last_modified(md: &fs::Metadata) -> u64 {
    use filetime::FileTime;
    FileTime::from_last_modification_time(md).seconds_relative_to_1970()
}

pub fn last_access(md: &fs::Metadata) -> u64 {
    use filetime::FileTime;
    FileTime::from_last_access_time(md).seconds_relative_to_1970()
}
