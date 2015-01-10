use quickcheck::TestResult;

use {CsvRecord, qcheck};
use workdir::Workdir;

#[test]
fn prop_fixlengths_all_maxlen() {
    fn p(rows: Vec<CsvRecord>) -> TestResult {
        let expected_len =
            match rows.iter().map(|r| r.as_slice().len()).max() {
                None => return TestResult::discard(),
                Some(n) => n,
            };

        let wrk = Workdir::new("fixlengths_all_maxlen").flexible(true);
        wrk.create("in.csv", rows);

        let mut cmd = wrk.command("fixlengths");
        cmd.arg("in.csv");

        let got: Vec<CsvRecord> = wrk.read_stdout(&cmd);
        let got_len = got.iter().map(|r| r.as_slice().len()).max().unwrap();
        for r in got.iter() { assert_eq!(r.as_slice().len(), got_len) }
        TestResult::from_bool(rassert_eq!(got_len, expected_len))
    }
    qcheck(p as fn(Vec<CsvRecord>) -> TestResult);
}

#[test]
fn prop_fixlengths_explicit_len() {
    fn p(rows: Vec<CsvRecord>, expected_len: usize) -> TestResult {
        if expected_len == 0 || rows.is_empty() {
            return TestResult::discard();
        }

        let wrk = Workdir::new("fixlengths_explicit_len").flexible(true);
        wrk.create("in.csv", rows);

        let mut cmd = wrk.command("fixlengths");
        cmd.arg("in.csv").args(&["-l", expected_len.to_string().as_slice()]);

        let got: Vec<CsvRecord> = wrk.read_stdout(&cmd);
        let got_len = got.iter().map(|r| r.as_slice().len()).max().unwrap();
        for r in got.iter() { assert_eq!(r.as_slice().len(), got_len) }
        TestResult::from_bool(rassert_eq!(got_len, expected_len))
    }
    qcheck(p as fn(Vec<CsvRecord>, usize) -> TestResult);
}
