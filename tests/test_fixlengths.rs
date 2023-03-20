use quickcheck::TestResult;

use workdir::Workdir;
use {qcheck, CsvRecord};

fn trim_trailing_empty(it: &CsvRecord) -> Vec<String> {
    let mut cloned = it.clone().unwrap();
    while cloned.len() > 1 && cloned.last().unwrap().is_empty() {
        cloned.pop();
    }
    cloned
}

#[test]
fn prop_fixlengths_all_maxlen() {
    fn p(rows: Vec<CsvRecord>) -> TestResult {
        let expected_len = match rows.iter().map(|r| trim_trailing_empty(r).len()).max() {
            None => return TestResult::discard(),
            Some(n) => n,
        };

        let wrk = Workdir::new("fixlengths_all_maxlen").flexible(true);
        wrk.create("in.csv", rows);

        let mut cmd = wrk.command("fixlengths");
        cmd.arg("in.csv");

        let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
        let got_len = got.iter().map(|r| r.len()).max().unwrap();
        for r in got.iter() {
            assert_eq!(r.len(), got_len)
        }
        TestResult::from_bool(rassert_eq!(got_len, expected_len))
    }
    qcheck(p as fn(Vec<CsvRecord>) -> TestResult);
}

#[test]
fn fixlengths_all_maxlen_trims() {
    let rows = vec![
        svec!["h1", "h2"],
        svec!["abcdef", "ghijkl", "", ""],
        svec!["mnopqr", "stuvwx", "", ""],
    ];

    let wrk = Workdir::new("fixlengths_all_maxlen_trims").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fixlengths");
    cmd.arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    for r in got.iter() {
        assert_eq!(r.len(), 2)
    }
}

#[test]
fn fixlengths_all_maxlen_trims_at_least_1() {
    let rows = vec![svec![""], svec!["", ""], svec!["", "", ""]];

    let wrk = Workdir::new("fixlengths_all_maxlen_trims_at_least_1").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fixlengths");
    cmd.arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    for r in got.iter() {
        assert_eq!(r.len(), 1)
    }
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
        cmd.arg("in.csv").args(&["-l", &*expected_len.to_string()]);

        let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
        let got_len = got.iter().map(|r| r.len()).max().unwrap();
        for r in got.iter() {
            assert_eq!(r.len(), got_len)
        }
        TestResult::from_bool(rassert_eq!(got_len, expected_len))
    }
    qcheck(p as fn(Vec<CsvRecord>, usize) -> TestResult);
}
