use std::borrow::ToOwned;
use std::process;

use crate::workdir::Workdir;

macro_rules! slice_tests {
    ($name:ident, $start:expr, $end:expr, $expected:expr) => (
        mod $name {
            use super::test_slice;

            #[test]
            fn headers_no_index() {
                let name = concat!(stringify!($name), "headers_no_index");
                test_slice(name, $start, $end, $expected, true, false, false);
            }

            #[test]
            fn no_headers_no_index() {
                let name = concat!(stringify!($name), "no_headers_no_index");
                test_slice(name, $start, $end, $expected, false, false, false);
            }

            #[test]
            fn headers_index() {
                let name = concat!(stringify!($name), "headers_index");
                test_slice(name, $start, $end, $expected, true, true, false);
            }

            #[test]
            fn no_headers_index() {
                let name = concat!(stringify!($name), "no_headers_index");
                test_slice(name, $start, $end, $expected, false, true, false);
            }

            #[test]
            fn headers_no_index_len() {
                let name = concat!(stringify!($name), "headers_no_index_len");
                test_slice(name, $start, $end, $expected, true, false, true);
            }

            #[test]
            fn no_headers_no_index_len() {
                let name = concat!(stringify!($name),
                                   "no_headers_no_index_len");
                test_slice(name, $start, $end, $expected, false, false, true);
            }

            #[test]
            fn headers_index_len() {
                let name = concat!(stringify!($name), "headers_index_len");
                test_slice(name, $start, $end, $expected, true, true, true);
            }

            #[test]
            fn no_headers_index_len() {
                let name = concat!(stringify!($name), "no_headers_index_len");
                test_slice(name, $start, $end, $expected, false, true, true);
            }
        }
    );
}

fn setup(name: &str, headers: bool, use_index: bool)
        -> (Workdir, process::Command) {
    let wrk = Workdir::new(name);
    let mut data = vec![
        svec!["a"], svec!["b"], svec!["c"], svec!["d"], svec!["e"]
    ];
    if headers { data.insert(0, svec!["header"]); }
    if use_index {
        wrk.create_indexed("in.csv", data);
    } else {
        wrk.create("in.csv", data);
    }

    let mut cmd = wrk.command("slice");
    cmd.arg("in.csv");

    (wrk, cmd)
}

fn test_slice(name: &str, start: Option<usize>, end: Option<usize>,
              expected: &[&str], headers: bool,
              use_index: bool, as_len: bool) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    if let Some(start) = start {
        cmd.arg("--start").arg(&start.to_string());
    }
    if let Some(end) = end {
        if as_len {
            let start = start.unwrap_or(0);
            cmd.arg("--len").arg(&(end - start).to_string());
        } else {
            cmd.arg("--end").arg(&end.to_string());
        }
    }
    if !headers {
        cmd.arg("--no-headers");
    }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let mut expected = expected.iter()
                               .map(|&s| vec![s.to_owned()])
                               .collect::<Vec<Vec<String>>>();
    if headers { expected.insert(0, svec!["header"]); }
    assert_eq!(got, expected);
}

fn test_index(name: &str, idx: usize, expected: &str,
              headers: bool, use_index: bool) {
    let (wrk, mut cmd) = setup(name, headers, use_index);
    cmd.arg("--index").arg(&idx.to_string());
    if !headers {
        cmd.arg("--no-headers");
    }

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let mut expected = vec![vec![expected.to_owned()]];
    if headers { expected.insert(0, svec!["header"]); }
    assert_eq!(got, expected);
}

slice_tests!(slice_simple, Some(0), Some(1), &["a"]);
slice_tests!(slice_simple_2, Some(1), Some(3), &["b", "c"]);
slice_tests!(slice_no_start, None, Some(1), &["a"]);
slice_tests!(slice_no_end, Some(3), None, &["d", "e"]);
slice_tests!(slice_all, None, None, &["a", "b", "c", "d", "e"]);

#[test]
fn slice_index() {
    test_index("slice_index", 1, "b", true, false);
}
#[test]
fn slice_index_no_headers() {
    test_index("slice_index_no_headers", 1, "b", false, false);
}
#[test]
fn slice_index_withindex() {
    test_index("slice_index_withindex", 1, "b", true, true);
}
#[test]
fn slice_index_no_headers_withindex() {
    test_index("slice_index_no_headers_withindex", 1, "b", false, true);
}
