use crate::workdir::Workdir;

macro_rules! select_test {
    ($name:ident, $select:expr, $select_no_headers:expr,
     $expected_headers:expr, $expected_rows:expr) => (
        mod $name {
            use crate::workdir::Workdir;
            use super::data;

            #[test]
            fn headers() {
                let wrk = Workdir::new(stringify!($name));
                wrk.create("data.csv", data(true));
                let mut cmd = wrk.command("select");
                cmd.arg("--").arg($select).arg("data.csv");
                let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);

                let expected = vec![
                    $expected_headers.iter()
                                     .map(|s| s.to_string())
                                     .collect::<Vec<String>>(),
                    $expected_rows.iter()
                                  .map(|s| s.to_string())
                                  .collect::<Vec<String>>(),
                ];
                assert_eq!(got, expected);
            }

            #[test]
            fn no_headers() {
                let wrk = Workdir::new(stringify!($name));
                wrk.create("data.csv", data(false));
                let mut cmd = wrk.command("select");
                cmd.arg("--no-headers")
                   .arg("--").arg($select_no_headers).arg("data.csv");
                let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);

                let expected = vec![
                    $expected_rows.iter()
                                  .map(|s| s.to_string())
                                  .collect::<Vec<String>>(),
                ];
                assert_eq!(got, expected);
            }
        }
    );
}

macro_rules! select_test_err {
    ($name:ident, $select:expr) => (
        #[test]
        fn $name() {
            let wrk = Workdir::new(stringify!($name));
            wrk.create("data.csv", data(true));
            let mut cmd = wrk.command("select");
            cmd.arg($select).arg("data.csv");
            wrk.assert_err(&mut cmd);
        }
    );
}

fn header_row() -> Vec<String> { svec!["h1", "h2", "h[]3", "h4", "h1"] }

fn data(headers: bool) -> Vec<Vec<String>> {
    let mut rows = vec![
        svec!["a", "b", "c", "d", "e"],
    ];
    if headers { rows.insert(0, header_row()) }
    rows
}

select_test!(select_simple, "h1", "1", ["h1"], ["a"]);
select_test!(select_simple_idx, "h1[0]", "1", ["h1"], ["a"]);
select_test!(select_simple_idx_2, "h1[1]", "5", ["h1"], ["e"]);

select_test!(select_quoted, r#""h[]3""#, "3", ["h[]3"], ["c"]);
select_test!(select_quoted_idx, r#""h[]3"[0]"#, "3", ["h[]3"], ["c"]);

select_test!(select_range, "h1-h4", "1-4",
             ["h1", "h2", "h[]3", "h4"], ["a", "b", "c", "d"]);

select_test!(select_range_multi, r#"h1-h2,"h[]3"-h4"#, "1-2,3-4",
             ["h1", "h2", "h[]3", "h4"], ["a", "b", "c", "d"]);
select_test!(select_range_multi_idx, r#"h1-h2,"h[]3"[0]-h4"#, "1-2,3-4",
             ["h1", "h2", "h[]3", "h4"], ["a", "b", "c", "d"]);

select_test!(select_reverse, "h1[1]-h1[0]", "5-1",
             ["h1", "h4", "h[]3", "h2", "h1"], ["e", "d", "c", "b", "a"]);

select_test!(select_not, r#"!"h[]3"[0]"#, "!3",
             ["h1", "h2", "h4", "h1"], ["a", "b", "d", "e"]);
select_test!(select_not_range, "!h1[1]-h2", "!5-2", ["h1"], ["a"]);

select_test!(select_duplicate, "h1,h1", "1,1", ["h1", "h1"], ["a", "a"]);
select_test!(select_duplicate_range, "h1-h2,h1-h2", "1-2,1-2",
             ["h1", "h2", "h1", "h2"], ["a", "b", "a", "b"]);
select_test!(select_duplicate_range_reverse, "h1-h2,h2-h1", "1-2,2-1",
             ["h1", "h2", "h2", "h1"], ["a", "b", "b", "a"]);

select_test!(select_range_no_end, "h4-", "4-", ["h4", "h1"], ["d", "e"]);
select_test!(select_range_no_start, "-h2", "-2", ["h1", "h2"], ["a", "b"]);
select_test!(select_range_no_end_cat, "h4-,h1", "4-,1",
             ["h4", "h1", "h1"], ["d", "e", "a"]);
select_test!(select_range_no_start_cat, "-h2,h1[1]", "-2,5",
             ["h1", "h2", "h1"], ["a", "b", "e"]);

select_test_err!(select_err_unknown_header, "dne");
select_test_err!(select_err_oob_low, "0");
select_test_err!(select_err_oob_high, "6");
select_test_err!(select_err_idx_as_name, "1[0]");
select_test_err!(select_err_idx_oob_high, "h1[2]");
select_test_err!(select_err_idx_not_int, "h1[2.0]");
select_test_err!(select_err_idx_not_int_2, "h1[a]");
select_test_err!(select_err_unclosed_quote, r#""h1"#);
select_test_err!(select_err_unclosed_bracket, r#""h1"[1"#);
select_test_err!(select_err_expected_end_of_field, "a-b-");
