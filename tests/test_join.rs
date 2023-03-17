use workdir::Workdir;

// This macro takes *two* identifiers: one for the test with headers
// and another for the test without headers.
macro_rules! join_test {
    ($name:ident, $fun:expr) => (
        mod $name {
            use std::process;

            use workdir::Workdir;
            use super::{make_rows, setup};

            #[test]
            fn headers() {
                let wrk = setup(stringify!($name), true);
                let mut cmd = wrk.command("join");
                cmd.args(&["city", "cities.csv", "city", "places.csv"]);
                $fun(wrk, cmd, true);
            }

            #[test]
            fn no_headers() {
                let n = stringify!(concat_idents!($name, _no_headers));
                let wrk = setup(n, false);
                let mut cmd = wrk.command("join");
                cmd.arg("--no-headers");
                cmd.args(&["1", "cities.csv", "1", "places.csv"]);
                $fun(wrk, cmd, false);
            }
        }
    );
}

fn setup(name: &str, headers: bool) -> Workdir {
    let mut cities = vec![
        svec!["Boston", "MA"],
        svec!["New York", "NY"],
        svec!["San Francisco", "CA"],
        svec!["Buffalo", "NY"],
    ];
    let mut places = vec![
        svec!["Boston", "Logan Airport"],
        svec!["Boston", "Boston Garden"],
        svec!["Buffalo", "Ralph Wilson Stadium"],
        svec!["Orlando", "Disney World"],
    ];
    if headers { cities.insert(0, svec!["city", "state"]); }
    if headers { places.insert(0, svec!["city", "place"]); }

    let wrk = Workdir::new(name);
    wrk.create("cities.csv", cities);
    wrk.create("places.csv", places);
    wrk
}

fn make_rows(headers: bool, rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    let mut all_rows = vec![];
    if headers {
        all_rows.push(svec!["city", "state", "city", "place"]);
    }
    all_rows.extend(rows.into_iter());
    all_rows
}

join_test!(join_inner,
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_left,
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--left");
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_right,
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--right");
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
});

join_test!(join_outer_full,
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--full");
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
});

#[test]
fn join_inner_issue11() {
    let a = vec![
        svec!["1", "2"],
        svec!["3", "4"],
        svec!["5", "6"],
    ];
    let b = vec![
        svec!["2", "1"],
        svec!["4", "3"],
        svec!["6", "5"],
    ];

    let wrk = Workdir::new("join_inner_issue11");
    wrk.create("a.csv", a);
    wrk.create("b.csv", b);

    let mut cmd = wrk.command("join");
    cmd.args(&["1,2", "a.csv", "2,1", "b.csv"]);

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["1", "2", "2", "1"],
        svec!["3", "4", "4", "3"],
        svec!["5", "6", "6", "5"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn join_cross() {
    let wrk = Workdir::new("join_cross");
    wrk.create("letters.csv",
               vec![svec!["h1", "h2"], svec!["a", "b"], svec!["c", "d"]]);
    wrk.create("numbers.csv",
               vec![svec!["h3", "h4"], svec!["1", "2"], svec!["3", "4"]]);

    let mut cmd = wrk.command("join");
    cmd.arg("--cross")
       .args(&["", "letters.csv", "", "numbers.csv"]);
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["h1", "h2", "h3", "h4"],
        svec!["a", "b", "1", "2"],
        svec!["a", "b", "3", "4"],
        svec!["c", "d", "1", "2"],
        svec!["c", "d", "3", "4"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn join_cross_no_headers() {
    let wrk = Workdir::new("join_cross_no_headers");
    wrk.create("letters.csv", vec![svec!["a", "b"], svec!["c", "d"]]);
    wrk.create("numbers.csv", vec![svec!["1", "2"], svec!["3", "4"]]);

    let mut cmd = wrk.command("join");
    cmd.arg("--cross").arg("--no-headers")
       .args(&["", "letters.csv", "", "numbers.csv"]);
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["a", "b", "1", "2"],
        svec!["a", "b", "3", "4"],
        svec!["c", "d", "1", "2"],
        svec!["c", "d", "3", "4"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn join_prefix() {
    let wrk = Workdir::new("join_prefix");
    wrk.create("fruits.csv",
               vec![svec!["idx", "fruit"], svec!["1", "apple"], svec!["2", "mango"]]);
    wrk.create("colors.csv",
               vec![svec!["idx", "color"], svec!["1", "blue"], svec!["2", "purple"]]);

    let mut cmd = wrk.command("join");
    cmd.arg("--prefix-left")
       .arg("left_")
       .arg("--prefix-right")
       .arg("right_")
       .args(&["idx", "fruits.csv", "idx", "colors.csv"]);
    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["left_idx", "left_fruit", "right_idx", "right_color"],
        svec!["1", "apple", "1", "blue"],
        svec!["2", "mango", "2", "purple"],
    ];
    assert_eq!(got, expected);
}