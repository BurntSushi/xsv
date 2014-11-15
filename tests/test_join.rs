use std::io::process;

use workdir::Workdir;

// This macro takes *two* identifiers: one for the test with headers
// and another for the test without headers.
macro_rules! join_test {
    ($name:ident $name_no_headers:ident $fun:expr) => (
        #[test]
        fn $name() {
            let wrk = setup(stringify!($name), true);
            let mut cmd = wrk.command("join");
            cmd.args(["city", "cities.csv", "city", "places.csv"]);
            $fun(wrk, cmd, true);
        }

        #[test]
        fn $name_no_headers() {
            let wrk = setup(stringify!($name_no_headers), false);
            let mut cmd = wrk.command("join");
            cmd.arg("--no-headers");
            cmd.args(["1", "cities.csv", "1", "places.csv"]);
            $fun(wrk, cmd, false);
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

join_test!(join_inner join_inner_no_headers
           |wrk: Workdir, cmd: process::Command, headers: bool| {
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
})

join_test!(join_outer_left join_outer_left_no_headers
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--left");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ]);
    assert_eq!(got, expected);
})

join_test!(join_outer_right join_outer_right_no_headers
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--right");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
})

join_test!(join_outer_full join_outer_full_no_headers
           |wrk: Workdir, mut cmd: process::Command, headers: bool| {
    cmd.arg("--full");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = make_rows(headers, vec![
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ]);
    assert_eq!(got, expected);
})

#[test]
fn join_outer_cross() {
    let wrk = Workdir::new("join_outer_cross");
    wrk.create("letters.csv", vec![svec!["a", "b"], svec!["c", "d"]]);
    wrk.create("numbers.csv", vec![svec!["1", "2"], svec!["3", "4"]]);

    let mut cmd = wrk.command("join");
    cmd.arg("--cross").arg("--no-headers")
       .args(["", "letters.csv", "", "numbers.csv"]);
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![
        svec!["a", "b", "1", "2"],
        svec!["a", "b", "3", "4"],
        svec!["c", "d", "1", "2"],
        svec!["c", "d", "3", "4"],
    ];
    assert_eq!(got, expected);
}
