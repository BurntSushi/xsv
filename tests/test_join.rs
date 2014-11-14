use std::io::process;

use workdir::Workdir;

macro_rules! join_test {
    ($name:ident $fun:expr) => (
        #[test]
        fn $name() {
            let wrk = setup(stringify!($name));
            let mut cmd = wrk.command("join");
            cmd.args(["city", "cities.csv", "city", "places.csv"]);
            $fun(wrk, cmd);
        }
    );
}

fn setup(name: &str) -> Workdir {
    let cities = vec![
        svec!["city", "state"],
        svec!["Boston", "MA"],
        svec!["New York", "NY"],
        svec!["San Francisco", "CA"],
        svec!["Buffalo", "NY"],
    ];
    let places = vec![
        svec!["city", "place"],
        svec!["Boston", "Logan Airport"],
        svec!["Boston", "Boston Garden"],
        svec!["Buffalo", "Ralph Wilson Stadium"],
        svec!["Orlando", "Disney World"],
    ];

    let wrk = Workdir::new(name);
    wrk.create("cities.csv", cities);
    wrk.create("places.csv", places);
    wrk
}

join_test!(join_inner |wrk: Workdir, cmd: process::Command| {
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![
        svec!["city", "state", "city", "place"],
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ];
    assert_eq!(got, expected);
})

join_test!(join_outer_left |wrk: Workdir, mut cmd: process::Command| {
    cmd.arg("--left");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![
        svec!["city", "state", "city", "place"],
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
    ];
    assert_eq!(got, expected);
})

join_test!(join_outer_right |wrk: Workdir, mut cmd: process::Command| {
    cmd.arg("--right");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![
        svec!["city", "state", "city", "place"],
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ];
    assert_eq!(got, expected);
})

join_test!(join_outer_full |wrk: Workdir, mut cmd: process::Command| {
    cmd.arg("--full");
    let got: Vec<Vec<String>> = wrk.read_stdout(&cmd);
    let expected = vec![
        svec!["city", "state", "city", "place"],
        svec!["Boston", "MA", "Boston", "Logan Airport"],
        svec!["Boston", "MA", "Boston", "Boston Garden"],
        svec!["New York", "NY", "", ""],
        svec!["San Francisco", "CA", "", ""],
        svec!["Buffalo", "NY", "Buffalo", "Ralph Wilson Stadium"],
        svec!["", "", "Orlando", "Disney World"],
    ];
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
