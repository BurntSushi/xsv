// use std::process;

// use workdir::Workdir;

// fn setup(name: &str) -> (Workdir, process::Command) {
//     let rows = vec![
//         svec!["h1", "h2"],
//         svec!["abcdef", "ghijkl"],
//         svec!["mnopqr", "stuvwx"],
//         svec!["mn", "stuvwx"],
//     ];

//     let wrk = Workdir::new(name);
//     wrk.create("in.csv", rows);

//     let mut cmd = wrk.command("flatten");
//     cmd.arg("in.csv");

//     (wrk, cmd)
// }

// #[test]
// fn flatten_basic() {
//     let (wrk, mut cmd) = setup("flatten_basic");
//     let got: String = wrk.stdout(&mut cmd);
//     let expected = "\
// h1 abcdef
// h2 ghijkl

// h1 mnopqr
// h2 stuvwx

// h1 mn
// h2 stuvwx\
// ";
//     assert_eq!(got, expected.to_string());
// }

// #[test]
// fn flatten_no_headers() {
//     let (wrk, mut cmd) = setup("flatten_no_headers");
//     cmd.arg("--no-headers");

//     let got: String = wrk.stdout(&mut cmd);
//     let expected = "\
// 0 h1
// 1 h2

// 0 abcdef
// 1 ghijkl

// 0 mnopqr
// 1 stuvwx

// 0 mn
// 1 stuvwx\
// ";
//     assert_eq!(got, expected.to_string());
// }

// #[test]
// fn flatten_separator() {
//     let (wrk, mut cmd) = setup("flatten_separator");
//     cmd.args(["--separator", "!mysep!"]);

//     let got: String = wrk.stdout(&mut cmd);
//     let expected = "\
// h1 abcdef
// h2 ghijkl
// !mysep!
// h1 mnopqr
// h2 stuvwx
// !mysep!
// h1 mn
// h2 stuvwx\
// ";
//     assert_eq!(got, expected.to_string());
// }

// #[test]
// fn flatten_condense() {
//     let (wrk, mut cmd) = setup("flatten_condense");
//     cmd.args(["--condense", "2"]);

//     let got: String = wrk.stdout(&mut cmd);
//     let expected = "\
// h1 a…
// h2 g…

// h1 m…
// h2 s…

// h1 mn
// h2 s…\
// ";
//     assert_eq!(got, expected.to_string());
// }
