use workdir::Workdir;

#[test]
fn dedup_normal() {
	let wrk = Workdir::new("dedup_normal");
	wrk.create("in.csv", vec![
			   svec!["N", "S"],
			   svec!["10", "a"],
			   svec!["10", "a"],
			   svec!["2", "b"],
			   svec!["2", "B"],
	]);

	let mut cmd = wrk.command("dedup");
	cmd.arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["2", "B"],
        svec!["2", "b"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn dedup_no_case() {
	let wrk = Workdir::new("dedup_no_case");
	wrk.create("in.csv", vec![
			   svec!["N", "S"],
			   svec!["10", "a"],
			   svec!["10", "a"],
			   svec!["2", "b"],
			   svec!["2", "B"],
	]);

	let mut cmd = wrk.command("dedup");
	cmd.arg("-C").arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["2", "b"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn dedup_select() {
	let wrk = Workdir::new("dedup_select");
	wrk.create("in.csv", vec![
			   svec!["N", "S"],
			   svec!["10", "a"],
			   svec!["10", "a"],
			   svec!["2", "b"],
			   svec!["2", "B"],
	]);

	let mut cmd = wrk.command("dedup");
	cmd.args(&["-s", "N"]).arg("in.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["N", "S"],
        svec!["10", "a"],
        svec!["2", "B"],
    ];
    assert_eq!(got, expected);
}
