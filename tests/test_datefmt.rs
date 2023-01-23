use workdir::Workdir;

#[test]
fn datefmt() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![
            svec!["datetime"],
            svec!["2021-04-30 21:14:10"],
            svec!["May 8, 2009 5:57:51 PM"],
            svec!["2012/03/19 10:11:59.3186369"],
            svec!["2021-05-01T01:17:02.604456Z"],
            svec!["1674467716"],
        ],
    );
    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2021-04-30 21:14:10", "2021-04-30 21:14:10 UTC"],
        svec!["May 8, 2009 5:57:51 PM", "2009-05-08 17:57:51 UTC"],
        svec![
            "2012/03/19 10:11:59.3186369",
            "2012-03-19 10:11:59.318636900 UTC"
        ],
        svec![
            "2021-05-01T01:17:02.604456Z",
            "2021-05-01 01:17:02.604456 UTC"
        ],
        svec!["1674467716", "2023-01-23 09:55:16 UTC"],
    ];
    assert_eq!(got, expected);
}
#[test]
fn datefmt_pacific_to_utc() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15 01:02:03"]],
    );
    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--intz")
        .arg("US/Pacific");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 01:02:03", "2020-03-15 08:02:03 UTC"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_alaska_to_hawaii() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15 01:02:03"]],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--intz")
        .arg("US/Alaska")
        .arg("--outtz")
        .arg("US/Hawaii");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 01:02:03", "2020-03-14 23:02:03 HST"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_utc_to_eastern() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15 01:02:03"]],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--outtz")
        .arg("US/Eastern");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 01:02:03", "2020-03-14 21:02:03 EDT"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_detect_eastern() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15 08:02:03 EDT"]],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--outtz")
        .arg("UTC");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 08:02:03 EDT", "2020-03-15 12:02:03 UTC"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_detect_despite_intz_option() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![
            svec!["datetime"],
            svec!["2020-03-15 08:02:03 EDT"],
            svec!["1674467716"],
        ],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--intz")
        .arg("US/Alaska")
        .arg("--outtz")
        .arg("UTC");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 08:02:03 EDT", "2020-03-15 12:02:03 UTC"],
        svec!["1674467716", "2023-01-23 09:55:16 UTC"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_infmt() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15T21:02:03"]],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--infmt")
        .arg("%Y-%m-%dT%H:%M:%S");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15T21:02:03", "2020-03-15 21:02:03 UTC"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_outfmt() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![svec!["datetime"], svec!["2020-03-15 21:02:03"]],
    );

    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime")
        .arg("data.csv")
        .arg("--outfmt")
        .arg("%Y-%m-%dT%H:%M:%S");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "formatted_date"],
        svec!["2020-03-15 21:02:03", "2020-03-15T21:02:03"],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_no_headers() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![
            svec!["2021-04-30 21:14:10"],
            svec!["May 8, 2009 5:57:51 PM"],
            svec!["2012/03/19 10:11:59.3186369"],
            svec!["2021-05-01T01:17:02.604456Z"],
        ],
    );
    let mut cmd = wrk.command("datefmt");
    cmd.arg("1").arg("--no-headers").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["2021-04-30 21:14:10", "2021-04-30 21:14:10 UTC"],
        svec!["May 8, 2009 5:57:51 PM", "2009-05-08 17:57:51 UTC"],
        svec![
            "2012/03/19 10:11:59.3186369",
            "2012-03-19 10:11:59.318636900 UTC"
        ],
        svec![
            "2021-05-01T01:17:02.604456Z",
            "2021-05-01 01:17:02.604456 UTC"
        ],
    ];
    assert_eq!(got, expected);
}

#[test]
fn datefmt_column_name() {
    let wrk = Workdir::new("datefmt");
    wrk.create(
        "data.csv",
        vec![
            svec!["datetime"],
            svec!["2021-04-30 21:14:10"],
            svec!["May 8, 2009 5:57:51 PM"],
            svec!["2012/03/19 10:11:59.3186369"],
            svec!["2021-05-01T01:17:02.604456Z"],
        ],
    );
    let mut cmd = wrk.command("datefmt");
    cmd.arg("datetime").arg("-c").arg("date").arg("data.csv");

    let got: Vec<Vec<String>> = wrk.read_stdout(&mut cmd);
    let expected = vec![
        svec!["datetime", "date"],
        svec!["2021-04-30 21:14:10", "2021-04-30 21:14:10 UTC"],
        svec!["May 8, 2009 5:57:51 PM", "2009-05-08 17:57:51 UTC"],
        svec![
            "2012/03/19 10:11:59.3186369",
            "2012-03-19 10:11:59.318636900 UTC"
        ],
        svec![
            "2021-05-01T01:17:02.604456Z",
            "2021-05-01 01:17:02.604456 UTC"
        ],
    ];
    assert_eq!(got, expected);
}
