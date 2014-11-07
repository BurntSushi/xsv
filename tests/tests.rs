extern crate csv;

mod workdir;

mod test_cat;

pub fn assert_csv_eq<S: Str, T: Str>(xs: Vec<Vec<S>>, ys: Vec<Vec<T>>) {
    assert_eq!(to_strings(xs), to_strings(ys));
}

fn to_strings<S: Str>(xs: Vec<Vec<S>>) -> Vec<Vec<String>> {
    xs.iter()
      .map(|row| row.iter()
                    .map(|field| field.as_slice().to_string())
                    .collect())
      .collect()
}
