pub fn downgrade_float(f: f64) -> Option<i64> {
    let t = f.trunc();

    if f - t <= f64::EPSILON {
        return Some(t as i64);
    }

    None
}
