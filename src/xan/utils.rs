pub fn downgrade_float(f: f64) -> Option<i64> {
    let t = f.trunc();

    if f - t <= f64::EPSILON {
        return Some(t as i64);
    }

    None
}

pub fn pop2<T>(v: &mut Vec<T>) -> Option<(T, T)> {
    match v.pop() {
        Some(a) => match v.pop() {
            Some(b) => Some((b, a)),
            None => None,
        },
        None => None,
    }
}
