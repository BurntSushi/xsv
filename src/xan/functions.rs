pub enum DynamicValue {
    String(String),
    Float(f64),
    Integer(i64),
    Boolean(bool),
    None,
}

impl DynamicValue {
    fn serialize(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Float(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Boolean(value) => String::from(if value { "true" } else { "false" }),
            Self::None => "".to_string(),
        }
    }
}
