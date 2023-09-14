use csv;

#[derive(Debug, PartialEq)]
pub enum ColumIndexation {
    ByName(String),
    ByNameAndNth((String, usize)),
    ByPos(usize),
}

impl ColumIndexation {
    pub fn find_column_index(&self, headers: &csv::ByteRecord) -> Option<usize> {
        match self {
            Self::ByPos(i) => {
                if i >= &headers.len() {
                    None
                } else {
                    Some(*i)
                }
            }
            Self::ByName(name) => {
                let name_bytes = name.as_bytes();

                for (i, cell) in headers.iter().enumerate() {
                    if cell == name_bytes {
                        return Some(i);
                    }
                }

                return None;
            }
            Self::ByNameAndNth((name, pos)) => {
                let mut i: usize = 0;
                let mut c = *pos;

                let name_bytes = name.as_bytes();

                for cell in headers {
                    if cell == name_bytes {
                        if c == 0 {
                            return Some(i);
                        }
                        c -= 1;
                    }

                    i += 1;
                }

                return None;
            }
        }
    }
}
