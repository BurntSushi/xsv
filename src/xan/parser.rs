// En tant que chef, je m'engage à ce que nous ne nous fassions pas *tous* tuer.
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, anychar, char, digit1, none_of, space0},
    combinator::{all_consuming, map, map_res, not, opt, recognize, value},
    multi::{fold_many0, many0, separated_list0},
    number::complete::double,
    sequence::{delimited, pair, preceded, terminated, tuple},
    IResult,
};

use xan::types::ColumIndexationBy;

#[derive(Debug, PartialEq, Clone)]
pub enum Argument {
    Identifier(String),
    SpecialIdentifier(String),
    Indexation(ColumIndexationBy),
    StringLiteral(String),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    RegexLiteral(String),
    Call(FunctionCall),
    Underscore,
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Argument>,
}

impl FunctionCall {
    pub fn has_underscore(&self) -> bool {
        self.args.iter().any(|arg| match arg {
            Argument::Call(sub_function_call) => sub_function_call.has_underscore(),
            Argument::Underscore => true,
            _ => false,
        })
    }

    pub fn count_underscores(&self) -> usize {
        self.args
            .iter()
            .map(|arg| match arg {
                Argument::Call(sub_function_call) => sub_function_call.count_underscores(),
                Argument::Underscore => 1,
                _ => 0,
            })
            .sum()
    }

    pub fn fill_underscore(&mut self, with: &FunctionCall) {
        for arg in self.args.iter_mut() {
            match arg {
                Argument::Call(sub) => {
                    sub.fill_underscore(with);
                }
                Argument::Underscore => {
                    *arg = Argument::Call(with.clone());
                }
                _ => (),
            }
        }
    }
}

pub type Pipeline = Vec<FunctionCall>;

fn boolean_literal(input: &str) -> IResult<&str, bool> {
    alt((value(true, tag("true")), value(false, tag("false"))))(input)
}

fn float_literal(input: &str) -> IResult<&str, f64> {
    double(input)
}

fn underscore(input: &str) -> IResult<&str, ()> {
    value((), char('_'))(input)
}

fn null(input: &str) -> IResult<&str, ()> {
    value((), tag("null"))(input)
}

fn inner_identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alpha1,
        many0(alt((alphanumeric1, tag("_"), tag("-"), tag(" ")))),
    ))(input)
}

fn inner_special_identifier(input: &str) -> IResult<&str, &str> {
    preceded(
        char('%'),
        recognize(pair(
            alpha1,
            many0(alt((alphanumeric1, tag("_"), tag("-"), tag(" ")))),
        )),
    )(input)
}

fn outer_identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alpha1,
        many0(alt((alphanumeric1, tag("_"), tag("-")))),
    ))(input)
}

fn integer_literal<T>(input: &str) -> IResult<&str, T>
where
    T: std::str::FromStr,
{
    map_res(
        recognize(pair(
            alt((digit1, tag("-"))),
            many0(alt((digit1, tag("_")))),
        )),
        |string: &str| string.replace('_', "").parse::<T>(),
    )(input)
}

fn unescape(c: char, delimiter: char) -> Result<char, ()> {
    if c == delimiter {
        return Ok(c);
    }

    Ok(match c {
        '\\' | '/' => c,
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        _ => return Err(()),
    })
}

fn double_quote_string_character_literal(input: &str) -> IResult<&str, char> {
    let (input, c) = none_of("\"")(input)?;

    if c == '\\' {
        let (input, c) = anychar(input)?;

        match unescape(c, '"') {
            Ok(c) => Ok((input, c)),
            Err(_) => Err(nom::Err::Failure(nom::error::ParseError::from_char(
                input, c,
            ))),
        }
    } else {
        Ok((input, c))
    }
}

fn single_quote_string_character_literal(input: &str) -> IResult<&str, char> {
    let (input, c) = none_of("'")(input)?;

    if c == '\\' {
        let (input, c) = anychar(input)?;

        match unescape(c, '\'') {
            Ok(c) => Ok((input, c)),
            Err(_) => Err(nom::Err::Failure(nom::error::ParseError::from_char(
                input, c,
            ))),
        }
    } else {
        Ok((input, c))
    }
}

fn regex_character_literal(input: &str) -> IResult<&str, char> {
    let (input, c) = none_of("/")(input)?;

    if c == '\\' {
        let (input2, c2) = anychar(input)?;

        if c2 == '/' {
            Ok((input2, c2))
        } else {
            Ok((input, c))
        }
    } else {
        Ok((input, c))
    }
}

fn string_literal(input: &str) -> IResult<&str, String> {
    alt((
        delimited(
            char('"'),
            fold_many0(
                double_quote_string_character_literal,
                String::new,
                |mut string, c| {
                    string.push(c);
                    string
                },
            ),
            char('"'),
        ),
        delimited(
            char('\''),
            fold_many0(
                single_quote_string_character_literal,
                String::new,
                |mut string, c| {
                    string.push(c);
                    string
                },
            ),
            char('\''),
        ),
    ))(input)
}

fn regex_literal(input: &str) -> IResult<&str, String> {
    map(
        pair(
            delimited(
                char('/'),
                fold_many0(regex_character_literal, String::new, |mut string, c| {
                    string.push(c);
                    string
                }),
                char('/'),
            ),
            opt(tag("i")),
        ),
        |(pattern, i)| match i {
            None => pattern,
            Some(_) => {
                let mut case_insensitive_pattern = String::from("(?i)");
                case_insensitive_pattern.push_str(&pattern);
                case_insensitive_pattern
            }
        },
    )(input)
}

fn argument_separator(input: &str) -> IResult<&str, ()> {
    value((), tuple((space0, char(','), space0)))(input)
}

fn indexation(input: &str) -> IResult<&str, ColumIndexationBy> {
    preceded(
        tag("row"),
        delimited(
            char('['),
            alt((
                map(
                    pair(
                        string_literal,
                        opt(preceded(argument_separator, integer_literal::<usize>)),
                    ),
                    |(string, index)| match index {
                        Some(pos) => ColumIndexationBy::NameAndNth((string, pos)),
                        None => ColumIndexationBy::Name(string),
                    },
                ),
                map(integer_literal::<usize>, ColumIndexationBy::Pos),
            )),
            char(']'),
        ),
    )(input)
}

fn argument(input: &str) -> IResult<&str, Argument> {
    alt((
        map(inner_function_call, Argument::Call),
        map(boolean_literal, Argument::BooleanLiteral),
        map(null, |_| Argument::Null),
        map(indexation, Argument::Indexation),
        map(inner_special_identifier, |name| {
            Argument::SpecialIdentifier(String::from(name))
        }),
        map(inner_identifier, |name| {
            Argument::Identifier(String::from(name))
        }),
        map(terminated(integer_literal, not(char('.'))), |value| {
            Argument::IntegerLiteral(value)
        }),
        map(float_literal, Argument::FloatLiteral),
        map(regex_literal, Argument::RegexLiteral),
        map(string_literal, Argument::StringLiteral),
        map(underscore, |_| Argument::Underscore),
    ))(input)
}

fn argument_list(input: &str) -> IResult<&str, Vec<Argument>> {
    separated_list0(argument_separator, argument)(input)
}

fn inner_function_call(input: &str) -> IResult<&str, FunctionCall> {
    map(
        pair(
            outer_identifier,
            delimited(
                pair(space0, char('(')),
                argument_list,
                pair(char(')'), space0),
            ),
        ),
        |(name, args)| FunctionCall {
            name: String::from(name),
            args,
        },
    )(input)
}

fn outer_function_call(input: &str) -> IResult<&str, FunctionCall> {
    map(
        pair(
            outer_identifier,
            opt(delimited(
                pair(space0, char('(')),
                argument_list,
                pair(char(')'), space0),
            )),
        ),
        |(name, args)| FunctionCall {
            name: String::from(name),
            args: args.unwrap_or_else(|| vec![Argument::Underscore]),
        },
    )(input)
}

fn pipe(input: &str) -> IResult<&str, ()> {
    value((), tuple((space0, char('|'), space0)))(input)
}

fn pipeline(input: &str) -> IResult<&str, Pipeline> {
    all_consuming(separated_list0(pipe, outer_function_call))(input)
}

// TODO: write this better
pub fn parse(code: &str) -> Result<Pipeline, ()> {
    match pipeline(code) {
        Ok(p) => Ok(p.1),
        Err(_) => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_literal() {
        assert_eq!(boolean_literal("true, test"), Ok((", test", true)));

        assert_eq!(boolean_literal("false"), Ok(("", false)));
    }

    #[test]
    fn test_float_literal() {
        assert_eq!(float_literal("3.56"), Ok(("", 3.56f64)))
    }

    #[test]
    fn test_integer_literal() {
        assert_eq!(integer_literal("456_400"), Ok(("", 456_400i64)));
        assert_eq!(integer_literal("-36, test"), Ok((", test", -36i64)));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(string_literal("\"\", 45"), Ok((", 45", String::from(""))));
        assert_eq!(string_literal("'', 45"), Ok((", 45", String::from(""))));
        assert_eq!(
            string_literal(r#""hello", 45"#),
            Ok((", 45", String::from("hello")))
        );
        assert_eq!(
            string_literal(r#""héllo", 45"#),
            Ok((", 45", String::from("héllo")))
        );
        assert_eq!(
            string_literal(r#""hel\nlo", 45"#),
            Ok((", 45", String::from("hel\nlo")))
        );
        assert_eq!(
            string_literal(r#""hello \"world\"", 45"#),
            Ok((", 45", String::from("hello \"world\"")))
        );
        assert_eq!(
            string_literal(r#"'hello \'world\'', 45"#),
            Ok((", 45", String::from("hello 'world'")))
        );
    }

    #[test]
    fn test_regex_literal() {
        assert_eq!(
            regex_literal(r#"/test/, ok"#),
            Ok((", ok", "test".to_string()))
        );

        assert_eq!(
            regex_literal(r#"/\nok[a]./, ok"#),
            Ok((", ok", "\\nok[a].".to_string()))
        );

        assert_eq!(
            regex_literal(r#"/\r/, ok"#),
            Ok((", ok", "\\r".to_string()))
        );

        assert_eq!(regex_literal(r#"/\//, ok"#), Ok((", ok", "/".to_string())));

        assert_eq!(regex_literal("/test/i"), Ok(("", "(?i)test".to_string())));
    }

    #[test]
    fn test_underscore() {
        assert_eq!(underscore("_, 45"), Ok((", 45", ())))
    }

    #[test]
    fn test_identifier() {
        assert_eq!(outer_identifier("input, test"), Ok((", test", "input")));
        assert_eq!(
            inner_identifier("PREFIXES AS URL, test"),
            Ok((", test", "PREFIXES AS URL"))
        );
        assert_eq!(
            inner_special_identifier("%index, ok"),
            Ok((", ok", "index"))
        );
    }

    #[test]
    fn test_indexation() {
        assert_eq!(
            indexation("row['name']"),
            Ok(("", ColumIndexationBy::Name("name".to_string())))
        );
        assert_eq!(
            indexation("row[\"name\"]"),
            Ok(("", ColumIndexationBy::Name("name".to_string())))
        );
        assert_eq!(
            indexation("row['name', 3]"),
            Ok(("", ColumIndexationBy::NameAndNth(("name".to_string(), 3))))
        );
        assert_eq!(indexation("row[34]"), Ok(("", ColumIndexationBy::Pos(34))));
    }

    #[test]
    fn test_argument() {
        assert_eq!(argument("true"), Ok(("", Argument::BooleanLiteral(true))));
        assert_eq!(
            argument("\"test\""),
            Ok(("", Argument::StringLiteral(String::from("test"))))
        );
        assert_eq!(
            argument("/test/, name"),
            Ok((", name", Argument::RegexLiteral(String::from("test"))))
        );
    }

    #[test]
    fn test_argument_list() {
        assert_eq!(argument_list(""), Ok(("", vec![])));
        assert_eq!(
            argument_list("true, _, col0"),
            Ok((
                "",
                vec![
                    Argument::BooleanLiteral(true),
                    Argument::Underscore,
                    Argument::Identifier(String::from("col0"))
                ]
            ))
        )
    }

    #[test]
    fn test_function_call() {
        assert_eq!(
            outer_function_call("trim()"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![]
                }
            ))
        );

        assert_eq!(
            outer_function_call("trim(_)"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![Argument::Underscore]
                }
            ))
        );

        assert_eq!(
            outer_function_call("trim(_, true, 4.5, 56, col)"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![
                        Argument::Underscore,
                        Argument::BooleanLiteral(true),
                        Argument::FloatLiteral(4.5),
                        Argument::IntegerLiteral(56),
                        Argument::Identifier(String::from("col"))
                    ]
                }
            ))
        );
    }

    #[test]
    fn test_pipeline() {
        assert!(pipeline("test |").is_err());

        assert_eq!(
            pipeline("trim(name) | len  (_, row['name'])"),
            Ok((
                "",
                vec![
                    FunctionCall {
                        name: String::from("trim"),
                        args: vec![Argument::Identifier(String::from("name"))]
                    },
                    FunctionCall {
                        name: String::from("len"),
                        args: vec![
                            Argument::Underscore,
                            Argument::Indexation(ColumIndexationBy::Name("name".to_string()))
                        ]
                    }
                ]
            ))
        );

        assert_eq!(
            pipeline("add(len(name), len(surname)) | len  (_, row['name'])"),
            Ok((
                "",
                vec![
                    FunctionCall {
                        name: String::from("add"),
                        args: vec![
                            Argument::Call(FunctionCall {
                                name: "len".to_string(),
                                args: vec![Argument::Identifier("name".to_string())]
                            }),
                            Argument::Call(FunctionCall {
                                name: "len".to_string(),
                                args: vec![Argument::Identifier("surname".to_string())]
                            })
                        ]
                    },
                    FunctionCall {
                        name: String::from("len"),
                        args: vec![
                            Argument::Underscore,
                            Argument::Indexation(ColumIndexationBy::Name("name".to_string()))
                        ]
                    }
                ]
            ))
        );

        assert_eq!(
            pipeline("if(true, len(name), len(surname))"),
            Ok((
                "",
                vec![FunctionCall {
                    name: String::from("if"),
                    args: vec![
                        Argument::BooleanLiteral(true),
                        Argument::Call(FunctionCall {
                            name: "len".to_string(),
                            args: vec![Argument::Identifier("name".to_string())]
                        }),
                        Argument::Call(FunctionCall {
                            name: "len".to_string(),
                            args: vec![Argument::Identifier("surname".to_string())]
                        })
                    ]
                }]
            ))
        );

        assert_eq!(
            pipeline("trim(name)|len  (_)"),
            Ok((
                "",
                vec![
                    FunctionCall {
                        name: String::from("trim"),
                        args: vec![Argument::Identifier(String::from("name"))]
                    },
                    FunctionCall {
                        name: String::from("len"),
                        args: vec![Argument::Underscore]
                    }
                ]
            ))
        );

        assert_eq!(
            pipeline("trim(name) | len(_)  "),
            Ok((
                "",
                vec![
                    FunctionCall {
                        name: String::from("trim"),
                        args: vec![Argument::Identifier(String::from("name"))]
                    },
                    FunctionCall {
                        name: String::from("len"),
                        args: vec![Argument::Underscore]
                    }
                ]
            ))
        );

        assert_eq!(
            pipeline("trim | len | coalesce(null)"),
            Ok((
                "",
                vec![
                    FunctionCall {
                        name: String::from("trim"),
                        args: vec![Argument::Underscore]
                    },
                    FunctionCall {
                        name: String::from("len"),
                        args: vec![Argument::Underscore]
                    },
                    FunctionCall {
                        name: String::from("coalesce"),
                        args: vec![Argument::Null]
                    }
                ]
            ))
        );
    }
}
