// En tant que chef, je m'engage à ce que nous ne nous fassions pas *tous* tuer.
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, anychar, char, digit1, none_of, space0},
    combinator::{all_consuming, map_res, not, opt, recognize, value},
    multi::{fold_many0, many0, separated_list0},
    number::complete::double,
    sequence::{delimited, pair, terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
enum Argument {
    Identifier(String),
    StringLiteral(String),
    FloatLiteral(f64),
    IntegerLiteral(i64),
    BooleanLiteral(bool),
    Underscore,
}

#[derive(Debug, PartialEq)]
struct FunctionCall {
    name: String,
    args: Vec<Argument>,
}

fn true_literal(input: &str) -> IResult<&str, Argument> {
    tag("true")(input).map(|t| (t.0, Argument::BooleanLiteral(true)))
}

fn false_literal(input: &str) -> IResult<&str, Argument> {
    tag("false")(input).map(|t| (t.0, Argument::BooleanLiteral(false)))
}

fn boolean_literal(input: &str) -> IResult<&str, Argument> {
    alt((true_literal, false_literal))(input)
}

fn float_literal(input: &str) -> IResult<&str, Argument> {
    double(input).map(|t| (t.0, Argument::FloatLiteral(t.1)))
}

fn underscore(input: &str) -> IResult<&str, Argument> {
    char('_')(input).map(|t| (t.0, Argument::Underscore))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alpha1,
        many0(alt((alphanumeric1, tag("_"), tag("-")))),
    ))(input)
}

fn integer_literal(input: &str) -> IResult<&str, Argument> {
    map_res(
        recognize(pair(digit1, many0(alt((digit1, tag("_")))))),
        |string: &str| {
            string
                .replace("_", "")
                .parse::<i64>()
                .map(|i| Argument::IntegerLiteral(i))
        },
    )(input)
}

fn string_character_literal(input: &str) -> IResult<&str, char> {
    let (input, c) = none_of("\"")(input)?;

    if c == '\\' {
        let (input, c) = anychar(input)?;

        Ok((
            input,
            match c {
                '"' | '\\' | '/' => c,
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                _ => {
                    return Err(nom::Err::Failure(nom::error::ParseError::from_char(
                        input, c,
                    )))
                }
            },
        ))
    } else {
        Ok((input, c))
    }
}

fn string_literal(input: &str) -> IResult<&str, String> {
    delimited(
        char('"'),
        fold_many0(string_character_literal, String::new, |mut string, c| {
            string.push(c);
            string
        }),
        char('"'),
    )(input)
}

fn argument_separator(input: &str) -> IResult<&str, ()> {
    value((), tuple((space0, char(','), space0)))(input)
}

fn argument(input: &str) -> IResult<&str, Argument> {
    alt((
        boolean_literal,
        map_res(identifier, |name| -> Result<Argument, ()> {
            Ok(Argument::Identifier(String::from(name)))
        }),
        underscore,
        terminated(integer_literal, not(char('.'))),
        float_literal,
    ))(input)
}

fn argument_list(input: &str) -> IResult<&str, Vec<Argument>> {
    separated_list0(argument_separator, argument)(input)
}

fn function_call(input: &str) -> IResult<&str, FunctionCall> {
    map_res(
        pair(
            identifier,
            opt(delimited(
                pair(space0, char('(')),
                argument_list,
                pair(char(')'), space0),
            )),
        ),
        |(name, args)| -> Result<FunctionCall, ()> {
            Ok(FunctionCall {
                name: String::from(name),
                args: args.unwrap_or_else(|| vec![Argument::Underscore]),
            })
        },
    )(input)
}

fn pipe(input: &str) -> IResult<&str, ()> {
    value((), tuple((space0, char('|'), space0)))(input)
}

fn pipeline(input: &str) -> IResult<&str, Vec<FunctionCall>> {
    all_consuming(separated_list0(pipe, function_call))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_literal() {
        assert_eq!(
            boolean_literal("true, test"),
            Ok((", test", Argument::BooleanLiteral(true)))
        );

        assert_eq!(
            boolean_literal("false"),
            Ok(("", Argument::BooleanLiteral(false)))
        );
    }

    #[test]
    fn test_float_literal() {
        assert_eq!(
            float_literal("3.56"),
            Ok(("", Argument::FloatLiteral(3.56)))
        )
    }

    #[test]
    fn test_integer_literal() {
        assert_eq!(
            integer_literal("456_400"),
            Ok(("", Argument::IntegerLiteral(456_400i64)))
        );
    }

    #[test]
    fn test_string_literal() {
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
    }

    #[test]
    fn test_underscore() {
        assert_eq!(underscore("_, 45"), Ok((", 45", Argument::Underscore)))
    }

    #[test]
    fn test_identifier() {
        assert_eq!(identifier("input, test"), Ok((", test", "input")));
    }

    #[test]
    fn test_argument() {
        assert_eq!(argument("true"), Ok(("", Argument::BooleanLiteral(true))));
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
            function_call("trim()"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![]
                }
            ))
        );

        assert_eq!(
            function_call("trim(_)"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![Argument::Underscore]
                }
            ))
        );

        assert_eq!(
            function_call("trim(_, true, 4.5, 56, col)"),
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
            pipeline("trim(name) | len  (_)"),
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
            pipeline("trim | len"),
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
                    }
                ]
            ))
        );
    }
}
