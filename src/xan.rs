// En tant que chef, je m'engage à ce que nous ne nous fassions pas *tous* tuer.
use nom::{
    branch::alt,
    bytes::complete::tag,
    character::complete::{alpha1, alphanumeric1, char, space0},
    combinator::{all_consuming, map_res, opt, recognize, value},
    multi::{many0, separated_list0},
    sequence::{delimited, pair, tuple},
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

fn underscore(input: &str) -> IResult<&str, Argument> {
    char('_')(input).map(|t| (t.0, Argument::Underscore))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        alpha1,
        many0(alt((alphanumeric1, tag("_"), tag("-")))),
    ))(input)
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
            function_call("trim(_, true)"),
            Ok((
                "",
                FunctionCall {
                    name: String::from("trim"),
                    args: vec![Argument::Underscore, Argument::BooleanLiteral(true)]
                }
            ))
        );
    }

    #[test]
    fn test_pipeline() {
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
