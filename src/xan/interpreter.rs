use fmt;
use std::borrow::Cow;

use csv::ByteRecord;
use regex::Regex;

use super::error::{
    BindingError, CallError, EvaluationError, PrepareError, SpecifiedBindingError,
    SpecifiedCallError,
};
use super::functions::{get_function, Function};
use super::parser::{parse, Argument, Pipeline};
use super::types::{
    BoundArgument, BoundArguments, ColumIndexationBy, DynamicValue, EvaluationResult, Variables,
};

#[derive(Debug, Clone)]
enum ConcreteArgument {
    Variable(String),
    Column(usize),
    StringLiteral(DynamicValue),
    FloatLiteral(DynamicValue),
    IntegerLiteral(DynamicValue),
    BooleanLiteral(DynamicValue),
    RegexLiteral(DynamicValue),
    Call(ConcreteFunctionCall),
    Null,
    Underscore,
}

impl ConcreteArgument {
    fn bind<'a>(
        &'a self,
        record: &'a ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> Result<BoundArgument<'a>, BindingError> {
        Ok(match self {
            Self::StringLiteral(value) => Cow::Borrowed(value),
            Self::FloatLiteral(value) => Cow::Borrowed(value),
            Self::IntegerLiteral(value) => Cow::Borrowed(value),
            Self::BooleanLiteral(value) => Cow::Borrowed(value),
            Self::Underscore => Cow::Borrowed(last_value),
            Self::Null => Cow::Owned(DynamicValue::None),
            Self::Column(index) => match record.get(*index) {
                None => return Err(BindingError::ColumnOutOfRange(*index)),
                Some(cell) => match std::str::from_utf8(cell) {
                    Err(_) => return Err(BindingError::UnicodeDecodeError),
                    Ok(value) => Cow::Owned(DynamicValue::from(value)),
                },
            },
            Self::Variable(name) => match variables.get::<str>(name) {
                Some(value) => Cow::Borrowed(value),
                None => return Err(BindingError::UnknownVariable(name.clone())),
            },
            Self::RegexLiteral(value) => Cow::Borrowed(value),
            Self::Call(_) => return Err(BindingError::IllegalBinding),
        })
    }

    fn evaluate<'a>(
        &'a self,
        record: &'a csv::ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> EvaluationResult<'a> {
        match self {
            Self::Call(function_call) => function_call.run(record, last_value, variables),
            _ => self.bind(record, last_value, variables).map_err(|err| {
                EvaluationError::Binding(SpecifiedBindingError {
                    function_name: "<expr>".to_string(),
                    arg_index: None,
                    reason: err,
                })
            }),
        }
    }

    fn is_index_variable(&self) -> bool {
        match self {
            Self::Variable(name) => name == "index",
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct ConcreteSubroutine {
    name: String,
    function: Function,
    args: Vec<ConcreteArgument>,
}

impl ConcreteSubroutine {
    fn run<'a>(
        &'a self,
        record: &'a csv::ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> EvaluationResult<'a> {
        let mut bound_args = BoundArguments::with_capacity(self.args.len());

        for (i, arg) in self.args.iter().enumerate() {
            match arg {
                ConcreteArgument::Call(sub_function_call) => {
                    bound_args.push(sub_function_call.run(record, last_value, variables)?);
                }
                _ => bound_args.push(arg.bind(record, last_value, variables).map_err(|err| {
                    EvaluationError::Binding(SpecifiedBindingError {
                        function_name: self.name.to_string(),
                        arg_index: Some(i),
                        reason: err,
                    })
                })?),
            }
        }

        match (self.function)(bound_args) {
            Ok(value) => Ok(Cow::Owned(value)),
            Err(err) => Err(EvaluationError::Call(SpecifiedCallError {
                function_name: self.name.clone(),
                reason: err,
            })),
        }
    }
}

// NOTE: in older rust versions, Debug cannot be derived
// correctly from `fn` and it will not compile without
// this custom `Debug` implementation
impl fmt::Debug for ConcreteSubroutine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcreteSubroutine")
            .field("name", &self.name)
            .field("function", &"<function>")
            .field("args", &self.args)
            .finish()
    }
}

#[derive(Debug, Clone)]
enum StatementKind {
    If(bool),
}

impl StatementKind {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "if" => Some(Self::If(false)),
            "unless" => Some(Self::If(true)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcreteStatement {
    kind: StatementKind,
    args: Vec<ConcreteArgument>,
}

impl ConcreteStatement {
    fn run<'a>(
        &'a self,
        record: &'a csv::ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> EvaluationResult<'a> {
        match self.kind {
            StatementKind::If(reverse) => {
                let arity = self.args.len();

                if !(2..=3).contains(&arity) {
                    return Err(EvaluationError::Call(SpecifiedCallError {
                        function_name: "if".to_string(),
                        reason: CallError::from_range_arity(2, 3, arity),
                    }));
                }

                let condition = &self.args[0];
                let result = condition.evaluate(record, last_value, variables)?;

                let mut branch: Option<&ConcreteArgument> = None;

                let mut go_left = result.is_truthy();

                if reverse {
                    go_left = !go_left;
                }

                if go_left {
                    branch = Some(&self.args[1]);
                } else if arity == 3 {
                    branch = Some(&self.args[2]);
                }

                match branch {
                    None => Ok(Cow::Owned(DynamicValue::None)),
                    Some(arg) => arg.evaluate(record, last_value, variables),
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ConcreteFunctionCall {
    Subroutine(ConcreteSubroutine),
    SpecialStatement(ConcreteStatement),
}

impl ConcreteFunctionCall {
    fn run<'a>(
        &'a self,
        record: &'a csv::ByteRecord,
        last_value: &'a DynamicValue,
        variables: &'a Variables,
    ) -> EvaluationResult<'a> {
        match self {
            Self::Subroutine(subroutine) => subroutine.run(record, last_value, variables),
            Self::SpecialStatement(statement) => statement.run(record, last_value, variables),
        }
    }

    fn has_index_variable(&self) -> bool {
        let args = match self {
            Self::SpecialStatement(statement) => &statement.args,
            Self::Subroutine(subroutine) => &subroutine.args,
        };

        args.iter().any(|arg| arg.is_index_variable())
    }
}

type ConcretePipeline = Vec<ConcreteFunctionCall>;

fn concretize_argument(
    argument: Argument,
    headers: &ByteRecord,
) -> Result<ConcreteArgument, PrepareError> {
    Ok(match argument {
        Argument::Underscore => ConcreteArgument::Underscore,
        Argument::Null => ConcreteArgument::Null,
        Argument::BooleanLiteral(v) => ConcreteArgument::BooleanLiteral(DynamicValue::Boolean(v)),
        Argument::FloatLiteral(v) => ConcreteArgument::FloatLiteral(DynamicValue::Float(v)),
        Argument::IntegerLiteral(v) => ConcreteArgument::IntegerLiteral(DynamicValue::Integer(v)),
        Argument::StringLiteral(v) => ConcreteArgument::StringLiteral(DynamicValue::String(v)),
        Argument::Identifier(name) => {
            let indexation = ColumIndexationBy::Name(name);

            match indexation.find_column_index(headers) {
                Some(index) => ConcreteArgument::Column(index),
                None => return Err(PrepareError::ColumnNotFound(indexation)),
            }
        }
        Argument::SpecialIdentifier(name) => ConcreteArgument::Variable(name),
        Argument::Indexation(indexation) => match indexation.find_column_index(headers) {
            Some(index) => ConcreteArgument::Column(index),
            None => return Err(PrepareError::ColumnNotFound(indexation)),
        },
        Argument::RegexLiteral(pattern) => match Regex::new(&pattern) {
            Ok(regex) => ConcreteArgument::RegexLiteral(DynamicValue::Regex(regex)),
            Err(_) => return Err(PrepareError::InvalidRegex(pattern)),
        },
        Argument::Call(call) => {
            let mut concrete_args = Vec::new();

            for arg in call.args {
                concrete_args.push(concretize_argument(arg, headers)?);
            }

            let function_name = call.name.to_lowercase();

            if let Some(kind) = StatementKind::parse(&function_name) {
                ConcreteArgument::Call(ConcreteFunctionCall::SpecialStatement(ConcreteStatement {
                    kind,
                    args: concrete_args,
                }))
            } else {
                ConcreteArgument::Call(ConcreteFunctionCall::Subroutine(ConcreteSubroutine {
                    name: function_name.clone(),
                    function: get_function(&function_name)?,
                    args: concrete_args,
                }))
            }
        }
    })
}

fn concretize_pipeline(
    pipeline: Pipeline,
    headers: &ByteRecord,
) -> Result<ConcretePipeline, PrepareError> {
    let mut concrete_pipeline: ConcretePipeline = Vec::new();

    for function_call in pipeline {
        let mut concrete_arguments: Vec<ConcreteArgument> = Vec::new();

        for argument in function_call.args {
            concrete_arguments.push(concretize_argument(argument, headers)?);
        }

        let function_name = function_call.name.to_lowercase();

        concrete_pipeline.push(if let Some(kind) = StatementKind::parse(&function_name) {
            ConcreteFunctionCall::SpecialStatement(ConcreteStatement {
                kind,
                args: concrete_arguments,
            })
        } else {
            ConcreteFunctionCall::Subroutine(ConcreteSubroutine {
                name: function_name.clone(),
                function: get_function(&function_name)?,
                args: concrete_arguments,
            })
        });
    }

    Ok(concrete_pipeline)
}

// Example: trim(a) | add(a, b) | trim | add(a, b) | len -> add(a, b) | len
fn trim_pipeline(pipeline: Pipeline) -> Pipeline {
    match pipeline
        .iter()
        .enumerate()
        .rev()
        .find(|(i, function_call)| *i != 0 && !function_call.has_underscore())
        .map(|r| r.0)
    {
        None => pipeline,
        Some(index) => pipeline[index..].to_vec(),
    }
}

// Example: trim(a) | len | add(b, _) -> add(b, len(trim(a)))
// NOTE: we apply this as an optimization to avoid too much cloning
fn unfurl_pipeline(mut pipeline: Pipeline) -> Pipeline {
    loop {
        match pipeline.pop() {
            None => break,
            Some(mut function_call) => {
                if function_call.count_underscores() != 1 {
                    pipeline.push(function_call);
                    break;
                }
                match pipeline.pop() {
                    Some(previous_function_call) => {
                        function_call.fill_underscore(&previous_function_call);
                        pipeline.push(function_call);
                    }
                    None => {
                        pipeline.push(function_call);
                        break;
                    }
                }
            }
        }
    }

    pipeline
}

// TODO: we could validate function arity at prepare step
pub fn prepare(code: &str, headers: &ByteRecord) -> Result<ConcretePipeline, PrepareError> {
    match parse(code) {
        Err(_) => Err(PrepareError::ParseError(code.to_string())),
        Ok(pipeline) => {
            let pipeline = trim_pipeline(pipeline);
            let pipeline = unfurl_pipeline(pipeline);

            concretize_pipeline(pipeline, headers)
        }
    }
}

pub fn eval(
    pipeline: &ConcretePipeline,
    record: &ByteRecord,
    variables: &Variables,
) -> Result<DynamicValue, EvaluationError> {
    let mut last_value = DynamicValue::None;

    for function_call in pipeline {
        last_value = function_call
            .run(record, &last_value, variables)?
            .into_owned();
    }

    Ok(last_value)
}

#[derive(Clone)]
pub struct Program<'a> {
    pipeline: ConcretePipeline,
    variables: Variables<'a>,
    should_bind_index: bool,
}

impl<'a> Program<'a> {
    pub fn parse(code: &str, headers: &ByteRecord) -> Result<Self, PrepareError> {
        let pipeline = prepare(code, headers)?;
        let should_bind_index = pipeline.iter().any(|call| call.has_index_variable());

        Ok(Program {
            pipeline,
            variables: Variables::new(),
            should_bind_index,
        })
    }

    pub fn run(&self, record: &ByteRecord) -> Result<DynamicValue, EvaluationError> {
        eval(&self.pipeline, record, &self.variables)
    }

    pub fn set<'b>(&'b mut self, key: &'a str, value: DynamicValue) {
        if key == "index" && !self.should_bind_index {
            return;
        }

        self.variables.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::super::error::RunError;
    use super::super::parser::FunctionCall;
    use super::*;

    #[test]
    fn test_trim_pipeline() {
        // Should give: add(a, b) | len
        let pipeline = parse("trim(a) | add(a, b) | trim | add(a, b) | len").unwrap();
        let pipeline = trim_pipeline(pipeline);

        assert_eq!(
            pipeline,
            vec![
                FunctionCall {
                    name: "add".to_string(),
                    args: vec![
                        Argument::Identifier("a".to_string()),
                        Argument::Identifier("b".to_string())
                    ]
                },
                FunctionCall {
                    name: "len".to_string(),
                    args: vec![Argument::Underscore]
                }
            ]
        );

        let pipeline = parse("trim(a) | len | add(b, _)").unwrap();
        let pipeline = trim_pipeline(pipeline);

        assert_eq!(
            pipeline,
            vec![
                FunctionCall {
                    name: "trim".to_string(),
                    args: vec![Argument::Identifier("a".to_string())]
                },
                FunctionCall {
                    name: "len".to_string(),
                    args: vec![Argument::Underscore]
                },
                FunctionCall {
                    name: "add".to_string(),
                    args: vec![Argument::Identifier("b".to_string()), Argument::Underscore]
                }
            ]
        );
    }

    #[test]
    fn test_unfurl_pipeline() {
        // Should give: add(b, len(trim(a)))
        let pipeline = parse("trim(a) | len | add(b, _)").unwrap();
        let pipeline = unfurl_pipeline(pipeline);

        assert_eq!(
            pipeline,
            vec![FunctionCall {
                name: "add".to_string(),
                args: vec![
                    Argument::Identifier("b".to_string()),
                    Argument::Call(FunctionCall {
                        name: "len".to_string(),
                        args: vec![Argument::Call(FunctionCall {
                            name: "trim".to_string(),
                            args: vec![Argument::Identifier("a".to_string())]
                        })]
                    })
                ]
            }]
        );
    }

    type TestResult = Result<DynamicValue, RunError>;

    fn eval_code(code: &str) -> TestResult {
        let mut headers = ByteRecord::new();
        headers.push_field(b"name");
        headers.push_field(b"surname");
        headers.push_field(b"a");
        headers.push_field(b"b");

        let mut program = Program::parse(code, &headers).map_err(RunError::Prepare)?;

        let mut record = ByteRecord::new();
        record.push_field(b"john");
        record.push_field(b"SMITH");
        record.push_field(b"34");
        record.push_field(b"62");

        program.set(&"index", DynamicValue::Integer(2));
        program.run(&record).map_err(RunError::Evaluation)
    }

    #[test]
    fn test_pipeline_optimization_correctness() {
        assert_eq!(
            eval_code("trim(a) | add(a, b) | trim | add(a, b) | len"),
            Ok(DynamicValue::Integer(2))
        );

        assert_eq!(
            eval_code("trim(a) | len | add(b, _)"),
            Ok(DynamicValue::Integer(64))
        );
    }

    #[test]
    fn test_variable_binding() {
        assert_eq!(eval_code("add(%index, 2)"), Ok(DynamicValue::from(4)));
    }

    #[test]
    fn test_typeof() {
        assert_eq!(eval_code("typeof(name)"), Ok(DynamicValue::from("string")));
        assert_eq!(eval_code("TYPEOF(name)"), Ok(DynamicValue::from("string")));
    }

    #[test]
    fn test_split_join() {
        assert_eq!(
            eval_code("split(name, 'o')"),
            Ok(DynamicValue::List(vec![
                DynamicValue::from("j"),
                DynamicValue::from("hn"),
            ]))
        );

        assert_eq!(
            eval_code("split(name, 'o', 1)"),
            Ok(DynamicValue::List(vec![
                DynamicValue::from("j"),
                DynamicValue::from("hn"),
            ]))
        );

        assert_eq!(
            eval_code("split(name, 'o') | join(_, '&')"),
            Ok(DynamicValue::from("j&hn"))
        )
    }

    #[test]
    fn test_arithmetics() {
        assert_eq!(eval_code("add(1, 2)"), Ok(DynamicValue::Integer(3)));
        assert_eq!(eval_code("sub(1, 2)"), Ok(DynamicValue::Integer(-1)));
        assert_eq!(eval_code("mul(1, 2)"), Ok(DynamicValue::Integer(2)));
        assert_eq!(eval_code("mul(3, 1.5)"), Ok(DynamicValue::Float(4.5)));
        assert_eq!(eval_code("div(3, 2)"), Ok(DynamicValue::Float(1.5)));
        assert_eq!(eval_code("idiv(4.5, 2)"), Ok(DynamicValue::Integer(2)));
        assert_eq!(eval_code("idiv(-4.5, 2)"), Ok(DynamicValue::Integer(-3)));
    }

    #[test]
    fn test_lower() {
        assert_eq!(eval_code("lower(surname)"), Ok(DynamicValue::from("smith")));
    }

    #[test]
    fn test_upper() {
        assert_eq!(eval_code("upper(name)"), Ok(DynamicValue::from("JOHN")));
    }

    #[test]
    fn test_count() {
        assert_eq!(eval_code("count(name, 'h')"), Ok(DynamicValue::Integer(1)));
    }

    #[test]
    fn test_concat() {
        assert_eq!(
            eval_code("concat(name, ' ', lower(surname))"),
            Ok(DynamicValue::from("john smith"))
        );
    }

    #[test]
    fn test_coalesce() {
        assert_eq!(
            eval_code("coalesce(null, false, 'test')"),
            Ok(DynamicValue::from("test"))
        );
    }

    #[test]
    fn test_bool() {
        assert_eq!(eval_code("not(true)"), Ok(DynamicValue::from(false)));
        assert_eq!(eval_code("and(true, false)"), Ok(DynamicValue::from(false)));
        assert_eq!(eval_code("or(true, false)"), Ok(DynamicValue::from(true)));
    }

    #[test]
    fn test_number_comparison() {
        assert_eq!(eval_code("eq(3, 4)"), Ok(DynamicValue::Boolean(false)));
        assert_eq!(eval_code("eq(4, 4)"), Ok(DynamicValue::Boolean(true)));
        assert_eq!(eval_code("eq(3, '3')"), Ok(DynamicValue::Boolean(true)));

        assert_eq!(eval_code("neq(3, 2)"), Ok(DynamicValue::Boolean(true)));
        assert_eq!(eval_code("lt(3, 2)"), Ok(DynamicValue::Boolean(false)));
        assert_eq!(eval_code("lte(3, 2)"), Ok(DynamicValue::Boolean(false)));
        assert_eq!(eval_code("gt(3, 2)"), Ok(DynamicValue::Boolean(true)));
        assert_eq!(eval_code("gte(3, 2)"), Ok(DynamicValue::Boolean(true)));
    }

    #[test]
    fn test_pathjoin() {
        assert_eq!(
            eval_code("pathjoin('one', 'two', 'three')"),
            Ok(DynamicValue::from("one/two/three"))
        );
    }

    #[test]
    fn test_first() {
        assert_eq!(eval_code("first(name)"), Ok(DynamicValue::from("j")));
        assert_eq!(
            eval_code("first(split(name, 'h', 1))"),
            Ok(DynamicValue::from("jo"))
        );
    }

    #[test]
    fn test_last() {
        assert_eq!(eval_code("last(name)"), Ok(DynamicValue::from("n")));
        assert_eq!(
            eval_code("last(split(name, 'o', 1))"),
            Ok(DynamicValue::from("hn"))
        );
    }

    #[test]
    fn test_slice() {
        assert_eq!(
            eval_code("slice('abcde', 2)"),
            Ok(DynamicValue::from("cde"))
        );
        assert_eq!(
            eval_code("slice('abcde', -2)"),
            Ok(DynamicValue::from("de"))
        );
        assert_eq!(
            eval_code("slice('abcde', -1, 3)"),
            Ok(DynamicValue::from(""))
        );
        assert_eq!(
            eval_code("slice('abcde', -1, -3)"),
            Ok(DynamicValue::from(""))
        );
        assert_eq!(
            eval_code("slice('abcde', 1, 3)"),
            Ok(DynamicValue::from("bc"))
        );
        assert_eq!(
            eval_code("slice('abcde', 1, -2)"),
            Ok(DynamicValue::from("bc"))
        );
        assert_eq!(eval_code("slice('abcde', 5)"), Ok(DynamicValue::from("")));
        assert_eq!(eval_code("slice('abcde', 10)"), Ok(DynamicValue::from("")));
        assert_eq!(
            eval_code("slice('abcde', -10)"),
            Ok(DynamicValue::from("abcde"))
        );
        assert_eq!(
            eval_code("slice('abcde', 10, -20)"),
            Ok(DynamicValue::from(""))
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(eval_code("trim(' test ')"), Ok(DynamicValue::from("test")));
        assert_eq!(
            eval_code("ltrim(' test ')"),
            Ok(DynamicValue::from("test "))
        );
        assert_eq!(
            eval_code("rtrim(' test ')"),
            Ok(DynamicValue::from(" test"))
        );

        assert_eq!(eval_code("trim('test', 't')"), Ok(DynamicValue::from("es")));
        assert_eq!(
            eval_code("ltrim('test', 't')"),
            Ok(DynamicValue::from("est"))
        );
        assert_eq!(
            eval_code("rtrim('test', 't')"),
            Ok(DynamicValue::from("tes"))
        );
    }

    #[test]
    fn test_abs() {
        assert_eq!(eval_code("abs(-5)"), Ok(DynamicValue::Integer(5)));
        assert_eq!(eval_code("abs(-5.0)"), Ok(DynamicValue::Float(5.0)));
    }

    #[test]
    fn test_match() {
        assert_eq!(
            eval_code("match('hello', /l{2}/)"),
            Ok(DynamicValue::from(true))
        );
        assert_eq!(
            eval_code("match('hello', /l{3}/)"),
            Ok(DynamicValue::from(false))
        );
        assert_eq!(
            eval_code("match('hello', /L{2}/i)"),
            Ok(DynamicValue::from(true))
        );
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            eval_code("replace('hello', 'l', 't')"),
            Ok(DynamicValue::from("hetto"))
        );
        assert_eq!(
            eval_code("replace('hello', /l+O/i, 't')"),
            Ok(DynamicValue::from("het"))
        );
        assert_eq!(
            eval_code("replace('hello', /(he)llo/i, '$1')"),
            Ok(DynamicValue::from("he"))
        );
    }

    #[test]
    fn test_if() {
        assert_eq!(eval_code("if(true, 3, 2)"), Ok(DynamicValue::from(3)));
        assert_eq!(
            eval_code("if(eq(2, 2), add(1, 3), sub(1, 0))"),
            Ok(DynamicValue::from(4))
        );
        assert_eq!(
            eval_code("if(if(if(true, true), true), if(false, add(1, 2), add(4, 5)))"),
            Ok(DynamicValue::from(9))
        );
    }

    #[test]
    fn test_unless() {
        assert_eq!(eval_code("unless(true, 3, 2)"), Ok(DynamicValue::from(2)));
    }

    #[test]
    fn test_compact() {
        assert_eq!(
            eval_code("compact(split('', '|'))"),
            Ok(DynamicValue::from(vec![]))
        );
    }
}
