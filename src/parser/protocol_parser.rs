use super::core::*;

type FirstByteResult<'a> = Option<(&'a str, &'a str)>;

const SIMPLE_STRING_FB: &str = "+";
const SIMPLE_ERROR_NULL_FB: &str = "-";
const INTEGER_FB: &str = ":";
const BULK_STRINGS_FB: &str = "$";
const ARRAY_FB: &str = "*";
const BOOLEAN_FB: &str = "#";
const DOUBLE_FB: &str = ",";
const BIG_NUMBER_FB: &str = "(";
const BULK_ERRORS_FB: &str = "!";
const VERBATIM_STRINGS_FB: &str = "=";
const MAP_FB: &str = "%";
const ATTRIBUTE_FB: &str = "|";
const SET_FB: &str = "~";
const PUSH_FB: &str = ">";

const FB: [&str; 14] = [
    SIMPLE_STRING_FB,
    SIMPLE_ERROR_NULL_FB,
    INTEGER_FB,
    BULK_STRINGS_FB,
    ARRAY_FB,
    BOOLEAN_FB,
    DOUBLE_FB,
    BIG_NUMBER_FB,
    BULK_ERRORS_FB,
    VERBATIM_STRINGS_FB,
    MAP_FB,
    ATTRIBUTE_FB,
    SET_FB,
    PUSH_FB,
];

#[derive(PartialEq, Debug, Clone)]
pub struct SimpleString {
    pub value: String,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Array {
    pub value: Vec<ParsedSegment>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Integer {
    pub value: i64,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParsedSegment {
    SimpleString(SimpleString),
    Integer(Integer),
    Array(Array),
}

type ParseCommandResult<'a> = Result<(&'a str, ParsedSegment), (&'a str, String)>;

fn parse_first_byte(source: &str) -> FirstByteResult<'_> {
    let mut result = None;
    for s in FB {
        let parser = parser_literal(s).parse(source);
        if let Ok((rest, _)) = parser {
            result = Some((rest, s));
            break;
        }
    }

    result
}

fn pars_string(source: &str) -> Result<(&str, &str), (&str, String)> {
    identifier(source)
}

pub fn parse(source: &'_ str) -> ParseCommandResult<'_> {
    let mut result: ParseCommandResult = Err((source, "Parsing failed".into()));
    if let Some((rest, first_byte)) = parse_first_byte(source) {
        match first_byte {
            SIMPLE_STRING_FB => {
                let (rest, string) = pars_string(rest)?;
                result = Ok((
                    rest,
                    ParsedSegment::SimpleString(SimpleString {
                        value: string.into(),
                    }),
                ));
            }
            SIMPLE_ERROR_NULL_FB => {
                todo!();
            }
            INTEGER_FB => {
                let (rest, int) = integer(rest)?;
                result = Ok((rest, ParsedSegment::Integer(Integer { value: int })));
            }
            BULK_STRINGS_FB => {
                todo!();
            }
            ARRAY_FB => {
                let mut array: Vec<ParsedSegment> = Vec::new();
                let mut count = 0;
                let (rest, length) = integer(rest)?;
                let (mut re, _) = parser_literal("\r\n").parse(rest)?;
                while count < length {
                    let (rest, _) = parser_literal("$").parse(re)?;
                    let (rest, _val_length) = integer(rest)?;
                    let (rest, _) = parser_literal("\r\n").parse(rest)?;
                    let (rest, e) = either(identifier, integer).parse(rest)?;
                    if count + 1 < length {
                        let (r, _) = parser_literal("\r\n").parse(rest)?;
                        re = r;
                    } else {
                        re = rest;
                    }

                    let val = match e {
                        Either::Left(l) => ParsedSegment::SimpleString(SimpleString {
                            value: l.to_string(),
                        }),
                        Either::Right(r) => ParsedSegment::Integer(Integer { value: r }),
                    };

                    array.push(val);

                    count += 1;
                }

                result = Ok((re, ParsedSegment::Array(Array { value: array })))
            }
            BOOLEAN_FB => {
                todo!();
            }
            DOUBLE_FB => {
                todo!();
            }
            BIG_NUMBER_FB => {
                todo!();
            }
            BULK_ERRORS_FB => {
                todo!();
            }
            VERBATIM_STRINGS_FB => {
                todo!();
            }
            MAP_FB => {
                todo!();
            }
            ATTRIBUTE_FB => {
                todo!();
            }
            SET_FB => {
                todo!();
            }
            PUSH_FB => {
                todo!();
            }
            _ => {
                todo!()
            }
        }
    } else {
        todo!()
    }

    let (rest, result) = result.unwrap();

    let (rest, _) = parser_literal("\r\n").parse(rest)?;

    Ok((rest, result))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_string() {
        let code = "+PING\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");

        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::SimpleString(SimpleString {
                value: "PING".into()
            })
        )
    }

    #[test]
    fn parse_positive_integer() {
        let code = ":+123\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");

        assert_eq!(rest, "");
        assert_eq!(result, ParsedSegment::Integer(Integer { value: 123 }))
    }

    #[test]
    fn parse_negative_integer() {
        let code = ":-123\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");

        assert_eq!(rest, "");
        assert_eq!(result, ParsedSegment::Integer(Integer { value: -123 }))
    }

    #[test]
    fn parse_signless_integer() {
        let code = ":123\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");

        assert_eq!(rest, "");
        assert_eq!(result, ParsedSegment::Integer(Integer { value: 123 }))
    }

    #[test]
    fn parse_command() {
        let code = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");
        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::Array(Array {
                value: vec![
                    ParsedSegment::SimpleString(SimpleString {
                        value: "ECHO".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "hey".into()
                    }),
                ]
            })
        )
    }

    #[test]
    fn parse_command_with_expiry() {
        let code =
            "*5\r\n$3\r\nSET\r\n$5\r\napple\r\n$10\r\nstrawberry\r\n$2\r\npx\r\n$3\r\n100\r\n";
        let (rest, result) = parse(code).expect("Parsing failed");
        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::Array(Array {
                value: vec![
                    ParsedSegment::SimpleString(SimpleString {
                        value: "SET".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "apple".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "strawberry".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString { value: "px".into() }),
                    ParsedSegment::Integer(Integer { value: 100 }),
                ]
            })
        )
    }

    #[test]
    fn parse_command_with_expiry_bytes() {
        let code =
            "*5\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$9\r\npineapple\r\n$2\r\npx\r\n$3\r\n100\r\n";
        let bytes = String::from_utf8_lossy(code.as_bytes());
        let (rest, result) = parse(&bytes).expect("Parsing failed");
        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::Array(Array {
                value: vec![
                    ParsedSegment::SimpleString(SimpleString {
                        value: "SET".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "apple".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "strawberry".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString { value: "px".into() }),
                    ParsedSegment::Integer(Integer { value: 100 }),
                ]
            })
        )
    }

    #[test]
    fn parse_array() {
        let code = "*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n";
        let bytes = String::from_utf8_lossy(code.as_bytes());
        let (rest, result) = parse(&bytes).expect("Parsing failed");
        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::Array(Array {
                value: vec![
                    ParsedSegment::SimpleString(SimpleString {
                        value: "GET".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "strawberry".into()
                    }),
                ]
            })
        )
    }

    #[test]
    fn parse_array_1() {
        let code = "*3\r\n$3\r\nSET\r\n$4\r\npear\r\n$10\r\nstrawberry\r\n";
        let bytes = String::from_utf8_lossy(code.as_bytes());
        let (rest, result) = parse(&bytes).expect("Parsing failed");
        assert_eq!(rest, "");
        assert_eq!(
            result,
            ParsedSegment::Array(Array {
                value: vec![
                    ParsedSegment::SimpleString(SimpleString {
                        value: "SET".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "pear".into()
                    }),
                    ParsedSegment::SimpleString(SimpleString {
                        value: "strawberry".into()
                    }),
                ]
            })
        )
    }
}
