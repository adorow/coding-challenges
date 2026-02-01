use crate::protocol::RespObject::{Array, BulkString, Error, Integer, NullArray, NullBulkString, SimpleString};
use std::fmt::Display;
use std::str::FromStr;

// todo: should they all be references? should they all own the data?
// todo: and then: are lifetimes needed (if using refs, probably yes)
#[derive(Debug, Eq, PartialEq)]
pub enum RespObject {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    NullBulkString,
    Array(Vec<RespObject>),
    NullArray,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RespObjectParseError {
    pub message: String,
}

impl FromStr for RespObject {
    type Err = RespObjectParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut input = &input[..];

        parse_(&mut input)
    }
}

// ===== Parsing (deserialising) logic =====

fn parse_(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let c = &input[..1];

    *input = &input[1..];
    match c {
        "+" => parse_simple_string(input),
        "-" => parse_error(input),
        ":" => parse_integer(input),
        "$" => parse_bulk_string(input),
        "*" => parse_array(input),
        _ => Err(RespObjectParseError {
            message: format!("Unexpected RESP type character: '{c}'"),
        }),
    }
}

fn parse_simple_string(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let text = read_until_cr(input)?;
    skip_crlf(input)?;

    Ok(SimpleString(text))
}

fn parse_error(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let text = read_until_cr(input)?;
    skip_crlf(input)?;

    Ok(Error(text))
}

fn parse_integer(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let text = read_until_cr(input)?;
    skip_crlf(input)?;

    text.parse::<i64>()
        .map_err(|_| RespObjectParseError { message: format!("Failed to parse integer '{text}'") } )
        .map(|int| Integer(int))
}

fn parse_bulk_string(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let length = read_length(input)?;
    let result = match length {
        -1 => NullBulkString,
        _ => {
            let text = read_until_length(input, length as usize)?;
            skip_crlf(input)?;
            BulkString(text)
        }
    };
    Ok(result)
}

fn parse_array(
    input: &mut &str,
) -> Result<RespObject, RespObjectParseError> {
    let length = read_length(input)?;
    let result = match length {
        -1 => NullArray,
        _ => {
            let mut array = Vec::new();
            for _ in 0..length {
                let resp_object = parse_(input)?;
                array.push(resp_object);
            }
            Array(array)
        }
    };
    Ok(result)
}

fn read_until_cr(
    input: &mut &str,
) -> Result<String, RespObjectParseError> {
    let end_word_index = input.find('\r').ok_or_else(|| RespObjectParseError { message: String::from("Unexpected end of input") })?;

    let word = String::from(&input[..end_word_index]);

    *input = &input[end_word_index..];

    Ok(word)
}

fn read_until_length(
    input: &mut &str,
    length: usize,
) -> Result<String, RespObjectParseError> {
    let word = String::from(&input[..length]);

    *input = &input[length..];

    Ok(word)
}

fn read_length(
    input: &mut &str,
) -> Result<i64, RespObjectParseError> {
    let text = read_until_cr(input)?;
    skip_crlf(input)?;

    let length = text
        .parse::<i64>()
        .map_err(|_| RespObjectParseError { message: format!("Failed to parse length '{text}'"), })
        .map(|int| int)?;


    if length < -1 {
        return Err(RespObjectParseError { message: format!("Expected length to be -1 or non-negative, got: '{text}'") });
    }
    Ok(length)
}

// just consumes the CRLF (\r\n) characters from the iterator, or fails otherwise
fn skip_crlf(
    input: &mut &str,
) -> Result<(), RespObjectParseError> {
    let crlf = &input[..2];
    if crlf != "\r\n" {
        return Err(RespObjectParseError {
            message: format!("Expected \\r\\n but got something else: {crlf}"),
        });
    }

    *input = &input[2..];

    Ok(())
}

// todo: using ToString/Display for serialisation now for simplicity, may need something better/more performant
impl Display for RespObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            SimpleString(value) => format!("+{value}\r\n"),
            Error(message) => format!("-{message}\r\n"),
            Integer(value) => format!(":{value}\r\n"),
            BulkString(value) => format!("${}\r\n{}\r\n", value.len(), value),
            NullBulkString => "$-1\r\n".to_string(),
            Array(entries) => format!("*{}\r\n{}", entries.len(), entries.iter().map(|e| e.to_string()).collect::<String>()),
            NullArray => "*-1\r\n".to_string(),
        };
        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod deserialization_tests {
    use super::*;

    #[test]
    fn parse_simple_string() {
        let result = RespObject::from_str("+Hello, World\r\n");
        assert_eq!(result, Ok(SimpleString("Hello, World".to_owned())));
    }

    #[test]
    fn fail_parse_simple_string_on_missing_crlf() {
        let result = RespObject::from_str("+Hello, World");
        assert!(result.is_err());
    }

    #[test]
    fn parse_error() {
        let result = RespObject::from_str("-Error message\r\n");
        assert_eq!(result, Ok(Error("Error message".to_owned())));
    }

    #[test]
    fn parse_integer() {
        let result = RespObject::from_str(":42\r\n");
        assert_eq!(result, Ok(Integer(42)));
    }

    #[test]
    fn parse_integer_with_explicit_plus_sign() {
        let result = RespObject::from_str(":+42\r\n");
        assert_eq!(result, Ok(Integer(42)));
    }

    #[test]
    fn parse_negative_integer() {
        let result = RespObject::from_str(":-10\r\n");
        assert_eq!(result, Ok(Integer(-10)));
    }

    #[test]
    fn fail_parse_integer_on_missing_content() {
        let result = RespObject::from_str(":\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn fail_parse_integer_on_non_numerical_input() {
        let result = RespObject::from_str(":NotANumber\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_bulk_string() {
        let result = RespObject::from_str("$0\r\n\r\n");
        assert_eq!(result, Ok(BulkString("".to_owned())));
    }

    #[test]
    fn parse_bulk_string() {
        let result = RespObject::from_str("$6\r\nfoobar\r\n");
        assert_eq!(result, Ok(BulkString("foobar".to_owned())));
    }

    #[test]
    fn parse_multiline_bulk_string() {
        let result = RespObject::from_str("$8\r\nfoo\r\nbar\r\n");
        assert_eq!(result, Ok(BulkString("foo\r\nbar".to_owned())));
    }

    #[test]
    fn parse_null_bulk_string() {
        let result = RespObject::from_str("$-1\r\n");
        assert_eq!(result, Ok(NullBulkString));
    }

    #[test]
    fn parse_empty_array() {
        let result = RespObject::from_str("*0\r\n");
        assert_eq!(result, Ok(Array(vec![])));
    }

    #[test]
    fn parse_array_example1() {
        let result = RespObject::from_str("*1\r\n$4\r\nping\r\n");
        assert_eq!(result, Ok(Array(vec![BulkString("ping".to_owned())])));
    }

    #[test]
    fn parse_array_example2() {
        let result = RespObject::from_str("*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n");
        assert_eq!(
            result,
            Ok(Array(vec![
                BulkString("echo".to_owned()),
                BulkString("hello world".to_owned())
            ]))
        );
    }

    #[test]
    fn parse_array_example3() {
        let result = RespObject::from_str("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n");
        assert_eq!(
            result,
            Ok(Array(vec![
                BulkString("get".to_owned()),
                BulkString("key".to_owned())
            ]))
        );
    }

    #[test]
    fn parse_null_array() {
        let result = RespObject::from_str("*-1\r\n");
        assert_eq!(result, Ok(NullArray));
    }

    #[test]
    fn fail_parse_on_unexpected_type_indicator() {
        let result = RespObject::from_str("?What is this\r\n");
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod serialization_tests {
    use super::*;

    #[test]
    fn write_simple_string() {
        let result = SimpleString("Hello, World".to_owned()).to_string();
        assert_eq!(result, "+Hello, World\r\n");
    }

    #[test]
    fn write_error() {
        let result = Error("Error message".to_owned()).to_string();
        assert_eq!(result, "-Error message\r\n");
    }

    #[test]
    fn write_integer() {
        let result = Integer(42).to_string();
        assert_eq!(result, ":42\r\n");
    }

    #[test]
    fn write_negative_integer() {
        let result = Integer(-10).to_string();
        assert_eq!(result, ":-10\r\n");
    }

    #[test]
    fn write_empty_bulk_string() {
        let result = BulkString("".to_owned()).to_string();
        assert_eq!(result, "$0\r\n\r\n");
    }

    #[test]
    fn write_bulk_string() {
        let result = BulkString("foobar".to_owned()).to_string();
        assert_eq!(result, "$6\r\nfoobar\r\n");
    }

    #[test]
    fn write_multiline_bulk_string() {
        let result = BulkString("foo\r\nbar".to_owned()).to_string();
        assert_eq!(result, "$8\r\nfoo\r\nbar\r\n");
    }

    #[test]
    fn write_null_bulk_string() {
        let result = NullBulkString.to_string();
        assert_eq!(result, "$-1\r\n");
    }

    #[test]
    fn write_empty_array() {
        let result = Array(vec![]).to_string();
        assert_eq!(result, "*0\r\n");
    }

    #[test]
    fn write_array_example1() {
        let result = Array(vec![BulkString("ping".to_owned())]).to_string();
        assert_eq!(result, "*1\r\n$4\r\nping\r\n");
    }

    #[test]
    fn write_array_example2() {
        let result = Array(vec![
            BulkString("echo".to_owned()),
            BulkString("hello world".to_owned())
        ]).to_string();
        assert_eq!(result, "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n");
    }

    #[test]
    fn write_array_example3() {
        let result = Array(vec![
            BulkString("get".to_owned()),
            BulkString("key".to_owned())
        ]).to_string();
        assert_eq!(result, "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n");
    }

    #[test]
    fn write_null_array() {
        let result = NullArray.to_string();
        assert_eq!(result, "*-1\r\n");
    }
}
