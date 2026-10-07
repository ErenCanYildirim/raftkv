//! The line-based text protocol spoken between clients and nodes.
//!
//! Requests (one per line, command names are case-insensitive):
//!
//! ```text
//! SET <key> <value>     value is the rest of the line and may contain spaces
//! GET <key>
//! DEL <key>
//! STATUS
//! ```
//!
//! Replies (one per line):
//!
//! ```text
//! OK | VALUE <v> | NIL | DELETED 1 | DELETED 0 | REDIRECT <addr> | STATUS <info> | ERR <message>
//! ```

use crate::types::{ClientOp, ClientReply};

/// Error message sent when no leader is known. Clients treat it as retryable.
pub const NO_LEADER: &str = "no leader known; retry";

/// Parse one request line.
///
/// # Errors
/// Returns a human-readable message if the line is not a valid request.
pub fn parse_request(line: &str) -> Result<ClientOp, String> {
    let line = line.trim();
    let (command, rest) = match line.split_once(char::is_whitespace) {
        Some((command, rest)) => (command, rest.trim_start()),
        None => (line, ""),
    };

    match command.to_ascii_uppercase().as_str() {
        "" => Err("empty command".to_string()),
        "GET" => single_key(rest, "GET").map(|key| ClientOp::Get { key }),
        "DEL" => single_key(rest, "DEL").map(|key| ClientOp::Del { key }),
        "SET" => {
            let usage = "usage: SET <key> <value>";
            let (key, value) = rest.split_once(char::is_whitespace).ok_or(usage)?;
            let value = value.trim_start();
            if value.is_empty() {
                return Err(usage.to_string());
            }
            Ok(ClientOp::Set {
                key: key.to_string(),
                value: value.to_string(),
            })
        }
        "STATUS" if rest.is_empty() => Ok(ClientOp::Status),
        "STATUS" => Err("usage: STATUS".to_string()),
        other => Err(format!(
            "unknown command '{other}' (use SET, GET, DEL, STATUS)"
        )),
    }
}

fn single_key(rest: &str, command: &str) -> Result<String, String> {
    if rest.is_empty() || rest.contains(char::is_whitespace) {
        return Err(format!("usage: {command} <key>"));
    }
    Ok(rest.to_string())
}

/// Render a reply as one line (without the trailing newline).
#[must_use]
pub fn render_reply(reply: &ClientReply) -> String {
    match reply {
        ClientReply::Ok => "OK".to_string(),
        ClientReply::Value(Some(value)) => format!("VALUE {value}"),
        ClientReply::Value(None) => "NIL".to_string(),
        ClientReply::Deleted(true) => "DELETED 1".to_string(),
        ClientReply::Deleted(false) => "DELETED 0".to_string(),
        ClientReply::Redirect(addr) => format!("REDIRECT {addr}"),
        ClientReply::Status(info) => format!("STATUS {info}"),
        ClientReply::Error(message) => format!("ERR {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_commands_case_insensitively() {
        assert_eq!(
            parse_request("get name"),
            Ok(ClientOp::Get { key: "name".into() })
        );
        assert_eq!(
            parse_request("Del name"),
            Ok(ClientOp::Del { key: "name".into() })
        );
        assert_eq!(parse_request("STATUS"), Ok(ClientOp::Status));
    }

    #[test]
    fn set_value_keeps_inner_spaces() {
        assert_eq!(
            parse_request("SET greeting hello   big world"),
            Ok(ClientOp::Set {
                key: "greeting".into(),
                value: "hello   big world".into()
            })
        );
    }

    #[test]
    fn rejects_malformed_requests() {
        assert!(parse_request("").is_err());
        assert!(parse_request("SET onlykey").is_err());
        assert!(parse_request("GET").is_err());
        assert!(parse_request("GET a b").is_err());
        assert!(parse_request("STATUS extra").is_err());
        assert!(parse_request("FLY away").is_err());
    }

    #[test]
    fn renders_replies() {
        assert_eq!(
            render_reply(&ClientReply::Value(Some("v".into()))),
            "VALUE v"
        );
        assert_eq!(render_reply(&ClientReply::Value(None)), "NIL");
        assert_eq!(render_reply(&ClientReply::Deleted(true)), "DELETED 1");
        assert_eq!(
            render_reply(&ClientReply::Redirect("127.0.0.1:8001".into())),
            "REDIRECT 127.0.0.1:8001"
        );
    }
}
