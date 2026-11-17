use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMessage {
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default)]
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub status: String,
    pub action: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    pub server_id: String,
    pub timestamp: String,
}

#[derive(Debug, Clone)]
pub struct ScriptEvent {
    pub event_type: String,
    pub user: String,
    pub channel: Option<String>,
    pub content: Option<String>,
}

#[derive(Serialize)]
struct HistoryEntry<'a, T: Serialize> {
    logged_at: String,
    actor: &'a str,
    direction: &'a str,
    payload: &'a T,
}

pub fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn is_valid_user(user: &str) -> bool {
    let len = user.len();
    (3..=24).contains(&len)
        && user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

pub fn is_valid_channel(channel: &str) -> bool {
    let len = channel.len();
    (2..=32).contains(&len)
        && channel
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

pub fn clean_script_text(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'^' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            if next == b'O' {
                i += 2;
                continue;
            } else if (next == b'W' || next == b'M') && i + 2 < bytes.len() && bytes[i + 2].is_ascii_digit() {
                i += 3;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                continue;
            }
        }
        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn parse_script_events(filepath: &str) -> Vec<ScriptEvent> {
    let Ok(raw) = fs::read_to_string(filepath) else {
        return Vec::new();
    };

    let mut events = Vec::new();
    let mut current_channel = "geral".to_string();

    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("---") || line.starts_with('!') {
            continue;
        }

        if line.starts_with("#T:") || line.starts_with("##T:") {
            if let Some((_, action_part)) = line.split_once(':') {
                let action_text = clean_script_text(action_part);
                if !action_text.is_empty() {
                    events.push(ScriptEvent {
                        event_type: "post_message".to_string(),
                        user: "system_irc".to_string(),
                        channel: Some(current_channel.clone()),
                        content: Some(format!("* {}", action_text)),
                    });
                }
            }
            continue;
        }

        if let Some(joined_user) = line.strip_prefix("#j") {
            let u = joined_user.trim();
            if is_valid_user(u) {
                events.push(ScriptEvent {
                    event_type: "login".to_string(),
                    user: u.to_string(),
                    channel: None,
                    content: None,
                });
            }
            continue;
        }

        if line.starts_with("#k") || line.starts_with("#l") {
            continue;
        }

        if let Some((speaker_raw, msg_raw)) = line.split_once(':') {
            let speaker = speaker_raw.trim();
            let cleaned_msg = clean_script_text(msg_raw);
            if cleaned_msg.is_empty() || !is_valid_user(speaker) {
                continue;
            }

            if cleaned_msg.to_lowercase().contains("#hacking") && current_channel != "hacking" {
                current_channel = "hacking".to_string();
                events.push(ScriptEvent {
                    event_type: "create_channel".to_string(),
                    user: speaker.to_string(),
                    channel: Some("hacking".to_string()),
                    content: None,
                });
            }

            events.push(ScriptEvent {
                event_type: "post_message".to_string(),
                user: speaker.to_string(),
                channel: Some(current_channel.clone()),
                content: Some(cleaned_msg),
            });
        }
    }

    events
}

pub fn pack_request(req: &RequestMessage) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(req)
}

pub fn unpack_request(bytes: &[u8]) -> Result<RequestMessage, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

pub fn pack_response(rep: &ResponseMessage) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(rep)
}

pub fn unpack_response(bytes: &[u8]) -> Result<ResponseMessage, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

fn record_message_history<T: Serialize>(actor: &str, direction: &str, payload: &T, summary: &str) -> String {
    let logged_at = now_iso();
    let log_file =
        env::var("MESSAGE_LOG_FILE").unwrap_or_else(|_| format!("/logs/{}.jsonl", actor));
    let transcript_file = "/logs/transcricao_chat.log";

    if let Some(parent) = Path::new(&log_file).parent() {
        let _ = fs::create_dir_all(parent);
    }

    let entry = HistoryEntry {
        logged_at: logged_at.clone(),
        actor,
        direction,
        payload,
    };

    if let Ok(line) = serde_json::to_string(&entry) {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_file) {
            let _ = writeln!(file, "{}", line);
        }
    }

    if let Ok(mut tf) = OpenOptions::new().create(true).append(true).open(transcript_file) {
        let _ = writeln!(tf, "[{}] [{}] {} | {}", logged_at, actor, direction, summary);
    }

    logged_at
}

pub fn format_request_log(actor: &str, direction: &str, req: &RequestMessage) -> String {
    let mut summary = format!("action={}", req.action);
    if let Some(ref u) = req.user {
        let _ = write!(summary, ", user={}", u);
    }
    if let Some(ref c) = req.channel {
        let _ = write!(summary, ", channel={}", c);
    }
    if let Some(ref msg) = req.content {
        let _ = write!(summary, ", content={}", msg);
    }
    let _ = write!(summary, ", timestamp={}", req.timestamp);

    let logged_at = record_message_history(actor, direction, req, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  action    : {}", req.action);
    if let Some(ref u) = req.user {
        let _ = writeln!(out, "  user      : {}", u);
    }
    if let Some(ref c) = req.channel {
        let _ = writeln!(out, "  channel   : {}", c);
    }
    if let Some(ref msg) = req.content {
        let _ = writeln!(out, "  content   : {}", msg);
    }
    let _ = writeln!(out, "  timestamp : {}", req.timestamp);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_response_log(actor: &str, direction: &str, rep: &ResponseMessage) -> String {
    let mut summary = format!(
        "status={}, action={}, message={}, server_id={}, timestamp={}",
        rep.status, rep.action, rep.message, rep.server_id, rep.timestamp
    );
    if let Some(ref chs) = rep.channels {
        let _ = write!(summary, ", channels={:?}", chs);
    }

    let logged_at = record_message_history(actor, direction, rep, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  status    : {}", rep.status);
    let _ = writeln!(out, "  action    : {}", rep.action);
    let _ = writeln!(out, "  message   : {}", rep.message);
    if let Some(ref chs) = rep.channels {
        let _ = writeln!(out, "  channels  : {:?}", chs);
    }
    let _ = writeln!(out, "  server_id : {}", rep.server_id);
    let _ = writeln!(out, "  timestamp : {}", rep.timestamp);
    let _ = write!(out, "{}", sep);
    out
}
