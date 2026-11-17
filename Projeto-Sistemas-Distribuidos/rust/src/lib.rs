use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Relógio Lógico de Lamport thread-safe
pub struct LogicalClock {
    counter: AtomicU64,
}

impl LogicalClock {
    pub fn new(initial: u64) -> Self {
        Self {
            counter: AtomicU64::new(initial),
        }
    }

    /// Incrementa antes de enviar cada mensagem e retorna o novo valor
    pub fn tick_send(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Atualiza com max(local, recebido) ao receber mensagem
    pub fn update_receive(&self, received: u64) -> u64 {
        let mut current = self.counter.load(Ordering::SeqCst);
        loop {
            if received <= current {
                return current;
            }
            match self.counter.compare_exchange(
                current,
                received,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return received,
                Err(actual) => current = actual,
            }
        }
    }

    pub fn value(&self) -> u64 {
        self.counter.load(Ordering::SeqCst)
    }
}

/// Sincronização do relógio físico do servidor com o serviço de referência
pub struct PhysicalClockSync {
    offset_ms: Mutex<f64>,
}

impl PhysicalClockSync {
    pub fn new() -> Self {
        Self {
            offset_ms: Mutex::new(0.0),
        }
    }

    pub fn sync_with_reference(&self, t0_local_ms: f64, ref_epoch_ms: f64, t1_local_ms: f64) -> f64 {
        let rtt_ms = (t1_local_ms - t0_local_ms).max(0.0);
        let estimated_ref_ms = ref_epoch_ms + (rtt_ms / 2.0);
        let offset = estimated_ref_ms - t1_local_ms;
        if let Ok(mut guard) = self.offset_ms.lock() {
            *guard = offset;
        }
        offset
    }

    pub fn now_iso(&self) -> String {
        let offset = self.offset_ms.lock().map(|g| *g).unwrap_or(0.0);
        let adjusted_ms = (Utc::now().timestamp_millis() as f64 + offset).round() as i64;
        DateTime::<Utc>::from_timestamp_millis(adjusted_ms)
            .unwrap_or_else(Utc::now)
            .to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

impl Default for PhysicalClockSync {
    fn default() -> Self {
        Self::new()
    }
}

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
    #[serde(default)]
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub status: String,
    pub action: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    pub server_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_rank: Option<u64>,
    pub timestamp: String,
    #[serde(default)]
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    pub channel: String,
    pub user: String,
    pub content: String,
    pub server_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_rank: Option<u64>,
    pub timestamp: String,
    #[serde(default)]
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceRequest {
    pub action: String,
    pub server_name: String,
    pub timestamp: String,
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerEntry {
    pub name: String,
    pub rank: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceResponse {
    pub status: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub servers: Option<Vec<ServerEntry>>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_timestamp: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_epoch_ms: Option<f64>,
    pub timestamp: String,
    #[serde(default)]
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReceivedSubDisplay {
    pub canal: String,
    pub autor: String,
    pub mensagem: String,
    pub servidor_pub: String,
    pub timestamp_envio: String,
    pub timestamp_recebimento: String,
    pub clock_recebido: u64,
    pub clock_local_atual: u64,
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

pub fn now_epoch_ms() -> f64 {
    Utc::now().timestamp_millis() as f64
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
            } else if (next == b'W' || next == b'M')
                && i + 2 < bytes.len()
                && bytes[i + 2].is_ascii_digit()
            {
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

pub fn load_sample_messages(filepath: &str) -> Vec<String> {
    let mut samples = Vec::new();
    if let Ok(raw) = fs::read_to_string(filepath) {
        for raw_line in raw.lines() {
            let line = raw_line.trim();
            if line.is_empty()
                || line.starts_with("---")
                || line.starts_with('!')
                || line.starts_with('#')
            {
                continue;
            }
            if let Some((speaker, msg)) = line.split_once(':') {
                let cleaned = clean_script_text(msg);
                if cleaned.len() >= 3 {
                    samples.push(format!("[{}] {}", speaker.trim(), cleaned));
                }
            }
        }
    }
    if samples.is_empty() {
        samples.push("Olá pessoal, testando publicação distribuída em Rust!".to_string());
        samples.push("Canal sincronizado via ZeroMQ PUB/SUB.".to_string());
        samples.push("Enviando mensagem serializada em MessagePack.".to_string());
    }
    samples
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

pub fn pack_pubsub(msg: &PubSubMessage) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(msg)
}

pub fn unpack_pubsub(bytes: &[u8]) -> Result<PubSubMessage, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

pub fn pack_ref_request(req: &ReferenceRequest) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    rmp_serde::to_vec_named(req)
}

pub fn unpack_ref_response(bytes: &[u8]) -> Result<ReferenceResponse, rmp_serde::decode::Error> {
    rmp_serde::from_slice(bytes)
}

fn record_message_history<T: Serialize>(
    actor: &str,
    direction: &str,
    payload: &T,
    summary: &str,
) -> String {
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
    let _ = write!(
        summary,
        ", timestamp={}, logical_clock={}",
        req.timestamp, req.logical_clock
    );

    let logged_at = record_message_history(actor, direction, req, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  action       : {}", req.action);
    if let Some(ref u) = req.user {
        let _ = writeln!(out, "  user         : {}", u);
    }
    if let Some(ref c) = req.channel {
        let _ = writeln!(out, "  channel      : {}", c);
    }
    if let Some(ref msg) = req.content {
        let _ = writeln!(out, "  content      : {}", msg);
    }
    let _ = writeln!(out, "  timestamp    : {}", req.timestamp);
    let _ = writeln!(out, "  logical_clock: {}", req.logical_clock);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_response_log(actor: &str, direction: &str, rep: &ResponseMessage) -> String {
    let mut summary = format!(
        "status={}, action={}, message={}, server_id={}, timestamp={}, logical_clock={}",
        rep.status, rep.action, rep.message, rep.server_id, rep.timestamp, rep.logical_clock
    );
    if let Some(ref chs) = rep.channels {
        let _ = write!(summary, ", channels={:?}", chs);
    }

    let logged_at = record_message_history(actor, direction, rep, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  status       : {}", rep.status);
    let _ = writeln!(out, "  action       : {}", rep.action);
    let _ = writeln!(out, "  message      : {}", rep.message);
    if let Some(ref chs) = rep.channels {
        let _ = writeln!(out, "  channels     : {:?}", chs);
    }
    let _ = writeln!(out, "  server_id    : {}", rep.server_id);
    if let Some(rk) = rep.server_rank {
        let _ = writeln!(out, "  server_rank  : {}", rk);
    }
    let _ = writeln!(out, "  timestamp    : {}", rep.timestamp);
    let _ = writeln!(out, "  logical_clock: {}", rep.logical_clock);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_pubsub_sent_log(actor: &str, msg: &PubSubMessage) -> String {
    let summary = format!(
        "channel={}, user={}, content={}, server_id={}, timestamp={}, logical_clock={}",
        msg.channel, msg.user, msg.content, msg.server_id, msg.timestamp, msg.logical_clock
    );
    let direction = format!(
        "PUBLICADO (PUB) -> TÓPICO '#{}' [clock={}]",
        msg.channel, msg.logical_clock
    );
    let logged_at = record_message_history(actor, &direction, msg, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  channel      : {}", msg.channel);
    let _ = writeln!(out, "  user         : {}", msg.user);
    let _ = writeln!(out, "  content      : {}", msg.content);
    let _ = writeln!(out, "  server_id    : {}", msg.server_id);
    let _ = writeln!(out, "  timestamp    : {}", msg.timestamp);
    let _ = writeln!(out, "  logical_clock: {}", msg.logical_clock);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_sub_received_log(actor: &str, display: &ReceivedSubDisplay) -> String {
    let summary = format!(
        "canal={}, autor={}, mensagem={}, servidor_pub={}, timestamp_envio={}, timestamp_recebimento={}, clock_recebido={}, clock_local_atual={}",
        display.canal,
        display.autor,
        display.mensagem,
        display.servidor_pub,
        display.timestamp_envio,
        display.timestamp_recebimento,
        display.clock_recebido,
        display.clock_local_atual
    );
    let direction = format!(
        "RECEBIDO (SUB) <- CANAL '#{}' [clock={}]",
        display.canal, display.clock_local_atual
    );
    let logged_at = record_message_history(actor, &direction, display, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  canal                : {}", display.canal);
    let _ = writeln!(out, "  autor                : {}", display.autor);
    let _ = writeln!(out, "  mensagem             : {}", display.mensagem);
    let _ = writeln!(out, "  servidor_pub         : {}", display.servidor_pub);
    let _ = writeln!(out, "  timestamp_envio      : {}", display.timestamp_envio);
    let _ = writeln!(out, "  timestamp_recebimento: {}", display.timestamp_recebimento);
    let _ = writeln!(out, "  clock_recebido       : {}", display.clock_recebido);
    let _ = writeln!(out, "  clock_local_atual    : {}", display.clock_local_atual);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_ref_req_log(actor: &str, req: &ReferenceRequest) -> String {
    let summary = format!(
        "action={}, server_name={}, timestamp={}, logical_clock={}",
        req.action, req.server_name, req.timestamp, req.logical_clock
    );
    let direction = format!("ENVIADO (REQ) -> REFERENCE [clock={}]", req.logical_clock);
    let logged_at = record_message_history(actor, &direction, req, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  action       : {}", req.action);
    let _ = writeln!(out, "  server_name  : {}", req.server_name);
    let _ = writeln!(out, "  timestamp    : {}", req.timestamp);
    let _ = writeln!(out, "  logical_clock: {}", req.logical_clock);
    let _ = write!(out, "{}", sep);
    out
}

pub fn format_ref_rep_log(actor: &str, local_clock: u64, rep: &ReferenceResponse) -> String {
    let summary = format!(
        "status={}, action={}, rank={:?}, servers={:?}, message={}, timestamp={}, logical_clock={}",
        rep.status, rep.action, rep.rank, rep.servers, rep.message, rep.timestamp, rep.logical_clock
    );
    let direction = format!("RECEBIDO (REP) <- REFERENCE [clock={}]", local_clock);
    let logged_at = record_message_history(actor, &direction, rep, &summary);
    let sep = "========================================================================";
    let mut out = String::new();
    let _ = writeln!(out, "{}", sep);
    let _ = writeln!(out, "[{}] [{}] {}", logged_at, actor, direction);
    let _ = writeln!(out, "  status       : {}", rep.status);
    let _ = writeln!(out, "  action       : {}", rep.action);
    if let Some(rk) = rep.rank {
        let _ = writeln!(out, "  rank         : {}", rk);
    }
    if let Some(ref svs) = rep.servers {
        let _ = writeln!(out, "  servers      : {:?}", svs);
    }
    let _ = writeln!(out, "  message      : {}", rep.message);
    let _ = writeln!(out, "  timestamp    : {}", rep.timestamp);
    let _ = writeln!(out, "  logical_clock: {}", rep.logical_clock);
    let _ = write!(out, "{}", sep);
    out
}
