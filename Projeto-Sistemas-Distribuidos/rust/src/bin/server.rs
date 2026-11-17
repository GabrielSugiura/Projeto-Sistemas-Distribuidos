use bbs_rust::{
    format_request_log, format_response_log, is_valid_channel, is_valid_user, now_iso,
    pack_response, unpack_request, RequestMessage, ResponseMessage,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoginRecord {
    user: String,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessageRecord {
    user: String,
    channel: String,
    content: String,
    timestamp: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorageState {
    #[serde(default)]
    logins: Vec<LoginRecord>,
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    messages: Vec<ChatMessageRecord>,
}

struct DiskStorage {
    filepath: String,
    state: StorageState,
}

impl DiskStorage {
    fn new(filepath: &str) -> Self {
        if let Some(parent) = Path::new(filepath).parent() {
            let _ = fs::create_dir_all(parent);
        }
        let state = Self::load(filepath);
        Self {
            filepath: filepath.to_string(),
            state,
        }
    }

    fn load(filepath: &str) -> StorageState {
        if Path::new(filepath).exists() {
            match fs::read(filepath) {
                Ok(bytes) => match rmp_serde::from_slice::<StorageState>(&bytes) {
                    Ok(state) => return state,
                    Err(err) => {
                        eprintln!(
                            "[{}] [STORAGE] Aviso ao desserializar {}: {}",
                            now_iso(),
                            filepath,
                            err
                        );
                    }
                },
                Err(err) => {
                    eprintln!(
                        "[{}] [STORAGE] Aviso ao ler {}: {}",
                        now_iso(),
                        filepath,
                        err
                    );
                }
            }
        }
        StorageState::default()
    }

    fn save(&self) {
        let tmp_path = format!("{}.tmp", self.filepath);
        if let Ok(bytes) = rmp_serde::to_vec_named(&self.state) {
            if fs::write(&tmp_path, bytes).is_ok() {
                let _ = fs::rename(&tmp_path, &self.filepath);
            }
        }
    }

    fn record_login(&mut self, user: &str, timestamp: &str) {
        self.state.logins.push(LoginRecord {
            user: user.to_string(),
            timestamp: timestamp.to_string(),
        });
        self.save();
    }

    fn list_channels(&self) -> Vec<String> {
        let mut list = self.state.channels.clone();
        list.sort();
        list
    }

    fn create_channel(&mut self, channel: &str) -> (bool, String) {
        if channel.is_empty() || !is_valid_channel(channel) {
            return (
                false,
                "Nome de canal inválido (use 2-32 caracteres: letras minúsculas, números, '_' ou '-')."
                    .to_string(),
            );
        }
        if self.state.channels.iter().any(|c| c == channel) {
            return (false, format!("Canal '{}' já existe.", channel));
        }
        self.state.channels.push(channel.to_string());
        self.save();
        (true, format!("Canal '{}' criado com sucesso.", channel))
    }

    fn record_message(&mut self, user: &str, channel: &str, content: &str, timestamp: &str) {
        if !self.state.channels.iter().any(|c| c == channel) && is_valid_channel(channel) {
            self.state.channels.push(channel.to_string());
        }
        self.state.messages.push(ChatMessageRecord {
            user: user.to_string(),
            channel: channel.to_string(),
            content: content.to_string(),
            timestamp: timestamp.to_string(),
        });
        self.save();
    }
}

fn handle_request(
    server_id: &str,
    storage: &mut DiskStorage,
    req: &RequestMessage,
) -> ResponseMessage {
    let action = req.action.trim();
    let user = req.user.as_deref().unwrap_or("").trim();
    let req_ts = req.timestamp.trim();

    if req_ts.is_empty() {
        return ResponseMessage {
            status: "error".to_string(),
            action: if action.is_empty() {
                "unknown".to_string()
            } else {
                action.to_string()
            },
            message: "Timestamp obrigatório ausente na requisição.".to_string(),
            channels: None,
            server_id: server_id.to_string(),
            timestamp: now_iso(),
        };
    }

    match action {
        "login" => {
            if user.is_empty() || !is_valid_user(user) {
                return ResponseMessage {
                    status: "error".to_string(),
                    action: "login".to_string(),
                    message: format!(
                        "Nome de usuário inválido: '{}'. Use 3-24 caracteres alfanuméricos, '_' ou '-'.",
                        user
                    ),
                    channels: None,
                    server_id: server_id.to_string(),
                    timestamp: now_iso(),
                };
            }
            storage.record_login(user, req_ts);
            ResponseMessage {
                status: "ok".to_string(),
                action: "login".to_string(),
                message: format!("Login realizado com sucesso para '{}'.", user),
                channels: None,
                server_id: server_id.to_string(),
                timestamp: now_iso(),
            }
        }
        "list_channels" => {
            let channels = storage.list_channels();
            let count = channels.len();
            ResponseMessage {
                status: "ok".to_string(),
                action: "list_channels".to_string(),
                message: format!("{} canal(is) encontrado(s).", count),
                channels: Some(channels),
                server_id: server_id.to_string(),
                timestamp: now_iso(),
            }
        }
        "create_channel" => {
            let channel = req.channel.as_deref().unwrap_or("").trim();
            let (created, msg) = storage.create_channel(channel);
            ResponseMessage {
                status: if created { "ok" } else { "error" }.to_string(),
                action: "create_channel".to_string(),
                message: msg,
                channels: None,
                server_id: server_id.to_string(),
                timestamp: now_iso(),
            }
        }
        "post_message" => {
            let channel = req.channel.as_deref().unwrap_or("geral").trim();
            let content = req.content.as_deref().unwrap_or("").trim();
            if content.is_empty() {
                return ResponseMessage {
                    status: "error".to_string(),
                    action: "post_message".to_string(),
                    message: "Conteúdo da mensagem não pode ser vazio.".to_string(),
                    channels: None,
                    server_id: server_id.to_string(),
                    timestamp: now_iso(),
                };
            }
            let author = if user.is_empty() { "anon" } else { user };
            storage.record_message(author, channel, content, req_ts);
            ResponseMessage {
                status: "ok".to_string(),
                action: "post_message".to_string(),
                message: format!("Mensagem de '{}' registrada em '#{}'.", author, channel),
                channels: None,
                server_id: server_id.to_string(),
                timestamp: now_iso(),
            }
        }
        _ => ResponseMessage {
            status: "error".to_string(),
            action: if action.is_empty() {
                "unknown".to_string()
            } else {
                action.to_string()
            },
            message: format!("Ação desconhecida: '{}'.", action),
            channels: None,
            server_id: server_id.to_string(),
            timestamp: now_iso(),
        },
    }
}

fn main() {
    let server_id = env::var("SERVER_ID").unwrap_or_else(|_| "server-rust".to_string());
    let broker_backend =
        env::var("BROKER_BACKEND").unwrap_or_else(|_| "tcp://broker:5556".to_string());
    let data_file = env::var("DATA_FILE").unwrap_or_else(|_| "/data/storage.msgpack".to_string());

    let mut storage = DiskStorage::new(&data_file);

    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP).expect("Falha ao criar socket REP");
    socket
        .connect(&broker_backend)
        .expect("Falha ao conectar ao backend do Broker");

    println!(
        "[{}] [{}] Conectado ao Broker em {} | Persistência: {}",
        now_iso(),
        server_id,
        broker_backend,
        data_file
    );
    let _ = io::stdout().flush();

    loop {
        let raw_req = match socket.recv_bytes(0) {
            Ok(bytes) => bytes,
            Err(err) => {
                eprintln!("[{}] [{}] Erro ZMQ ao receber: {}", now_iso(), server_id, err);
                continue;
            }
        };

        let rep = match unpack_request(&raw_req) {
            Ok(req) => {
                println!("{}", format_request_log(&server_id, "RECEBIDO <- CLIENTE", &req));
                let _ = io::stdout().flush();
                handle_request(&server_id, &mut storage, &req)
            }
            Err(err) => ResponseMessage {
                status: "error".to_string(),
                action: "unknown".to_string(),
                message: format!("Erro ao decodificar mensagem binária MessagePack: {}", err),
                channels: None,
                server_id: server_id.clone(),
                timestamp: now_iso(),
            },
        };

        println!("{}", format_response_log(&server_id, "ENVIADO -> CLIENTE", &rep));
        let _ = io::stdout().flush();
        if let Ok(packed) = pack_response(&rep) {
            let _ = socket.send(packed, 0);
        }
    }
}
