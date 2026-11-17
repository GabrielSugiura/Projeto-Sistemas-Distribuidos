use bbs_rust::{
    format_pubsub_sent_log, format_request_log, format_response_log, is_valid_channel,
    is_valid_user, now_iso, pack_pubsub, pack_response, unpack_request, PubSubMessage,
    RequestMessage, ResponseMessage,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoginRecord {
    user: String,
    timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicationRecord {
    channel: String,
    user: String,
    content: String,
    server_id: String,
    req_timestamp: String,
    published_at: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorageState {
    #[serde(default)]
    logins: Vec<LoginRecord>,
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    publications: Vec<PublicationRecord>,
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

    fn record_publication(
        &mut self,
        channel: &str,
        user: &str,
        content: &str,
        server_id: &str,
        req_timestamp: &str,
        published_at: &str,
    ) {
        if !self.state.channels.iter().any(|c| c == channel) && is_valid_channel(channel) {
            self.state.channels.push(channel.to_string());
        }
        self.state.publications.push(PublicationRecord {
            channel: channel.to_string(),
            user: user.to_string(),
            content: content.to_string(),
            server_id: server_id.to_string(),
            req_timestamp: req_timestamp.to_string(),
            published_at: published_at.to_string(),
        });
        self.save();
    }
}

fn handle_request(
    server_id: &str,
    storage: &mut DiskStorage,
    pub_socket: &zmq::Socket,
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
        "publish" | "post_message" => {
            let channel = req.channel.as_deref().unwrap_or("").trim();
            let content = req.content.as_deref().unwrap_or("").trim();

            if channel.is_empty() || !is_valid_channel(channel) {
                return ResponseMessage {
                    status: "error".to_string(),
                    action: "publish".to_string(),
                    message: format!("Canal inválido para publicação: '{}'.", channel),
                    channels: None,
                    server_id: server_id.to_string(),
                    timestamp: now_iso(),
                };
            }
            if content.is_empty() {
                return ResponseMessage {
                    status: "error".to_string(),
                    action: "publish".to_string(),
                    message: "Conteúdo da publicação não pode ser vazio.".to_string(),
                    channels: None,
                    server_id: server_id.to_string(),
                    timestamp: now_iso(),
                };
            }

            let pub_ts = now_iso();
            let author = if user.is_empty() { "anon" } else { user };
            let pub_msg = PubSubMessage {
                channel: channel.to_string(),
                user: author.to_string(),
                content: content.to_string(),
                server_id: server_id.to_string(),
                timestamp: pub_ts.clone(),
            };

            match pack_pubsub(&pub_msg) {
                Ok(packed) => {
                    if let Err(err) = pub_socket.send_multipart([channel.as_bytes(), &packed], 0) {
                        return ResponseMessage {
                            status: "error".to_string(),
                            action: "publish".to_string(),
                            message: format!("Erro ao publicar no canal '#{}': {}", channel, err),
                            channels: None,
                            server_id: server_id.to_string(),
                            timestamp: now_iso(),
                        };
                    }
                    println!("{}", format_pubsub_sent_log(server_id, &pub_msg));
                    let _ = io::stdout().flush();
                }
                Err(err) => {
                    return ResponseMessage {
                        status: "error".to_string(),
                        action: "publish".to_string(),
                        message: format!("Erro ao serializar publicação MessagePack: {}", err),
                        channels: None,
                        server_id: server_id.to_string(),
                        timestamp: now_iso(),
                    };
                }
            }

            storage.record_publication(channel, author, content, server_id, req_ts, &pub_ts);

            ResponseMessage {
                status: "ok".to_string(),
                action: "publish".to_string(),
                message: format!("Mensagem publicada no canal '#{}' com sucesso.", channel),
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
    let pubsub_xsub =
        env::var("PUBSUB_XSUB").unwrap_or_else(|_| "tcp://pubsub-proxy:5557".to_string());
    let data_file = env::var("DATA_FILE").unwrap_or_else(|_| "/data/storage.msgpack".to_string());

    let mut storage = DiskStorage::new(&data_file);

    let ctx = zmq::Context::new();
    let rep_socket = ctx.socket(zmq::REP).expect("Falha ao criar socket REP");
    rep_socket
        .connect(&broker_backend)
        .expect("Falha ao conectar ao backend do Broker");

    let pub_socket = ctx.socket(zmq::PUB).expect("Falha ao criar socket PUB");
    pub_socket.set_linger(0).expect("Falha ao definir LINGER");
    pub_socket
        .connect(&pubsub_xsub)
        .expect("Falha ao conectar ao XSUB do proxy Pub/Sub");

    thread::sleep(Duration::from_millis(500));

    println!(
        "[{}] [{}] Conectado | Broker REP: {} | Proxy PUB: {} | Storage: {}",
        now_iso(),
        server_id,
        broker_backend,
        pubsub_xsub,
        data_file
    );
    let _ = io::stdout().flush();

    loop {
        let raw_req = match rep_socket.recv_bytes(0) {
            Ok(bytes) => bytes,
            Err(err) => {
                eprintln!("[{}] [{}] Erro ZMQ ao receber: {}", now_iso(), server_id, err);
                continue;
            }
        };

        let rep = match unpack_request(&raw_req) {
            Ok(req) => {
                println!(
                    "{}",
                    format_request_log(&server_id, "RECEBIDO (REQ) <- CLIENTE", &req)
                );
                let _ = io::stdout().flush();
                handle_request(&server_id, &mut storage, &pub_socket, &req)
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

        println!(
            "{}",
            format_response_log(&server_id, "ENVIADO (REP) -> CLIENTE", &rep)
        );
        let _ = io::stdout().flush();
        if let Ok(packed) = pack_response(&rep) {
            let _ = rep_socket.send(packed, 0);
        }
    }
}
