use bbs_rust::{
    format_pubsub_sent_log, format_ref_rep_log, format_ref_req_log, format_request_log,
    format_response_log, is_valid_channel, is_valid_user, now_epoch_ms, now_iso, pack_pubsub,
    pack_ref_request, pack_response, unpack_ref_response, unpack_request, LogicalClock,
    PhysicalClockSync, PubSubMessage, ReferenceRequest, ReferenceResponse, RequestMessage,
    ResponseMessage,
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
    logical_clock: u64,
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
            if let Ok(bytes) = fs::read(filepath) {
                if let Ok(state) = rmp_serde::from_slice::<StorageState>(&bytes) {
                    return state;
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

    #[allow(clippy::too_many_arguments)]
    fn record_publication(
        &mut self,
        channel: &str,
        user: &str,
        content: &str,
        server_id: &str,
        req_timestamp: &str,
        published_at: &str,
        logical_clock: u64,
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
            logical_clock,
        });
        self.save();
    }
}

fn configure_ref_socket(ctx: &zmq::Context, ref_addr: &str) -> zmq::Socket {
    let sock = ctx.socket(zmq::REQ).expect("Falha ao criar socket REQ Ref");
    sock.set_rcvtimeo(5000).expect("Falha RCVTIMEO");
    sock.set_sndtimeo(5000).expect("Falha SNDTIMEO");
    sock.set_linger(0).expect("Falha LINGER");
    sock.connect(ref_addr)
        .expect("Falha ao conectar ao serviço de referência");
    sock
}

fn call_reference(
    ctx: &zmq::Context,
    ref_socket: &mut zmq::Socket,
    ref_addr: &str,
    server_id: &str,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
    action: &str,
) -> Option<ReferenceResponse> {
    let req = ReferenceRequest {
        action: action.to_string(),
        server_name: server_id.to_string(),
        timestamp: phys_clock.now_iso(),
        logical_clock: clock.tick_send(),
    };
    println!("{}", format_ref_req_log(server_id, &req));
    let _ = io::stdout().flush();

    let packed = match pack_ref_request(&req) {
        Ok(b) => b,
        Err(_) => return None,
    };

    let t0_ms = now_epoch_ms();
    if let Err(err) = ref_socket.send(packed, 0) {
        println!(
            "[{}] [{}] Erro ao enviar ao Reference ({}). Reconectando...",
            phys_clock.now_iso(),
            server_id,
            err
        );
        *ref_socket = configure_ref_socket(ctx, ref_addr);
        return None;
    }

    match ref_socket.recv_bytes(0) {
        Ok(raw) => {
            let t1_ms = now_epoch_ms();
            if let Ok(rep) = unpack_ref_response(&raw) {
                let updated_clk = clock.update_receive(rep.logical_clock);
                if let Some(ref_ms) = rep.reference_epoch_ms {
                    let offset = phys_clock.sync_with_reference(t0_ms, ref_ms, t1_ms);
                    println!(
                        "[{}] [{}] [CLOCK-SYNC] Relógio físico sincronizado com referência | offset={:+.2}ms",
                        phys_clock.now_iso(),
                        server_id,
                        offset
                    );
                }
                println!("{}", format_ref_rep_log(server_id, updated_clk, &rep));
                let _ = io::stdout().flush();
                return Some(rep);
            }
            None
        }
        Err(err) => {
            println!(
                "[{}] [{}] Timeout ao aguardar Reference ({}). Reconectando...",
                phys_clock.now_iso(),
                server_id,
                err
            );
            *ref_socket = configure_ref_socket(ctx, ref_addr);
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_request(
    server_id: &str,
    rank: u64,
    storage: &mut DiskStorage,
    pub_socket: &zmq::Socket,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
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
            server_rank: Some(rank),
            timestamp: phys_clock.now_iso(),
            logical_clock: clock.tick_send(),
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
                    server_rank: Some(rank),
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                };
            }
            storage.record_login(user, req_ts);
            ResponseMessage {
                status: "ok".to_string(),
                action: "login".to_string(),
                message: format!("Login realizado com sucesso para '{}'.", user),
                channels: None,
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
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
                server_rank: Some(rank),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
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
                server_rank: Some(rank),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
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
                    server_rank: Some(rank),
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                };
            }
            if content.is_empty() {
                return ResponseMessage {
                    status: "error".to_string(),
                    action: "publish".to_string(),
                    message: "Conteúdo da publicação não pode ser vazio.".to_string(),
                    channels: None,
                    server_id: server_id.to_string(),
                    server_rank: Some(rank),
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                };
            }

            let pub_ts = phys_clock.now_iso();
            let pub_clock = clock.tick_send();
            let author = if user.is_empty() { "anon" } else { user };
            let pub_msg = PubSubMessage {
                channel: channel.to_string(),
                user: author.to_string(),
                content: content.to_string(),
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                timestamp: pub_ts.clone(),
                logical_clock: pub_clock,
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
                            server_rank: Some(rank),
                            timestamp: phys_clock.now_iso(),
                            logical_clock: clock.tick_send(),
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
                        server_rank: Some(rank),
                        timestamp: phys_clock.now_iso(),
                        logical_clock: clock.tick_send(),
                    };
                }
            }

            storage.record_publication(
                channel,
                author,
                content,
                server_id,
                req_ts,
                &pub_ts,
                pub_clock,
            );

            ResponseMessage {
                status: "ok".to_string(),
                action: "publish".to_string(),
                message: format!("Mensagem publicada no canal '#{}' com sucesso.", channel),
                channels: None,
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
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
            server_rank: Some(rank),
            timestamp: phys_clock.now_iso(),
            logical_clock: clock.tick_send(),
        },
    }
}

fn main() {
    let server_id = env::var("SERVER_ID").unwrap_or_else(|_| "server-rust".to_string());
    let broker_backend =
        env::var("BROKER_BACKEND").unwrap_or_else(|_| "tcp://broker:5556".to_string());
    let pubsub_xsub =
        env::var("PUBSUB_XSUB").unwrap_or_else(|_| "tcp://pubsub-proxy:5557".to_string());
    let reference_addr =
        env::var("REFERENCE_ADDR").unwrap_or_else(|_| "tcp://reference:5559".to_string());
    let heartbeat_every: u64 = env::var("HEARTBEAT_EVERY")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
    let data_file = env::var("DATA_FILE").unwrap_or_else(|_| "/data/storage.msgpack".to_string());

    let mut storage = DiskStorage::new(&data_file);
    let clock = LogicalClock::new(0);
    let phys_clock = PhysicalClockSync::new();

    let ctx = zmq::Context::new();
    let mut ref_socket = configure_ref_socket(&ctx, &reference_addr);

    // 1. Ao iniciar, solicita seu rank ao serviço de referência
    let mut rank: Option<u64> = None;
    while rank.is_none() {
        if let Some(rep) = call_reference(
            &ctx,
            &mut ref_socket,
            &reference_addr,
            &server_id,
            &clock,
            &phys_clock,
            "rank",
        ) {
            if rep.status == "ok" {
                rank = rep.rank.or(Some(1));
                break;
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    let server_rank = rank.unwrap_or(1);

    // Consulta lista inicial de servidores disponíveis
    let _ = call_reference(
        &ctx,
        &mut ref_socket,
        &reference_addr,
        &server_id,
        &clock,
        &phys_clock,
        "list",
    );

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
        "[{}] [{}] Rank={} | Broker REP: {} | Proxy PUB: {} | Reference: {}",
        phys_clock.now_iso(),
        server_id,
        server_rank,
        broker_backend,
        pubsub_xsub,
        reference_addr
    );
    let _ = io::stdout().flush();

    let mut client_msg_count: u64 = 0;

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
                let updated_clk = clock.update_receive(req.logical_clock);
                client_msg_count += 1;
                let dir = format!(
                    "RECEBIDO (REQ #{}) <- CLIENTE [clock={}]",
                    client_msg_count, updated_clk
                );
                println!("{}", format_request_log(&server_id, &dir, &req));
                let _ = io::stdout().flush();
                handle_request(
                    &server_id,
                    server_rank,
                    &mut storage,
                    &pub_socket,
                    &clock,
                    &phys_clock,
                    &req,
                )
            }
            Err(err) => ResponseMessage {
                status: "error".to_string(),
                action: "unknown".to_string(),
                message: format!("Erro ao decodificar mensagem binária MessagePack: {}", err),
                channels: None,
                server_id: server_id.clone(),
                server_rank: Some(server_rank),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
            },
        };

        let rep_dir = format!("ENVIADO (REP) -> CLIENTE [clock={}]", rep.logical_clock);
        println!("{}", format_response_log(&server_id, &rep_dir, &rep));
        let _ = io::stdout().flush();
        if let Ok(packed) = pack_response(&rep) {
            let _ = rep_socket.send(packed, 0);
        }

        // A cada 10 mensagens de clientes recebidas, envia heartbeat e sincroniza relógio físico
        if client_msg_count > 0 && client_msg_count % heartbeat_every == 0 {
            let _ = call_reference(
                &ctx,
                &mut ref_socket,
                &reference_addr,
                &server_id,
                &clock,
                &phys_clock,
                "heartbeat",
            );
            let _ = call_reference(
                &ctx,
                &mut ref_socket,
                &reference_addr,
                &server_id,
                &clock,
                &phys_clock,
                "list",
            );
        }
    }
}
