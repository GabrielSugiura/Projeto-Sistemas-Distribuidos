use bbs_rust::{
    format_pubsub_sent_log, format_ref_rep_log, format_ref_req_log, format_request_log,
    format_response_log, is_valid_channel, is_valid_user, now_iso, pack_coordinator_announcement,
    pack_peer_request, pack_peer_response, pack_pubsub, pack_ref_request, pack_response,
    record_message_history, unpack_coordinator_announcement, unpack_peer_request,
    unpack_peer_response, unpack_ref_response, unpack_request, CoordinatorAnnouncement,
    LogicalClock, PeerRequest, PeerResponse, PhysicalClockSync, PubSubMessage, ReferenceRequest,
    ReferenceResponse, RequestMessage, ResponseMessage,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

struct CoordinatorState {
    coordinator: Mutex<Option<String>>,
    election_in_progress: AtomicBool,
}

impl CoordinatorState {
    fn new() -> Self {
        Self {
            coordinator: Mutex::new(None),
            election_in_progress: AtomicBool::new(false),
        }
    }

    fn get(&self) -> Option<String> {
        self.coordinator.lock().unwrap().clone()
    }

    fn set(&self, name: Option<String>) {
        *self.coordinator.lock().unwrap() = name;
    }

    fn try_begin_election(&self) -> bool {
        self.election_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn end_election(&self) {
        self.election_in_progress.store(false, Ordering::SeqCst);
    }
}

fn configure_req_socket(ctx: &zmq::Context, addr: &str, timeout_ms: i32) -> zmq::Socket {
    let sock = ctx.socket(zmq::REQ).expect("Falha ao criar socket REQ");
    sock.set_rcvtimeo(timeout_ms).expect("Falha RCVTIMEO");
    sock.set_sndtimeo(timeout_ms).expect("Falha SNDTIMEO");
    sock.set_linger(0).expect("Falha LINGER");
    sock.connect(addr).expect("Falha ao conectar socket REQ");
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

    let packed = pack_ref_request(&req).ok()?;
    if let Err(err) = ref_socket.send(packed, 0) {
        println!(
            "[{}] [{}] Erro ao enviar ao Reference ({}). Reconectando...",
            phys_clock.now_iso(),
            server_id,
            err
        );
        *ref_socket = configure_req_socket(ctx, ref_addr, 5000);
        return None;
    }

    match ref_socket.recv_bytes(0) {
        Ok(raw) => {
            if let Ok(rep) = unpack_ref_response(&raw) {
                let updated_clk = clock.update_receive(rep.logical_clock);
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
            *ref_socket = configure_req_socket(ctx, ref_addr, 5000);
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn query_peer(
    ctx: &zmq::Context,
    peer_name: &str,
    peer_port: u16,
    server_id: &str,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
    mut payload: PeerRequest,
) -> Option<(PeerResponse, f64)> {
    let addr = format!("tcp://{}:{}", peer_name, peer_port);
    let sock = configure_req_socket(ctx, &addr, 2000);
    payload.from_server = server_id.to_string();
    payload.timestamp = phys_clock.now_iso();
    payload.logical_clock = clock.tick_send();

    let summary = format!(
        "action={}, from={}, timestamp={}, logical_clock={}",
        payload.action, payload.from_server, payload.timestamp, payload.logical_clock
    );
    let dir = format!(
        "ENVIADO (PEER-REQ) -> '{}' [clock={}]",
        peer_name, payload.logical_clock
    );
    let _ = record_message_history(server_id, &dir, &payload, &summary);

    let packed = pack_peer_request(&payload).ok()?;
    let t0 = phys_clock.current_epoch_ms();
    if sock.send(packed, 0).is_err() {
        return None;
    }
    match sock.recv_bytes(0) {
        Ok(raw) => {
            let t1 = phys_clock.current_epoch_ms();
            if let Ok(rep) = unpack_peer_response(&raw) {
                let updated_clk = clock.update_receive(rep.logical_clock);
                let rep_sum = format!(
                    "status={}, action={}, epoch_ms={:?}, timestamp={}, logical_clock={}",
                    rep.status, rep.action, rep.epoch_ms, rep.timestamp, rep.logical_clock
                );
                let rep_dir = format!(
                    "RECEBIDO (PEER-REP) <- '{}' [clock={}]",
                    peer_name, updated_clk
                );
                let _ = record_message_history(server_id, &rep_dir, &rep, &rep_sum);
                return Some((rep, (t1 - t0).max(0.0)));
            }
            None
        }
        Err(_) => None,
    }
}

fn publish_coordinator_announcement(
    pub_socket: &Mutex<zmq::Socket>,
    server_id: &str,
    my_rank: u64,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
) {
    let pub_clk = clock.tick_send();
    let ann = CoordinatorAnnouncement {
        topic: "servers".to_string(),
        event: "coordinator_elected".to_string(),
        coordinator: server_id.to_string(),
        server_id: server_id.to_string(),
        rank: my_rank,
        timestamp: phys_clock.now_iso(),
        logical_clock: pub_clk,
    };

    if let Ok(packed) = pack_coordinator_announcement(&ann) {
        if let Ok(guard) = pub_socket.lock() {
            let _ = guard.send_multipart([b"servers".as_slice(), &packed], 0);
        }
        let sum = format!(
            "topic=servers, coordinator={}, rank={}, timestamp={}, logical_clock={}",
            ann.coordinator, ann.rank, ann.timestamp, ann.logical_clock
        );
        let dir = format!(
            "PUBLICADO (PUB) -> TÓPICO 'servers' (COORDENADOR='{}') [clock={}]",
            server_id, pub_clk
        );
        let _ = record_message_history(server_id, &dir, &ann, &sum);
        println!(
            "[{}] [{}] [ELEIÇÃO] Anunciado como COORDENADOR no tópico 'servers' [clock={}]",
            phys_clock.now_iso(),
            server_id,
            pub_clk
        );
        let _ = io::stdout().flush();
    }
}

#[allow(clippy::too_many_arguments)]
fn run_election(
    ctx: &zmq::Context,
    ref_socket: &mut zmq::Socket,
    ref_addr: &str,
    pub_socket: &Mutex<zmq::Socket>,
    server_id: &str,
    my_rank: u64,
    peer_port: u16,
    coord_state: &CoordinatorState,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
) {
    if !coord_state.try_begin_election() {
        return;
    }

    println!(
        "[{}] [{}] [ELEIÇÃO] Iniciando eleição (meu rank={})...",
        phys_clock.now_iso(),
        server_id,
        my_rank
    );
    let _ = io::stdout().flush();

    let servers = match call_reference(
        ctx,
        ref_socket,
        ref_addr,
        server_id,
        clock,
        phys_clock,
        "list",
    ) {
        Some(rep) => rep.servers.unwrap_or_default(),
        None => Vec::new(),
    };

    let mut higher_responded = false;
    for peer in &servers {
        if peer.name != server_id && peer.rank > my_rank {
            let req = PeerRequest {
                action: "election".to_string(),
                from_server: server_id.to_string(),
                candidate: Some(server_id.to_string()),
                rank: Some(my_rank),
                timestamp: String::new(),
                logical_clock: 0,
            };
            if let Some((rep, _)) =
                query_peer(ctx, &peer.name, peer_port, server_id, clock, phys_clock, req)
            {
                if rep.status == "ok" {
                    higher_responded = true;
                }
            }
        }
    }

    if !higher_responded {
        coord_state.set(Some(server_id.to_string()));
        println!(
            "[{}] [{}] [ELEIÇÃO] Eleito como COORDENADOR (rank={})!",
            phys_clock.now_iso(),
            server_id,
            my_rank
        );
        publish_coordinator_announcement(pub_socket, server_id, my_rank, clock, phys_clock);
    } else {
        println!(
            "[{}] [{}] [ELEIÇÃO] Servidor superior respondeu OK; aguardando publicação no tópico 'servers'...",
            phys_clock.now_iso(),
            server_id
        );
        thread::sleep(Duration::from_millis(1500));
        if coord_state.get().is_none() {
            if let Some(best) = servers.iter().max_by_key(|s| s.rank) {
                coord_state.set(Some(best.name.clone()));
            } else {
                coord_state.set(Some(server_id.to_string()));
            }
        }
    }

    coord_state.end_election();
}

#[allow(clippy::too_many_arguments)]
fn synchronize_clock_berkeley(
    ctx: &zmq::Context,
    ref_socket: &mut zmq::Socket,
    ref_addr: &str,
    pub_socket: &Mutex<zmq::Socket>,
    server_id: &str,
    my_rank: u64,
    peer_port: u16,
    coord_state: &CoordinatorState,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
) {
    let coordinator = match coord_state.get() {
        Some(c) => c,
        None => {
            run_election(
                ctx,
                ref_socket,
                ref_addr,
                pub_socket,
                server_id,
                my_rank,
                peer_port,
                coord_state,
                clock,
                phys_clock,
            );
            return;
        }
    };

    if coordinator == server_id {
        let servers = match call_reference(
            ctx,
            ref_socket,
            ref_addr,
            server_id,
            clock,
            phys_clock,
            "list",
        ) {
            Some(rep) => rep.servers.unwrap_or_default(),
            None => Vec::new(),
        };

        let mut deltas: Vec<f64> = vec![0.0];
        for peer in &servers {
            if peer.name.is_empty() || peer.name == server_id {
                continue;
            }
            let req = PeerRequest {
                action: "clock".to_string(),
                from_server: server_id.to_string(),
                candidate: None,
                rank: None,
                timestamp: String::new(),
                logical_clock: 0,
            };
            if let Some((rep, rtt_ms)) =
                query_peer(ctx, &peer.name, peer_port, server_id, clock, phys_clock, req)
            {
                if rep.status == "ok" {
                    if let Some(peer_ms) = rep.epoch_ms {
                        let local_now = phys_clock.current_epoch_ms();
                        let peer_est = peer_ms + (rtt_ms / 2.0);
                        deltas.push(peer_est - local_now);
                    }
                }
            }
        }

        let avg_delta = deltas.iter().sum::<f64>() / (deltas.len() as f64);
        let total_offset = phys_clock.apply_offset_delta(avg_delta);
        println!(
            "[{}] [{}] [BERKELEY-COORD] Média calculada entre {} servidor(es): ajuste={:+.2}ms (offset_total={:+.2}ms)",
            phys_clock.now_iso(),
            server_id,
            deltas.len(),
            avg_delta,
            total_offset
        );
        let _ = io::stdout().flush();
        return;
    }

    // Seguidor: requisita hora ao coordenador
    let req = PeerRequest {
        action: "clock".to_string(),
        from_server: server_id.to_string(),
        candidate: None,
        rank: None,
        timestamp: String::new(),
        logical_clock: 0,
    };

    if let Some((rep, rtt_ms)) =
        query_peer(ctx, &coordinator, peer_port, server_id, clock, phys_clock, req)
    {
        if rep.status == "ok" {
            if let Some(coord_ms) = rep.epoch_ms {
                let local_now = phys_clock.current_epoch_ms();
                let coord_est = coord_ms + (rtt_ms / 2.0);
                let delta = coord_est - local_now;
                let total_offset = phys_clock.apply_offset_delta(delta);
                println!(
                    "[{}] [{}] [BERKELEY-SYNC] Relógio sincronizado com coordenador '{}' | ajuste={:+.2}ms (offset_total={:+.2}ms)",
                    phys_clock.now_iso(),
                    server_id,
                    coordinator,
                    delta,
                    total_offset
                );
                let _ = io::stdout().flush();
                return;
            }
        }
    }

    println!(
        "[{}] [{}] [ELEIÇÃO] Coordenador '{}' indisponível! Disparando nova eleição...",
        phys_clock.now_iso(),
        server_id,
        coordinator
    );
    coord_state.set(None);
    run_election(
        ctx,
        ref_socket,
        ref_addr,
        pub_socket,
        server_id,
        my_rank,
        peer_port,
        coord_state,
        clock,
        phys_clock,
    );
}

#[allow(clippy::too_many_arguments)]
fn peer_server_worker(
    ctx: zmq::Context,
    peer_port: u16,
    server_id: String,
    my_rank: u64,
    clock: Arc<LogicalClock>,
    phys_clock: Arc<PhysicalClockSync>,
    trigger_election: Arc<AtomicBool>,
) {
    let peer_rep = ctx.socket(zmq::REP).expect("Falha ao criar socket PEER REP");
    peer_rep.set_linger(0).expect("Falha LINGER");
    peer_rep
        .bind(&format!("tcp://*:{}", peer_port))
        .expect("Falha ao bindar porta PEER");

    println!(
        "[{}] [{}] [PEER-REP] Escutando requisições entre servidores na porta {}",
        phys_clock.now_iso(),
        server_id,
        peer_port
    );

    loop {
        let raw = match peer_rep.recv_bytes(0) {
            Ok(b) => b,
            Err(_) => break,
        };

        if let Ok(req) = unpack_peer_request(&raw) {
            let updated_clk = clock.update_receive(req.logical_clock);
            let action = req.action.trim().to_lowercase();
            let in_sum = format!(
                "action={}, from={}, timestamp={}, logical_clock={}",
                action, req.from_server, req.timestamp, req.logical_clock
            );
            let in_dir = format!(
                "RECEBIDO (PEER-REQ) <- '{}' [clock={}]",
                req.from_server, updated_clk
            );
            let _ = record_message_history(&server_id, &in_dir, &req, &in_sum);

            let rep = if action == "election" {
                trigger_election.store(true, Ordering::SeqCst);
                PeerResponse {
                    status: "ok".to_string(),
                    action: "election".to_string(),
                    message: Some("OK".to_string()),
                    server_id: Some(server_id.clone()),
                    rank: Some(my_rank),
                    epoch_ms: None,
                    hora_correta: None,
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                }
            } else if action == "clock" || action == "berkeley" {
                PeerResponse {
                    status: "ok".to_string(),
                    action: "clock".to_string(),
                    message: None,
                    server_id: Some(server_id.clone()),
                    rank: Some(my_rank),
                    epoch_ms: Some(phys_clock.current_epoch_ms()),
                    hora_correta: Some(phys_clock.now_iso()),
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                }
            } else {
                PeerResponse {
                    status: "error".to_string(),
                    action,
                    message: Some("Ação peer desconhecida".to_string()),
                    server_id: Some(server_id.clone()),
                    rank: Some(my_rank),
                    epoch_ms: None,
                    hora_correta: None,
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                }
            };

            if let Ok(packed) = pack_peer_response(&rep) {
                let _ = peer_rep.send(packed, 0);
            }
        }
    }
}

fn servers_topic_subscriber(
    ctx: zmq::Context,
    pubsub_xpub: String,
    server_id: String,
    coord_state: Arc<CoordinatorState>,
    clock: Arc<LogicalClock>,
    phys_clock: Arc<PhysicalClockSync>,
) {
    let sub = ctx.socket(zmq::SUB).expect("Falha ao criar socket SUB");
    sub.set_rcvtimeo(500).expect("Falha RCVTIMEO");
    sub.set_linger(0).expect("Falha LINGER");
    sub.connect(&pubsub_xpub)
        .expect("Falha ao conectar SUB ao XPUB");
    sub.set_subscribe(b"servers")
        .expect("Falha ao assinar tópico servers");

    println!(
        "[{}] [{}] [SUB-SERVERS] Inscrito no tópico 'servers' em {}",
        phys_clock.now_iso(),
        server_id,
        pubsub_xpub
    );

    loop {
        match sub.recv_multipart(0) {
            Ok(parts) => {
                if parts.len() >= 2 {
                    if let Ok(ann) = unpack_coordinator_announcement(&parts[1]) {
                        let updated_clk = clock.update_receive(ann.logical_clock);
                        let elected = if !ann.coordinator.is_empty() {
                            ann.coordinator.clone()
                        } else {
                            ann.server_id.clone()
                        };
                        if !elected.is_empty() {
                            coord_state.set(Some(elected.clone()));
                            let sum = format!(
                                "coordinator={}, rank={}, timestamp={}, logical_clock={}",
                                elected, ann.rank, ann.timestamp, ann.logical_clock
                            );
                            let dir = format!(
                                "RECEBIDO (SUB) <- TÓPICO 'servers' (COORDENADOR='{}') [clock={}]",
                                elected, updated_clk
                            );
                            let _ = record_message_history(&server_id, &dir, &ann, &sum);
                            println!(
                                "[{}] [{}] [SUB-SERVERS] Coordenador atualizado para '{}' [clock={}]",
                                phys_clock.now_iso(),
                                server_id,
                                elected,
                                updated_clk
                            );
                            let _ = io::stdout().flush();
                        }
                    }
                }
            }
            Err(zmq::Error::EAGAIN) => continue,
            Err(_) => break,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_client_request(
    server_id: &str,
    rank: u64,
    coordinator: Option<String>,
    storage: &mut DiskStorage,
    pub_socket: &Mutex<zmq::Socket>,
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
            coordinator,
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
                    coordinator,
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
                coordinator,
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
                coordinator,
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
                coordinator,
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
                    coordinator,
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
                    coordinator,
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
                coordinator: coordinator.clone(),
                timestamp: pub_ts.clone(),
                logical_clock: pub_clock,
            };

            match pack_pubsub(&pub_msg) {
                Ok(packed) => {
                    let send_res = {
                        let guard = pub_socket.lock().unwrap();
                        guard.send_multipart([channel.as_bytes(), &packed], 0)
                    };
                    if let Err(err) = send_res {
                        return ResponseMessage {
                            status: "error".to_string(),
                            action: "publish".to_string(),
                            message: format!("Erro ao publicar no canal '#{}': {}", channel, err),
                            channels: None,
                            server_id: server_id.to_string(),
                            server_rank: Some(rank),
                            coordinator,
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
                        coordinator,
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
                coordinator,
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
            coordinator,
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
    let pubsub_xpub =
        env::var("PUBSUB_XPUB").unwrap_or_else(|_| "tcp://pubsub-proxy:5558".to_string());
    let reference_addr =
        env::var("REFERENCE_ADDR").unwrap_or_else(|_| "tcp://reference:5559".to_string());
    let peer_port: u16 = env::var("PEER_PORT")
        .unwrap_or_else(|_| "5560".to_string())
        .parse()
        .unwrap_or(5560);
    let heartbeat_every: u64 = env::var("HEARTBEAT_EVERY")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);
    let sync_every: u64 = env::var("SYNC_EVERY")
        .unwrap_or_else(|_| "15".to_string())
        .parse()
        .unwrap_or(15);
    let data_file = env::var("DATA_FILE").unwrap_or_else(|_| "/data/storage.msgpack".to_string());

    let mut storage = DiskStorage::new(&data_file);
    let clock = Arc::new(LogicalClock::new(0));
    let phys_clock = Arc::new(PhysicalClockSync::new());
    let coord_state = Arc::new(CoordinatorState::new());
    let trigger_election = Arc::new(AtomicBool::new(false));

    let ctx = zmq::Context::new();
    let mut ref_socket = configure_req_socket(&ctx, &reference_addr, 5000);

    // 1. Obtém rank no serviço de referência
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

    let raw_pub = ctx.socket(zmq::PUB).expect("Falha ao criar socket PUB");
    raw_pub.set_linger(0).expect("Falha LINGER");
    raw_pub
        .connect(&pubsub_xsub)
        .expect("Falha ao conectar ao XSUB");
    let pub_socket = Arc::new(Mutex::new(raw_pub));

    // Inicia subscriber do tópico 'servers' e listener peer-to-peer (eleição + Berkeley)
    {
        let sub_ctx = ctx.clone();
        let xpub = pubsub_xpub.clone();
        let sid = server_id.clone();
        let cstate = Arc::clone(&coord_state);
        let clk = Arc::clone(&clock);
        let pclk = Arc::clone(&phys_clock);
        thread::spawn(move || {
            servers_topic_subscriber(sub_ctx, xpub, sid, cstate, clk, pclk);
        });
    }

    {
        let p_ctx = ctx.clone();
        let sid = server_id.clone();
        let clk = Arc::clone(&clock);
        let pclk = Arc::clone(&phys_clock);
        let trig = Arc::clone(&trigger_election);
        thread::spawn(move || {
            peer_server_worker(p_ctx, peer_port, sid, server_rank, clk, pclk, trig);
        });
    }

    thread::sleep(Duration::from_secs(1));

    if coord_state.get().is_none() {
        run_election(
            &ctx,
            &mut ref_socket,
            &reference_addr,
            &pub_socket,
            &server_id,
            server_rank,
            peer_port,
            &coord_state,
            &clock,
            &phys_clock,
        );
    }

    let rep_socket = ctx.socket(zmq::REP).expect("Falha ao criar socket REP");
    rep_socket.set_rcvtimeo(1000).expect("Falha RCVTIMEO");
    rep_socket
        .connect(&broker_backend)
        .expect("Falha ao conectar ao backend do Broker");

    println!(
        "[{}] [{}] Rank={} | Coordenador='{:?}' | Broker={}",
        phys_clock.now_iso(),
        server_id,
        server_rank,
        coord_state.get(),
        broker_backend
    );
    let _ = io::stdout().flush();

    let mut client_msg_count: u64 = 0;
    let mut exchanged_messages: u64 = 0;

    loop {
        if trigger_election.swap(false, Ordering::SeqCst) {
            run_election(
                &ctx,
                &mut ref_socket,
                &reference_addr,
                &pub_socket,
                &server_id,
                server_rank,
                peer_port,
                &coord_state,
                &clock,
                &phys_clock,
            );
        }

        let raw_req = match rep_socket.recv_bytes(0) {
            Ok(bytes) => bytes,
            Err(zmq::Error::EAGAIN) => continue,
            Err(err) => {
                eprintln!("[{}] [{}] Erro ZMQ ao receber: {}", now_iso(), server_id, err);
                continue;
            }
        };

        let rep = match unpack_request(&raw_req) {
            Ok(req) => {
                let updated_clk = clock.update_receive(req.logical_clock);
                client_msg_count += 1;
                exchanged_messages += 1;
                let dir = format!(
                    "RECEBIDO (REQ #{}) <- CLIENTE [clock={}]",
                    client_msg_count, updated_clk
                );
                println!("{}", format_request_log(&server_id, &dir, &req));
                let _ = io::stdout().flush();
                handle_client_request(
                    &server_id,
                    server_rank,
                    coord_state.get(),
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
                coordinator: coord_state.get(),
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
            },
        };

        let rep_dir = format!("ENVIADO (REP) -> CLIENTE [clock={}]", rep.logical_clock);
        println!("{}", format_response_log(&server_id, &rep_dir, &rep));
        let _ = io::stdout().flush();
        if let Ok(packed) = pack_response(&rep) {
            let _ = rep_socket.send(packed, 0);
            exchanged_messages += 1;
        }

        // Heartbeat a cada 10 mensagens de clientes (sem hora física no Reference)
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
        }

        // Sincronização de relógio (Algoritmo de Berkeley) a cada 15 mensagens trocadas
        if exchanged_messages >= sync_every {
            exchanged_messages = 0;
            synchronize_clock_berkeley(
                &ctx,
                &mut ref_socket,
                &reference_addr,
                &pub_socket,
                &server_id,
                server_rank,
                peer_port,
                &coord_state,
                &clock,
                &phys_clock,
            );
        }
    }
}
