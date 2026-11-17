use bbs_rust::{
    format_pubsub_sent_log, format_ref_rep_log, format_ref_req_log, format_request_log,
    format_response_log, is_valid_channel, is_valid_user, now_iso, pack_coordinator_announcement,
    pack_peer_request, pack_peer_response, pack_pubsub, pack_ref_request, pack_replication_event,
    pack_response, record_message_history, unpack_coordinator_announcement, unpack_peer_request,
    unpack_peer_response, unpack_ref_response, unpack_replication_event, unpack_request,
    CoordinatorAnnouncement, LogicalClock, LoginRecord, PeerRequest, PeerResponse,
    PhysicalClockSync, PubSubMessage, PublicationRecord, ReferenceRequest, ReferenceResponse,
    ReplicationEvent, RequestMessage, ResponseMessage,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Debug, Default, Serialize, Deserialize)]
struct StorageState {
    #[serde(default)]
    logins: Vec<LoginRecord>,
    #[serde(default)]
    channels: Vec<String>,
    #[serde(default)]
    publications: Vec<PublicationRecord>,
    #[serde(default)]
    applied_ops: Vec<String>,
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
        let mut state = Self::load(filepath);
        Self::sort_publications(&mut state);
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

    fn sort_publications(state: &mut StorageState) {
        state.publications.sort_by(|a, b| {
            a.logical_clock
                .cmp(&b.logical_clock)
                .then_with(|| a.published_at.cmp(&b.published_at))
                .then_with(|| a.server_id.cmp(&b.server_id))
                .then_with(|| a.op_id.cmp(&b.op_id))
        });
    }

    fn save(&self) {
        let tmp_path = format!("{}.tmp", self.filepath);
        if let Ok(bytes) = rmp_serde::to_vec_named(&self.state) {
            if fs::write(&tmp_path, bytes).is_ok() {
                let _ = fs::rename(&tmp_path, &self.filepath);
            }
        }
    }

    fn record_login(
        &mut self,
        op_id: &str,
        user: &str,
        timestamp: &str,
        server_id: &str,
        logical_clock: u64,
    ) -> bool {
        if !op_id.is_empty() && self.state.applied_ops.iter().any(|o| o == op_id) {
            return false;
        }
        if !op_id.is_empty() {
            self.state.applied_ops.push(op_id.to_string());
        }
        self.state.logins.push(LoginRecord {
            op_id: op_id.to_string(),
            user: user.to_string(),
            timestamp: timestamp.to_string(),
            server_id: server_id.to_string(),
            logical_clock,
        });
        self.save();
        true
    }

    fn list_channels(&self) -> Vec<String> {
        let mut list = self.state.channels.clone();
        list.sort();
        list
    }

    fn create_channel(&mut self, channel: &str, op_id: &str) -> (bool, String) {
        if channel.is_empty() || !is_valid_channel(channel) {
            return (
                false,
                "Nome de canal inválido (use 2-32 caracteres: letras minúsculas, números, '_' ou '-')."
                    .to_string(),
            );
        }
        if !op_id.is_empty() && self.state.applied_ops.iter().any(|o| o == op_id) {
            return (false, format!("Operação '{}' já replicada.", op_id));
        }
        if !op_id.is_empty() {
            self.state.applied_ops.push(op_id.to_string());
        }
        if self.state.channels.iter().any(|c| c == channel) {
            self.save();
            return (false, format!("Canal '{}' já existe.", channel));
        }
        self.state.channels.push(channel.to_string());
        self.state.channels.sort();
        self.save();
        (true, format!("Canal '{}' criado com sucesso.", channel))
    }

    #[allow(clippy::too_many_arguments)]
    fn record_publication(
        &mut self,
        op_id: &str,
        channel: &str,
        user: &str,
        content: &str,
        server_id: &str,
        req_timestamp: &str,
        published_at: &str,
        logical_clock: u64,
    ) -> bool {
        if !op_id.is_empty() && self.state.applied_ops.iter().any(|o| o == op_id) {
            return false;
        }
        if !op_id.is_empty() {
            self.state.applied_ops.push(op_id.to_string());
        }
        if !self.state.channels.iter().any(|c| c == channel) && is_valid_channel(channel) {
            self.state.channels.push(channel.to_string());
            self.state.channels.sort();
        }
        self.state.publications.push(PublicationRecord {
            op_id: op_id.to_string(),
            channel: channel.to_string(),
            user: user.to_string(),
            content: content.to_string(),
            server_id: server_id.to_string(),
            req_timestamp: req_timestamp.to_string(),
            published_at: published_at.to_string(),
            logical_clock,
        });
        Self::sort_publications(&mut self.state);
        self.save();
        true
    }

    fn get_history(&self, channel: Option<&str>, limit: usize) -> (usize, Vec<PublicationRecord>) {
        let filtered: Vec<PublicationRecord> = match channel {
            Some(ch) => self
                .state
                .publications
                .iter()
                .filter(|p| p.channel == ch)
                .cloned()
                .collect(),
            None => self.state.publications.clone(),
        };
        let total = filtered.len();
        let start = total.saturating_sub(limit);
        (total, filtered[start..].to_vec())
    }

    fn export_snapshot(
        &self,
    ) -> (
        Vec<LoginRecord>,
        Vec<String>,
        Vec<PublicationRecord>,
        Vec<String>,
    ) {
        (
            self.state.logins.clone(),
            self.state.channels.clone(),
            self.state.publications.clone(),
            self.state.applied_ops.clone(),
        )
    }

    fn merge_snapshot(&mut self, rep: &PeerResponse) -> usize {
        let mut added = 0;
        let mut applied_set: HashSet<String> = self.state.applied_ops.iter().cloned().collect();
        let mut pub_keys: HashSet<String> = self
            .state
            .publications
            .iter()
            .map(|p| {
                if !p.op_id.is_empty() {
                    p.op_id.clone()
                } else {
                    format!(
                        "{}:{}:{}:{}",
                        p.server_id, p.logical_clock, p.published_at, p.content
                    )
                }
            })
            .collect();
        let mut login_keys: HashSet<String> = self
            .state
            .logins
            .iter()
            .map(|l| {
                if !l.op_id.is_empty() {
                    l.op_id.clone()
                } else {
                    format!("{}:{}:{}", l.server_id, l.user, l.timestamp)
                }
            })
            .collect();

        if let Some(ref chs) = rep.channels {
            for ch in chs {
                if !ch.is_empty()
                    && !self.state.channels.iter().any(|c| c == ch)
                    && is_valid_channel(ch)
                {
                    self.state.channels.push(ch.clone());
                    added += 1;
                }
            }
        }

        if let Some(ref lgs) = rep.logins {
            for lg in lgs {
                let key = if !lg.op_id.is_empty() {
                    lg.op_id.clone()
                } else {
                    format!("{}:{}:{}", lg.server_id, lg.user, lg.timestamp)
                };
                if (!lg.op_id.is_empty() && applied_set.contains(&lg.op_id))
                    || login_keys.contains(&key)
                {
                    continue;
                }
                if !lg.op_id.is_empty() {
                    applied_set.insert(lg.op_id.clone());
                }
                login_keys.insert(key);
                self.state.logins.push(lg.clone());
                added += 1;
            }
        }

        if let Some(ref pubs) = rep.publications {
            for pub_item in pubs {
                let key = if !pub_item.op_id.is_empty() {
                    pub_item.op_id.clone()
                } else {
                    format!(
                        "{}:{}:{}:{}",
                        pub_item.server_id,
                        pub_item.logical_clock,
                        pub_item.published_at,
                        pub_item.content
                    )
                };
                if (!pub_item.op_id.is_empty() && applied_set.contains(&pub_item.op_id))
                    || pub_keys.contains(&key)
                {
                    continue;
                }
                if !pub_item.op_id.is_empty() {
                    applied_set.insert(pub_item.op_id.clone());
                }
                pub_keys.insert(key);
                if !pub_item.channel.is_empty()
                    && !self.state.channels.iter().any(|c| c == &pub_item.channel)
                    && is_valid_channel(&pub_item.channel)
                {
                    self.state.channels.push(pub_item.channel.clone());
                }
                self.state.publications.push(pub_item.clone());
                added += 1;
            }
        }

        if let Some(ref ops) = rep.applied_ops {
            for op in ops {
                if !op.is_empty() {
                    applied_set.insert(op.clone());
                }
            }
        }

        let mut sorted_ops: Vec<String> = applied_set.into_iter().collect();
        sorted_ops.sort();
        self.state.applied_ops = sorted_ops;
        self.state.channels.sort();
        Self::sort_publications(&mut self.state);
        if added > 0 {
            self.save();
        }
        added
    }

    fn counts(&self) -> (usize, usize, usize) {
        (
            self.state.logins.len(),
            self.state.channels.len(),
            self.state.publications.len(),
        )
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
    timeout_ms: i32,
) -> Option<(PeerResponse, f64)> {
    let addr = format!("tcp://{}:{}", peer_name, peer_port);
    let sock = configure_req_socket(ctx, &addr, timeout_ms);
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

#[allow(clippy::too_many_arguments)]
fn broadcast_replication_event(
    pub_socket: &Mutex<zmq::Socket>,
    server_id: &str,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
    op_id: &str,
    op_type: &str,
    user: Option<String>,
    channel: Option<String>,
    content: Option<String>,
    req_timestamp: Option<String>,
    event_timestamp: Option<String>,
) {
    let repl_clk = clock.tick_send();
    let ts = event_timestamp.unwrap_or_else(|| phys_clock.now_iso());
    let ev = ReplicationEvent {
        topic: "replication".to_string(),
        op_id: op_id.to_string(),
        op_type: op_type.to_string(),
        origin_server: server_id.to_string(),
        user,
        channel,
        content,
        req_timestamp,
        timestamp: ts,
        logical_clock: repl_clk,
    };

    if let Ok(packed) = pack_replication_event(&ev) {
        if let Ok(guard) = pub_socket.lock() {
            let _ = guard.send_multipart([b"replication".as_slice(), &packed], 0);
        }
        let sum = format!(
            "op_id={}, op_type={}, origin={}, logical_clock={}",
            ev.op_id, ev.op_type, ev.origin_server, ev.logical_clock
        );
        let dir = format!(
            "PUBLICADO (REPL-PUB) -> TÓPICO 'replication' ({}) [clock={}]",
            ev.op_type, repl_clk
        );
        let _ = record_message_history(server_id, &dir, &ev, &sum);
    }
}

#[allow(clippy::too_many_arguments)]
fn sync_state_with_cluster(
    ctx: &zmq::Context,
    ref_socket: &mut zmq::Socket,
    ref_addr: &str,
    server_id: &str,
    peer_port: u16,
    coord_state: &CoordinatorState,
    storage: &Mutex<DiskStorage>,
    clock: &LogicalClock,
    phys_clock: &PhysicalClockSync,
) {
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

    let mut peers: Vec<String> = servers
        .into_iter()
        .map(|s| s.name)
        .filter(|name| !name.is_empty() && name != server_id)
        .collect();

    if let Some(coord) = coord_state.get() {
        if coord != server_id {
            if let Some(pos) = peers.iter().position(|p| p == &coord) {
                peers.remove(pos);
                peers.insert(0, coord);
            }
        }
    }

    for peer_name in peers {
        let req = PeerRequest {
            action: "sync_state".to_string(),
            from_server: server_id.to_string(),
            candidate: None,
            rank: None,
            timestamp: String::new(),
            logical_clock: 0,
        };
        if let Some((rep, _)) =
            query_peer(ctx, &peer_name, peer_port, server_id, clock, phys_clock, req, 3000)
        {
            if rep.status == "ok" {
                let (added, counts) = {
                    let mut guard = storage.lock().unwrap();
                    let a = guard.merge_snapshot(&rep);
                    let c = guard.counts();
                    (a, c)
                };
                println!(
                    "[{}] [{}] [ANTI-ENTROPIA] Estado sincronizado com '{}' | novos_registros={} | total=(logins={}, canais={}, msgs={})",
                    phys_clock.now_iso(),
                    server_id,
                    peer_name,
                    added,
                    counts.0,
                    counts.1,
                    counts.2
                );
                let _ = io::stdout().flush();
                break;
            }
        }
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
                query_peer(ctx, &peer.name, peer_port, server_id, clock, phys_clock, req, 2000)
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
    storage: &Mutex<DiskStorage>,
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
            sync_state_with_cluster(
                ctx,
                ref_socket,
                ref_addr,
                server_id,
                peer_port,
                coord_state,
                storage,
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
                query_peer(ctx, &peer.name, peer_port, server_id, clock, phys_clock, req, 2000)
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
        sync_state_with_cluster(
            ctx,
            ref_socket,
            ref_addr,
            server_id,
            peer_port,
            coord_state,
            storage,
            clock,
            phys_clock,
        );
        return;
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
        query_peer(ctx, &coordinator, peer_port, server_id, clock, phys_clock, req, 2000)
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
                sync_state_with_cluster(
                    ctx,
                    ref_socket,
                    ref_addr,
                    server_id,
                    peer_port,
                    coord_state,
                    storage,
                    clock,
                    phys_clock,
                );
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
    sync_state_with_cluster(
        ctx,
        ref_socket,
        ref_addr,
        server_id,
        peer_port,
        coord_state,
        storage,
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
    storage: Arc<Mutex<DiskStorage>>,
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
        "[{}] [{}] [PEER-REP] Escutando peers (eleição/clock/sync_state) na porta {}",
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
                    logins: None,
                    channels: None,
                    publications: None,
                    applied_ops: None,
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
                    logins: None,
                    channels: None,
                    publications: None,
                    applied_ops: None,
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                }
            } else if action == "sync_state" {
                let (logins, channels, publications, applied_ops) = {
                    let guard = storage.lock().unwrap();
                    guard.export_snapshot()
                };
                let msg = format!("Snapshot enviado ({} mensagens).", publications.len());
                PeerResponse {
                    status: "ok".to_string(),
                    action: "sync_state".to_string(),
                    message: Some(msg),
                    server_id: Some(server_id.clone()),
                    rank: Some(my_rank),
                    epoch_ms: None,
                    hora_correta: None,
                    logins: Some(logins),
                    channels: Some(channels),
                    publications: Some(publications),
                    applied_ops: Some(applied_ops),
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
                    logins: None,
                    channels: None,
                    publications: None,
                    applied_ops: None,
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

fn cluster_topics_subscriber(
    ctx: zmq::Context,
    pubsub_xpub: String,
    server_id: String,
    coord_state: Arc<CoordinatorState>,
    storage: Arc<Mutex<DiskStorage>>,
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
    sub.set_subscribe(b"replication")
        .expect("Falha ao assinar tópico replication");

    println!(
        "[{}] [{}] [SUB-CLUSTER] Inscrito nos tópicos 'servers' e 'replication' em {}",
        phys_clock.now_iso(),
        server_id,
        pubsub_xpub
    );

    loop {
        match sub.recv_multipart(0) {
            Ok(parts) => {
                if parts.len() < 2 {
                    continue;
                }
                let topic = String::from_utf8_lossy(&parts[0]).to_string();
                if topic == "servers" {
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
                } else if topic == "replication" {
                    if let Ok(ev) = unpack_replication_event(&parts[1]) {
                        let updated_clk = clock.update_receive(ev.logical_clock);
                        if ev.origin_server.is_empty() || ev.origin_server == server_id {
                            continue;
                        }
                        let ev_clk = if ev.logical_clock > 0 {
                            ev.logical_clock
                        } else {
                            updated_clk
                        };
                        let ev_ts = if ev.timestamp.is_empty() {
                            phys_clock.now_iso()
                        } else {
                            ev.timestamp.clone()
                        };

                        let (applied, counts) = {
                            let mut guard = storage.lock().unwrap();
                            let ok = match ev.op_type.as_str() {
                                "login" => {
                                    if let Some(ref u) = ev.user {
                                        let rts = ev.req_timestamp.as_deref().unwrap_or(&ev_ts);
                                        guard.record_login(
                                            &ev.op_id,
                                            u,
                                            rts,
                                            &ev.origin_server,
                                            ev_clk,
                                        )
                                    } else {
                                        false
                                    }
                                }
                                "create_channel" => {
                                    if let Some(ref ch) = ev.channel {
                                        guard.create_channel(ch, &ev.op_id).0
                                    } else {
                                        false
                                    }
                                }
                                "publish" => {
                                    if let (Some(ref ch), Some(ref cnt)) =
                                        (&ev.channel, &ev.content)
                                    {
                                        let u = ev.user.as_deref().unwrap_or("anon");
                                        let rts = ev.req_timestamp.as_deref().unwrap_or(&ev_ts);
                                        guard.record_publication(
                                            &ev.op_id,
                                            ch,
                                            u,
                                            cnt,
                                            &ev.origin_server,
                                            rts,
                                            &ev_ts,
                                            ev_clk,
                                        )
                                    } else {
                                        false
                                    }
                                }
                                _ => false,
                            };
                            (ok, guard.counts())
                        };

                        if applied {
                            let sum = format!(
                                "op_id={}, op_type={}, origin={}, total_msgs={}",
                                ev.op_id, ev.op_type, ev.origin_server, counts.2
                            );
                            let dir = format!(
                                "APLICADO (REPL-SUB) <- '{}' ({}) | total_msgs={} [clock={}]",
                                ev.origin_server, ev.op_type, counts.2, updated_clk
                            );
                            let _ = record_message_history(&server_id, &dir, &ev, &sum);
                            println!(
                                "[{}] [{}] [REPL-SUB] Operação '{}' de '{}' aplicada | total=(logins={}, canais={}, msgs={}) [clock={}]",
                                phys_clock.now_iso(),
                                server_id,
                                ev.op_type,
                                ev.origin_server,
                                counts.0,
                                counts.1,
                                counts.2,
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
    storage: &Mutex<DiskStorage>,
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
            total_messages: None,
            messages: None,
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
                    total_messages: None,
                    messages: None,
                    server_id: server_id.to_string(),
                    server_rank: Some(rank),
                    coordinator,
                    timestamp: phys_clock.now_iso(),
                    logical_clock: clock.tick_send(),
                };
            }
            let op_clock = clock.tick_send();
            let op_id = format!("{}:{}:{}", server_id, op_clock, req_ts);
            {
                let mut guard = storage.lock().unwrap();
                guard.record_login(&op_id, user, req_ts, server_id, op_clock);
            }
            broadcast_replication_event(
                pub_socket,
                server_id,
                clock,
                phys_clock,
                &op_id,
                "login",
                Some(user.to_string()),
                None,
                None,
                Some(req_ts.to_string()),
                Some(req_ts.to_string()),
            );
            ResponseMessage {
                status: "ok".to_string(),
                action: "login".to_string(),
                message: format!("Login realizado com sucesso para '{}'.", user),
                channels: None,
                total_messages: None,
                messages: None,
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                coordinator,
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
            }
        }
        "list_channels" => {
            let channels = {
                let guard = storage.lock().unwrap();
                guard.list_channels()
            };
            let count = channels.len();
            ResponseMessage {
                status: "ok".to_string(),
                action: "list_channels".to_string(),
                message: format!("{} canal(is) encontrado(s).", count),
                channels: Some(channels),
                total_messages: None,
                messages: None,
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                coordinator,
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
            }
        }
        "create_channel" => {
            let channel = req.channel.as_deref().unwrap_or("").trim();
            let op_clock = clock.tick_send();
            let op_id = format!("{}:{}:{}", server_id, op_clock, channel);
            let (created, msg) = {
                let mut guard = storage.lock().unwrap();
                guard.create_channel(channel, &op_id)
            };
            if created {
                broadcast_replication_event(
                    pub_socket,
                    server_id,
                    clock,
                    phys_clock,
                    &op_id,
                    "create_channel",
                    Some(if user.is_empty() { "anon" } else { user }.to_string()),
                    Some(channel.to_string()),
                    None,
                    Some(req_ts.to_string()),
                    None,
                );
            }
            ResponseMessage {
                status: if created { "ok" } else { "error" }.to_string(),
                action: "create_channel".to_string(),
                message: msg,
                channels: None,
                total_messages: None,
                messages: None,
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
                    total_messages: None,
                    messages: None,
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
                    total_messages: None,
                    messages: None,
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
            let op_id = format!("{}:{}:{}", server_id, pub_clock, pub_ts);

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
                            total_messages: None,
                            messages: None,
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
                        total_messages: None,
                        messages: None,
                        server_id: server_id.to_string(),
                        server_rank: Some(rank),
                        coordinator,
                        timestamp: phys_clock.now_iso(),
                        logical_clock: clock.tick_send(),
                    };
                }
            }

            {
                let mut guard = storage.lock().unwrap();
                guard.record_publication(
                    &op_id,
                    channel,
                    author,
                    content,
                    server_id,
                    req_ts,
                    &pub_ts,
                    pub_clock,
                );
            }

            broadcast_replication_event(
                pub_socket,
                server_id,
                clock,
                phys_clock,
                &op_id,
                "publish",
                Some(author.to_string()),
                Some(channel.to_string()),
                Some(content.to_string()),
                Some(req_ts.to_string()),
                Some(pub_ts),
            );

            ResponseMessage {
                status: "ok".to_string(),
                action: "publish".to_string(),
                message: format!(
                    "Mensagem publicada e replicada no canal '#{}' com sucesso.",
                    channel
                ),
                channels: None,
                total_messages: None,
                messages: None,
                server_id: server_id.to_string(),
                server_rank: Some(rank),
                coordinator,
                timestamp: phys_clock.now_iso(),
                logical_clock: clock.tick_send(),
            }
        }
        "get_history" | "history" | "list_messages" => {
            let ch_filter = req
                .channel
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let limit = req.limit.unwrap_or(50);
            let (total, messages) = {
                let guard = storage.lock().unwrap();
                guard.get_history(ch_filter, limit)
            };
            ResponseMessage {
                status: "ok".to_string(),
                action: "get_history".to_string(),
                message: format!(
                    "Histórico replicado: {} mensagem(ns) armazenada(s) em '{}'.",
                    total, server_id
                ),
                channels: None,
                total_messages: Some(total),
                messages: Some(messages),
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
            total_messages: None,
            messages: None,
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

    let storage = Arc::new(Mutex::new(DiskStorage::new(&data_file)));
    let clock = Arc::new(LogicalClock::new(0));
    let phys_clock = Arc::new(PhysicalClockSync::new());
    let coord_state = Arc::new(CoordinatorState::new());
    let trigger_election = Arc::new(AtomicBool::new(false));

    let ctx = zmq::Context::new();
    let mut ref_socket = configure_req_socket(&ctx, &reference_addr, 5000);

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

    {
        let sub_ctx = ctx.clone();
        let xpub = pubsub_xpub.clone();
        let sid = server_id.clone();
        let cstate = Arc::clone(&coord_state);
        let st = Arc::clone(&storage);
        let clk = Arc::clone(&clock);
        let pclk = Arc::clone(&phys_clock);
        thread::spawn(move || {
            cluster_topics_subscriber(sub_ctx, xpub, sid, cstate, st, clk, pclk);
        });
    }

    {
        let p_ctx = ctx.clone();
        let sid = server_id.clone();
        let st = Arc::clone(&storage);
        let clk = Arc::clone(&clock);
        let pclk = Arc::clone(&phys_clock);
        let trig = Arc::clone(&trigger_election);
        thread::spawn(move || {
            peer_server_worker(p_ctx, peer_port, sid, server_rank, st, clk, pclk, trig);
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

    sync_state_with_cluster(
        &ctx,
        &mut ref_socket,
        &reference_addr,
        &server_id,
        peer_port,
        &coord_state,
        &storage,
        &clock,
        &phys_clock,
    );

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
                    &storage,
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
                total_messages: None,
                messages: None,
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
                &storage,
                &clock,
                &phys_clock,
            );
        }
    }
}