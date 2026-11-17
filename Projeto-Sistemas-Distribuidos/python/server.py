import copy
import os
import signal
import sys
import threading
import time
import zmq
import msgpack
from protocol import (
    USER_REGEX,
    CHANNEL_REGEX,
    LogicalClock,
    PhysicalClockSync,
    pack_message,
    unpack_message,
    format_log,
)


class DiskStorage:
    """Armazenamento replicado thread-safe com ordenação total e idempotência."""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "rb") as f:
                    data = msgpack.unpackb(f.read(), raw=False)
                    if isinstance(data, dict):
                        data.setdefault("logins", [])
                        data.setdefault("channels", [])
                        data.setdefault("publications", [])
                        data.setdefault("applied_ops", [])
                        self._sort_publications_unlocked(data)
                        return data
            except Exception as exc:
                print(f"[{time.time()}] [STORAGE] Aviso ao ler {self.filepath}: {exc}", flush=True)
        return {"logins": [], "channels": [], "publications": [], "applied_ops": []}

    @staticmethod
    def _sort_publications_unlocked(state: dict) -> None:
        state["publications"].sort(
            key=lambda p: (
                int(p.get("logical_clock", 0)),
                str(p.get("published_at", "")),
                str(p.get("server_id", "")),
                str(p.get("op_id", "")),
            )
        )

    def _save_unlocked(self) -> None:
        tmp_path = f"{self.filepath}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(msgpack.packb(self.state, use_bin_type=True))
        os.replace(tmp_path, self.filepath)

    def record_login(
        self,
        op_id: str,
        user: str,
        timestamp: str,
        server_id: str,
        logical_clock: int,
    ) -> bool:
        with self._lock:
            if op_id and op_id in self.state["applied_ops"]:
                return False
            if op_id:
                self.state["applied_ops"].append(op_id)
            self.state["logins"].append(
                {
                    "op_id": op_id,
                    "user": user,
                    "timestamp": timestamp,
                    "server_id": server_id,
                    "logical_clock": int(logical_clock),
                }
            )
            self._save_unlocked()
            return True

    def list_channels(self) -> list[str]:
        with self._lock:
            return sorted(self.state["channels"])

    def create_channel(self, channel: str, op_id: str = "") -> tuple[bool, str]:
        if not channel or not CHANNEL_REGEX.match(channel):
            return (
                False,
                "Nome de canal inválido (use 2-32 caracteres: letras minúsculas, números, '_' ou '-').",
            )
        with self._lock:
            if op_id and op_id in self.state["applied_ops"]:
                return False, f"Operação '{op_id}' já replicada."
            if op_id:
                self.state["applied_ops"].append(op_id)
            if channel in self.state["channels"]:
                self._save_unlocked()
                return False, f"Canal '{channel}' já existe."
            self.state["channels"].append(channel)
            self.state["channels"].sort()
            self._save_unlocked()
            return True, f"Canal '{channel}' criado com sucesso."

    def record_publication(
        self,
        op_id: str,
        channel: str,
        user: str,
        content: str,
        server_id: str,
        req_timestamp: str,
        published_at: str,
        logical_clock: int,
    ) -> bool:
        with self._lock:
            if op_id and op_id in self.state["applied_ops"]:
                return False
            if op_id:
                self.state["applied_ops"].append(op_id)
            if channel not in self.state["channels"] and CHANNEL_REGEX.match(channel):
                self.state["channels"].append(channel)
                self.state["channels"].sort()
            self.state["publications"].append(
                {
                    "op_id": op_id,
                    "channel": channel,
                    "user": user,
                    "content": content,
                    "server_id": server_id,
                    "req_timestamp": req_timestamp,
                    "published_at": published_at,
                    "logical_clock": int(logical_clock),
                }
            )
            self._sort_publications_unlocked(self.state)
            self._save_unlocked()
            return True

    def get_history(self, channel: str | None = None, limit: int = 50) -> tuple[int, list[dict]]:
        with self._lock:
            pubs = self.state["publications"]
            if channel:
                filtered = [p for p in pubs if p.get("channel") == channel]
            else:
                filtered = list(pubs)
            total = len(filtered)
            return total, copy.deepcopy(filtered[-limit:])

    def export_snapshot(self) -> dict:
        with self._lock:
            return {
                "logins": copy.deepcopy(self.state["logins"]),
                "channels": list(self.state["channels"]),
                "publications": copy.deepcopy(self.state["publications"]),
                "applied_ops": list(self.state["applied_ops"]),
            }

    def merge_snapshot(self, snapshot: dict) -> int:
        """Funde estado recebido via Anti-Entropia (catch-up) de forma idempotente."""
        added_ops = 0
        with self._lock:
            applied_set = set(self.state["applied_ops"])
            existing_pub_keys = {
                (
                    p.get("op_id")
                    or f"{p.get('server_id')}:{p.get('logical_clock')}:{p.get('published_at')}:{p.get('content')}"
                )
                for p in self.state["publications"]
            }
            existing_login_keys = {
                (
                    lg.get("op_id")
                    or f"{lg.get('server_id')}:{lg.get('user')}:{lg.get('timestamp')}"
                )
                for lg in self.state["logins"]
            }

            for ch in snapshot.get("channels") or []:
                ch_str = str(ch).strip()
                if ch_str and ch_str not in self.state["channels"] and CHANNEL_REGEX.match(ch_str):
                    self.state["channels"].append(ch_str)
                    added_ops += 1
            self.state["channels"].sort()

            for lg in snapshot.get("logins") or []:
                if not isinstance(lg, dict):
                    continue
                op_id = str(lg.get("op_id") or "")
                key = op_id or f"{lg.get('server_id')}:{lg.get('user')}:{lg.get('timestamp')}"
                if (op_id and op_id in applied_set) or key in existing_login_keys:
                    continue
                if op_id:
                    applied_set.add(op_id)
                existing_login_keys.add(key)
                self.state["logins"].append(
                    {
                        "op_id": op_id,
                        "user": str(lg.get("user", "")),
                        "timestamp": str(lg.get("timestamp", "")),
                        "server_id": str(lg.get("server_id", "")),
                        "logical_clock": int(lg.get("logical_clock", 0)),
                    }
                )
                added_ops += 1

            for pub in snapshot.get("publications") or []:
                if not isinstance(pub, dict):
                    continue
                op_id = str(pub.get("op_id") or "")
                key = (
                    op_id
                    or f"{pub.get('server_id')}:{pub.get('logical_clock')}:{pub.get('published_at')}:{pub.get('content')}"
                )
                if (op_id and op_id in applied_set) or key in existing_pub_keys:
                    continue
                if op_id:
                    applied_set.add(op_id)
                existing_pub_keys.add(key)
                ch_str = str(pub.get("channel", "")).strip()
                if ch_str and ch_str not in self.state["channels"] and CHANNEL_REGEX.match(ch_str):
                    self.state["channels"].append(ch_str)
                self.state["publications"].append(
                    {
                        "op_id": op_id,
                        "channel": ch_str,
                        "user": str(pub.get("user", "anon")),
                        "content": str(pub.get("content", "")),
                        "server_id": str(pub.get("server_id", "")),
                        "req_timestamp": str(pub.get("req_timestamp", "")),
                        "published_at": str(pub.get("published_at", "")),
                        "logical_clock": int(pub.get("logical_clock", 0)),
                    }
                )
                added_ops += 1

            for op in snapshot.get("applied_ops") or []:
                if op:
                    applied_set.add(str(op))

            self.state["applied_ops"] = sorted(applied_set)
            self.state["channels"].sort()
            self._sort_publications_unlocked(self.state)
            if added_ops > 0:
                self._save_unlocked()
        return added_ops

    def counts(self) -> tuple[int, int, int]:
        with self._lock:
            return (
                len(self.state["logins"]),
                len(self.state["channels"]),
                len(self.state["publications"]),
            )


class CoordinatorState:
    """Mantém o nome do servidor eleito como coordenador de forma thread-safe."""

    def __init__(self) -> None:
        self._coordinator: str | None = None
        self._election_in_progress: bool = False
        self._lock = threading.Lock()

    def get(self) -> str | None:
        with self._lock:
            return self._coordinator

    def set(self, name: str | None) -> None:
        with self._lock:
            self._coordinator = name

    def try_begin_election(self) -> bool:
        with self._lock:
            if self._election_in_progress:
                return False
            self._election_in_progress = True
            return True

    def end_election(self) -> None:
        with self._lock:
            self._election_in_progress = False


def create_req_socket(context: zmq.Context, addr: str, timeout_ms: int = 2500) -> zmq.Socket:
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(addr)
    return sock


def call_reference(
    context: zmq.Context,
    ref_socket: zmq.Socket,
    ref_addr: str,
    server_id: str,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
    payload: dict,
) -> tuple[zmq.Socket, dict | None]:
    payload["timestamp"] = phys_clock.now_iso()
    payload["logical_clock"] = clock.tick_send()
    print(
        format_log(server_id, f"ENVIADO (REQ) -> REFERENCE [clock={payload['logical_clock']}]", payload),
        flush=True,
    )
    try:
        ref_socket.send(pack_message(payload))
        raw_rep = ref_socket.recv()
        rep = unpack_message(raw_rep)
        clock.update_receive(rep.get("logical_clock"))
        print(
            format_log(server_id, f"RECEBIDO (REP) <- REFERENCE [clock={clock.value}]", rep),
            flush=True,
        )
        return ref_socket, rep
    except zmq.ZMQError as exc:
        print(
            f"[{phys_clock.now_iso()}] [{server_id}] Erro/Timeout ao contatar Reference ({exc}). Reconectando...",
            flush=True,
        )
        ref_socket.close(0)
        return create_req_socket(context, ref_addr, 5000), None


def query_peer(
    context: zmq.Context,
    peer_name: str,
    peer_port: int,
    server_id: str,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
    payload: dict,
    timeout_ms: int = 2500,
    verbose_log: bool = True,
) -> tuple[dict | None, float]:
    """Envia REQ direto para outro servidor na porta peer_port e mede RTT."""
    addr = f"tcp://{peer_name}:{peer_port}"
    sock = create_req_socket(context, addr, timeout_ms=timeout_ms)
    payload["from_server"] = server_id
    payload["timestamp"] = phys_clock.now_iso()
    payload["logical_clock"] = clock.tick_send()
    if verbose_log:
        print(
            format_log(server_id, f"ENVIADO (REQ) -> PEER '{peer_name}' [clock={payload['logical_clock']}]", payload),
            flush=True,
        )
    t0 = phys_clock.current_epoch_ms()
    try:
        sock.send(pack_message(payload))
        raw_rep = sock.recv()
        t1 = phys_clock.current_epoch_ms()
        rep = unpack_message(raw_rep)
        clock.update_receive(rep.get("logical_clock"))
        if verbose_log:
            # Evita imprimir listas gigantes no console durante sync_state
            log_view = {
                k: (f"<{len(v)} itens>" if isinstance(v, list) and len(v) > 10 else v)
                for k, v in rep.items()
            }
            print(
                format_log(server_id, f"RECEBIDO (REP) <- PEER '{peer_name}' [clock={clock.value}]", log_view),
                flush=True,
            )
        return rep, max(0.0, t1 - t0)
    except zmq.ZMQError:
        return None, 0.0
    finally:
        sock.close(0)


def broadcast_replication_event(
    pub_socket: zmq.Socket,
    pub_lock: threading.Lock,
    server_id: str,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
    op_id: str,
    op_type: str,
    user: str | None = None,
    channel: str | None = None,
    content: str | None = None,
    req_timestamp: str | None = None,
    event_timestamp: str | None = None,
) -> None:
    """Publica operação mutável no tópico 'replication' (Replicação Ativa)."""
    repl_clock = clock.tick_send()
    ts = event_timestamp or phys_clock.now_iso()
    event = {
        "topic": "replication",
        "op_id": op_id,
        "op_type": op_type,
        "origin_server": server_id,
        "user": user,
        "channel": channel,
        "content": content,
        "req_timestamp": req_timestamp,
        "timestamp": ts,
        "logical_clock": repl_clock,
    }
    with pub_lock:
        pub_socket.send_multipart([b"replication", pack_message(event)])
    print(
        format_log(
            server_id,
            f"PUBLICADO (REPL-PUB) -> TÓPICO 'replication' ({op_type}) [clock={repl_clock}]",
            event,
        ),
        flush=True,
    )


def sync_state_with_cluster(
    context: zmq.Context,
    ref_socket: zmq.Socket,
    ref_addr: str,
    server_id: str,
    peer_port: int,
    coord_state: CoordinatorState,
    storage: DiskStorage,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
) -> zmq.Socket:
    """Anti-Entropia: sincroniza estado completo com o coordenador ou pares ativos."""
    ref_socket, rep_list = call_reference(
        context,
        ref_socket,
        ref_addr,
        server_id,
        clock,
        phys_clock,
        {"action": "list", "server_name": server_id},
    )
    servers = (rep_list or {}).get("servers") or []
    peers = [str(s.get("name")) for s in servers if str(s.get("name")) and str(s.get("name")) != server_id]

    coord = coord_state.get()
    if coord and coord != server_id and coord in peers:
        peers.remove(coord)
        peers.insert(0, coord)

    for peer_name in peers:
        rep_sync, _ = query_peer(
            context,
            peer_name,
            peer_port,
            server_id,
            clock,
            phys_clock,
            {"action": "sync_state"},
            timeout_ms=3000,
        )
        if rep_sync and rep_sync.get("status") == "ok":
            added = storage.merge_snapshot(rep_sync)
            n_logins, n_ch, n_pubs = storage.counts()
            print(
                f"[{phys_clock.now_iso()}] [{server_id}] [ANTI-ENTROPIA] Estado sincronizado com '{peer_name}' | novos_registros={added} | total=(logins={n_logins}, canais={n_ch}, msgs={n_pubs})",
                flush=True,
            )
            break

    return ref_socket


def publish_coordinator_announcement(
    pub_socket: zmq.Socket,
    pub_lock: threading.Lock,
    server_id: str,
    rank: int,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
) -> None:
    pub_clock = clock.tick_send()
    announcement = {
        "topic": "servers",
        "event": "coordinator_elected",
        "coordinator": server_id,
        "server_id": server_id,
        "rank": rank,
        "timestamp": phys_clock.now_iso(),
        "logical_clock": pub_clock,
    }
    with pub_lock:
        pub_socket.send_multipart([b"servers", pack_message(announcement)])
    print(
        format_log(
            server_id,
            f"PUBLICADO (PUB) -> TÓPICO 'servers' (COORDENADOR='{server_id}') [clock={pub_clock}]",
            announcement,
        ),
        flush=True,
    )


def run_election(
    context: zmq.Context,
    ref_socket: zmq.Socket,
    ref_addr: str,
    pub_socket: zmq.Socket,
    pub_lock: threading.Lock,
    server_id: str,
    my_rank: int,
    peer_port: int,
    coord_state: CoordinatorState,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
) -> zmq.Socket:
    if not coord_state.try_begin_election():
        return ref_socket

    try:
        print(
            f"[{phys_clock.now_iso()}] [{server_id}] [ELEIÇÃO] Iniciando eleição (meu rank={my_rank})...",
            flush=True,
        )
        ref_socket, rep_list = call_reference(
            context,
            ref_socket,
            ref_addr,
            server_id,
            clock,
            phys_clock,
            {"action": "list", "server_name": server_id},
        )
        servers = (rep_list or {}).get("servers") or []

        higher_peers = [
            s for s in servers if str(s.get("name")) != server_id and int(s.get("rank", 0)) > my_rank
        ]

        higher_responded = False
        for peer in higher_peers:
            peer_name = str(peer["name"])
            rep_elec, _ = query_peer(
                context,
                peer_name,
                peer_port,
                server_id,
                clock,
                phys_clock,
                {"action": "election", "candidate": server_id, "rank": my_rank},
            )
            if rep_elec and rep_elec.get("status") == "ok":
                higher_responded = True

        if not higher_responded:
            coord_state.set(server_id)
            print(
                f"[{phys_clock.now_iso()}] [{server_id}] [ELEIÇÃO] Eleito como COORDENADOR (rank={my_rank})!",
                flush=True,
            )
            publish_coordinator_announcement(
                pub_socket,
                pub_lock,
                server_id,
                my_rank,
                clock,
                phys_clock,
            )
        else:
            print(
                f"[{phys_clock.now_iso()}] [{server_id}] [ELEIÇÃO] Servidor de rank superior respondeu OK; aguardando anúncio no tópico 'servers'...",
                flush=True,
            )
            time.sleep(1.5)
            if coord_state.get() is None:
                best = max(servers, key=lambda s: int(s.get("rank", 0)), default={"name": server_id})
                coord_state.set(str(best["name"]))
    finally:
        coord_state.end_election()

    return ref_socket


def synchronize_clock_berkeley(
    context: zmq.Context,
    ref_socket: zmq.Socket,
    ref_addr: str,
    pub_socket: zmq.Socket,
    pub_lock: threading.Lock,
    server_id: str,
    my_rank: int,
    peer_port: int,
    coord_state: CoordinatorState,
    storage: DiskStorage,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
) -> zmq.Socket:
    coordinator = coord_state.get()
    if not coordinator:
        ref_socket = run_election(
            context,
            ref_socket,
            ref_addr,
            pub_socket,
            pub_lock,
            server_id,
            my_rank,
            peer_port,
            coord_state,
            clock,
            phys_clock,
        )
        return sync_state_with_cluster(
            context,
            ref_socket,
            ref_addr,
            server_id,
            peer_port,
            coord_state,
            storage,
            clock,
            phys_clock,
        )

    if coordinator == server_id:
        ref_socket, rep_list = call_reference(
            context,
            ref_socket,
            ref_addr,
            server_id,
            clock,
            phys_clock,
            {"action": "list", "server_name": server_id},
        )
        servers = (rep_list or {}).get("servers") or []
        deltas_ms: list[float] = [0.0]

        for peer in servers:
            peer_name = str(peer.get("name", ""))
            if not peer_name or peer_name == server_id:
                continue
            rep_clk, rtt_ms = query_peer(
                context,
                peer_name,
                peer_port,
                server_id,
                clock,
                phys_clock,
                {"action": "clock"},
            )
            if rep_clk and rep_clk.get("status") == "ok" and "epoch_ms" in rep_clk:
                local_now = phys_clock.current_epoch_ms()
                peer_estimated = float(rep_clk["epoch_ms"]) + (rtt_ms / 2.0)
                deltas_ms.append(peer_estimated - local_now)

        avg_delta = sum(deltas_ms) / len(deltas_ms)
        total_offset = phys_clock.apply_offset_delta(avg_delta)
        print(
            f"[{phys_clock.now_iso()}] [{server_id}] [BERKELEY-COORD] Média calculada entre {len(deltas_ms)} servidor(es): ajuste={avg_delta:+.2f}ms (offset_total={total_offset:+.2f}ms)",
            flush=True,
        )
        return sync_state_with_cluster(
            context,
            ref_socket,
            ref_addr,
            server_id,
            peer_port,
            coord_state,
            storage,
            clock,
            phys_clock,
        )

    rep_coord, rtt_ms = query_peer(
        context,
        coordinator,
        peer_port,
        server_id,
        clock,
        phys_clock,
        {"action": "clock"},
    )
    if rep_coord and rep_coord.get("status") == "ok" and "epoch_ms" in rep_coord:
        local_now = phys_clock.current_epoch_ms()
        coord_estimated = float(rep_coord["epoch_ms"]) + (rtt_ms / 2.0)
        delta = coord_estimated - local_now
        total_offset = phys_clock.apply_offset_delta(delta)
        print(
            f"[{phys_clock.now_iso()}] [{server_id}] [BERKELEY-SYNC] Relógio sincronizado com coordenador '{coordinator}' | ajuste={delta:+.2f}ms (offset_total={total_offset:+.2f}ms)",
            flush=True,
        )
        return sync_state_with_cluster(
            context,
            ref_socket,
            ref_addr,
            server_id,
            peer_port,
            coord_state,
            storage,
            clock,
            phys_clock,
        )

    print(
        f"[{phys_clock.now_iso()}] [{server_id}] [ELEIÇÃO] Coordenador '{coordinator}' indisponível! Disparando nova eleição...",
        flush=True,
    )
    coord_state.set(None)
    ref_socket = run_election(
        context,
        ref_socket,
        ref_addr,
        pub_socket,
        pub_lock,
        server_id,
        my_rank,
        peer_port,
        coord_state,
        clock,
        phys_clock,
    )
    return sync_state_with_cluster(
        context,
        ref_socket,
        ref_addr,
        server_id,
        peer_port,
        coord_state,
        storage,
        clock,
        phys_clock,
    )


def peer_server_worker(
    context: zmq.Context,
    peer_port: int,
    server_id: str,
    my_rank: int,
    storage: DiskStorage,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
    trigger_election_event: threading.Event,
) -> None:
    """Atende requisições diretas de outros servidores (eleição, Berkeley e sync_state)."""
    peer_rep = context.socket(zmq.REP)
    peer_rep.setsockopt(zmq.LINGER, 0)
    peer_rep.bind(f"tcp://*:{peer_port}")
    print(
        f"[{phys_clock.now_iso()}] [{server_id}] [PEER-REP] Escutando peers (eleição/clock/sync_state) na porta {peer_port}",
        flush=True,
    )

    while True:
        try:
            raw = peer_rep.recv()
            req = unpack_message(raw)
            clock.update_receive(req.get("logical_clock"))
            action = str(req.get("action") or "").strip().lower()
            sender = str(req.get("from_server") or "desconhecido")

            print(
                format_log(server_id, f"RECEBIDO (PEER-REQ) <- '{sender}' [clock={clock.value}]", req),
                flush=True,
            )

            if action == "election":
                rep = {
                    "status": "ok",
                    "action": "election",
                    "message": "OK",
                    "server_id": server_id,
                    "rank": my_rank,
                    "timestamp": phys_clock.now_iso(),
                    "logical_clock": clock.tick_send(),
                }
                peer_rep.send(pack_message(rep))
                trigger_election_event.set()
            elif action in ("clock", "berkeley"):
                rep = {
                    "status": "ok",
                    "action": "clock",
                    "server_id": server_id,
                    "rank": my_rank,
                    "epoch_ms": phys_clock.current_epoch_ms(),
                    "hora_correta": phys_clock.now_iso(),
                    "timestamp": phys_clock.now_iso(),
                    "logical_clock": clock.tick_send(),
                }
                peer_rep.send(pack_message(rep))
            elif action == "sync_state":
                snapshot = storage.export_snapshot()
                rep = {
                    "status": "ok",
                    "action": "sync_state",
                    "message": f"Snapshot enviado ({len(snapshot['publications'])} mensagens).",
                    "server_id": server_id,
                    "rank": my_rank,
                    "logins": snapshot["logins"],
                    "channels": snapshot["channels"],
                    "publications": snapshot["publications"],
                    "applied_ops": snapshot["applied_ops"],
                    "timestamp": phys_clock.now_iso(),
                    "logical_clock": clock.tick_send(),
                }
                peer_rep.send(pack_message(rep))
            else:
                rep = {
                    "status": "error",
                    "action": action,
                    "message": f"Ação peer desconhecida: '{action}'",
                    "server_id": server_id,
                    "timestamp": phys_clock.now_iso(),
                    "logical_clock": clock.tick_send(),
                }
                peer_rep.send(pack_message(rep))

            log_rep = {
                k: (f"<{len(v)} itens>" if isinstance(v, list) and len(v) > 10 else v)
                for k, v in rep.items()
            }
            print(
                format_log(server_id, f"ENVIADO (PEER-REP) -> '{sender}' [clock={rep['logical_clock']}]", log_rep),
                flush=True,
            )
        except zmq.ZMQError:
            break
        except Exception as exc:
            print(f"[{phys_clock.now_iso()}] [{server_id}] [PEER-REP] Erro: {exc}", flush=True)


def cluster_topics_subscriber(
    context: zmq.Context,
    pubsub_xpub: str,
    server_id: str,
    coord_state: CoordinatorState,
    storage: DiskStorage,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
) -> None:
    """Assina os tópicos 'servers' (eleição) e 'replication' (replicação ativa)."""
    sub = context.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVTIMEO, 500)
    sub.setsockopt(zmq.LINGER, 0)
    sub.connect(pubsub_xpub)
    sub.setsockopt(zmq.SUBSCRIBE, b"servers")
    sub.setsockopt(zmq.SUBSCRIBE, b"replication")

    print(
        f"[{phys_clock.now_iso()}] [{server_id}] [SUB-CLUSTER] Inscrito nos tópicos 'servers' e 'replication' em {pubsub_xpub}",
        flush=True,
    )

    while True:
        try:
            parts = sub.recv_multipart()
            if len(parts) < 2:
                continue
            topic = parts[0].decode("utf-8", errors="replace")
            msg = unpack_message(parts[1])
            updated_clk = clock.update_receive(msg.get("logical_clock"))

            if topic == "servers":
                elected = str(msg.get("coordinator") or msg.get("server_id") or "").strip()
                if elected:
                    coord_state.set(elected)
                    print(
                        format_log(
                            server_id,
                            f"RECEBIDO (SUB) <- TÓPICO 'servers' (COORDENADOR='{elected}') [clock={updated_clk}]",
                            msg,
                        ),
                        flush=True,
                    )
            elif topic == "replication":
                origin = str(msg.get("origin_server") or "").strip()
                if not origin or origin == server_id:
                    continue

                op_id = str(msg.get("op_id") or "").strip()
                op_type = str(msg.get("op_type") or "").strip()
                ev_clock = int(msg.get("logical_clock") or updated_clk)
                ev_ts = str(msg.get("timestamp") or phys_clock.now_iso())

                applied = False
                if op_type == "login":
                    user = str(msg.get("user") or "").strip()
                    req_ts = str(msg.get("req_timestamp") or ev_ts)
                    if user:
                        applied = storage.record_login(op_id, user, req_ts, origin, ev_clock)
                elif op_type == "create_channel":
                    channel = str(msg.get("channel") or "").strip()
                    if channel:
                        applied, _ = storage.create_channel(channel, op_id=op_id)
                elif op_type == "publish":
                    channel = str(msg.get("channel") or "").strip()
                    user = str(msg.get("user") or "anon").strip()
                    content = str(msg.get("content") or "").strip()
                    req_ts = str(msg.get("req_timestamp") or ev_ts)
                    if channel and content:
                        applied = storage.record_publication(
                            op_id=op_id,
                            channel=channel,
                            user=user,
                            content=content,
                            server_id=origin,
                            req_timestamp=req_ts,
                            published_at=ev_ts,
                            logical_clock=ev_clock,
                        )

                if applied:
                    n_logins, n_ch, n_pubs = storage.counts()
                    print(
                        format_log(
                            server_id,
                            f"APLICADO (REPL-SUB) <- '{origin}' ({op_type}) | total_msgs={n_pubs}, canais={n_ch}, logins={n_logins} [clock={updated_clk}]",
                            msg,
                        ),
                        flush=True,
                    )
        except zmq.Again:
            continue
        except zmq.ZMQError:
            break


def handle_client_request(
    server_id: str,
    rank: int,
    coordinator: str | None,
    storage: DiskStorage,
    pub_socket: zmq.Socket,
    pub_lock: threading.Lock,
    clock: LogicalClock,
    phys_clock: PhysicalClockSync,
    req: dict,
) -> dict:
    action = str(req.get("action") or "").strip()
    user = str(req.get("user") or "").strip()
    req_ts = str(req.get("timestamp") or "").strip()

    if not req_ts:
        return {
            "status": "error",
            "action": action or "unknown",
            "message": "Timestamp obrigatório ausente na requisição.",
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "login":
        if not user or not USER_REGEX.match(user):
            return {
                "status": "error",
                "action": "login",
                "message": f"Nome de usuário inválido: '{user}'. Use 3-24 caracteres alfanuméricos, '_' ou '-'.",
                "server_id": server_id,
                "server_rank": rank,
                "coordinator": coordinator,
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }
        op_clock = clock.tick_send()
        op_id = f"{server_id}:{op_clock}:{req_ts}"
        storage.record_login(op_id=op_id, user=user, timestamp=req_ts, server_id=server_id, logical_clock=op_clock)
        broadcast_replication_event(
            pub_socket=pub_socket,
            pub_lock=pub_lock,
            server_id=server_id,
            clock=clock,
            phys_clock=phys_clock,
            op_id=op_id,
            op_type="login",
            user=user,
            req_timestamp=req_ts,
            event_timestamp=req_ts,
        )
        return {
            "status": "ok",
            "action": "login",
            "message": f"Login realizado com sucesso para '{user}'.",
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "list_channels":
        channels = storage.list_channels()
        return {
            "status": "ok",
            "action": "list_channels",
            "message": f"{len(channels)} canal(is) encontrado(s).",
            "channels": channels,
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "create_channel":
        channel = str(req.get("channel") or "").strip()
        op_clock = clock.tick_send()
        op_id = f"{server_id}:{op_clock}:{channel}"
        created, msg = storage.create_channel(channel, op_id=op_id)
        if created:
            broadcast_replication_event(
                pub_socket=pub_socket,
                pub_lock=pub_lock,
                server_id=server_id,
                clock=clock,
                phys_clock=phys_clock,
                op_id=op_id,
                op_type="create_channel",
                user=user or "anon",
                channel=channel,
                req_timestamp=req_ts,
            )
        return {
            "status": "ok" if created else "error",
            "action": "create_channel",
            "message": msg,
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action in ("publish", "post_message"):
        channel = str(req.get("channel") or "").strip()
        content = str(req.get("content") or req.get("message") or "").strip()

        if not channel or not CHANNEL_REGEX.match(channel):
            return {
                "status": "error",
                "action": "publish",
                "message": f"Canal inválido para publicação: '{channel}'.",
                "server_id": server_id,
                "server_rank": rank,
                "coordinator": coordinator,
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }
        if not content:
            return {
                "status": "error",
                "action": "publish",
                "message": "Conteúdo da publicação não pode ser vazio.",
                "server_id": server_id,
                "server_rank": rank,
                "coordinator": coordinator,
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }

        pub_ts = phys_clock.now_iso()
        pub_clock = clock.tick_send()
        author = user if user else "anon"
        op_id = f"{server_id}:{pub_clock}:{pub_ts}"

        pub_payload = {
            "channel": channel,
            "user": author,
            "content": content,
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": pub_ts,
            "logical_clock": pub_clock,
        }

        with pub_lock:
            pub_socket.send_multipart([channel.encode("utf-8"), pack_message(pub_payload)])
        print(
            format_log(server_id, f"PUBLICADO (PUB) -> TÓPICO '#{channel}' [clock={pub_clock}]", pub_payload),
            flush=True,
        )

        storage.record_publication(
            op_id=op_id,
            channel=channel,
            user=author,
            content=content,
            server_id=server_id,
            req_timestamp=req_ts,
            published_at=pub_ts,
            logical_clock=pub_clock,
        )

        broadcast_replication_event(
            pub_socket=pub_socket,
            pub_lock=pub_lock,
            server_id=server_id,
            clock=clock,
            phys_clock=phys_clock,
            op_id=op_id,
            op_type="publish",
            user=author,
            channel=channel,
            content=content,
            req_timestamp=req_ts,
            event_timestamp=pub_ts,
        )

        return {
            "status": "ok",
            "action": "publish",
            "message": f"Mensagem publicada e replicada no canal '#{channel}' com sucesso.",
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action in ("get_history", "history", "list_messages"):
        channel_filter = str(req.get("channel") or "").strip() or None
        limit = int(req.get("limit") or 50)
        total_msgs, messages = storage.get_history(channel=channel_filter, limit=limit)
        return {
            "status": "ok",
            "action": "get_history",
            "message": f"Histórico replicado: {total_msgs} mensagem(ns) armazenada(s) em '{server_id}'.",
            "total_messages": total_msgs,
            "messages": messages,
            "server_id": server_id,
            "server_rank": rank,
            "coordinator": coordinator,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    return {
        "status": "error",
        "action": action or "unknown",
        "message": f"Ação desconhecida: '{action}'.",
        "server_id": server_id,
        "server_rank": rank,
        "coordinator": coordinator,
        "timestamp": phys_clock.now_iso(),
        "logical_clock": clock.tick_send(),
    }


def main() -> None:
    server_id = os.getenv("SERVER_ID", "server-python")
    broker_backend = os.getenv("BROKER_BACKEND", "tcp://broker:5556")
    pubsub_xsub = os.getenv("PUBSUB_XSUB", "tcp://pubsub-proxy:5557")
    pubsub_xpub = os.getenv("PUBSUB_XPUB", "tcp://pubsub-proxy:5558")
    reference_addr = os.getenv("REFERENCE_ADDR", "tcp://reference:5559")
    peer_port = int(os.getenv("PEER_PORT", "5560"))
    heartbeat_every = int(os.getenv("HEARTBEAT_EVERY", "10"))
    sync_every = int(os.getenv("SYNC_EVERY", "15"))
    data_file = os.getenv("DATA_FILE", "/data/storage.msgpack")

    storage = DiskStorage(data_file)
    clock = LogicalClock(0)
    phys_clock = PhysicalClockSync()
    coord_state = CoordinatorState()
    pub_lock = threading.Lock()
    trigger_election_event = threading.Event()

    context = zmq.Context()
    ref_socket = create_req_socket(context, reference_addr, timeout_ms=5000)

    rank = -1
    while rank < 0:
        ref_socket, rep_rank = call_reference(
            context,
            ref_socket,
            reference_addr,
            server_id,
            clock,
            phys_clock,
            {"action": "rank", "server_name": server_id},
        )
        if rep_rank and rep_rank.get("status") == "ok":
            rank = int(rep_rank.get("rank", 1))
        else:
            time.sleep(1.0)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.setsockopt(zmq.LINGER, 0)
    pub_socket.connect(pubsub_xsub)

    sub_thread = threading.Thread(
        target=cluster_topics_subscriber,
        args=(context, pubsub_xpub, server_id, coord_state, storage, clock, phys_clock),
        daemon=True,
    )
    sub_thread.start()

    peer_thread = threading.Thread(
        target=peer_server_worker,
        args=(
            context,
            peer_port,
            server_id,
            rank,
            storage,
            clock,
            phys_clock,
            trigger_election_event,
        ),
        daemon=True,
    )
    peer_thread.start()

    time.sleep(1.0)

    if coord_state.get() is None:
        ref_socket = run_election(
            context,
            ref_socket,
            reference_addr,
            pub_socket,
            pub_lock,
            server_id,
            rank,
            peer_port,
            coord_state,
            clock,
            phys_clock,
        )

    # Catch-up inicial via Anti-Entropia para garantir consistência ao entrar no cluster
    ref_socket = sync_state_with_cluster(
        context,
        ref_socket,
        reference_addr,
        server_id,
        peer_port,
        coord_state,
        storage,
        clock,
        phys_clock,
    )

    rep_socket = context.socket(zmq.REP)
    rep_socket.setsockopt(zmq.RCVTIMEO, 1000)
    rep_socket.connect(broker_backend)

    print(
        f"[{phys_clock.now_iso()}] [{server_id}] Rank={rank} | Coordenador='{coord_state.get()}' | Broker={broker_backend}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{phys_clock.now_iso()}] [{server_id}] Encerrando servidor...", flush=True)
        rep_socket.close(0)
        pub_socket.close(0)
        ref_socket.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    exchanged_messages = 0
    client_messages = 0

    while True:
        if trigger_election_event.is_set():
            trigger_election_event.clear()
            ref_socket = run_election(
                context,
                ref_socket,
                reference_addr,
                pub_socket,
                pub_lock,
                server_id,
                rank,
                peer_port,
                coord_state,
                clock,
                phys_clock,
            )

        try:
            raw_req = rep_socket.recv()
        except zmq.Again:
            continue

        try:
            req = unpack_message(raw_req)
            clock.update_receive(req.get("logical_clock"))
            client_messages += 1
            exchanged_messages += 1

            print(
                format_log(
                    server_id,
                    f"RECEBIDO (REQ #{client_messages}) <- CLIENTE [clock={clock.value}]",
                    req,
                ),
                flush=True,
            )

            rep = handle_client_request(
                server_id,
                rank,
                coord_state.get(),
                storage,
                pub_socket,
                pub_lock,
                clock,
                phys_clock,
                req,
            )
            log_rep = {
                k: (f"<{len(v)} itens>" if isinstance(v, list) and len(v) > 10 else v)
                for k, v in rep.items()
            }
            print(
                format_log(
                    server_id,
                    f"ENVIADO (REP) -> CLIENTE [clock={rep['logical_clock']}]",
                    log_rep,
                ),
                flush=True,
            )
            rep_socket.send(pack_message(rep))
            exchanged_messages += 1

            if client_messages % heartbeat_every == 0:
                ref_socket, _ = call_reference(
                    context,
                    ref_socket,
                    reference_addr,
                    server_id,
                    clock,
                    phys_clock,
                    {"action": "heartbeat", "server_name": server_id},
                )

            if exchanged_messages >= sync_every:
                exchanged_messages = 0
                ref_socket = synchronize_clock_berkeley(
                    context,
                    ref_socket,
                    reference_addr,
                    pub_socket,
                    pub_lock,
                    server_id,
                    rank,
                    peer_port,
                    coord_state,
                    storage,
                    clock,
                    phys_clock,
                )
        except Exception as exc:
            err_rep = {
                "status": "error",
                "action": "unknown",
                "message": f"Erro ao processar mensagem binária: {exc}",
                "server_id": server_id,
                "server_rank": rank,
                "coordinator": coord_state.get(),
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }
            print(format_log(server_id, "ENVIADO (ERRO REP) -> CLIENTE", err_rep), flush=True)
            rep_socket.send(pack_message(err_rep))


if __name__ == "__main__":
    main()
