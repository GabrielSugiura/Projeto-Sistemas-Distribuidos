import os
import signal
import sys
import time
import zmq
import msgpack
from protocol import (
    USER_REGEX,
    CHANNEL_REGEX,
    LogicalClock,
    PhysicalClockSync,
    now_epoch_ms,
    pack_message,
    unpack_message,
    format_log,
)


class DiskStorage:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
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
                        return data
            except Exception as exc:
                print(f"[{time.time()}] [STORAGE] Aviso ao ler {self.filepath}: {exc}", flush=True)
        return {"logins": [], "channels": [], "publications": []}

    def _save(self) -> None:
        tmp_path = f"{self.filepath}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(msgpack.packb(self.state, use_bin_type=True))
        os.replace(tmp_path, self.filepath)

    def record_login(self, user: str, timestamp: str) -> None:
        self.state["logins"].append({"user": user, "timestamp": timestamp})
        self._save()

    def list_channels(self) -> list[str]:
        return sorted(self.state["channels"])

    def create_channel(self, channel: str) -> tuple[bool, str]:
        if not channel or not CHANNEL_REGEX.match(channel):
            return (
                False,
                "Nome de canal inválido (use 2-32 caracteres: letras minúsculas, números, '_' ou '-').",
            )
        if channel in self.state["channels"]:
            return False, f"Canal '{channel}' já existe."
        self.state["channels"].append(channel)
        self._save()
        return True, f"Canal '{channel}' criado com sucesso."

    def record_publication(
        self,
        channel: str,
        user: str,
        content: str,
        server_id: str,
        req_timestamp: str,
        published_at: str,
        logical_clock: int,
    ) -> None:
        if channel not in self.state["channels"] and CHANNEL_REGEX.match(channel):
            self.state["channels"].append(channel)
        self.state["publications"].append(
            {
                "channel": channel,
                "user": user,
                "content": content,
                "server_id": server_id,
                "req_timestamp": req_timestamp,
                "published_at": published_at,
                "logical_clock": logical_clock,
            }
        )
        self._save()


def create_ref_socket(context: zmq.Context, ref_addr: str) -> zmq.Socket:
    sock = context.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    sock.setsockopt(zmq.SNDTIMEO, 5000)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(ref_addr)
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
    t0_ms = now_epoch_ms()
    try:
        ref_socket.send(pack_message(payload))
        raw_rep = ref_socket.recv()
        t1_ms = now_epoch_ms()
        rep = unpack_message(raw_rep)
        clock.update_receive(rep.get("logical_clock"))

        if "reference_epoch_ms" in rep:
            offset = phys_clock.sync_with_reference(t0_ms, float(rep["reference_epoch_ms"]), t1_ms)
            print(
                f"[{phys_clock.now_iso()}] [{server_id}] [CLOCK-SYNC] Relógio físico sincronizado com referência | offset={offset:+.2f}ms",
                flush=True,
            )

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
        return create_ref_socket(context, ref_addr), None


def handle_request(
    server_id: str,
    rank: int,
    storage: DiskStorage,
    pub_socket: zmq.Socket,
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
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }
        storage.record_login(user=user, timestamp=req_ts)
        return {
            "status": "ok",
            "action": "login",
            "message": f"Login realizado com sucesso para '{user}'.",
            "server_id": server_id,
            "server_rank": rank,
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
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "create_channel":
        channel = str(req.get("channel") or "").strip()
        created, msg = storage.create_channel(channel)
        return {
            "status": "ok" if created else "error",
            "action": "create_channel",
            "message": msg,
            "server_id": server_id,
            "server_rank": rank,
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
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }

        pub_ts = phys_clock.now_iso()
        pub_clock = clock.tick_send()
        author = user if user else "anon"
        pub_payload = {
            "channel": channel,
            "user": author,
            "content": content,
            "server_id": server_id,
            "server_rank": rank,
            "timestamp": pub_ts,
            "logical_clock": pub_clock,
        }

        pub_socket.send_multipart([channel.encode("utf-8"), pack_message(pub_payload)])
        print(
            format_log(server_id, f"PUBLICADO (PUB) -> TÓPICO '#{channel}' [clock={pub_clock}]", pub_payload),
            flush=True,
        )

        storage.record_publication(
            channel=channel,
            user=author,
            content=content,
            server_id=server_id,
            req_timestamp=req_ts,
            published_at=pub_ts,
            logical_clock=pub_clock,
        )

        return {
            "status": "ok",
            "action": "publish",
            "message": f"Mensagem publicada no canal '#{channel}' com sucesso.",
            "server_id": server_id,
            "server_rank": rank,
            "timestamp": phys_clock.now_iso(),
            "logical_clock": clock.tick_send(),
        }

    return {
        "status": "error",
        "action": action or "unknown",
        "message": f"Ação desconhecida: '{action}'.",
        "server_id": server_id,
        "server_rank": rank,
        "timestamp": phys_clock.now_iso(),
        "logical_clock": clock.tick_send(),
    }


def main() -> None:
    server_id = os.getenv("SERVER_ID", "server-python")
    broker_backend = os.getenv("BROKER_BACKEND", "tcp://broker:5556")
    pubsub_xsub = os.getenv("PUBSUB_XSUB", "tcp://pubsub-proxy:5557")
    reference_addr = os.getenv("REFERENCE_ADDR", "tcp://reference:5559")
    heartbeat_every = int(os.getenv("HEARTBEAT_EVERY", "10"))
    data_file = os.getenv("DATA_FILE", "/data/storage.msgpack")

    storage = DiskStorage(data_file)
    clock = LogicalClock(0)
    phys_clock = PhysicalClockSync()

    context = zmq.Context()

    ref_socket = create_ref_socket(context, reference_addr)

    # 1. Ao iniciar, solicita seu rank ao serviço de referência (sem repetição de nome)
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

    # Consulta lista de servidores disponíveis
    ref_socket, _ = call_reference(
        context,
        ref_socket,
        reference_addr,
        server_id,
        clock,
        phys_clock,
        {"action": "list", "server_name": server_id},
    )

    rep_socket = context.socket(zmq.REP)
    rep_socket.connect(broker_backend)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.setsockopt(zmq.LINGER, 0)
    pub_socket.connect(pubsub_xsub)

    time.sleep(0.5)

    print(
        f"[{phys_clock.now_iso()}] [{server_id}] Rank={rank} | Broker REP: {broker_backend} | Proxy PUB: {pubsub_xsub} | Reference: {reference_addr}",
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

    client_msg_count = 0

    while True:
        try:
            raw_req = rep_socket.recv()
            req = unpack_message(raw_req)
            clock.update_receive(req.get("logical_clock"))
            client_msg_count += 1

            print(
                format_log(
                    server_id,
                    f"RECEBIDO (REQ #{client_msg_count}) <- CLIENTE [clock={clock.value}]",
                    req,
                ),
                flush=True,
            )

            rep = handle_request(server_id, rank, storage, pub_socket, clock, phys_clock, req)
            print(
                format_log(
                    server_id,
                    f"ENVIADO (REP) -> CLIENTE [clock={rep['logical_clock']}]",
                    rep,
                ),
                flush=True,
            )
            rep_socket.send(pack_message(rep))

            # A cada 10 mensagens de clientes recebidas, envia heartbeat e sincroniza o relógio físico
            if client_msg_count % heartbeat_every == 0:
                ref_socket, _ = call_reference(
                    context,
                    ref_socket,
                    reference_addr,
                    server_id,
                    clock,
                    phys_clock,
                    {"action": "heartbeat", "server_name": server_id},
                )
                ref_socket, _ = call_reference(
                    context,
                    ref_socket,
                    reference_addr,
                    server_id,
                    clock,
                    phys_clock,
                    {"action": "list", "server_name": server_id},
                )
        except Exception as exc:
            err_rep = {
                "status": "error",
                "action": "unknown",
                "message": f"Erro ao processar mensagem binária: {exc}",
                "server_id": server_id,
                "server_rank": rank,
                "timestamp": phys_clock.now_iso(),
                "logical_clock": clock.tick_send(),
            }
            print(format_log(server_id, "ENVIADO (ERRO REP) -> CLIENTE", err_rep), flush=True)
            rep_socket.send(pack_message(err_rep))


if __name__ == "__main__":
    main()
