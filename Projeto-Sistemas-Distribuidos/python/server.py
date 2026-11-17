import os
import signal
import sys
import time
import zmq
import msgpack
from protocol import (
    USER_REGEX,
    CHANNEL_REGEX,
    now_iso,
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
                print(f"[{now_iso()}] [STORAGE] Aviso ao ler {self.filepath}: {exc}", flush=True)
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
            }
        )
        self._save()


def handle_request(
    server_id: str,
    storage: DiskStorage,
    pub_socket: zmq.Socket,
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
            "timestamp": now_iso(),
        }

    if action == "login":
        if not user or not USER_REGEX.match(user):
            return {
                "status": "error",
                "action": "login",
                "message": f"Nome de usuário inválido: '{user}'. Use 3-24 caracteres alfanuméricos, '_' ou '-'.",
                "server_id": server_id,
                "timestamp": now_iso(),
            }
        storage.record_login(user=user, timestamp=req_ts)
        return {
            "status": "ok",
            "action": "login",
            "message": f"Login realizado com sucesso para '{user}'.",
            "server_id": server_id,
            "timestamp": now_iso(),
        }

    if action == "list_channels":
        channels = storage.list_channels()
        return {
            "status": "ok",
            "action": "list_channels",
            "message": f"{len(channels)} canal(is) encontrado(s).",
            "channels": channels,
            "server_id": server_id,
            "timestamp": now_iso(),
        }

    if action == "create_channel":
        channel = str(req.get("channel") or "").strip()
        created, msg = storage.create_channel(channel)
        return {
            "status": "ok" if created else "error",
            "action": "create_channel",
            "message": msg,
            "server_id": server_id,
            "timestamp": now_iso(),
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
                "timestamp": now_iso(),
            }
        if not content:
            return {
                "status": "error",
                "action": "publish",
                "message": "Conteúdo da publicação não pode ser vazio.",
                "server_id": server_id,
                "timestamp": now_iso(),
            }

        pub_ts = now_iso()
        author = user if user else "anon"
        pub_payload = {
            "channel": channel,
            "user": author,
            "content": content,
            "server_id": server_id,
            "timestamp": pub_ts,
        }

        pub_socket.send_multipart([channel.encode("utf-8"), pack_message(pub_payload)])
        print(format_log(server_id, f"PUBLICADO (PUB) -> TÓPICO '#{channel}'", pub_payload), flush=True)

        storage.record_publication(
            channel=channel,
            user=author,
            content=content,
            server_id=server_id,
            req_timestamp=req_ts,
            published_at=pub_ts,
        )

        return {
            "status": "ok",
            "action": "publish",
            "message": f"Mensagem publicada no canal '#{channel}' com sucesso.",
            "server_id": server_id,
            "timestamp": now_iso(),
        }

    return {
        "status": "error",
        "action": action or "unknown",
        "message": f"Ação desconhecida: '{action}'.",
        "server_id": server_id,
        "timestamp": now_iso(),
    }


def main() -> None:
    server_id = os.getenv("SERVER_ID", "server-python")
    broker_backend = os.getenv("BROKER_BACKEND", "tcp://broker:5556")
    pubsub_xsub = os.getenv("PUBSUB_XSUB", "tcp://pubsub-proxy:5557")
    data_file = os.getenv("DATA_FILE", "/data/storage.msgpack")

    storage = DiskStorage(data_file)
    context = zmq.Context()

    rep_socket = context.socket(zmq.REP)
    rep_socket.connect(broker_backend)

    pub_socket = context.socket(zmq.PUB)
    pub_socket.setsockopt(zmq.LINGER, 0)
    pub_socket.connect(pubsub_xsub)

    time.sleep(0.5)

    print(
        f"[{now_iso()}] [{server_id}] Conectado | Broker REP: {broker_backend} | Proxy PUB: {pubsub_xsub} | Storage: {data_file}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{now_iso()}] [{server_id}] Encerrando servidor...", flush=True)
        rep_socket.close(0)
        pub_socket.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            raw_req = rep_socket.recv()
            req = unpack_message(raw_req)
            print(format_log(server_id, "RECEBIDO (REQ) <- CLIENTE", req), flush=True)

            rep = handle_request(server_id, storage, pub_socket, req)
            print(format_log(server_id, "ENVIADO (REP) -> CLIENTE", rep), flush=True)
            rep_socket.send(pack_message(rep))
        except Exception as exc:
            err_rep = {
                "status": "error",
                "action": "unknown",
                "message": f"Erro ao processar mensagem binária: {exc}",
                "server_id": server_id,
                "timestamp": now_iso(),
            }
            print(format_log(server_id, "ENVIADO (ERRO REP) -> CLIENTE", err_rep), flush=True)
            rep_socket.send(pack_message(err_rep))


if __name__ == "__main__":
    main()
