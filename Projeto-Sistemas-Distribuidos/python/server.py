import os
import signal
import sys
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
                        data.setdefault("messages", [])
                        return data
            except Exception as exc:
                print(f"[{now_iso()}] [STORAGE] Aviso ao ler {self.filepath}: {exc}", flush=True)
        return {"logins": [], "channels": [], "messages": []}

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

    def record_message(self, user: str, channel: str, content: str, timestamp: str) -> None:
        if channel not in self.state["channels"] and CHANNEL_REGEX.match(channel):
            self.state["channels"].append(channel)
        self.state["messages"].append(
            {
                "user": user,
                "channel": channel,
                "content": content,
                "timestamp": timestamp,
            }
        )
        self._save()


def handle_request(server_id: str, storage: DiskStorage, req: dict) -> dict:
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

    if action == "post_message":
        channel = str(req.get("channel") or "geral").strip()
        content = str(req.get("content") or "").strip()
        if not content:
            return {
                "status": "error",
                "action": "post_message",
                "message": "Conteúdo da mensagem não pode ser vazio.",
                "server_id": server_id,
                "timestamp": now_iso(),
            }
        storage.record_message(user=user or "anon", channel=channel, content=content, timestamp=req_ts)
        return {
            "status": "ok",
            "action": "post_message",
            "message": f"Mensagem de '{user}' registrada em '#{channel}'.",
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
    data_file = os.getenv("DATA_FILE", "/data/storage.msgpack")

    storage = DiskStorage(data_file)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(broker_backend)

    print(
        f"[{now_iso()}] [{server_id}] Conectado ao Broker em {broker_backend} | Persistência: {data_file}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{now_iso()}] [{server_id}] Encerrando servidor...", flush=True)
        socket.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            raw_req = socket.recv()
            req = unpack_message(raw_req)
            print(format_log(server_id, "RECEBIDO <- CLIENTE", req), flush=True)

            rep = handle_request(server_id, storage, req)
            print(format_log(server_id, "ENVIADO -> CLIENTE", rep), flush=True)
            socket.send(pack_message(rep))
        except Exception as exc:
            err_rep = {
                "status": "error",
                "action": "unknown",
                "message": f"Erro ao processar mensagem binária: {exc}",
                "server_id": server_id,
                "timestamp": now_iso(),
            }
            print(format_log(server_id, "ENVIADO (ERRO) -> CLIENTE", err_rep), flush=True)
            socket.send(pack_message(err_rep))


if __name__ == "__main__":
    main()
