import os
import signal
import sys
import threading
import time
import zmq
import msgpack
from protocol import (
    LogicalClock,
    now_iso,
    pack_message,
    unpack_message,
    format_log,
)


class ReferenceRegistry:
    """Armazena ranks únicos de servidores e controla disponibilidade via heartbeat."""

    def __init__(self, filepath: str, heartbeat_timeout: float) -> None:
        self.filepath = filepath
        self.heartbeat_timeout = heartbeat_timeout
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.state = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "rb") as f:
                    data = msgpack.unpackb(f.read(), raw=False)
                    if isinstance(data, dict):
                        data.setdefault("next_rank", 1)
                        data.setdefault("servers", {})
                        return data
            except Exception as exc:
                print(f"[{now_iso()}] [REFERENCE] Aviso ao carregar {self.filepath}: {exc}", flush=True)
        return {"next_rank": 1, "servers": {}}

    def _save(self) -> None:
        tmp_path = f"{self.filepath}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(msgpack.packb(self.state, use_bin_type=True))
        os.replace(tmp_path, self.filepath)

    def register_or_get_rank(self, server_name: str) -> int:
        with self._lock:
            now_mono = time.monotonic()
            now_str = now_iso()
            servers = self.state["servers"]
            if server_name in servers:
                info = servers[server_name]
                info["available"] = True
                info["last_seen_mono"] = now_mono
                info["last_seen_iso"] = now_str
                self._save()
                return int(info["rank"])

            rank = int(self.state["next_rank"])
            self.state["next_rank"] = rank + 1
            servers[server_name] = {
                "name": server_name,
                "rank": rank,
                "available": True,
                "last_seen_mono": now_mono,
                "last_seen_iso": now_str,
            }
            self._save()
            return rank

    def heartbeat(self, server_name: str) -> int:
        return self.register_or_get_rank(server_name)

    def list_available_servers(self) -> list[dict]:
        with self._lock:
            now_mono = time.monotonic()
            active = []
            changed = False
            for name, info in self.state["servers"].items():
                last_seen = float(info.get("last_seen_mono", 0.0))
                if info.get("available") and (now_mono - last_seen) <= self.heartbeat_timeout:
                    active.append({"name": name, "rank": int(info["rank"])})
                elif info.get("available"):
                    info["available"] = False
                    changed = True
            if changed:
                self._save()
            active.sort(key=lambda item: item["rank"])
            return active

    def purge_expired(self) -> list[str]:
        with self._lock:
            now_mono = time.monotonic()
            removed: list[str] = []
            for name, info in self.state["servers"].items():
                last_seen = float(info.get("last_seen_mono", 0.0))
                if info.get("available") and (now_mono - last_seen) > self.heartbeat_timeout:
                    info["available"] = False
                    removed.append(f"{name} (rank={info['rank']})")
            if removed:
                self._save()
            return removed


def heartbeat_watchdog(registry: ReferenceRegistry, interval: float = 5.0) -> None:
    while True:
        time.sleep(interval)
        removed = registry.purge_expired()
        for entry in removed:
            print(
                f"[{now_iso()}] [REFERENCE] [WATCHDOG] Servidor removido da lista de disponíveis por timeout de heartbeat: {entry}",
                flush=True,
            )


def handle_reference_request(
    registry: ReferenceRegistry,
    clock: LogicalClock,
    req: dict,
) -> dict:
    action = str(req.get("action") or "").strip().lower()
    server_name = str(req.get("server_name") or req.get("user") or req.get("name") or "").strip()

    if action in ("rank", "get_rank", "register"):
        if not server_name:
            return {
                "status": "error",
                "action": "rank",
                "message": "Nome do servidor obrigatório para obter rank.",
                "timestamp": now_iso(),
                "logical_clock": clock.tick_send(),
            }
        rank = registry.register_or_get_rank(server_name)
        return {
            "status": "ok",
            "action": "rank",
            "server_name": server_name,
            "rank": rank,
            "message": f"Servidor '{server_name}' registrado com rank {rank}.",
            "timestamp": now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "list":
        servers = registry.list_available_servers()
        return {
            "status": "ok",
            "action": "list",
            "servers": servers,
            "message": f"{len(servers)} servidor(es) disponível(is).",
            "timestamp": now_iso(),
            "logical_clock": clock.tick_send(),
        }

    if action == "heartbeat":
        if not server_name:
            return {
                "status": "error",
                "action": "heartbeat",
                "message": "Nome do servidor obrigatório no heartbeat.",
                "timestamp": now_iso(),
                "logical_clock": clock.tick_send(),
            }
        rank = registry.heartbeat(server_name)
        # Na Parte 4 o serviço de referência responde apenas OK, sem hora física
        return {
            "status": "ok",
            "action": "heartbeat",
            "server_name": server_name,
            "rank": rank,
            "message": "OK",
            "timestamp": now_iso(),
            "logical_clock": clock.tick_send(),
        }

    return {
        "status": "error",
        "action": action or "unknown",
        "message": f"Ação desconhecida no serviço de referência: '{action}'.",
        "timestamp": now_iso(),
        "logical_clock": clock.tick_send(),
    }


def main() -> None:
    bind_addr = os.getenv("REFERENCE_BIND", "tcp://*:5559")
    heartbeat_timeout = float(os.getenv("HEARTBEAT_TIMEOUT", "45.0"))
    data_file = os.getenv("DATA_FILE", "/data/reference.msgpack")

    registry = ReferenceRegistry(data_file, heartbeat_timeout)
    clock = LogicalClock(0)

    watchdog = threading.Thread(
        target=heartbeat_watchdog,
        args=(registry, 5.0),
        daemon=True,
    )
    watchdog.start()

    context = zmq.Context()
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(bind_addr)

    print(
        f"[{now_iso()}] [REFERENCE] Serviço de Referência (Parte 4) iniciado em {bind_addr} | Timeout Heartbeat: {heartbeat_timeout}s | Storage: {data_file}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{now_iso()}] [REFERENCE] Encerrando...", flush=True)
        rep_socket.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while True:
        try:
            raw_req = rep_socket.recv()
            req = unpack_message(raw_req)
            clock.update_receive(req.get("logical_clock"))
            print(format_log("reference", f"RECEBIDO (REQ) <- SERVIDOR [clock={clock.value}]", req), flush=True)

            rep = handle_reference_request(registry, clock, req)
            print(format_log("reference", f"ENVIADO (REP) -> SERVIDOR [clock={rep['logical_clock']}]", rep), flush=True)
            rep_socket.send(pack_message(rep))
        except Exception as exc:
            err_rep = {
                "status": "error",
                "action": "unknown",
                "message": f"Erro no serviço de referência: {exc}",
                "timestamp": now_iso(),
                "logical_clock": clock.tick_send(),
            }
            print(format_log("reference", "ENVIADO (ERRO REP) -> SERVIDOR", err_rep), flush=True)
            rep_socket.send(pack_message(err_rep))


if __name__ == "__main__":
    main()
