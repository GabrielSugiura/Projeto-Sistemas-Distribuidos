from datetime import datetime, timezone
import json
import os
import re
import threading
import time
import msgpack

USER_REGEX = re.compile(r"^[a-zA-Z0-9_-]{3,24}$")
CHANNEL_REGEX = re.compile(r"^[a-z0-9_-]{2,32}$")
CONTROL_CODES_REGEX = re.compile(r"\^(?:W\d+|M\d+|O)")


class LogicalClock:
    """Relógio Lógico de Lamport thread-safe."""

    def __init__(self, initial: int = 0) -> None:
        self._counter = initial
        self._lock = threading.Lock()

    def tick_send(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter

    def update_receive(self, received_counter: int | None) -> int:
        with self._lock:
            if isinstance(received_counter, int) and received_counter > self._counter:
                self._counter = received_counter
            return self._counter

    @property
    def value(self) -> int:
        with self._lock:
            return self._counter


class PhysicalClockSync:
    """Mantém o offset (ms) do relógio físico sincronizado via algoritmo de Berkeley."""

    def __init__(self) -> None:
        self._offset_ms: float = 0.0
        self._lock = threading.Lock()

    def apply_offset_delta(self, delta_ms: float) -> float:
        with self._lock:
            self._offset_ms += delta_ms
            return self._offset_ms

    def sync_with_reference(self, t0_local_ms: float, ref_epoch_ms: float, t1_local_ms: float) -> float:
        rtt_ms = max(0.0, t1_local_ms - t0_local_ms)
        estimated_ref_ms = ref_epoch_ms + (rtt_ms / 2.0)
        offset = estimated_ref_ms - t1_local_ms
        with self._lock:
            self._offset_ms = offset
            return self._offset_ms

    def current_epoch_ms(self) -> float:
        with self._lock:
            return (time.time() * 1000.0) + self._offset_ms

    def now_iso(self) -> str:
        adjusted_sec = self.current_epoch_ms() / 1000.0
        return (
            datetime.fromtimestamp(adjusted_sec, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    @property
    def offset_ms(self) -> float:
        with self._lock:
            return self._offset_ms


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_epoch_ms() -> float:
    return time.time() * 1000.0


def clean_script_text(text: str) -> str:
    cleaned = CONTROL_CODES_REGEX.sub("", text)
    return re.sub(r"\s+", " ", cleaned).strip()


def pack_message(payload: dict) -> bytes:
    if "timestamp" not in payload:
        payload["timestamp"] = now_iso()
    return msgpack.packb(payload, use_bin_type=True)


def unpack_message(raw: bytes) -> dict:
    data = msgpack.unpackb(raw, raw=False)
    if not isinstance(data, dict):
        raise ValueError("Payload MessagePack inválido: esperado um mapa/dicionário.")
    return data


def record_message_history(actor: str, direction: str, payload: dict) -> str:
    logged_at = now_iso()
    log_file = os.getenv("MESSAGE_LOG_FILE", f"/logs/{actor}.jsonl")
    transcript_file = "/logs/transcricao_chat.log"
    clean_payload = {k: v for k, v in payload.items() if v is not None}

    try:
        parent = os.path.dirname(log_file)
        if parent:
            os.makedirs(parent, exist_ok=True)
        entry = {
            "logged_at": logged_at,
            "actor": actor,
            "direction": direction,
            "payload": clean_payload,
        }
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        summary_fields = ", ".join(f"{k}={v}" for k, v in clean_payload.items())
        with open(transcript_file, "a", encoding="utf-8") as tf:
            tf.write(f"[{logged_at}] [{actor}] {direction} | {summary_fields}\n")
    except Exception as exc:
        print(f"[{logged_at}] [{actor}] Aviso ao salvar histórico em {log_file}: {exc}", flush=True)

    return logged_at


def format_log(actor: str, direction: str, payload: dict) -> str:
    logged_at = record_message_history(actor, direction, payload)
    sep = "=" * 72
    lines = [
        sep,
        f"[{logged_at}] [{actor}] {direction}",
    ]
    for k, v in payload.items():
        if v is not None:
            lines.append(f"  {k:<22}: {v}")
    lines.append(sep)
    return "\n".join(lines)


def load_sample_messages(filepath: str) -> list[str]:
    samples: list[str] = []
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("---") or line.startswith("!") or line.startswith("#"):
                    continue
                if ":" in line:
                    speaker, msg = line.split(":", 1)
                    cleaned = clean_script_text(msg)
                    if cleaned and len(cleaned) >= 3:
                        samples.append(f"[{speaker.strip()}] {cleaned}")
    if not samples:
        samples = [
            "Olá pessoal, testando publicação distribuída via ZeroMQ!",
            "Alguém online neste canal?",
            "Status dos nós operando normalmente.",
            "Sincronizando estado entre Python e Rust via MessagePack.",
        ]
    return samples
