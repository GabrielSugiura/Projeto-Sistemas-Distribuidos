from datetime import datetime, timezone
import json
import os
import re
import msgpack

USER_REGEX = re.compile(r"^[a-zA-Z0-9_-]{3,24}$")
CHANNEL_REGEX = re.compile(r"^[a-z0-9_-]{2,32}$")
CONTROL_CODES_REGEX = re.compile(r"\^(?:W\d+|M\d+|O)")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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
            lines.append(f"  {k:<10}: {v}")
    lines.append(sep)
    return "\n".join(lines)


def parse_script_events(filepath: str) -> list[dict]:
    if not os.path.exists(filepath):
        return []

    events: list[dict] = []
    current_channel = "geral"

    with open(filepath, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("---") or line.startswith("!"):
                continue

            if line.startswith("#T:") or line.startswith("##T:"):
                action_text = clean_script_text(line.split(":", 1)[1])
                if action_text:
                    events.append(
                        {
                            "type": "post_message",
                            "user": "system_irc",
                            "channel": current_channel,
                            "content": f"* {action_text}",
                        }
                    )
                continue

            if line.startswith("#j"):
                joined_user = line[2:].strip()
                if USER_REGEX.match(joined_user):
                    events.append({"type": "login", "user": joined_user})
                continue

            if line.startswith("#k") or line.startswith("#l"):
                continue

            if ":" in line:
                speaker, msg = line.split(":", 1)
                speaker = speaker.strip()
                cleaned_msg = clean_script_text(msg)
                if not cleaned_msg or not USER_REGEX.match(speaker):
                    continue

                if "#hacking" in cleaned_msg.lower() and current_channel != "hacking":
                    current_channel = "hacking"
                    events.append(
                        {
                            "type": "create_channel",
                            "user": speaker,
                            "channel": "hacking",
                        }
                    )

                events.append(
                    {
                        "type": "post_message",
                        "user": speaker,
                        "channel": current_channel,
                        "content": cleaned_msg,
                    }
                )

    return events
