import os
import random
import threading
import time
import zmq
from protocol import (
    LogicalClock,
    now_iso,
    pack_message,
    unpack_message,
    format_log,
    load_sample_messages,
)


def create_req_socket(context: zmq.Context, endpoint: str) -> zmq.Socket:
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.RCVTIMEO, 5000)
    socket.setsockopt(zmq.SNDTIMEO, 5000)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(endpoint)
    return socket


def send_and_recv(
    context: zmq.Context,
    socket: zmq.Socket,
    endpoint: str,
    bot_name: str,
    clock: LogicalClock,
    payload: dict,
) -> tuple[zmq.Socket, dict | None]:
    payload["timestamp"] = now_iso()
    payload["logical_clock"] = clock.tick_send()
    print(
        format_log(bot_name, f"ENVIADO (REQ) -> SERVIDOR [clock={payload['logical_clock']}]", payload),
        flush=True,
    )
    try:
        socket.send(pack_message(payload))
        raw_rep = socket.recv()
        rep = unpack_message(raw_rep)
        clock.update_receive(rep.get("logical_clock"))
        log_rep = {
            k: (f"<{len(v)} itens>" if isinstance(v, list) and len(v) > 5 else v)
            for k, v in rep.items()
        }
        print(
            format_log(bot_name, f"RECEBIDO (REP) <- SERVIDOR [clock={clock.value}]", log_rep),
            flush=True,
        )
        return socket, rep
    except zmq.ZMQError as exc:
        print(
            f"[{now_iso()}] [{bot_name}] Timeout/Erro ZMQ ({exc}). Reconectando socket...",
            flush=True,
        )
        socket.close(0)
        new_socket = create_req_socket(context, endpoint)
        return new_socket, None


def subscriber_worker(
    context: zmq.Context,
    pubsub_xpub: str,
    bot_name: str,
    clock: LogicalClock,
    subscribed_channels: set[str],
    sub_lock: threading.Lock,
) -> None:
    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.RCVTIMEO, 500)
    sub_socket.setsockopt(zmq.LINGER, 0)
    sub_socket.connect(pubsub_xpub)

    applied_topics: set[str] = set()

    while True:
        with sub_lock:
            pending = subscribed_channels - applied_topics
        for topic in pending:
            sub_socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
            applied_topics.add(topic)
            print(
                f"[{now_iso()}] [{bot_name}] [SUB] Inscrito no tópico/canal '#{topic}' em {pubsub_xpub}",
                flush=True,
            )

        try:
            parts = sub_socket.recv_multipart()
            received_at = now_iso()
            if len(parts) >= 2:
                topic = parts[0].decode("utf-8", errors="replace")
                msg_data = unpack_message(parts[1])
                msg_clock = msg_data.get("logical_clock")
                updated_clock = clock.update_receive(msg_clock)
                display_payload = {
                    "canal": topic,
                    "autor": msg_data.get("user", "desconhecido"),
                    "mensagem": msg_data.get("content", ""),
                    "servidor_pub": msg_data.get("server_id", ""),
                    "timestamp_envio": msg_data.get("timestamp", ""),
                    "timestamp_recebimento": received_at,
                    "clock_recebido": msg_clock,
                    "clock_local_atual": updated_clock,
                }
                print(
                    format_log(
                        bot_name,
                        f"RECEBIDO (SUB) <- CANAL '#{topic}' [clock={updated_clock}]",
                        display_payload,
                    ),
                    flush=True,
                )
        except zmq.Again:
            continue
        except zmq.ZMQError as exc:
            print(f"[{now_iso()}] [{bot_name}] [SUB] Erro ZMQ: {exc}", flush=True)
            time.sleep(1)


def main() -> None:
    bot_name = os.getenv("BOT_NAME", "bot_py")
    broker_frontend = os.getenv("BROKER_FRONTEND", "tcp://broker:5555")
    pubsub_xpub = os.getenv("PUBSUB_XPUB", "tcp://pubsub-proxy:5558")
    candidates_env = os.getenv(
        "CANDIDATE_CHANNELS",
        "geral,hacking,python-dev,sistemas-distribuidos,avisos,random,arquitetura",
    )
    messages_file = os.getenv("MESSAGES_FILE", "/app/messages.txt")
    message_interval = float(os.getenv("MESSAGE_INTERVAL", "1.0"))
    startup_delay = float(os.getenv("STARTUP_DELAY", "3.0"))

    candidate_channels = [c.strip() for c in candidates_env.split(",") if c.strip()]
    sample_messages = load_sample_messages(messages_file)

    time.sleep(startup_delay)

    clock = LogicalClock(0)
    context = zmq.Context()
    req_socket = create_req_socket(context, broker_frontend)

    subscribed_channels: set[str] = set()
    sub_lock = threading.Lock()

    sub_thread = threading.Thread(
        target=subscriber_worker,
        args=(context, pubsub_xpub, bot_name, clock, subscribed_channels, sub_lock),
        daemon=True,
    )
    sub_thread.start()

    print(
        f"[{now_iso()}] [{bot_name}] Conectado | Broker REQ: {broker_frontend} | Proxy SUB: {pubsub_xpub} | clock={clock.value}",
        flush=True,
    )

    logged_in = False
    while not logged_in:
        req_socket, rep = send_and_recv(
            context,
            req_socket,
            broker_frontend,
            bot_name,
            clock,
            {"action": "login", "user": bot_name},
        )
        if rep and rep.get("status") == "ok":
            logged_in = True
        else:
            time.sleep(1.0)

    cycle = 0
    while True:
        cycle += 1

        available_channels: list[str] = []
        req_socket, rep_list = send_and_recv(
            context,
            req_socket,
            broker_frontend,
            bot_name,
            clock,
            {"action": "list_channels", "user": bot_name},
        )
        if rep_list and rep_list.get("status") == "ok":
            available_channels = list(rep_list.get("channels") or [])

        if len(available_channels) < 5:
            missing = [c for c in candidate_channels if c not in available_channels]
            new_channel = (
                random.choice(missing)
                if missing
                else f"canal-{bot_name}-{random.randint(100, 999)}"
            )
            req_socket, rep_create = send_and_recv(
                context,
                req_socket,
                broker_frontend,
                bot_name,
                clock,
                {"action": "create_channel", "user": bot_name, "channel": new_channel},
            )
            if rep_create and rep_create.get("status") == "ok":
                available_channels.append(new_channel)

        with sub_lock:
            current_subs_count = len(subscribed_channels)
            unsubscribed = [c for c in available_channels if c not in subscribed_channels]

        if current_subs_count < 3 and unsubscribed:
            chosen_sub = random.choice(unsubscribed)
            with sub_lock:
                subscribed_channels.add(chosen_sub)
            time.sleep(0.3)

        if not available_channels:
            available_channels = ["geral"]

        target_channel = random.choice(available_channels)
        print(
            f"[{now_iso()}] [{bot_name}] Ciclo #{cycle}: publicando 10 mensagens em '#{target_channel}' (intervalo: {message_interval}s)...",
            flush=True,
        )

        for msg_idx in range(1, 11):
            base_text = random.choice(sample_messages)
            content = f"({msg_idx}/10) {base_text}"
            req_socket, _ = send_and_recv(
                context,
                req_socket,
                broker_frontend,
                bot_name,
                clock,
                {
                    "action": "publish",
                    "user": bot_name,
                    "channel": target_channel,
                    "content": content,
                },
            )
            time.sleep(message_interval)

        req_socket, rep_hist = send_and_recv(
            context,
            req_socket,
            broker_frontend,
            bot_name,
            clock,
            {"action": "get_history", "user": bot_name, "limit": 20},
        )
        if rep_hist and rep_hist.get("status") == "ok":
            print(
                f"[{now_iso()}] [{bot_name}] [HISTÓRICO REPLICADO] Servidor '{rep_hist.get('server_id')}' possui {rep_hist.get('total_messages')} mensagens totais replicadas.",
                flush=True,
            )


if __name__ == "__main__":
    main()
