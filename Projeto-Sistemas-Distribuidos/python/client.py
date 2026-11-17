import os
import time
import zmq
from protocol import (
    now_iso,
    pack_message,
    unpack_message,
    format_log,
    parse_script_events,
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
    payload: dict,
) -> tuple[zmq.Socket, dict | None]:
    payload["timestamp"] = now_iso()
    print(format_log(bot_name, "ENVIADO -> SERVIDOR", payload), flush=True)
    try:
        socket.send(pack_message(payload))
        raw_rep = socket.recv()
        rep = unpack_message(raw_rep)
        print(format_log(bot_name, "RECEBIDO <- SERVIDOR", rep), flush=True)
        return socket, rep
    except zmq.ZMQError as exc:
        print(
            f"[{now_iso()}] [{bot_name}] Timeout/Erro ZMQ ({exc}). Reconectando socket...",
            flush=True,
        )
        socket.close(0)
        new_socket = create_req_socket(context, endpoint)
        return new_socket, None


def main() -> None:
    bot_name = os.getenv("BOT_NAME", "bot_py")
    bot_index = int(os.getenv("BOT_INDEX", "0"))
    total_bots = int(os.getenv("TOTAL_BOTS", "4"))
    broker_frontend = os.getenv("BROKER_FRONTEND", "tcp://broker:5555")
    target_channels_env = os.getenv("TARGET_CHANNELS", "geral,hacking,python-dev")
    messages_file = os.getenv("MESSAGES_FILE", "/app/messages.txt")
    message_interval = float(os.getenv("MESSAGE_INTERVAL", "2.0"))
    startup_delay = float(os.getenv("STARTUP_DELAY", "3.0"))

    target_channels = [c.strip() for c in target_channels_env.split(",") if c.strip()]

    time.sleep(startup_delay)

    context = zmq.Context()
    socket = create_req_socket(context, broker_frontend)

    print(f"[{now_iso()}] [{bot_name}] Conectado ao Broker em {broker_frontend}", flush=True)

    # 1. Login do próprio bot (repete até obter sucesso)
    logged_in = False
    while not logged_in:
        socket, rep = send_and_recv(
            context,
            socket,
            broker_frontend,
            bot_name,
            {"action": "login", "user": bot_name},
        )
        if rep and rep.get("status") == "ok":
            logged_in = True
        else:
            time.sleep(message_interval)

    time.sleep(message_interval)

    # 2. Listar canais disponíveis
    existing_channels: list[str] = []
    socket, rep_list = send_and_recv(
        context,
        socket,
        broker_frontend,
        bot_name,
        {"action": "list_channels", "user": bot_name},
    )
    if rep_list and rep_list.get("status") == "ok":
        existing_channels = list(rep_list.get("channels") or [])

    time.sleep(message_interval)

    # 3. Criar canais iniciais que ainda não existem
    for channel in target_channels:
        if channel in existing_channels and channel != "geral":
            continue
        socket, _ = send_and_recv(
            context,
            socket,
            broker_frontend,
            bot_name,
            {"action": "create_channel", "user": bot_name, "channel": channel},
        )
        time.sleep(message_interval)

    # 4. Simular mensagens e eventos do arquivo messages.txt com intervalo de 2s
    script_events = parse_script_events(messages_file)
    my_events = [ev for idx, ev in enumerate(script_events) if idx % total_bots == bot_index]

    print(
        f"[{now_iso()}] [{bot_name}] Iniciando simulação de {len(my_events)} mensagens/eventos de {messages_file} (intervalo: {message_interval}s)...",
        flush=True,
    )

    for ev in my_events:
        ev_type = ev["type"]
        if ev_type == "login":
            payload = {"action": "login", "user": ev["user"]}
        elif ev_type == "create_channel":
            payload = {
                "action": "create_channel",
                "user": ev["user"],
                "channel": ev["channel"],
            }
        else:
            payload = {
                "action": "post_message",
                "user": ev["user"],
                "channel": ev["channel"],
                "content": ev["content"],
            }

        socket, _ = send_and_recv(context, socket, broker_frontend, bot_name, payload)
        time.sleep(message_interval)

    # 5. Listagem final de canais
    socket, _ = send_and_recv(
        context,
        socket,
        broker_frontend,
        bot_name,
        {"action": "list_channels", "user": bot_name},
    )

    print(f"[{now_iso()}] [{bot_name}] Simulação concluída com sucesso.", flush=True)

    while True:
        time.sleep(30)


if __name__ == "__main__":
    main()
