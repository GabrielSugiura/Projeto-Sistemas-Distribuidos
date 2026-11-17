import os
import signal
import sys
import zmq
from protocol import now_iso


def main() -> None:
    frontend_addr = os.getenv("FRONTEND_ADDR", "tcp://*:5555")
    backend_addr = os.getenv("BACKEND_ADDR", "tcp://*:5556")

    context = zmq.Context()
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.DEALER)

    frontend.bind(frontend_addr)
    backend.bind(backend_addr)

    print(
        f"[{now_iso()}] [BROKER] Iniciado | Frontend (ROUTER): {frontend_addr} <-> Backend (DEALER): {backend_addr}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{now_iso()}] [BROKER] Encerrando...", flush=True)
        frontend.close(0)
        backend.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        zmq.proxy(frontend, backend)
    except zmq.ZMQError as exc:
        print(f"[{now_iso()}] [BROKER] Erro ZMQ: {exc}", flush=True)


if __name__ == "__main__":
    main()
