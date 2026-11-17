import os
import signal
import sys
import zmq
from protocol import now_iso


def main() -> None:
    xsub_addr = os.getenv("XSUB_ADDR", "tcp://*:5557")
    xpub_addr = os.getenv("XPUB_ADDR", "tcp://*:5558")

    context = zmq.Context()
    xsub = context.socket(zmq.XSUB)
    xpub = context.socket(zmq.XPUB)

    xpub.setsockopt(zmq.XPUB_VERBOSE, 1)

    xsub.bind(xsub_addr)
    xpub.bind(xpub_addr)

    print(
        f"[{now_iso()}] [PUBSUB-PROXY] Iniciado | Publishers (XSUB): {xsub_addr} -> Subscribers (XPUB): {xpub_addr}",
        flush=True,
    )

    def shutdown(signum, frame):
        print(f"[{now_iso()}] [PUBSUB-PROXY] Encerrando...", flush=True)
        xsub.close(0)
        xpub.close(0)
        context.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        zmq.proxy(xsub, xpub)
    except zmq.ZMQError as exc:
        print(f"[{now_iso()}] [PUBSUB-PROXY] Erro ZMQ: {exc}", flush=True)


if __name__ == "__main__":
    main()
