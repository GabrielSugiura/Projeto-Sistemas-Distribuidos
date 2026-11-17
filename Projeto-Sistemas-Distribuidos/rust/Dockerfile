# Estágio de compilação
FROM rust:slim-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libzmq3-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Cache de dependências
COPY Cargo.toml ./
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/server.rs && \
    echo "fn main() {}" > src/bin/client.rs && \
    echo "" > src/lib.rs && \
    cargo build --release && \
    rm -rf src

# Compilação do código-fonte real
COPY src ./src
RUN touch src/lib.rs src/bin/server.rs src/bin/client.rs && \
    cargo build --release

# Estágio final enxuto
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN mkdir -p /app/bin /data /logs

COPY --from=builder /usr/src/app/target/release/server /app/bin/server
COPY --from=builder /usr/src/app/target/release/client /app/bin/client
