# ─── Build stage ─────────────────────────────────────────────────────────────
FROM docker.io/library/rust:1.95-slim-trixie AS builder

WORKDIR /build

# Cache dependencies before copying source (faster incremental builds).
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release 2>/dev/null || true \
    && rm -rf src

COPY src ./src
RUN cargo build --release

# ─── Runtime stage ───────────────────────────────────────────────────────────
FROM docker.io/library/debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/testbed /usr/local/bin/testbed

# Default: run a 30-second bench at 10k ev/s against the bench single-node.
# Override via CMD or env vars.
ENV KURRENTDB_URL="esdb://eventstore-bench:2113?tls=false"
ENV RABBITMQ_URL="amqp://guest:guest@rabbitmq:5672"
ENV RUST_LOG="info"

EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/testbed"]
CMD ["bench", "--target-rate", "10000", "--duration-secs", "30", "--json"]
