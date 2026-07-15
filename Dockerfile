# syntax=docker/dockerfile:1.7
FROM rust:1.88 AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY config ./config
COPY src ./src
COPY tests ./tests
COPY migrations ./migrations
RUN --mount=type=cache,id=vault-bridge-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=vault-bridge-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=vault-bridge-cargo-target,target=/app/target,sharing=locked \
    cargo build --release --bin vault_bridge \
    && cp /app/target/release/vault_bridge /tmp/vault_bridge

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /tmp/vault_bridge /usr/local/bin/vault_bridge

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/vault_bridge"]
