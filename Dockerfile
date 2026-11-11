# syntax=docker/dockerfile:1

FROM rust:1-alpine AS chef
RUN apk add --no-cache gcc musl-dev protobuf-dev git
ENV PROTOC=/usr/bin/protoc CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo install cargo-chef --locked --version 0.1.78
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETARCH
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-musl > /rust-target ;; \
        arm64) echo aarch64-unknown-linux-musl > /rust-target ;; \
        *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=secret,id=core_token,required=false \
    if [ -f /run/secrets/core_token ]; then \
        git config --global url."https://x-access-token:$(cat /run/secrets/core_token)@github.com/moshms/core.git".insteadOf "ssh://git@github.com/moshms/core.git"; \
    fi && \
    cargo chef cook --release --target "$(cat /rust-target)" --recipe-path recipe.json
COPY . .
RUN --mount=type=secret,id=core_token,required=false \
    if [ -f /run/secrets/core_token ]; then \
        git config --global url."https://x-access-token:$(cat /run/secrets/core_token)@github.com/moshms/core.git".insteadOf "ssh://git@github.com/moshms/core.git"; \
    fi && \
    cargo build --release --target "$(cat /rust-target)" -p mc-transcoder \
    && cp "target/$(cat /rust-target)/release/mc-transcoder" /mc-transcoder

FROM alpine:3.21
RUN apk add --no-cache ffmpeg ca-certificates \
    && adduser -D -u 10001 transcoder \
    && mkdir -p /cache \
    && chown transcoder:transcoder /cache
COPY --from=builder /mc-transcoder /usr/local/bin/mc-transcoder
ENV MC_BIND_ADDR=0.0.0.0:50051 \
    MC_LIBRARY_PATH=/library \
    MC_CACHE_DIR=/cache \
    MC_TRANSCODER_URL=http://127.0.0.1:50051
USER transcoder
EXPOSE 50051
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 CMD ["/usr/local/bin/mc-transcoder", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/mc-transcoder"]
