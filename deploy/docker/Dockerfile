FROM rust:1.98.1-bookworm AS builder

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
COPY tools/test-contract ./tools/test-contract

RUN cargo build --locked --release \
    --bin relaygate-server \
    --bin relaygate-echo-listener \
    --bin relaygate-echo-probe

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:54df941ed0d06a1bd95ef5e0ce391fd8d9f94b64782dc9a60062727849ee3f97 AS server-runtime

WORKDIR /

COPY --from=builder /workspace/target/release/relaygate-server /usr/local/bin/relaygate-server

USER 10001:10001

FROM server-runtime AS gateway

EXPOSE 27420 27421

ENTRYPOINT ["/usr/local/bin/relaygate-server"]
CMD ["gateway"]

FROM server-runtime AS route-table

EXPOSE 27430

ENTRYPOINT ["/usr/local/bin/relaygate-server"]
CMD ["route-table"]

FROM server-runtime AS test

COPY --from=builder /workspace/target/release/relaygate-echo-listener /usr/local/bin/relaygate-echo-listener
COPY --from=builder /workspace/target/release/relaygate-echo-probe /usr/local/bin/relaygate-echo-probe

CMD ["relaygate-server"]
