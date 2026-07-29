FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY migrations-archive ./migrations-archive
RUN cargo build --locked --release --bin foreman-memory-v6 \
    --bin foreman_migrate --bin foreman_archive_migrate \
    --bin foreman_healthcheck --bin embedder_proxy

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
COPY --from=build /src/target/release/foreman-memory-v6 /usr/local/bin/foreman-memory-v6
COPY --from=build /src/target/release/foreman_migrate /usr/local/bin/foreman-migrate
COPY --from=build /src/target/release/foreman_archive_migrate /usr/local/bin/foreman-archive-migrate
COPY --from=build /src/target/release/foreman_healthcheck /usr/local/bin/foreman-healthcheck
COPY --from=build /src/target/release/embedder_proxy /usr/local/bin/embedder-proxy
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/foreman-memory-v6"]
