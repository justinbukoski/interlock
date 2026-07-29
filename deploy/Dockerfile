FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY migrations-archive ./migrations-archive
RUN cargo build --locked --release --bin interlock \
    --bin interlock_migrate --bin interlock_archive_migrate \
    --bin interlock_healthcheck --bin embedder_proxy

FROM debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818
COPY --from=build /src/target/release/interlock /usr/local/bin/interlock
COPY --from=build /src/target/release/interlock_migrate /usr/local/bin/interlock-migrate
COPY --from=build /src/target/release/interlock_archive_migrate /usr/local/bin/interlock-archive-migrate
COPY --from=build /src/target/release/interlock_healthcheck /usr/local/bin/interlock-healthcheck
COPY --from=build /src/target/release/embedder_proxy /usr/local/bin/embedder-proxy
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/interlock"]
