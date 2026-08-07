####################################################################################################
## Builder
####################################################################################################
FROM rust:latest AS builder

WORKDIR /app
COPY Cargo.toml .
COPY Cargo.lock .
COPY benches benches
COPY src src

RUN --mount=type=cache,target=/app/target \
    --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release --bin beam && \
    mv /app/target/release/beam .

####################################################################################################
## Final image
####################################################################################################
FROM gcr.io/distroless/cc
COPY --from=builder /app/beam /
EXPOSE 4944 4945
ENTRYPOINT ["./beam"]
