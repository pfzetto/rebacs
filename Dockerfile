FROM debian:bookworm-slim AS final
WORKDIR /app
COPY ./target/release/rebacs ./server
CMD [ "/app/server" ]
