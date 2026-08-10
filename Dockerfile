FROM node:22-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:1.97-bookworm AS backend-builder
WORKDIR /app/backend
COPY backend/Cargo.toml backend/Cargo.lock ./
RUN mkdir src && printf 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY backend/migrations ./migrations
COPY backend/src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates chromium fonts-noto-cjk libsqlite3-0 novnc python3 websockify x11vnc xvfb && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=backend-builder /app/backend/target/release/luma-server /usr/local/bin/luma-server
COPY --from=frontend-builder /app/frontend/dist /app/web
ENV LUMA_BIND=0.0.0.0:3000
ENV DATABASE_URL=sqlite:///data/luma.db?mode=rwc
ENV LUMA_DATA_DIR=/data
ENV LUMA_STATIC_DIR=/app/web
ENV LUMA_BROWSER_ENABLED=true
ENV LUMA_CHROMIUM_PATH=/usr/bin/chromium
ENV LUMA_BROWSER_DATA_DIR=/data/browser-profiles
ENV LUMA_BROWSER_HEADLESS=true
ENV LUMA_BROWSER_SESSION_PORT=6080
ENV RUST_LOG=luma_server=info,tower_http=info
VOLUME ["/data"]
EXPOSE 3000 6080
CMD ["luma-server"]

