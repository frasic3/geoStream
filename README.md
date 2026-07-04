# Georuggine

Trip-tracking app: one server, multiple clients (CLI or browser). Live map of Turin with a vehicle moving on a road graph, user authentication and real-time chat.

## Overview

Cargo workspace with 5 crates:

- `common` — `Message` protocol (JSON enum) + validators
- `db` — SQLite via `sqlx`, migrations, in-memory sessions
- `server` — HTTP + WebSocket (`axum`), serves map and assets
- `client` — CLI REPL over WebSocket
- `sim` — vehicle simulator compiled to WebAssembly, runs in the browser

Transport: full-duplex WebSocket on `/ws`, port `7878`. Storage: local SQLite (`georuggine.db`).

## Quick start

```bash
cargo run -p server     # terminal 1
cargo run -p client     # terminal 2
```

Then open `http://127.0.0.1:7878` in the browser for the map.

## Tests

```bash
cargo test --workspace
```

## Documentation

- [User manual](docs/user-manual.md) — installation, startup, commands, LAN usage
- [Designer manual](docs/designer-manual.md) — architecture, design choices, protocol, security, evaluation
