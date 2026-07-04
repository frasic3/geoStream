# Georuggine — Designer manual

## Architecture

Cargo workspace, 5 crates:

- `common` — `Message` protocol (tagged JSON enum) + validators, shared by client and server
- `db` — SQLite pool (`sqlx`), migrations, users/trips API, in-memory sessions
- `server` — HTTP + WebSocket server (`axum`): message dispatch, chat broadcast, serves the map and static assets
- `client` — CLI REPL over WebSocket
- `sim` — vehicle movement simulator on the Turin road graph, compiled to WebAssembly, runs in the browser

One server process, N clients. Transport: full-duplex WebSocket on `/ws`, one `Text` frame per JSON `Message`. A single port, 7878, also serves the map page (`/`), the WASM artifacts (`/pkg`) and the graph (`/data`).

## Design choices

- **Rust** — memory safety + a single language for server, client and simulator (via WASM).
- **WebSocket** instead of HTTP polling — chat and positions need real-time server→client push.
- **axum + tokio** — async, handles many concurrent connections with few threads.
- **SQLite + sqlx** — zero setup, local file, compile-time-checked queries; versioned migrations applied at boot.
- **WASM for the simulator** — the same Rust logic runs in the browser, no plugins.
- **Tagged `Message` enum** — a single type serialized to JSON, safe parsing, protocol extended by adding variants.

## Data

SQLite (`georuggine.db`), schema in `db/migrations/`. Tables: users, trips, positions. Foreign key with `ON DELETE CASCADE` from trip to user.

## Concurrency and sessions

Chat via a tokio broadcast channel. In-memory sessions (UUIDv4 tokens, valid per process). Single-session policy: a second login by the same user → `ERROR AUTH_FAILED`.

## Security

- Passwords: bcrypt hash (cost 10)
- Session tokens: in-memory UUIDv4
- Input validation: username regex, minimum password length, maximum chat length
- SQLite FKs with cascade
- Known limits: tokens in cleartext over `ws://` (Internet exposure requires TLS via a reverse proxy), no rate-limiting on login

## Tests and benchmarks

- **62 tests** (`cargo test --workspace`): unit tests on the validators (`common`), on `db` and `auth`, on the simulator (`sim`); end-to-end integration (`server/tests/integration.rs`); multi-client concurrency (`server/tests/concurrent.rs`).
- **Built-in CPU profiling**: `server/src/cpu_log.rs` samples the server's CPU usage every 2 min into `server_cpu.log`. Observed measurements: idle ~0.0–0.3%, peak ~1.7% under activity.

### Stress test

CPU consumption under registrations (`server/tests/stress.rs`, `#[ignore]`): users register one at a time at a constant rate (`STRESS_TRICKLE_DELAY_MS`). The test samples the server's CPU% (via `sysinfo`, normalized over logical cores) and register latency.

```powershell
cargo test --release --test stress -- --ignored --nocapture
```

Results (release, 16 logical cores):

| Arrival rate | Avg CPU | Peak CPU | avg latency | worst latency |
|---|---|---|---|---|
| 1 every 0.1 s (~5/s) | 2.8% | 3.5% | ~97 ms | ~135 ms |
| 1 every 1 s (~1/s) | 0.5% | 1.7% | ~102 ms | ~125 ms |

Latency (~100 ms) is dominated by **bcrypt (cost 10)**: an intentional cost (anti-brute-force), isolated on `spawn_blocking`. CPU stays low (steady-state idle ~0.02%); the floor is the per-hash cost (~100 ms of one core).

## Evaluation

- Binaries (Windows x86_64): `server.exe` ≈ 5.9 MB, `client.exe` ≈ 1.3 MB
- Server CPU usage tracked at runtime (`server/cpu_log.rs` → `server_cpu.log`)
- Cross-platform with no changes (Windows / Linux / macOS)
- Possible extensions: TLS/`wss://`, rate-limiting, DB-persisted sessions, maps of other cities
