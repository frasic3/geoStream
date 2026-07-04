use anyhow::Result;
use axum::{
    extract::{
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use common::{decode, encode, validate_chat, Message};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;

use crate::status::UserStatus;
use crate::trips::{drop_trip, is_tracked, update_trip};

mod auth;
mod cpu_log;
mod status;
mod trips;
// Default TCP address (host:port) the server listens on when the
// `SERVER_ADDR` environment variable is not set. `127.0.0.1` = loopback,
// reachable only from the same machine; exposing it externally would
// require `0.0.0.0`.
const ADDR_DEFAULT: &str = "127.0.0.1:7878";

// Capacity of the `tokio::sync::broadcast` channel used to forward chat
// messages between the WebSocket tasks. If a slow client accumulates more
// than 256 unread messages, the oldest ones are dropped (lagging receiver).
// This prevents a single slow client from growing memory without bounds.
const BROADCAST_CAP: usize = 256;

// Chat event traveling on the broadcast channel shared by all WebSocket
// connections. Published ONLY by the admin task (server stdin): clients
// don't chat with other clients, they only talk to the server. Each
// per-client task subscribes to the channel and forwards to its own socket.
//
// Why a dedicated type instead of passing `common::Message` directly:
// we need to know who the sender is (`from`) — here always "SERVER".
#[derive(Debug, Clone)]
struct ChatBroadcast {
    from: String,
    text: String,
}

/// Global registry of WS sinks per authenticated user.
/// Used by the admin CLI (stdin) to deliver DMs to a specific user.
/// Populated after LOGIN/REGISTER, removed on connection cleanup.
static SINKS: OnceLock<Mutex<HashMap<String, SharedSink>>> = OnceLock::new();
fn sinks_registry() -> &'static Mutex<HashMap<String, SharedSink>> {
    SINKS.get_or_init(|| Mutex::new(HashMap::new()))
}

// Shared state of the Axum application, cloned into every handler.
// Holds the `Sender` of the broadcast channel: each per-client task clones
// it to publish new messages and calls `tx.subscribe()` to get its own
// `Receiver` for receiving other users' messages.
//
// `Clone` is required by Axum because the state is cloned for every
// incoming request; `broadcast::Sender` is cheap to clone (internally an `Arc`).
#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<ChatBroadcast>,
    /// HTML of the test page, loaded once at startup.
    /// `Arc<str>` avoids re-reading the file from disk on every GET `/` and
    /// allows cloning the state at zero cost (pointer + refcount).
    index_html: Arc<str>,
}

/// UNIX timestamp in seconds. Used as a fallback when the client
/// disconnects without sending an explicit `ts` (e.g. an abrupt WS
/// close before the `EndTrip`).
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Locates the `web/` folder by trying the two typical paths (workspace
/// root and under `server/`, depending on the cwd `cargo run` is launched
/// with). From here both `index.html` and the WASM assets in `web/pkg/`
/// are served. Fallback: `web` relative to the cwd.
fn web_base() -> PathBuf {
    for p in [PathBuf::from("web"), PathBuf::from("../web")] {
        if p.join("index.html").exists() {
            return p;
        }
    }
    PathBuf::from("web")
}

/// Loads the content of `index.html` from the given web folder. If missing,
/// returns a minimal fallback so GET `/` never responds 500.
async fn load_index_html(base: &std::path::Path) -> String {
    match tokio::fs::read_to_string(base.join("index.html")).await {
        Ok(body) => body,
        Err(_) => "<h1>georuggine</h1><p>web/index.html not found</p>".to_string(),
    }
}

// Type aliases to reduce noise in function signatures.
//
// `WsSink`: the "write" half of a WebSocket connection. `WebSocket` is
// divided with `.split()` into `SplitSink` (write) + `SplitStream` (read),
// so the two tasks (loop reading from the client and loop writing to the
// client) can work in parallel without contending for the whole socket.
type WsSink = SplitSink<WebSocket, WsMessage>;

// `SharedSink`: the write sink shared among multiple tasks of the same
// connection (e.g. the task reading from the client and the one receiving
// from the broadcast channel both write to the same socket).
// - `Arc` = shared ownership across tasks.
// - `Mutex` (tokio's, async) = serializes writes: two concurrent `send()`
//   calls on the same socket would corrupt the WebSocket framing.
type SharedSink = Arc<Mutex<WsSink>>;

// `SharedUser`: username associated with the connection, shared among the
// tasks of the same connection. `Option<String>` because the connection is
// initially anonymous (no login) and gets populated after a successful
// `LOGIN`/`REGISTER`. Needed to filter DMs and to know "who I am" when
// publishing to the chat.
type SharedUser = Arc<Mutex<Option<String>>>;

/// Server entry point.
/// What it does: initializes logger and DB, creates the chat broadcast
/// channel, mounts the HTTP routes (`/` and `/ws`) and starts Axum with
/// graceful shutdown.
/// Why: it concentrates all the bootstrap in a single place, so subsequent
/// tasks receive a ready state (DB open, channel active).
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the global logger with default formatting.
    tracing_subscriber::fmt()
        // Reads the log level from `RUST_LOG` (e.g. "debug", "server=trace").
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // Fallback: if `RUST_LOG` is not set use the "info" level.
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        // Registers the subscriber as the global default for the whole process.
        .init();

    // Opens the SQLite pool and applies the migrations.
    db::ensure_file_exists().await?;

    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| ADDR_DEFAULT.to_string());
    let (tx, _rx) = broadcast::channel::<ChatBroadcast>(BROADCAST_CAP);
    let base = web_base();
    let index_html: Arc<str> = Arc::from(load_index_html(&base).await);
    let state = AppState { tx: tx.clone(), index_html };

    // `/pkg/*` serves the WASM artifacts generated by wasm-pack (sim.js,
    // sim_bg.wasm). They are static files: one disk read, negligible CPU
    // cost (the simulation runs in the browser, not here).
    let app = Router::new()
        .route("/", get(serve_index))
        .route("/ws", get(ws_upgrade))
        .route("/cpu_log", get(serve_cpu_log))
        .nest_service("/pkg", ServeDir::new(base.join("pkg")))
        .nest_service("/data", ServeDir::new(base.join("data")))
        .nest_service("/assets", ServeDir::new(base.join("assets")))
        .with_state(state);

    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("server listening on http://{addr} (ws: /ws)");

    tokio::spawn(cpu_log::start_cpu_logger());
    tokio::spawn(admin_stdin(tx.clone()));

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Force exit: the admin_stdin task runs on a blocking tokio::io::stdin
    // thread (Windows) which doesn't stop at runtime shutdown, leaving the
    // process hanging until the next Enter keypress.
    tracing::info!("exit");
    std::process::exit(0);
}

/// Admin CLI: reads lines from the server's stdin and routes them as chat
/// messages to the clients.
///
/// Syntax:
/// - `@user text`   → DM to the specific user (if connected)
/// - `/list`        → lists the connected users on stdout
/// - `<text>`       → broadcast to all connected clients
///
/// The `from` of the sent messages is always `"SERVER"`, so the client can
/// distinguish them from its own (even though the client no longer chats
/// with other clients: the only incoming messages come from the server).
async fn admin_stdin(tx: broadcast::Sender<ChatBroadcast>) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    println!("admin CLI ready. Commands: '@user text' (DM), '/list', otherwise broadcast.");
    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!("admin stdin error: {e}");
                break;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "/list" {
            let map = sinks_registry().lock().await;
            if map.is_empty() {
                println!("(no users connected)");
            } else {
                let users: Vec<&String> = map.keys().collect();
                println!("connected ({}): {users:?}", users.len());
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix('@') {
            let Some((user, text)) = rest.split_once(char::is_whitespace) else {
                println!("use: @user text");
                continue;
            };
            let text = text.trim().to_string();
            if text.is_empty() {
                println!("empty message");
                continue;
            }
            let sink_opt = sinks_registry().lock().await.get(user).cloned();
            let Some(sink) = sink_opt else {
                println!("user '{user}' not connected");
                continue;
            };
            let msg = Message::ChatFromServer {
                from: Some("SERVER".into()),
                text,
            };
            match encode(&msg) {
                Ok(line) => {
                    let mut s = sink.lock().await;
                    if let Err(e) = s.send(WsMessage::Text(line)).await {
                        println!("sending DM to '{user}' failed: {e}");
                    } else {
                        println!("→ DM to '{user}' sent");
                    }
                }
                Err(e) => println!("encode failed: {e}"),
            }
            continue;
        }
        // Broadcast to all connected clients.
        let n = sinks_registry().lock().await.len();
        let _ = tx.send(ChatBroadcast {
            from: "SERVER".into(),
            text: trimmed.to_string(),
        });
        println!("→ broadcast sent ({n} clients connected)");
    }
}

/// Waits for Ctrl-C and logs the event.
/// Why: passed to `with_graceful_shutdown`, it lets Axum close the
/// connections cleanly instead of killing the process cold.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}

/// GET / → serves the test HTML page from the in-memory cache.
/// The file is read only once at startup (see `load_index_html`):
/// disk I/O stays out of the request critical path.
async fn serve_index(State(state): State<AppState>) -> impl IntoResponse {
    Html(state.index_html.to_string())
}

/// GET /cpu_log → returns the current content of `server_cpu.log` (text/plain).
/// Read on every request: the file grows every 2 minutes (cpu_log::start_cpu_logger).
async fn serve_cpu_log() -> impl IntoResponse {
    let body = tokio::fs::read_to_string("server_cpu.log")
        .await
        .unwrap_or_default();
    ([(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")], body)
}

/// GET /ws → HTTP→WebSocket upgrade handler.
/// What it does: accepts the `Upgrade: websocket` header, completes the
/// handshake and hands the open socket to `handle_client`. Logs client
/// errors without propagating them (a broken connection must not take the
/// server down).
/// Why here: this is the point where HTTP "becomes" WebSocket; from here on
/// the protocol is full-duplex over JSON text frames.
async fn ws_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_client(socket, state.tx).await {
            tracing::warn!("client error: {e:?}");
        }
    })
}

/// Serializes a `Message` to JSON and ships it over the WebSocket.
/// Why the lock: the sink is shared (see `SharedSink`), writes must be
/// serialized to avoid interleaved frames from different tasks on the same
/// connection.
async fn send(sink: &SharedSink, msg: &Message) -> Result<()> {
    let line = encode(msg)?;
    let mut s = sink.lock().await;
    s.send(WsMessage::Text(line)).await?;
    Ok(())
}

/// Helper: sends a `Message::Error` with code and description to the client.
/// Why it exists: reduces boilerplate in the dispatcher's many error branches.
async fn send_error(sink: &SharedSink, code: &str, message: &str) -> Result<()> {
    send(
        sink,
        &Message::Error {
            code: code.into(),
            message: message.into(),
        },
    )
    .await
}

/// Main per-connection loop.
/// What it does:
/// - splits the WebSocket into read+write, shares the write half with `Arc<Mutex>`;
/// - spawns a secondary task (`chat_task`) subscribed to the broadcast
///   channel that forwards other users' messages to the socket (filter on `from != me`);
/// - dispatches incoming messages: Login/Register, StartTrip/EndTrip,
///   Chat, Disconnect; every operation requiring identity goes through
///   `validate_token`;
/// - final cleanup: invalidates the token and aborts the chat task.
/// Why two tasks: the reader is blocked in `stream.next()` while chat
/// messages from other clients arrive; they must run in parallel, otherwise
/// chat would be delivered only when the client sends something.
async fn handle_client(ws: WebSocket, tx: broadcast::Sender<ChatBroadcast>) -> Result<()> {
    let (sink, mut stream): (WsSink, SplitStream<WebSocket>) = ws.split();
    let sink: SharedSink = Arc::new(Mutex::new(sink));

    let user_state: SharedUser = Arc::new(Mutex::new(None));

    let mut rx = tx.subscribe();
    let sink_for_chat = sink.clone();
    let user_state_chat = user_state.clone();
    let chat_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(bc) => {
                    let me = user_state_chat.lock().await.clone();
                    let Some(me) = me else { continue };
                    if bc.from == me {
                        continue;
                    }
                    let msg = Message::ChatFromServer {
                        from: Some(bc.from),
                        text: bc.text,
                    };
                    if let Ok(line) = encode(&msg) {
                        let mut s = sink_for_chat.lock().await;
                        if s.send(WsMessage::Text(line)).await.is_err() {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("chat lag: {n} messages dropped");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    loop {
        let ws_msg = match stream.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::info!("ws read error: {e}");
                break;
            }
            None => break,
        };

        let line = match ws_msg {
            WsMessage::Text(t) => t,
            WsMessage::Binary(b) => match String::from_utf8(b) {
                Ok(t) => t,
                Err(_) => {
                    send_error(&sink, "BAD_REQUEST", "binary payload is not UTF-8").await?;
                    continue;
                }
            },
            WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
            WsMessage::Close(_) => break,
        };

        if line.is_empty() {
            continue;
        }

        let msg = match decode(&line) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("parse error: {e}");
                send_error(&sink, "BAD_REQUEST", "invalid message").await?;
                continue;
            }
        };

        match msg {
            Message::Login { ref username, .. } => {
                let current_user = user_state.lock().await.clone();
                // Already authenticated as this user on this connection:
                // another /login is redundant. Reject instead of issuing a new
                // token (no multiple AUTH_OKs for the same user).
                if current_user.as_deref() == Some(username.as_str()) {
                    send_error(
                        &sink,
                        "ALREADY_AUTHENTICATED",
                        &format!("already authenticated as {username}"),
                    )
                    .await?;
                    continue;
                }
                // Otherwise it's an account switch (or a login on an anonymous
                // connection). The previous session is invalidated only if the
                // new login succeeds: a failed attempt (e.g. wrong password)
                // does not disconnect the user already authenticated on the
                // connection.
                match auth::login(&msg).await {
                    Ok((username, token)) => {
                        if let Some(u) = current_user.as_deref() {
                            db::invalidate_user_sessions(u).await;
                            status::remove(u).await;
                            sinks_registry().lock().await.remove(u);
                        }
                        *user_state.lock().await = Some(username.clone());
                        status::set(&username, UserStatus::Stationary).await;
                        sinks_registry().lock().await.insert(username.clone(), sink.clone());
                        send(&sink, &Message::AuthOk { token }).await?;
                    }
                    Err(e) => {
                        send_error(&sink, "AUTH_FAILED", &e.to_string()).await?;
                    }
                }
            }
            Message::Register { .. } => {
                // Register: the username is always new, so it can never be the
                // "same user" already authenticated. We invalidate the current
                // session only after the register succeeds, so a failure
                // (e.g. user already exists) does not disconnect the active
                // session, if any.
                let current_user = user_state.lock().await.clone();
                match auth::register(&msg).await {
                    Ok((username, token)) => {
                        if let Some(u) = current_user.as_deref() {
                            db::invalidate_user_sessions(u).await;
                            status::remove(u).await;
                            sinks_registry().lock().await.remove(u);
                        }
                        *user_state.lock().await = Some(username.clone());
                        status::set(&username, UserStatus::Stationary).await;
                        sinks_registry().lock().await.insert(username.clone(), sink.clone());
                        send(&sink, &Message::AuthOk { token }).await?;
                    }
                    Err(e) => {
                        send_error(&sink, "REGISTER_FAILED", &e.to_string()).await?;
                    }
                }
            }
            Message::StartTrip {
                token,
                lat,
                lon,
                ts,
            } => {
                let user = match validate_token(&token, &user_state).await {
                    Some(u) => u,
                    None => {
                        send_error(&sink, "UNAUTHORIZED", "invalid token").await?;
                        continue;
                    }
                };

                if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                    send_error(&sink, "BAD_REQUEST", "coordinates out of range").await?;
                    continue;
                }
                if ts <= 0 {
                    send_error(&sink, "BAD_REQUEST", "invalid timestamp").await?;
                    continue;
                }

                match trips::initialize_trips(user.clone(), lat, lon, ts).await {
                    Ok(trip_id) => {
                        tracing::info!(user = %user, trip_id, ts, "trip started");
                        send(
                            &sink,
                            &Message::TripStarted {
                                trip_id,
                                lat,
                                lon,
                                ts,
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_error(&sink, "START_TRIP_FAILED", &e).await?;
                    }
                }
            }
            Message::Position {
                token,
                trip_id,
                lat,
                lon,
                ts,
            } => {
                let user = match validate_token(&token, &user_state).await {
                    Some(u) => u,
                    None => {
                        send_error(&sink, "UNAUTHORIZED", "invalid token").await?;
                        continue;
                    }
                };

                if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                    send_error(&sink, "BAD_REQUEST", "coordinates out of range").await?;
                    continue;
                }
                if ts <= 0 {
                    send_error(&sink, "BAD_REQUEST", "invalid timestamp").await?;
                    continue;
                }

                match db::trip_open_for(trip_id, &user).await {
                    Ok(true) => {}
                    Ok(false) => {
                        send_error(
                            &sink,
                            "BAD_REQUEST",
                            "trip missing, not yours or already closed",
                        )
                        .await?;
                        continue;
                    }
                    Err(e) => {
                        send_error(&sink, "BAD_REQUEST", &e.to_string()).await?;
                        continue;
                    }
                }

                // Trip open in the DB but absent from the in-memory store →
                // orphan (typically after a server restart). The movement
                // state is lost: close it immediately and tell the client
                // not to retry.
                if !is_tracked(trip_id).await {
                    if let Err(e) = db::end_trip(trip_id, &user, ts).await {
                        tracing::warn!(user = %user, trip_id, "closing orphan trip failed: {e}");
                    } else {
                        tracing::info!(user = %user, trip_id, ts, "orphan trip closed");
                    }
                    send_error(
                        &sink,
                        "TRIP_TERMINATED",
                        "trip closed by the server (state lost): open a new trip",
                    )
                    .await?;
                    continue;
                }

                match update_trip(trip_id, &user, lat, lon, ts).await {
                    Ok(()) => {
                        tracing::info!(user = %user, trip_id, ts, lat, lon, "position updated");
                        send(&sink, &Message::Ack).await?;
                    }
                    Err(e) => {
                        send_error(&sink, "UPDATE_TRIP_FAILED", &e).await?;
                    }
                }
            }
            Message::EndTrip { token, trip_id, ts } => {
                let user = match validate_token(&token, &user_state).await {
                    Some(u) => u,
                    None => {
                        send_error(&sink, "UNAUTHORIZED", "invalid token").await?;
                        continue;
                    }
                };

                match trips::terminate_trip(trip_id, user.clone(), ts).await {
                    Ok(()) => {
                        tracing::info!(user = %user, trip_id, ts, "trip ended");
                        send(&sink, &Message::Ack).await?;
                    }
                    Err(e) => {
                        send_error(&sink, "BAD_REQUEST", &e).await?;
                    }
                }
            }
            Message::Stats {
                token,
                from_ts,
                to_ts,
            } => {
                let user = match validate_token(&token, &user_state).await {
                    Some(u) => u,
                    None => {
                        send_error(&sink, "UNAUTHORIZED", "invalid token").await?;
                        continue;
                    }
                };

                match db::movement_stats_for_user(&user, from_ts, to_ts).await {
                    Ok(stats) => {
                        tracing::info!(
                            user = %stats.username,
                            points = stats.points,
                            distance_m = stats.distance_m,
                            movement_secs = stats.movement_secs,
                            pause_secs = stats.pause_secs,
                            avg_speed_kmh = stats.avg_speed_kmh,
                            "statistics computed"
                        );
                        send(
                            &sink,
                            &Message::StatsResult {
                                username: stats.username,
                                from_ts: stats.from_ts,
                                to_ts: stats.to_ts,
                                distance_m: stats.distance_m,
                                movement_secs: stats.movement_secs,
                                pause_secs: stats.pause_secs,
                                total_secs: stats.total_secs,
                                avg_speed_mps: stats.avg_speed_mps,
                                avg_speed_kmh: stats.avg_speed_kmh,
                                points: stats.points,
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        send_error(&sink, "BAD_REQUEST", &e.to_string()).await?;
                    }
                }
            }
            Message::ChatToServer { token, text } => {
                let user = match validate_token(&token, &user_state).await {
                    Some(u) => u,
                    None => {
                        send_error(&sink, "UNAUTHORIZED", "invalid token").await?;
                        continue;
                    }
                };
                if let Err(e) = validate_chat(&text) {
                    send_error(&sink, "BAD_REQUEST", e).await?;
                    continue;
                }
                // Current model: the client talks ONLY to the server. No
                // fan-out to other clients. The server logs via tracing +
                // stdout so the operator can reply through the admin CLI.
                tracing::info!(user = %user, text = %text, "chat msg from client");
                println!("[chat] {user}: {text}");
                send(&sink, &Message::Ack).await?;
            }
            Message::Disconnect { token } => {
                let state_user = user_state.lock().await.clone();
                let db_user = db::get_user_by_token(&token).await;
                if db_user.is_some() && db_user == state_user {
                    let user = db_user.unwrap();
                    close_open_trips(&user).await;
                    status::remove(&user).await;
                    sinks_registry().lock().await.remove(&user);
                    db::invalidate_token(&token).await;
                    *user_state.lock().await = None;
                }
                send(&sink, &Message::Ack).await?;
                let mut s = sink.lock().await;
                let _ = s.send(WsMessage::Close(None)).await;
                break;
            }

            Message::TripStarted { .. }
            | Message::AuthOk { .. }
            | Message::Ack
            | Message::Error { .. }
            | Message::StatsResult { .. }
            | Message::ChatFromServer { .. } => {
                send_error(&sink, "BAD_REQUEST", "invalid message type from client").await?;
            }
        }
    }

    // Connection cleanup: closes the trips left open by the client, marks
    // the user as disconnected and invalidates the token. Prevents orphan
    // entries piling up in `last_coordinates` and trips staying open forever
    // in the DB for clients that close the WS without sending `Disconnect`.
    let final_user = user_state.lock().await.clone();
    if let Some(user) = final_user.as_deref() {
        close_open_trips(user).await;
        status::remove(user).await;
        sinks_registry().lock().await.remove(user);
        db::invalidate_user_sessions(user).await;
    }
    chat_task.abort();
    Ok(())
}

/// Closes all trips still open for `user` in the DB and frees the
/// corresponding entries from the in-memory store. Used both on an explicit
/// `Disconnect` and on WebSocket cleanup. DB errors are only logged: the
/// cleanup must never propagate a panic.
async fn close_open_trips(user: &str) {
    let ts = now_secs();
    let ids = match db::open_trip_ids_for(user).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(user, "open trips lookup failed: {e}");
            return;
        }
    };
    for id in ids {
        if let Err(e) = db::end_trip(id, user, ts).await {
            tracing::warn!(user, trip_id = id, "closing trip during cleanup failed: {e}");
        }
        drop_trip(id).await;
    }
}

/// Verifies that the token is valid and consistent with the connection.
/// What it checks, in order:
/// 1. the token exists in the session store and maps to a username;
/// 2. the store's username matches the one stored in the connection
///    state → defense in depth against inconsistent states.
/// Returns `Some(username)` only if all checks pass.
async fn validate_token(
    token: &str,
    user_state: &SharedUser,
) -> Option<String> {
    let user = db::get_user_by_token(token).await?;
    let state_user = user_state.lock().await.clone();
    if state_user.as_deref() != Some(user.as_str()) {
        return None;
    }
    Some(user)
}
