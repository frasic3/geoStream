// Integration tests: they launch the server binary with isolated port and DB
// to be parallel-safe. Prerequisite: `cargo build -p server`.
use common::{decode, encode, Message};
use futures_util::{SinkExt, StreamExt};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message as WsMessage;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(17878);

fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn server_binary() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let exe = if cfg!(windows) { "server.exe" } else { "server" };
    format!("{manifest}/../target/debug/{exe}")
}

fn ws_url(addr: &str) -> String {
    format!("ws://{addr}/ws")
}

struct Harness {
    server: Child,
    addr: String,
    db_path: String,
}

impl Harness {
    fn start() -> Self {
        let port = next_port();
        let addr = format!("127.0.0.1:{port}");
        let db_path = std::env::temp_dir()
            .join(format!("rust_proj_{port}.sqlite"))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_file(&db_path);
        let database_url = format!("sqlite://{}?mode=rwc", db_path.replace('\\', "/"));

        let server = Command::new(server_binary())
            .env("SERVER_ADDR", &addr)
            .env("DATABASE_URL", &database_url)
            .spawn()
            .expect("server binary not found — run `cargo build -p server` first");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(&addr).is_ok() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("server did not start within 5s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Self {
            server,
            addr,
            db_path,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.kill().ok();
        self.server.wait().ok();
        let _ = std::fs::remove_file(&self.db_path);
        let _ = std::fs::remove_file(format!("{}-shm", self.db_path));
        let _ = std::fs::remove_file(format!("{}-wal", self.db_path));
    }
}

async fn send_and_recv(addr: &str, msg: &Message) -> Message {
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(addr))
        .await
        .expect("connect ws");
    let payload = encode(msg).unwrap();
    ws.send(WsMessage::Text(payload)).await.unwrap();
    loop {
        match ws.next().await.expect("stream closed").expect("ws error") {
            WsMessage::Text(t) => return decode(&t).unwrap(),
            _ => continue,
        }
    }
}

#[tokio::test]
async fn register_replies_with_auth_ok() {
    let h = Harness::start();
    let response = send_and_recv(
        &h.addr,
        &Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        },
    )
    .await;
    match response {
        Message::AuthOk { token } => assert!(!token.is_empty()),
        other => panic!("unexpected response: {other:?}"),
    }
}

#[tokio::test]
async fn login_after_register_replies_with_auth_ok() {
    let h = Harness::start();
    send_and_recv(
        &h.addr,
        &Message::Register {
            username: "luigi".into(),
            password: "pass123".into(),
        },
    )
    .await;
    let response = send_and_recv(
        &h.addr,
        &Message::Login {
            username: "luigi".into(),
            password: "pass123".into(),
        },
    )
    .await;
    match response {
        Message::AuthOk { token } => assert!(!token.is_empty()),
        other => panic!("unexpected response: {other:?}"),
    }
}

#[tokio::test]
async fn login_wrong_password_replies_with_error() {
    let h = Harness::start();
    send_and_recv(
        &h.addr,
        &Message::Register {
            username: "peach".into(),
            password: "correct1".into(),
        },
    )
    .await;
    let response = send_and_recv(
        &h.addr,
        &Message::Login {
            username: "peach".into(),
            password: "wrongpass".into(),
        },
    )
    .await;
    assert!(matches!(response, Message::Error { .. }));
}

#[tokio::test]
async fn login_nonexistent_user_replies_with_error() {
    let h = Harness::start();
    let response = send_and_recv(
        &h.addr,
        &Message::Login {
            username: "ghost".into(),
            password: "whatever".into(),
        },
    )
    .await;
    assert!(matches!(response, Message::Error { .. }));
}

#[tokio::test]
async fn duplicate_register_replies_with_error() {
    let h = Harness::start();
    let msg = Message::Register {
        username: "bowser".into(),
        password: "secret".into(),
    };
    send_and_recv(&h.addr, &msg).await;
    let response = send_and_recv(&h.addr, &msg).await;
    assert!(matches!(response, Message::Error { .. }));
}

#[tokio::test]
async fn start_and_end_trip_lifecycle() {
    let h = Harness::start();
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(&h.addr))
        .await
        .unwrap();

    ws.send(WsMessage::Text(
        encode(&Message::Register {
            username: "yoshi".into(),
            password: "secret".into(),
        })
        .unwrap(),
    ))
    .await
    .unwrap();
    let token = loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Text(t) => match decode(&t).unwrap() {
                Message::AuthOk { token } => break token,
                other => panic!("expected AuthOk, got {other:?}"),
            },
            _ => continue,
        }
    };

    ws.send(WsMessage::Text(
        encode(&Message::StartTrip {
            token: token.clone(),
            lat: 45.07,
            lon: 7.69,
            ts: 1_700_000_000,
        })
        .unwrap(),
    ))
    .await
    .unwrap();
    let trip_id = loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Text(t) => match decode(&t).unwrap() {
                Message::TripStarted { trip_id, .. } => break trip_id,
                other => panic!("expected TripStarted, got {other:?}"),
            },
            _ => continue,
        }
    };
    assert!(trip_id > 0);

    ws.send(WsMessage::Text(
        encode(&Message::EndTrip { token, trip_id, ts: 0 }).unwrap(),
    ))
    .await
    .unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            WsMessage::Text(t) => {
                assert!(matches!(decode(&t).unwrap(), Message::Ack));
                break;
            }
            _ => continue,
        }
    }
}

#[tokio::test]
async fn start_trip_requires_valid_token() {
    let h = Harness::start();
    let response = send_and_recv(
        &h.addr,
        &Message::StartTrip {
            token: "fake".into(),
            lat: 0.0,
            lon: 0.0,
            ts: 0,
        },
    )
    .await;
    match response {
        Message::Error { code, .. } => assert_eq!(code, "UNAUTHORIZED"),
        other => panic!("unexpected response: {other:?}"),
    }
}
