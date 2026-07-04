// Concurrency tests: multiple parallel clients on the same server.
// Prerequisite: `cargo build -p server`.
use common::{decode, encode, Message};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(27878);
const IO_TIMEOUT: Duration = Duration::from_secs(5);

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
            .join(format!("rust_proj_conc_{port}.sqlite"))
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

type WsConn = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Client {
    reader: SplitStream<WsConn>,
    writer: SplitSink<WsConn, WsMessage>,
}

impl Client {
    async fn connect(addr: &str) -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(ws_url(addr))
            .await
            .expect("connect ws");
        let (writer, reader) = ws.split();
        Self { reader, writer }
    }

    async fn send(&mut self, msg: &Message) {
        let line = encode(msg).unwrap();
        self.writer.send(WsMessage::Text(line)).await.unwrap();
    }

    async fn recv(&mut self) -> Message {
        loop {
            let item = timeout(IO_TIMEOUT, self.reader.next())
                .await
                .expect("timeout on recv")
                .expect("stream closed")
                .expect("ws error");
            match item {
                WsMessage::Text(t) => return decode(&t).unwrap(),
                WsMessage::Close(_) => panic!("conn closed"),
                _ => continue,
            }
        }
    }

    async fn recv_opt(&mut self) -> Option<Message> {
        loop {
            match timeout(IO_TIMEOUT, self.reader.next()).await {
                Ok(Some(Ok(WsMessage::Text(t)))) => return Some(decode(&t).unwrap()),
                Ok(Some(Ok(WsMessage::Close(_)))) => return None,
                Ok(Some(Ok(_))) => continue,
                _ => return None,
            }
        }
    }

    async fn assert_silent(&mut self, dur: Duration) {
        loop {
            match timeout(dur, self.reader.next()).await {
                Ok(Some(Ok(WsMessage::Text(t)))) => panic!("expected silence, got: {t}"),
                Ok(Some(Ok(_))) => continue,
                _ => return,
            }
        }
    }

    async fn register(&mut self, user: &str, pass: &str) -> Message {
        self.send(&Message::Register {
            username: user.into(),
            password: pass.into(),
        })
        .await;
        self.recv().await
    }

    async fn register_ok(&mut self, user: &str, pass: &str) -> String {
        match self.register(user, pass).await {
            Message::AuthOk { token } => token,
            other => panic!("expected AuthOk for {user}, got {other:?}"),
        }
    }

    async fn start_trip(&mut self, token: &str) -> i64 {
        self.send(&Message::StartTrip {
            token: token.into(),
            lat: 45.07,
            lon: 7.69,
            ts: 1_700_000_000,
        })
        .await;
        match self.recv().await {
            Message::TripStarted { trip_id, .. } => trip_id,
            other => panic!("expected TripStarted, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn register_n_parallel_users_all_ok() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let n = 10;
    let mut handles = Vec::new();
    for i in 0..n {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = Client::connect(&addr).await;
            c.register_ok(&format!("user{i}"), "secret").await
        }));
    }

    let mut tokens = Vec::new();
    for h in handles {
        tokens.push(h.await.unwrap());
    }
    assert_eq!(tokens.len(), n);
    let mut sorted = tokens.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), n, "duplicate tokens: {tokens:?}");
}

#[tokio::test]
async fn concurrent_registers_same_username_only_one_ok() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let n = 5;
    let mut handles = Vec::new();
    for _ in 0..n {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = Client::connect(&addr).await;
            c.register("duplicate", "secret").await
        }));
    }

    let mut ok_count = 0;
    let mut err_count = 0;
    for h in handles {
        match h.await.unwrap() {
            Message::AuthOk { .. } => ok_count += 1,
            Message::Error { .. } => err_count += 1,
            other => panic!("unexpected response: {other:?}"),
        }
    }
    assert_eq!(ok_count, 1, "expected exactly 1 AuthOk, got {ok_count}");
    assert_eq!(err_count, n - 1);
}

#[tokio::test]
async fn chat_from_client_goes_only_to_server_and_is_not_forwarded() {
    // New model: the client talks ONLY to the server. A ChatToServer
    // produces an Ack to the sender and is not forwarded to any other
    // client. Messages towards clients originate only from the server
    // (admin CLI), which cannot be verified from here.
    let h = Harness::start();
    let addr = h.addr.clone();

    let mut alice = Client::connect(&addr).await;
    let mut bob = Client::connect(&addr).await;
    let mut carol = Client::connect(&addr).await;

    let alice_tok = alice.register_ok("alice", "secret").await;
    bob.register_ok("bob", "secret").await;
    carol.register_ok("carol", "secret").await;

    alice
        .send(&Message::ChatToServer {
            token: alice_tok,
            text: "hello server".into(),
        })
        .await;

    assert!(matches!(alice.recv().await, Message::Ack));

    // Neither bob nor carol must receive anything.
    bob.assert_silent(Duration::from_millis(300)).await;
    carol.assert_silent(Duration::from_millis(300)).await;
}

#[tokio::test]
async fn parallel_trips_all_receive_distinct_ids() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let n = 5;
    let mut handles = Vec::new();
    for i in 0..n {
        let addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut c = Client::connect(&addr).await;
            let token = c.register_ok(&format!("t{i}"), "secret").await;
            c.start_trip(&token).await
        }));
    }
    let mut ids = Vec::new();
    for h in handles {
        ids.push(h.await.unwrap());
    }
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), n, "duplicate trip_ids: {ids:?}");
}

#[tokio::test]
async fn end_trip_of_another_user_fails() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let mut alice = Client::connect(&addr).await;
    let mut bob = Client::connect(&addr).await;

    let alice_tok = alice.register_ok("alice", "secret").await;
    let bob_tok = bob.register_ok("bob", "secret").await;

    let alice_trip = alice.start_trip(&alice_tok).await;

    bob.send(&Message::EndTrip {
        token: bob_tok,
        trip_id: alice_trip,
        ts: 0,
    })
    .await;
    match bob.recv().await {
        Message::Error { code, .. } => assert_eq!(code, "BAD_REQUEST"),
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn second_login_same_user_rejected() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let mut c1 = Client::connect(&addr).await;
    c1.register_ok("mario", "secret").await;

    // A second connection attempts a login for the same user -> ERROR.
    let mut c2 = Client::connect(&addr).await;
    c2.send(&Message::Login {
        username: "mario".into(),
        password: "secret".into(),
    })
    .await;
    match c2.recv().await {
        Message::Error { code, message } => {
            assert_eq!(code, "AUTH_FAILED");
            assert!(message.contains("already logged in"));
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn relogin_same_user_same_conn_rejected() {
    let h = Harness::start();
    let mut c = Client::connect(&h.addr).await;
    c.register_ok("mario", "secret").await;

    // Second login for the same user on the SAME connection: redundant,
    // no new AUTH_OK.
    c.send(&Message::Login {
        username: "mario".into(),
        password: "secret".into(),
    })
    .await;
    match c.recv().await {
        Message::Error { code, .. } => assert_eq!(code, "ALREADY_AUTHENTICATED"),
        other => panic!("expected Error ALREADY_AUTHENTICATED, got {other:?}"),
    }
}

#[tokio::test]
async fn after_disconnect_another_login_is_possible() {
    let h = Harness::start();
    let addr = h.addr.clone();

    let mut c1 = Client::connect(&addr).await;
    let token = c1.register_ok("mario", "secret").await;
    c1.send(&Message::Disconnect { token }).await;
    assert!(matches!(c1.recv().await, Message::Ack));
    // Wait for conn 1 to close on the server side (Close frame + token cleanup).
    let _ = c1.recv_opt().await;

    let mut c2 = Client::connect(&addr).await;
    c2.send(&Message::Login {
        username: "mario".into(),
        password: "secret".into(),
    })
    .await;
    assert!(matches!(c2.recv().await, Message::AuthOk { .. }));
}

#[tokio::test]
async fn disconnect_closes_the_connection() {
    let h = Harness::start();
    let mut c = Client::connect(&h.addr).await;
    let token = c.register_ok("disc", "secret").await;

    c.send(&Message::Disconnect { token }).await;
    assert!(matches!(c.recv().await, Message::Ack));
    assert!(c.recv_opt().await.is_none(), "expected EOF after Disconnect");
}
