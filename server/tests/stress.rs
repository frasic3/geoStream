// CPU stress test: users register one at a time every
// STRESS_TRICKLE_DELAY_MS ms. Measures the server's CPU% (via sysinfo,
// normalized over logical cores like cpu_log.rs) and register latency.
// `#[ignore]`: run with
//   cargo build --release -p server
//   cargo test --release --test stress -- --ignored --nocapture
// Reference (16 cores): 1/0.1s -> ~2.8% CPU ~135ms lat; 1/1s -> ~0.5% ~125ms.

use common::{decode, encode, Message};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

static PORT_COUNTER: AtomicU16 = AtomicU16::new(37878);
const IO_TIMEOUT: Duration = Duration::from_secs(60);

fn env_num(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn server_binary() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let exe = if cfg!(windows) { "server.exe" } else { "server" };
    let rel = format!("{manifest}/../target/release/{exe}");
    if std::path::Path::new(&rel).exists() { rel } else { format!("{manifest}/../target/debug/{exe}") }
}

struct Harness {
    server: Child,
    addr: String,
    db_path: String,
}

impl Harness {
    fn start() -> Self {
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);
        let addr = format!("127.0.0.1:{port}");
        let db_path = std::env::temp_dir()
            .join(format!("rust_proj_stress_{port}.sqlite"))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_file(&db_path);
        let database_url = format!("sqlite://{}?mode=rwc", db_path.replace('\\', "/"));

        let server = Command::new(server_binary())
            .env("SERVER_ADDR", &addr)
            .env("DATABASE_URL", &database_url)
            .env("RUST_LOG", "warn")
            .spawn()
            .expect("server binary not found — `cargo build --release -p server`");

        let deadline = Instant::now() + Duration::from_secs(10);
        while std::net::TcpStream::connect(&addr).is_err() {
            if Instant::now() > deadline {
                panic!("server did not start within 10s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Self { server, addr, db_path }
    }

    fn pid(&self) -> u32 {
        self.server.id()
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
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .expect("connect ws");
        let (writer, reader) = ws.split();
        Self { reader, writer }
    }

    async fn register_ok(&mut self, user: &str, pass: &str) -> String {
        let req = Message::Register { username: user.into(), password: pass.into() };
        self.writer.send(WsMessage::Text(encode(&req).unwrap())).await.unwrap();
        let item = timeout(IO_TIMEOUT, self.reader.next())
            .await
            .expect("timeout recv")
            .expect("stream closed")
            .expect("ws error");
        match item {
            WsMessage::Text(t) => match decode(&t).unwrap() {
                Message::AuthOk { token } => token,
                other => panic!("expected AuthOk, got {other:?}"),
            },
            other => panic!("expected Text, got {other:?}"),
        }
    }
}

/// Samples the CPU% of process `pid` every 400 ms in a background thread,
/// normalized over logical cores (100% = all cores). Returns (average, peak).
struct CpuSampler {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<(f64, f64)>>,
}

impl CpuSampler {
    fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_c = stop.clone();
        let handle = std::thread::spawn(move || {
            let ncpu = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
            let pid = Pid::from_u32(pid);
            let mut sys = System::new();
            sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true); // baseline
            let (mut sum, mut cnt, mut peak) = (0.0f64, 0u64, 0.0f64);
            while !stop_c.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(400));
                sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
                if let Some(p) = sys.process(pid) {
                    let pct = p.cpu_usage() as f64 / ncpu; // 100% cpu_usage = 1 core
                    sum += pct;
                    cnt += 1;
                    peak = peak.max(pct);
                }
            }
            (if cnt > 0 { sum / cnt as f64 } else { 0.0 }, peak)
        });
        Self { stop, handle: Some(handle) }
    }

    fn stop(mut self) -> (f64, f64) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn stress_trickle() {
    let n = env_num("STRESS_TRICKLE_USERS", 80);
    let delay_ms = env_num("STRESS_TRICKLE_DELAY_MS", 100) as u64;
    let h = Harness::start();

    let sampler = CpuSampler::start(h.pid());
    let t0 = Instant::now();
    let (mut lat_sum, mut lat_max) = (0.0, 0.0f64);
    let mut conns = Vec::with_capacity(n); // keeps the sessions alive until the end of the measurement
    for i in 0..n {
        let op = Instant::now();
        let mut c = Client::connect(&h.addr).await;
        c.register_ok(&format!("trk_{i}"), "secret").await;
        let ms = op.elapsed().as_secs_f64() * 1000.0;
        lat_sum += ms;
        lat_max = lat_max.max(ms);
        conns.push(c);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    let rate = n as f64 / t0.elapsed().as_secs_f64();
    let (cpu_avg, cpu_peak) = sampler.stop();

    println!(
        "[STRESS] trickle  users={n}  rate={rate:.1}/s  \
         CPU_avg={cpu_avg:.2}%  CPU_peak={cpu_peak:.2}%  \
         lat_avg={:.1} ms  lat_max={:.1} ms",
        lat_sum / n as f64,
        lat_max
    );
}
