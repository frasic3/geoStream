// Interactive WebSocket client. Available commands:
//   /register <user> <pass>
//   /login    <user> <pass>
//   /start [lat lon]                 opens a new trip with the current timestamp
//   /pos <lat> <lon> [ts]            sends a position for the current trip
//   /stats day|week|month            statistics over current UTC intervals
//   /stats-range <from_ts> <to_ts>   statistics over a manual interval
//   /end                             closes the current trip
//   /chat     <text>                 sends a chat message to the server
//                                    (the server logs it and replies ACK; there
//                                    is no forwarding to other clients)
//   /quit
use anyhow::Result;
use common::{decode, encode, Message};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

// Default WebSocket URL the client connects to when the `SERVER_ADDR`
// environment variable is not set. `ws://` scheme = cleartext WebSocket
// (TLS would require `wss://`). Host:port + `/ws` path must match the
// route exposed by the server (see server/src/main.rs).
const ADDR_DEFAULT: &str = "ws://127.0.0.1:7878/ws";
const SECS_PER_DAY: i64 = 86_400;

type WsConn = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsConn, WsMessage>;
type WsStream = SplitStream<WsConn>;
type SharedSink = Arc<Mutex<WsSink>>;
type SharedToken = Arc<Mutex<Option<String>>>;
type SharedTrip = Arc<Mutex<Option<i64>>>;

type SharedAuto = Arc<Mutex<bool>>;
type SharedPosition = Arc<Mutex<(f64, f64)>>;

const AUTOPOS_SECS: u64 = 30;

// Fictitious displacement at each sample.
// Small values: they simulate gradual movement.
const AUTOPOS_STEP_LAT: f64 = 0.00045;
const AUTOPOS_STEP_LON: f64 = 0.00045;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = std::env::var("SERVER_ADDR").unwrap_or_else(|_| ADDR_DEFAULT.to_string());
    let (ws, _resp) = tokio_tungstenite::connect_async(&addr).await?;
    println!("connected to {addr}");

    let (sink, mut stream): (WsSink, WsStream) = ws.split();
    let sink: SharedSink = Arc::new(Mutex::new(sink));

    let token: SharedToken = Arc::new(Mutex::new(None));
    let trip: SharedTrip = Arc::new(Mutex::new(None));
    let autopos_enabled: SharedAuto = Arc::new(Mutex::new(false));
    let current_pos: SharedPosition = Arc::new(Mutex::new((45.000, 9.000)));
    let token_auto = token.clone();
    let trip_auto = trip.clone();
    let sink_auto = sink.clone();
    let autopos_enabled_auto = autopos_enabled.clone();
    let current_pos_auto = current_pos.clone();

    let autopos_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(AUTOPOS_SECS));

        // Consume the first immediate tick: this way the first POSITION goes out after 30 seconds.
        interval.tick().await;

        loop {
            interval.tick().await;

            if !*autopos_enabled_auto.lock().await {
                continue;
            }

            let token = match token_auto.lock().await.clone() {
                Some(t) => t,
                None => continue,
            };

            let trip_id = match *trip_auto.lock().await {
                Some(id) => id,
                None => continue,
            };

            let (lat, lon) = {
                let mut pos = current_pos_auto.lock().await;
                pos.0 += AUTOPOS_STEP_LAT;
                pos.1 += AUTOPOS_STEP_LON;
                *pos
            };

            let msg = Message::Position {
                token,
                trip_id,
                lat,
                lon,
                ts: now_secs(),
            };

            if let Err(e) = send_msg(&sink_auto, &msg).await {
                eprintln!("[autopos] failed to send position: {e}");
                break;
            }

            println!("[autopos] POSITION lat={lat:.6} lon={lon:.6}");
        }
    });
    let token_recv = token.clone();
    let trip_recv = trip.clone();
    let recv_task = tokio::spawn(async move {
        loop {
            match stream.next().await {
                Some(Ok(WsMessage::Text(line))) => match decode(&line) {
                    Ok(Message::AuthOk { token: t }) => {
                        println!("[server] AUTH_OK token={t}");
                        *token_recv.lock().await = Some(t);
                    }
                    Ok(Message::TripStarted {
                        trip_id,
                        lat,
                        lon,
                        ts,
                    }) => {
                        println!("[server] TRIP_STARTED id={trip_id} lat={lat} lon={lon} ts={ts}");
                        *trip_recv.lock().await = Some(trip_id);
                    }
                    Ok(Message::StatsResult {
                        username,
                        from_ts,
                        to_ts,
                        distance_m,
                        movement_secs,
                        pause_secs,
                        total_secs,
                        avg_speed_mps,
                        avg_speed_kmh,
                        points,
                    }) => {
                        println!(
                            "[stats] user={username} points={points} interval=[{from_ts}, {to_ts}]\n\
                             \tdistance: {:.2} m\n\
                             \tmovement: {} ({movement_secs} s)\n\
                             \tpauses:     {} ({pause_secs} s)\n\
                             \ttotal:      {} ({total_secs} s)\n\
                             \taverage speed: {:.2} m/s = {:.2} km/h",
                            distance_m,
                            fmt_duration(movement_secs),
                            fmt_duration(pause_secs),
                            fmt_duration(total_secs),
                            avg_speed_mps,
                            avg_speed_kmh,
                        );
                    }
                    Ok(Message::Error { code, message }) => {
                        println!("[server] ERROR {code}: {message}");
                    }
                    Ok(Message::Ack) => println!("[server] ACK"),
                    Ok(Message::ChatFromServer { from, text }) => match from {
                        Some(f) => println!("[chat from {f}] {text}"),
                        None => println!("[chat broadcast] {text}"),
                    },
                    Ok(other) => println!("[server] {other:?}"),
                    Err(e) => println!("[server] parse error: {e}"),
                },
                Some(Ok(WsMessage::Close(_))) => {
                    println!("[server] connection closed");
                    break;
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    eprintln!("[client] ws error: {e}");
                    break;
                }
                None => {
                    println!("[server] stream end");
                    break;
                }
            }
        }
    });

    println!("commands: /register <u> <p> | /login <u> <p> | /start [lat lon] | /pos <lat> <lon> [ts] | /stats day|week|month | /stats-range <from_ts> <to_ts> | /end | /chat <t> | /quit");

    let stdin = tokio::io::stdin();
    let mut stdin = BufReader::new(stdin).lines();
    while let Some(line) = stdin.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let msg = match parse_cmd(line, &token, &trip, &autopos_enabled, &current_pos).await {
            Some(m) => m,
            None => continue,
        };
        if matches!(msg, Message::EndTrip { .. }) {
            *trip.lock().await = None;
            *autopos_enabled.lock().await = false;
        }
        let is_quit = matches!(msg, Message::Disconnect { .. });
        send_msg(&sink, &msg).await?;
        if is_quit {
            tokio::time::sleep(Duration::from_millis(200)).await;
            break;
        }
    }

    {
        let mut s = sink.lock().await;
        let _ = s.send(WsMessage::Close(None)).await;
    }
    autopos_task.abort();
    recv_task.abort();
    Ok(())
}

async fn send_msg(sink: &SharedSink, msg: &Message) -> Result<()> {
    let line = encode(msg)?;
    let mut s = sink.lock().await;
    s.send(WsMessage::Text(line)).await?;
    Ok(())
}

async fn parse_cmd(line: &str, token: &SharedToken, trip: &SharedTrip, autopos_enabled: &SharedAuto, current_pos: &SharedPosition) -> Option<Message> {
    let (cmd, rest) = line.split_once(' ').unwrap_or((line, ""));
    let rest = rest.trim();
    match cmd {
        "/register" => {
            let (u, p) = rest.split_once(' ')?;
            Some(Message::Register {
                username: u.into(),
                password: p.into(),
            })
        }
        "/login" => {
            let (u, p) = rest.split_once(' ')?;
            Some(Message::Login {
                username: u.into(),
                password: p.into(),
            })
        }
        "/start" => {
            let t = require_token(token).await?;
            let mut parts = rest.split_whitespace();
            let lat = parts.next().and_then(|s| s.parse().ok()).unwrap_or(45.000);
            let lon = parts.next().and_then(|s| s.parse().ok()).unwrap_or(9.000);

            *current_pos.lock().await = (lat, lon);
            *autopos_enabled.lock().await = true;

            println!(
                "[autopos] active: will send a POSITION every {AUTOPOS_SECS} seconds after TRIP_STARTED"
            );

            Some(Message::StartTrip {
                token: t,
                lat,
                lon,
                ts: now_secs(),
            })
        }
        "/pos" => {
            let t = require_token(token).await?;
            let trip_id = require_trip(trip).await?;
            let mut parts = rest.split_whitespace();
            let lat: f64 = match parts.next().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    println!("usage: /pos <lat> <lon> [ts]");
                    return None;
                }
            };
            let lon: f64 = match parts.next().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    println!("usage: /pos <lat> <lon> [ts]");
                    return None;
                }
            };
            let ts: i64 = match parts.next().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    println!("usage: /pos <lat> <lon> <ts>");
                    return None;
                }
            };
            *current_pos.lock().await = (lat, lon);
            Some(Message::Position {
                token: t,
                trip_id,
                lat,
                lon,
                ts,
            })
        }
        "/stats" => {
            let t = require_token(token).await?;
            let (from_ts, to_ts, label) = match stats_interval(rest) {
                Some(v) => v,
                None => {
                    println!("usage: /stats day|week|month");
                    return None;
                }
            };
            println!("requesting statistics for {label} UTC: [{from_ts}, {to_ts}]");
            Some(Message::Stats {
                token: t,
                from_ts,
                to_ts,
            })
        }
        "/stats-range" => {
            let t = require_token(token).await?;
            let mut parts = rest.split_whitespace();
            let from_ts: i64 = match parts.next().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    println!("usage: /stats-range <from_ts> <to_ts>");
                    return None;
                }
            };
            let to_ts: i64 = match parts.next().and_then(|s| s.parse().ok()) {
                Some(v) => v,
                None => {
                    println!("usage: /stats-range <from_ts> <to_ts>");
                    return None;
                }
            };
            Some(Message::Stats {
                token: t,
                from_ts,
                to_ts,
            })
        }
        "/end" => {
            let t = require_token(token).await?;
            let trip_id = require_trip(trip).await?;
            Some(Message::EndTrip {
                token: t,
                trip_id,
                ts: now_secs(),
            })
        }
        "/chat" => {
            if rest.is_empty() {
                println!("usage: /chat <text>");
                return None;
            }
            let t = require_token(token).await?;
            Some(Message::ChatToServer {
                token: t,
                text: rest.into(),
            })
        }
        "/quit" => {
            let t = token.lock().await.clone().unwrap_or_default();
            Some(Message::Disconnect { token: t })
        }
        _ => {
            println!("invalid command: {cmd}");
            None
        }
    }
}

async fn require_token(token: &SharedToken) -> Option<String> {
    match token.lock().await.clone() {
        Some(t) => Some(t),
        None => {
            println!("you must /register or /login first");
            None
        }
    }
}

async fn require_trip(trip: &SharedTrip) -> Option<i64> {
    match trip.lock().await.clone() {
        Some(info) => Some(info),
        None => {
            println!("no open trip: use /start first");
            None
        }
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn fmt_duration(secs: i64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h}h {m}m {s}s")
}

fn stats_interval(kind: &str) -> Option<(i64, i64, &'static str)> {
    let now = now_secs();
    let days = now.div_euclid(SECS_PER_DAY);
    match kind {
        "day" => {
            let start = days * SECS_PER_DAY;
            Some((start, start + SECS_PER_DAY - 1, "current day"))
        }
        "week" => {
            // 1970-01-01 was a Thursday. With a Monday-Sunday week: Monday=0.
            let weekday_monday0 = (days + 3).rem_euclid(7);
            let start_days = days - weekday_monday0;
            let start = start_days * SECS_PER_DAY;
            Some((start, start + 7 * SECS_PER_DAY - 1, "current week"))
        }
        "month" => {
            let (year, month, _) = civil_from_days(days);
            let start_days = days_from_civil(year, month, 1);
            let (next_year, next_month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            let next_start_days = days_from_civil(next_year, next_month, 1);
            Some((
                start_days * SECS_PER_DAY,
                next_start_days * SECS_PER_DAY - 1,
                "current month",
            ))
        }
        _ => None,
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let month = month as i64;
    let day = day as i64;
    let y = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
