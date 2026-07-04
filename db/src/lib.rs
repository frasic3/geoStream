use anyhow::{anyhow, Context, Result};
use bcrypt::{hash, verify};
use common::Message;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const BCRYPT_COST: u32 = 10;

// ---------------------------------------------------------------------------
// Global SQLite pool.
//
// What the pool is for:
// - Opening a SQLite connection is costly (file open, PRAGMA setup): the pool
//   opens N connections at startup and reuses them, avoiding the per-query cost.
// - It caps concurrency (here max 8 connections, see `init()`): it prevents
//   saturating the DB and mitigates the file locks typical of SQLite.
// - It allows parallel async queries: each task takes a free connection
//   from the pool and releases it when done.
//
// Why static (`OnceLock`):
// - Single instance per process, shared by all modules without having to
//   pass `&SqlitePool` as a parameter down the whole call stack.
// - `OnceLock` guarantees lazy, thread-safe init: the pool is set exactly
//   once inside `init()` and is read-only afterwards.
// ---------------------------------------------------------------------------
static POOL: OnceLock<SqlitePool> = OnceLock::new();

// Returns the database URL: uses the `DATABASE_URL` environment variable
// if present, otherwise falls back to a local SQLite file in the working dir.
fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://./georuggine.db".to_string())
}

// Global access to the already-initialized pool. Panics if `init()` has not
// been called yet: the pool must exist before any query.
pub fn pool() -> &'static SqlitePool {
    POOL.get()
        .expect("DB not initialized: call db::init() first")
}

/// Opens the pool, enables foreign_keys and applies the migrations.
pub async fn init() -> Result<()> {
    let url = database_url();
    // WAL + synchronous=Normal: concurrent writes don't block readers and
    // only one fsync per commit is paid instead of two. Acceptable trade-off:
    // if the OS crashes, at most the last transactions not yet checkpointed
    // are lost, but no corruption.
    // `:memory:` doesn't support WAL, so stick to the default in that case.
    let mut opts = SqliteConnectOptions::from_str(&url)
        .context("invalid DATABASE_URL")?
        .create_if_missing(true)
        .foreign_keys(true);
    if url != "sqlite::memory:" {
        opts = opts
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal);
    }

    let max_connections = if url == "sqlite::memory:" { 1 } else { 8 };
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await
        .context("opening sqlite pool")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("migrations")?;

    POOL.set(pool)
        .map_err(|_| anyhow!("pool already initialized"))?;
    Ok(())
}

/// Compat: the server calls `ensure_file_exists` at boot.
pub async fn ensure_file_exists() -> Result<()> {
    if POOL.get().is_none() {
        init().await?;
    }
    Ok(())
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// In-memory sessions (token <-> username).
// ---------------------------------------------------------------------------

pub struct AuthStore {
    sessions: HashMap<String, String>,
}

impl AuthStore {
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }
}

impl Default for AuthStore {
    fn default() -> Self {
        Self::new()
    }
}

static AUTH_STORE: OnceLock<Mutex<AuthStore>> = OnceLock::new();

pub fn get_auth_store() -> &'static Mutex<AuthStore> {
    AUTH_STORE.get_or_init(|| Mutex::new(AuthStore::new()))
}

pub async fn insert_token(username: &str, token: &str) {
    let mut store = get_auth_store().lock().await;
    let old: Vec<String> = store
        .sessions
        .iter()
        .filter_map(|(t, u)| (u == username).then(|| t.clone()))
        .collect();
    for t in old {
        store.sessions.remove(&t);
    }
    store
        .sessions
        .insert(token.to_string(), username.to_string());
}

/// Inserts the token only if `username` doesn't already have an active session.
/// Check + insert are atomic under the same lock: two concurrent logins
/// for the same user cannot both pass the check.
pub async fn try_insert_token(username: &str, token: &str) -> Result<()> {
    let mut store = get_auth_store().lock().await;
    if store.sessions.values().any(|u| u == username) {
        return Err(anyhow!("user already logged in from another session"));
    }
    store
        .sessions
        .insert(token.to_string(), username.to_string());
    Ok(())
}

pub async fn get_user_by_token(token: &str) -> Option<String> {
    let store = get_auth_store().lock().await;
    store.sessions.get(token).cloned()
}

pub async fn invalidate_token(token: &str) {
    let mut store = get_auth_store().lock().await;
    store.sessions.remove(token);
}

/// Invalidates all active sessions for `username`. Used when a new LOGIN
/// overwrites a previous session (single-session policy): it removes the
/// token so that, if the old client came back to talk, any authenticated
/// request would be rejected with `UNAUTHORIZED`.
pub async fn invalidate_user_sessions(username: &str) -> usize {
    let mut store = get_auth_store().lock().await;
    let tokens: Vec<String> = store
        .sessions
        .iter()
        .filter_map(|(t, u)| (u == username).then(|| t.clone()))
        .collect();
    let n = tokens.len();
    for t in tokens {
        store.sessions.remove(&t);
    }
    n
}

/// True if the user appears in an active session (any connection).
pub async fn is_logged_in(username: &str) -> bool {
    let store = get_auth_store().lock().await;
    store.sessions.values().any(|u| u == username)
}

pub async fn clear_sessions() {
    let mut store = get_auth_store().lock().await;
    store.sessions.clear();
}

/// Full wipe: useful in tests to start from a clean state.
pub async fn reset_all_for_tests() -> Result<()> {
    sqlx::query("DELETE FROM positions").execute(pool()).await?;
    sqlx::query("DELETE FROM trips").execute(pool()).await?;
    sqlx::query("DELETE FROM users").execute(pool()).await?;
    clear_sessions().await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Users.
// ---------------------------------------------------------------------------

pub async fn key_exists(username: &str) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM users WHERE username = ?")
        .bind(username)
        .fetch_optional(pool())
        .await?;
    Ok(row.is_some())
}

pub async fn check_credentials(msg: &Message) -> Result<()> {
    let (username, password) = match msg {
        Message::Login { username, password } => (username.clone(), password.clone()),
        Message::Register { username, password } => (username.clone(), password.clone()),
        _ => return Err(anyhow!("invalid message")),
    };

    let row = sqlx::query("SELECT password_hash FROM users WHERE username = ?")
        .bind(&username)
        .fetch_optional(pool())
        .await?;
    let Some(row) = row else {
        return Err(anyhow!("user not found"));
    };
    let stored: String = row.try_get("password_hash")?;

    let ok = tokio::task::spawn_blocking(move || verify(&password, &stored).unwrap_or(false))
        .await
        .context("join verify")?;
    if ok {
        Ok(())
    } else {
        Err(anyhow!("invalid credentials"))
    }
}

pub async fn save_register(msg: &Message) -> Result<()> {
    let (username, password) = match msg {
        Message::Register { username, password } => (username.clone(), password.clone()),
        _ => return Err(anyhow!("save_register: not a Register variant")),
    };

    let password_hash = tokio::task::spawn_blocking(move || hash(password, BCRYPT_COST))
        .await
        .context("join hash")??;

    let res =
        sqlx::query("INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)")
            .bind(&username)
            .bind(&password_hash)
            .bind(now_secs())
            .execute(pool())
            .await;

    match res {
        Ok(_) => {
            tracing::info!("user '{username}' registered");
            Ok(())
        }
        Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
            Err(anyhow!("user already exists"))
        }
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// Trips.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TripRecord {
    pub id: i64,
    pub username: String,
    pub started_at: i64,
    pub ended_at: Option<i64>,
}

/// Opens a new trip for the user. Returns `trip_id`.
pub async fn start_trip(username: &str, lat: f64, lon: f64, ts: i64) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO trips (username, started_at, ended_at) VALUES (?, ?, NULL) RETURNING id",
    )
    .bind(username)
    .bind(ts)
    .fetch_one(pool())
    .await?;
    let id: i64 = row.try_get("id")?;
    insert_position(id, lat, lon, ts, false).await?;
    Ok(id)
}

pub async fn insert_position(
    trip_id: i64,
    lat: f64,
    lon: f64,
    ts: i64,
    stopped: bool,
) -> Result<()> {
    sqlx::query("INSERT INTO positions (trip_id, ts, lat, lon, stopped) VALUES (?, ?, ?, ?, ?)")
        .bind(trip_id)
        .bind(ts)
        .bind(lat)
        .bind(lon)
        .bind(stopped)
        .execute(pool())
        .await?;
    Ok(())
}

/// Closes a trip: validates ownership + that it is not already closed.
pub async fn end_trip(trip_id: i64, username: &str, ts: i64) -> Result<()> {
    let res = sqlx::query(
        "UPDATE trips SET ended_at = ? \
         WHERE id = ? AND username = ? AND ended_at IS NULL",
    )
    .bind(ts)
    .bind(trip_id)
    .bind(username)
    .execute(pool())
    .await?;
    if res.rows_affected() == 0 {
        return Err(anyhow!("trip missing, not yours or already closed"));
    }
    Ok(())
}

/// Returns whether the trip is open and owned by the user.
pub async fn trip_open_for(trip_id: i64, username: &str) -> Result<bool> {
    let row = sqlx::query("SELECT 1 FROM trips WHERE id = ? AND username = ? AND ended_at IS NULL")
        .bind(trip_id)
        .bind(username)
        .fetch_optional(pool())
        .await?;
    Ok(row.is_some())
}

/// IDs of all trips still open for a user. Used during WebSocket connection
/// cleanup to close trips left pending when the client disconnects without
/// sending `EndTrip`.
pub async fn open_trip_ids_for(username: &str) -> Result<Vec<i64>> {
    let rows = sqlx::query("SELECT id FROM trips WHERE username = ? AND ended_at IS NULL")
        .bind(username)
        .fetch_all(pool())
        .await?;
    rows.into_iter()
        .map(|row| row.try_get::<i64, _>("id").map_err(Into::into))
        .collect()
}

// ---------------------------------------------------------------------------
// Positions and statistics.
// ---------------------------------------------------------------------------

// Tolerance in meters to prevent minimal GPS oscillations from being
// counted as real movement. Exposed publicly because the server-side
// `trips` module also uses it to decide whether two points are "equal",
// so the same threshold governs both the `stopped` flag and the
// statistical computation of pauses.
pub const MOVEMENT_EPS_METERS: f64 = 5.0;

/// Maximum accepted gap between two consecutive samples in the same trip
/// when computing statistics. Spec: nominal cadence 30 s, so
/// 90 s = 3× tolerance covers small delays/jitter. Beyond this threshold
/// the segment is unreliable (e.g. client offline, app crashed, server
/// restarted) and must be excluded from the counts: otherwise a gap of
/// hours would be counted as "pause" or "movement", inflating the statistics.
const MAX_SAMPLE_GAP_SECS: i64 = 90;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PositionRecord {
    pub trip_id: i64,
    pub ts: i64,
    pub lat: f64,
    pub lon: f64,
    pub stopped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MovementStats {
    pub username: String,
    pub from_ts: i64,
    pub to_ts: i64,
    pub distance_m: f64,
    pub movement_secs: i64,
    pub pause_secs: i64,
    pub total_secs: i64,
    pub avg_speed_mps: f64,
    pub avg_speed_kmh: f64,
    pub points: i64,
}

pub async fn trajectory_for_user(
    username: &str,
    from_ts: i64,
    to_ts: i64,
) -> Result<Vec<PositionRecord>> {
    if from_ts > to_ts {
        return Err(anyhow!("invalid time interval"));
    }

    let rows = sqlx::query(
        "SELECT p.trip_id, p.ts, p.lat, p.lon, p.stopped
         FROM positions p
         JOIN trips t ON t.id = p.trip_id
         WHERE t.username = ?
           AND p.ts >= ?
           AND p.ts <= ?
         ORDER BY p.trip_id, p.ts",
    )
    .bind(username)
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(pool())
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(PositionRecord {
                trip_id: row.try_get("trip_id")?,
                ts: row.try_get("ts")?,
                lat: row.try_get("lat")?,
                lon: row.try_get("lon")?,
                stopped: row.try_get("stopped")?,
            })
        })
        .collect()
}

pub async fn movement_stats_for_user(
    username: &str,
    from_ts: i64,
    to_ts: i64,
) -> Result<MovementStats> {
    let points = trajectory_for_user(username, from_ts, to_ts).await?;
    Ok(compute_movement_stats(username, from_ts, to_ts, &points))
}

pub fn compute_movement_stats(
    username: &str,
    from_ts: i64,
    to_ts: i64,
    points: &[PositionRecord],
) -> MovementStats {
    let mut distance_m = 0.0;
    let mut movement_secs = 0_i64;
    let mut pause_secs = 0_i64;
    let mut total_secs = 0_i64;

    for window in points.windows(2) {
        let a = &window[0];
        let b = &window[1];

        // Don't artificially connect two different trips.
        if a.trip_id != b.trip_id {
            continue;
        }

        let dt = b.ts - a.ts;
        if dt <= 0 || dt > MAX_SAMPLE_GAP_SECS {
            continue;
        }

        total_secs += dt;
        let d = haversine_m(a.lat, a.lon, b.lat, b.lon);

        // The updated model already carries the `stopped` boolean: use it as
        // the primary signal. Distance below the threshold remains a useful
        // protection against GPS noise or old data without a coherent flag.
        let is_pause = b.stopped || d <= MOVEMENT_EPS_METERS;

        if is_pause {
            pause_secs += dt;
        } else {
            distance_m += d;
            movement_secs += dt;
        }
    }

    let avg_speed_mps = if movement_secs > 0 {
        distance_m / movement_secs as f64
    } else {
        0.0
    };

    MovementStats {
        username: username.to_string(),
        from_ts,
        to_ts,
        distance_m,
        movement_secs,
        pause_secs,
        total_secs,
        avg_speed_mps,
        avg_speed_kmh: avg_speed_mps * 3.6,
        points: points.len() as i64,
    }
}

pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0_f64;

    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let d_phi = (lat2 - lat1).to_radians();
    let d_lambda = (lon2 - lon1).to_radians();

    let a = (d_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);

    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    static TEST_GUARD: StdMutex<()> = StdMutex::new(());

    async fn setup() {
        if POOL.get().is_none() {
            std::fs::create_dir_all("target").ok();
            std::fs::remove_file("target/db_tests.sqlite").ok();
            std::fs::remove_file("target/db_tests.sqlite-shm").ok();
            std::fs::remove_file("target/db_tests.sqlite-wal").ok();

            std::env::set_var(
                "DATABASE_URL",
                "sqlite://./target/db_tests.sqlite",
            );

            init().await.unwrap();
        }

        reset_all_for_tests().await.unwrap();
    }

    #[tokio::test]
    async fn register_and_check_credentials_ok() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let reg = Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        };
        save_register(&reg).await.unwrap();
        assert!(key_exists("mario").await.unwrap());
        let login = Message::Login {
            username: "mario".into(),
            password: "secret".into(),
        };
        assert!(check_credentials(&login).await.is_ok());
        let bad = Message::Login {
            username: "mario".into(),
            password: "wrong".into(),
        };
        assert!(check_credentials(&bad).await.is_err());
    }

    #[tokio::test]
    async fn duplicate_save_register_fails() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        let reg = Message::Register {
            username: "luigi".into(),
            password: "secret".into(),
        };
        save_register(&reg).await.unwrap();
        assert!(save_register(&reg).await.is_err());
    }

    #[tokio::test]
    async fn trip_lifecycle() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        save_register(&Message::Register {
            username: "mario".into(),
            password: "secret".into(),
        })
        .await
        .unwrap();

        let trip_id = start_trip("mario", 45.07, 12.29, 0).await.unwrap();
        assert!(trip_open_for(trip_id, "mario").await.unwrap());
        assert!(!trip_open_for(trip_id, "other").await.unwrap());

        end_trip(trip_id, "mario", 1).await.unwrap();
        assert!(!trip_open_for(trip_id, "mario").await.unwrap());
        assert!(end_trip(trip_id, "mario", 1).await.is_err());
    }

    #[tokio::test]
    async fn end_trip_of_another_user_fails() {
        let _g = TEST_GUARD.lock().unwrap_or_else(|p| p.into_inner());
        setup().await;
        for u in ["mario", "luigi"] {
            save_register(&Message::Register {
                username: u.into(),
                password: "secret".into(),
            })
            .await
            .unwrap();
        }
        let trip_id = start_trip("mario", 45.07, 12.29, 0).await.unwrap();
        assert!(end_trip(trip_id, "luigi", 1).await.is_err());
    }

    #[test]
    fn statistics_use_stopped_and_distance() {
        let points = vec![
            PositionRecord {
                trip_id: 1,
                ts: 0,
                lat: 45.000,
                lon: 9.000,
                stopped: false,
            },
            PositionRecord {
                trip_id: 1,
                ts: 60,
                lat: 45.000,
                lon: 9.000,
                stopped: true,
            },
            PositionRecord {
                trip_id: 1,
                ts: 120,
                lat: 45.001,
                lon: 9.000,
                stopped: false,
            },
            PositionRecord {
                trip_id: 1,
                ts: 180,
                lat: 45.002,
                lon: 9.000,
                stopped: false,
            },
        ];

        let stats = compute_movement_stats("mario", 0, 180, &points);
        assert_eq!(stats.points, 4);
        assert_eq!(stats.pause_secs, 60);
        assert_eq!(stats.movement_secs, 120);
        assert_eq!(stats.total_secs, 180);
        assert!(stats.distance_m > 200.0 && stats.distance_m < 230.0);
        assert!(stats.avg_speed_kmh > 6.0 && stats.avg_speed_kmh < 7.0);
    }
}
