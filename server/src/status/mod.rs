use std::collections::HashMap;
use std::sync::OnceLock;

use tokio::sync::Mutex;

/// Logical state of the connected user (spec: "stationary", "moving").
/// The "disconnected" state is represented by the user's absence from the
/// map (see `remove`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserStatus {
    Stationary,
    Moving,
}

impl UserStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stationary => "stationary",
            Self::Moving => "moving",
        }
    }
}

static STATUSES: OnceLock<Mutex<HashMap<String, UserStatus>>> = OnceLock::new();

fn store() -> &'static Mutex<HashMap<String, UserStatus>> {
    STATUSES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Sets a user's status.
/// Logs only actual transitions to avoid noise (many consecutive POSITIONs
/// with a new coordinate remain `Moving`).
pub async fn set(user: &str, new_status: UserStatus) {
    let mut s = store().lock().await;
    let prev_status = s.get(user).copied();
    s.insert(user.to_string(), new_status);
    if prev_status != Some(new_status) {
        tracing::info!(user = %user, status = new_status.as_str(), "user status change");
    }
}

/// Removes the user from the map (equivalent to `Disconnected`).
pub async fn remove(user: &str) {
    let mut s = store().lock().await;
    if s.remove(user).is_some() {
        tracing::info!(user = %user, status = "disconnected", "user status change");
    }
}
