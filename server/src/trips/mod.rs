use std::{collections::HashMap, sync::OnceLock};

use tokio::sync::Mutex;

use crate::status::{self, UserStatus};

#[derive(Debug, Clone, Default)]
struct CoordState {
    lat: f64,
    lon: f64,
    n: u32,
    inserted: bool,
}

pub struct TripsStore {
    // Maps trip_id to the last known coordinate
    last_coordinates: HashMap<i64, CoordState>,
}

impl TripsStore {
    pub fn new() -> Self {
        Self {
            last_coordinates: HashMap::new(),
        }
    }
}

impl Default for TripsStore {
    fn default() -> Self {
        Self::new()
    }
}

static TRIPS_STORE: OnceLock<Mutex<TripsStore>> = OnceLock::new();

pub fn get_trips_store() -> &'static Mutex<TripsStore> {
    TRIPS_STORE.get_or_init(|| Mutex::new(TripsStore::new()))
}

pub async fn initialize_trips(user: String, lat: f64, lon: f64, ts: i64) -> Result<i64, String> {
    let trip_id = db::start_trip(&user, lat, lon, ts)
        .await
        .map_err(|e| format!("Failed to start trip: {}", e))?;

    {
        let mut store = get_trips_store().lock().await;
        store.last_coordinates.insert(
            trip_id,
            CoordState {
                lat,
                lon,
                n: 1,
                inserted: false,
            },
        );
    }

    // Spec: right after login/start the user is "stationary"; they switch to
    // "moving" only at the first coordinate change.
    status::set(&user, UserStatus::Stationary).await;

    Ok(trip_id)
}

pub async fn terminate_trip(trip_id: i64, user: String, ts: i64) -> Result<(), String> {
    db::end_trip(trip_id, &user, ts)
        .await
        .map_err(|e| format!("Failed to end trip: {}", e))?;

    let mut store = get_trips_store().lock().await;
    store.last_coordinates.remove(&trip_id);

    Ok(())
}

/// Removes a trip from the in-memory store without touching the DB. Used by
/// the connection cleanup procedure when closing an orphan trip (the DB row
/// has already been updated by the caller via `db::end_trip`).
pub async fn drop_trip(trip_id: i64) {
    let mut store = get_trips_store().lock().await;
    store.last_coordinates.remove(&trip_id);
}

// Number of consecutive `POSITION` samples with identical coordinates beyond
// which the user is marked "stationary". With a client-side cadence of 30 s
// (spec), 6 samples = 180 s = 3 min, as required by the specification.
const STOP_THRESHOLD_SAMPLES: u32 = 6;

/// `true` if the trip is present in the in-memory store.
/// Used by the dispatcher to tell a "live" trip apart from a trip left open
/// in the DB after a server crash/restart (in that case the movement state
/// is lost and unrecoverable: better to close it).
pub async fn is_tracked(trip_id: i64) -> bool {
    let store = get_trips_store().lock().await;
    store.last_coordinates.contains_key(&trip_id)
}

/// Decides whether two coordinates can be considered "equal" for pause
/// detection purposes. Uses the haversine distance under the same threshold
/// adopted by `compute_movement_stats`, so the `stopped` flag written to
/// the DB and the pause computation stay consistent: no `==` comparisons
/// on `f64`, which would be fragile (numeric noise, client rounding).
fn coords_equal(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> bool {
    db::haversine_m(a_lat, a_lon, b_lat, b_lon) <= db::MOVEMENT_EPS_METERS
}

pub async fn update_trip(
    trip_id: i64,
    user: &str,
    lat: f64,
    lon: f64,
    ts: i64,
) -> Result<(), String> {
    enum InsertAction {
        Insert { stopped: bool },
        Skip,
    }

    enum StatusTransition {
        ToMoving,
        ToStationary,
        Unchanged,
    }

    let (action, transition) = {
        let mut store = get_trips_store().lock().await;
        match store.last_coordinates.get_mut(&trip_id) {
            Some(coord) => {
                if coords_equal(coord.lat, coord.lon, lat, lon) {
                    if coord.n < STOP_THRESHOLD_SAMPLES {
                        coord.n += 1;
                        (
                            InsertAction::Insert { stopped: false },
                            StatusTransition::Unchanged,
                        )
                    } else if !coord.inserted {
                        coord.inserted = true;
                        (
                            InsertAction::Insert { stopped: true },
                            StatusTransition::ToStationary,
                        )
                    } else {
                        (InsertAction::Skip, StatusTransition::Unchanged)
                    }
                } else {
                    *coord = CoordState {
                        lat,
                        lon,
                        n: 1,
                        inserted: false,
                    };
                    (
                        InsertAction::Insert { stopped: false },
                        StatusTransition::ToMoving,
                    )
                }
            }
            None => return Err(format!("Trip ID {} not found in store", trip_id)),
        }
    };

    match action {
        InsertAction::Insert { stopped } => db::insert_position(trip_id, lat, lon, ts, stopped)
            .await
            .map_err(|e| format!("Failed to insert position: {}", e))?,
        InsertAction::Skip => {}
    }

    match transition {
        StatusTransition::ToMoving => status::set(user, UserStatus::Moving).await,
        StatusTransition::ToStationary => status::set(user, UserStatus::Stationary).await,
        StatusTransition::Unchanged => {}
    }

    Ok(())
}
