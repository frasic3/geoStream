//! Vehicle movement simulator on a road graph, compiled to WebAssembly.
//!
//! Runs in the browser (zero load on the server: the server only receives
//! `POSITION` every 30 s). The vehicle walks along the edges of a Turin road
//! graph (exported from OpenStreetMap, see `tools/export_turin_graph.py`):
//! it follows the real shape of the streets, picks a direction at
//! intersections, drives at urban speeds (30–70 km/h) and takes occasional
//! pauses (traffic lights/stops).
//!
//! `advance(dt_secs)` advances the simulation by `dt_secs` *logical* seconds.
//! The caller invokes it often with a small dt (e.g. 0.1 s) for smooth
//! movement, and samples/sends the position to the server every 30 s, as per
//! spec. Decoupling animation (smooth) from sending (every 30 s) avoids "jumps".

use serde::Deserialize;
use wasm_bindgen::prelude::*;

// --- Physical constants ------------------------------------------------------

/// Urban speed range, km/h.
const SPEED_MIN_KMH: f64 = 30.0;
const SPEED_MAX_KMH: f64 = 70.0;

/// Driving time (logical seconds) between one pause and the next.
const DRIVE_MIN_SECS: f64 = 90.0;
const DRIVE_MAX_SECS: f64 = 600.0;

/// Duration of a pause (logical seconds). Some exceed 180 s ⇒ the server
/// recognizes them as the "stationary" state; the shorter ones (traffic
/// lights) remain "moving". Consistent with the spec.
const PAUSE_MIN_SECS: f64 = 10.0;
const PAUSE_MAX_SECS: f64 = 240.0;

/// Iteration cap for `advance`: defense against cycles of ~0-length edges
/// in the graph (duplicated coordinates).
const MAX_STEPS: u32 = 10_000;

/// Mean Earth radius in meters (for haversine).
const EARTH_R_M: f64 = 6_371_000.0;

// --- Graph ------------------------------------------------------------------

#[derive(Deserialize)]
struct Graph {
    /// `nodes[i] = [lat, lon]`.
    nodes: Vec<[f64; 2]>,
    /// `adj[i]` = indices of the nodes adjacent to `i` (undirected graph).
    adj: Vec<Vec<u32>>,
}

impl Graph {
    fn coord(&self, i: usize) -> (f64, f64) {
        let n = self.nodes[i];
        (n[0], n[1])
    }

    fn seg_len_m(&self, a: usize, b: usize) -> f64 {
        let (alat, alon) = self.coord(a);
        let (blat, blon) = self.coord(b);
        haversine_m(alat, alon, blat, blon)
    }

    /// A node is an intersection if it doesn't have exactly 2 neighbors:
    /// degree-2 nodes are simple shape points along a road (no real choice).
    fn is_intersection(&self, i: usize) -> bool {
        self.adj[i].len() != 2
    }
}

/// Distance in meters between two coordinates (haversine formula).
fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dphi = (lat2 - lat1).to_radians();
    let dlmb = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlmb / 2.0).sin().powi(2);
    2.0 * EARTH_R_M * a.sqrt().asin()
}

// --- PRNG -------------------------------------------------------------------

/// xorshift64*: minimal PRNG, deterministic given the seed. No external
/// dependencies (avoids `getrandom` and its configuration for the wasm target).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform f64 in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.unit()
    }

    /// Uniform index in [0, n).
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// --- Simulator -------------------------------------------------------------

#[wasm_bindgen]
pub struct Simulator {
    graph: Graph,
    rng: Rng,
    /// Node we come from in the current segment.
    from: usize,
    /// Node we are heading towards.
    to: usize,
    /// Distance already traveled along the `from → to` segment, in meters.
    offset_m: f64,
    /// Current speed in m/s (resampled at intersections).
    speed_mps: f64,
    /// Driving seconds left before the next pause.
    drive_secs_left: f64,
    /// Pause seconds left; > 0 ⇒ stationary (coordinate unchanged).
    pause_secs_left: f64,
    /// Speed shown in the UI (0 during pauses).
    last_speed_kmh: f64,
    lat: f64,
    lon: f64,
}

#[wasm_bindgen]
impl Simulator {
    /// Creates the simulator from the graph JSON (`{nodes, adj}`) and a seed
    /// (e.g. `Date.now()` on the JS side). Initial position: random node with
    /// at least one neighbor. Returns an error if the JSON is invalid or the
    /// graph is empty.
    #[wasm_bindgen(constructor)]
    pub fn new(graph_json: &str, seed: f64) -> Result<Simulator, JsValue> {
        // `JsValue` is unusable outside wasm (it panics): keep the logic in
        // `build` with a `String` error, testable natively, and here only
        // map the error to the JS type.
        Self::build(graph_json, seed).map_err(|e| JsValue::from_str(&e))
    }

    fn build(graph_json: &str, seed: f64) -> Result<Simulator, String> {
        let graph: Graph = serde_json::from_str(graph_json)
            .map_err(|e| format!("invalid graph JSON: {e}"))?;
        if graph.nodes.is_empty() {
            return Err("empty graph".to_string());
        }

        let mut rng = Rng::new(seed as u64);

        // Look for a starting node with at least one outgoing edge.
        let n = graph.nodes.len();
        let mut from = rng.below(n);
        for _ in 0..n {
            if !graph.adj[from].is_empty() {
                break;
            }
            from = (from + 1) % n;
        }
        if graph.adj[from].is_empty() {
            return Err("graph without edges".to_string());
        }

        let to = graph.adj[from][rng.below(graph.adj[from].len())] as usize;
        let (lat, lon) = graph.coord(from);
        let speed_mps = rng.range(SPEED_MIN_KMH, SPEED_MAX_KMH) / 3.6;
        let drive_secs_left = rng.range(DRIVE_MIN_SECS, DRIVE_MAX_SECS);

        Ok(Simulator {
            graph,
            rng,
            from,
            to,
            offset_m: 0.0,
            speed_mps,
            drive_secs_left,
            pause_secs_left: 0.0,
            last_speed_kmh: 0.0,
            lat,
            lon,
        })
    }

    fn resample_speed(&mut self) {
        self.speed_mps = self.rng.range(SPEED_MIN_KMH, SPEED_MAX_KMH) / 3.6;
    }

    /// Picks the next node from `node`, avoiding going back to `avoid`
    /// (the node we just came from) if alternatives exist.
    /// On a dead end it turns around (U-turn).
    fn pick_next(&mut self, node: usize, avoid: usize) -> usize {
        let neigh = &self.graph.adj[node];
        let alt = neigh.iter().filter(|&&x| x as usize != avoid).count();
        if alt == 0 {
            return avoid; // dead end
        }
        let mut k = self.rng.below(alt);
        for &x in neigh {
            if x as usize == avoid {
                continue;
            }
            if k == 0 {
                return x as usize;
            }
            k -= 1;
        }
        avoid // unreachable
    }

    /// Advances the simulation by `dt_secs` logical seconds. Called often
    /// with a small dt (smooth animation); the position should be
    /// sampled/sent by the caller every 30 s.
    pub fn advance(&mut self, dt_secs: f64) {
        if dt_secs <= 0.0 {
            return;
        }

        // Paused: stationary, coordinate unchanged.
        if self.pause_secs_left > 0.0 {
            self.pause_secs_left -= dt_secs;
            if self.pause_secs_left > 0.0 {
                self.last_speed_kmh = 0.0;
                return;
            }
            // Pause over: restart with a new speed.
            self.pause_secs_left = 0.0;
            self.resample_speed();
        }

        // Driving time expired → start a pause at the current point
        // (even mid-road: a stop in traffic).
        self.drive_secs_left -= dt_secs;
        if self.drive_secs_left <= 0.0 {
            self.pause_secs_left = self.rng.range(PAUSE_MIN_SECS, PAUSE_MAX_SECS);
            self.drive_secs_left = self.rng.range(DRIVE_MIN_SECS, DRIVE_MAX_SECS);
            self.last_speed_kmh = 0.0;
            return;
        }

        self.last_speed_kmh = self.speed_mps * 3.6;
        let mut budget_m = self.speed_mps * dt_secs;

        let mut steps = 0;
        loop {
            steps += 1;
            if steps > MAX_STEPS {
                break;
            }

            let seg = self.graph.seg_len_m(self.from, self.to);
            let remain = seg - self.offset_m;

            if budget_m < remain {
                self.offset_m += budget_m;
                break;
            }

            // Reached node `to`: continue onto a new edge.
            budget_m -= remain.max(0.0);
            let arrived = self.to;
            let came_from = self.from;
            let next = self.pick_next(arrived, came_from);
            self.from = arrived;
            self.to = next;
            self.offset_m = 0.0;
            // At a real intersection, change speed (new road).
            if self.graph.is_intersection(arrived) {
                self.resample_speed();
            }
        }

        self.update_position();
    }

    /// Updates `lat`/`lon` by interpolating along the `from → to` segment.
    fn update_position(&mut self) {
        let (flat, flon) = self.graph.coord(self.from);
        let (tlat, tlon) = self.graph.coord(self.to);
        let seg = self.graph.seg_len_m(self.from, self.to);
        let frac = if seg > 0.0 {
            (self.offset_m / seg).clamp(0.0, 1.0)
        } else {
            0.0
        };
        self.lat = flat + (tlat - flat) * frac;
        self.lon = flon + (tlon - flon) * frac;
    }

    #[wasm_bindgen(getter)]
    pub fn lat(&self) -> f64 {
        self.lat
    }

    #[wasm_bindgen(getter)]
    pub fn lon(&self) -> f64 {
        self.lon
    }

    /// `true` if the vehicle is moving (not paused).
    #[wasm_bindgen(getter)]
    pub fn moving(&self) -> bool {
        self.pause_secs_left <= 0.0
    }

    /// Current speed in km/h (0 while paused).
    #[wasm_bindgen(getter)]
    pub fn speed_kmh(&self) -> f64 {
        self.last_speed_kmh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Linear graph 0-1-2-3-4 with coordinates spaced ~127 m apart in longitude.
    fn line_graph_json() -> String {
        let nodes: Vec<String> = (0..5)
            .map(|i| format!("[45.07,{:.6}]", 7.68 + i as f64 * 0.00127))
            .collect();
        let adj = ["[1]", "[0,2]", "[1,3]", "[2,4]", "[3]"];
        format!(
            "{{\"nodes\":[{}],\"adj\":[{}]}}",
            nodes.join(","),
            adj.join(",")
        )
    }

    #[test]
    fn parses_and_starts_on_a_node() {
        let s = Simulator::build(&line_graph_json(), 1.0).expect("construction");
        assert!((s.lat() - 45.07).abs() < 1e-6);
    }

    #[test]
    fn rejects_bad_json() {
        assert!(Simulator::build("not-json", 1.0).is_err());
    }

    #[test]
    fn deterministic_with_same_seed() {
        let mut a = Simulator::build(&line_graph_json(), 999.0).unwrap();
        let mut b = Simulator::build(&line_graph_json(), 999.0).unwrap();
        for _ in 0..300 {
            a.advance(0.5);
            b.advance(0.5);
        }
        assert_eq!(a.lat(), b.lat());
        assert_eq!(a.lon(), b.lon());
    }

    #[test]
    fn stays_on_the_road_line() {
        // On a linear graph the latitude stays constant and the longitude
        // stays within the graph's extent: the vehicle doesn't "fly" off road.
        let mut s = Simulator::build(&line_graph_json(), 7.0).unwrap();
        for _ in 0..1000 {
            s.advance(0.3);
            assert!((s.lat() - 45.07).abs() < 1e-6);
            assert!(s.lon() >= 7.68 - 1e-3);
            assert!(s.lon() <= 7.68 + 4.0 * 0.00127 + 1e-3);
        }
    }

    #[test]
    fn small_steps_move_smoothly() {
        // Small steps ⇒ small displacements (no "jumps"): at ~50 km/h in 0.1 s
        // you move ~1.4 m, well below the length of a segment.
        let mut s = Simulator::build(&line_graph_json(), 3.0).unwrap();
        // Skip any initial pauses: ensure a driving state.
        s.pause_secs_left = 0.0;
        s.drive_secs_left = 1000.0;
        let (lat0, lon0) = (s.lat(), s.lon());
        s.advance(0.1);
        let moved = haversine_m(lat0, lon0, s.lat(), s.lon());
        assert!(moved < 5.0, "per-step displacement too large: {moved} m");
    }

    #[test]
    fn speed_zero_while_paused() {
        let mut s = Simulator::build(&line_graph_json(), 42.0).unwrap();
        s.pause_secs_left = 100.0;
        let (lat0, lon0) = (s.lat(), s.lon());
        s.advance(1.0);
        assert_eq!(s.speed_kmh(), 0.0);
        assert!(!s.moving());
        assert_eq!(s.lat(), lat0);
        assert_eq!(s.lon(), lon0);
    }
}
