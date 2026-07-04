use cpu_time::ProcessTime;
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;

const LOG_INTERVAL: Duration = Duration::from_secs(120);
const LOG_FILE: &str = "server_cpu.log";

/// Background task: every 2 minutes it measures the CPU time used by the
/// process and appends it to `server_cpu.log`.
///
/// It reports two values per line:
/// - `cpu_total`: cumulative CPU time since the logger started (user+system).
/// - `cpu_interval`: CPU time consumed over the last 2 minutes.
pub async fn start_cpu_logger() {
    tracing::info!("cpu_log: started, logging to '{LOG_FILE}' every {}s", LOG_INTERVAL.as_secs());

    let t_start = ProcessTime::now();
    let mut t_prev = ProcessTime::now();

    let mut ticker = time::interval(LOG_INTERVAL);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ticker.tick().await; // skip the initial immediate tick

    loop {
        ticker.tick().await;

        let cpu_total = t_start.elapsed();
        let cpu_interval = t_prev.elapsed();
        t_prev = ProcessTime::now();

        write_entry(cpu_total, cpu_interval);
    }
}

fn write_entry(total: Duration, interval: Duration) {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let h = (wall / 3600) % 24;
    let m = (wall / 60) % 60;
    let s = wall % 60;

    // Normalize the % over the number of logical cores. Example: 1 core
    // saturated for 2 minutes on an 8-core machine = 1/8 = 12.5%, not 100%.
    // Without normalization the percentage can exceed 100 (multiple cores
    // saturated at once) and loses readability.
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1) as f64;
    let pct = (interval.as_secs_f64() / (LOG_INTERVAL.as_secs_f64() * cores)) * 100.0;

    let line = format!(
        "[{wall}] [{h:02}:{m:02}:{s:02} UTC]  cpu_total={:.3}s  cpu_interval_2min={:.3}s  cpu_pct={:.2}%  cores={}\n",
        total.as_secs_f64(),
        interval.as_secs_f64(),
        pct,
        cores as u64,
    );

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(LOG_FILE)
    {
        Ok(mut f) => {
            if let Err(e) = f.write_all(line.as_bytes()) {
                tracing::warn!("cpu_log: writing to {LOG_FILE} failed: {e}");
            }
        }
        Err(e) => tracing::warn!("cpu_log: opening {LOG_FILE} failed: {e}"),
    }
}
