use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

pub struct WatchdogState {
    pub last_heartbeat:  Arc<AtomicU64>,
    pub degraded_mode:   Arc<AtomicBool>,
    pub reset_requested: Arc<AtomicBool>,
}

impl WatchdogState {
    pub fn new() -> Self {
        Self {
            last_heartbeat:  Arc::new(AtomicU64::new(now_ms())),
            degraded_mode:   Arc::new(AtomicBool::new(false)),
            reset_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn heartbeat(&self) {
        self.last_heartbeat.store(now_ms(), Ordering::Relaxed);
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded_mode.load(Ordering::Relaxed)
    }

    pub fn reset_requested(&self) -> bool {
        self.reset_requested.load(Ordering::Relaxed)
    }

    pub fn clear_reset(&self) {
        self.reset_requested.store(false, Ordering::Relaxed);
    }

    pub fn clone_state(&self) -> WatchdogState {
        WatchdogState {
            last_heartbeat:  Arc::clone(&self.last_heartbeat),
            degraded_mode:   Arc::clone(&self.degraded_mode),
            reset_requested: Arc::clone(&self.reset_requested),
        }
    }
}

pub struct JitterTracker {
    pub recent_drifts:       Vec<f64>,
    pub window_size:         usize,
    pub jitter_threshold_ms: f64,
}

impl JitterTracker {
    pub fn new(window_size: usize, threshold_ms: f64) -> Self {
        Self {
            recent_drifts: Vec::with_capacity(window_size),
            window_size,
            jitter_threshold_ms: threshold_ms,
        }
    }

    pub fn record(&mut self, drift_ms: f64) {
        if self.recent_drifts.len() >= self.window_size {
            self.recent_drifts.remove(0);
        }
        self.recent_drifts.push(drift_ms);
    }

    pub fn average_jitter(&self) -> f64 {
        if self.recent_drifts.is_empty() { return 0.0; }
        self.recent_drifts.iter().sum::<f64>() / self.recent_drifts.len() as f64
    }

    pub fn is_jitter_exceeded(&self) -> bool {
        self.recent_drifts.len() >= self.window_size &&
        self.average_jitter() > self.jitter_threshold_ms
    }
}

pub fn start_watchdog(
    state:         WatchdogState,
    timeout_secs:  u64,
    jitter_threshold: f64,
) {
    thread::spawn(move || {
        println!("[WATCHDOG] Started — timeout: {}s, jitter threshold: {}ms",
            timeout_secs, jitter_threshold);

        let timeout_ms = timeout_secs * 1000;
        let mut tick   = 0u64;

        loop {
            thread::sleep(Duration::from_secs(1));
            tick += 1;

            let now        = now_ms();
            let last       = state.last_heartbeat.load(Ordering::Relaxed);
            let silence_ms = now.saturating_sub(last);

            // CHECK 1: Network timeout
            if silence_ms >= timeout_ms {
                println!(
                    "\n[WATCHDOG] NETWORK RESET triggered! \
                     No data for {}ms (threshold: {}ms)",
                    silence_ms, timeout_ms
                );
                println!("  - Resetting heartbeat clock...");
                state.reset_requested.store(true,    Ordering::Relaxed);
                state.last_heartbeat.store(now_ms(), Ordering::Relaxed);
            }

            // CHECK 2: Early warning
            else if silence_ms >= 2000 {
                println!(
                    "⚠️  [WATCHDOG] No data for {}ms — watching...",
                    silence_ms
                );
            }

            // CHECK 3: Recovery from degraded mode 
            if state.degraded_mode.load(Ordering::Relaxed) && silence_ms < 1000 {
                println!("[WATCHDOG] System recovered — exiting degraded mode");
                state.degraded_mode.store(false, Ordering::Relaxed);
            }

            // STATUS: every 5 ticks 
            if tick % 5 == 0 {
                let mode = if silence_ms >= timeout_ms {
                    "NETWORK TIMEOUT"
                } else if state.degraded_mode.load(Ordering::Relaxed) {
                    "DEGRADED"
                } else if silence_ms >= 3000 {
                    "WARNING"
                } else {
                    "NORMAL"
                };
                println!(
                    "[WATCHDOG] Status: {} | Last data: {}ms ago",
                    mode, silence_ms
                );
            }
        }
    });
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}