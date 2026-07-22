// ── THREADED ARCHITECTURE ─────────────────────────────────────────────────────
// Uses std::thread with mpsc channels and priority-based recv strategy.
// Implements: zero-copy parsing, priority scheduling (humans > bots),
// deadline enforcement, degraded mode, watchdog, and sync benchmarking.

use std::io::{BufRead, BufReader};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{SystemTime, Instant};

use crate::leaderboard::{MutexLeaderboard, RwLockLeaderboard, AtomicLeaderboard, SyncBenchmark};
use crate::watchdog::{WatchdogState, JitterTracker, start_watchdog};
use crate::reports;

const CHANNEL_CAPACITY: usize = 10;
const DEADLINE_MS: f64 = 2.0;
const WATCHDOG_SECS: u64 = 10;
const JITTER_WINDOW: usize = 20;
const JITTER_THRESH_MS: f64 = 50.0;

// ZERO-COPY PARSE MODEL 
use serde::Deserialize;

#[derive(Deserialize)]
struct WikiEditRef<'a> {
    #[serde(rename = "server_name", borrow)]
    domain: &'a str,
    #[serde(borrow)]
    user: &'a str,
    bot: bool,
}



// Owned model
#[derive(Debug, Clone)]
pub struct WikiEdit {
    pub domain: Arc<str>,
    pub user: Arc<str>,
    pub is_bot: bool,
    pub enqueue_time: Instant,
}


fn timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;
    format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis)
}

fn parse_edit(line: &str) -> Option<WikiEdit> {
    let json_str = line.strip_prefix("data: ")?;
    let parsed: WikiEditRef = serde_json::from_str(json_str).ok()?;

    Some(WikiEdit {
        domain: Arc::<str>::from(parsed.domain),
        user: Arc::<str>::from(parsed.user),
        is_bot: parsed.bot,
        enqueue_time: Instant::now(),
    })
}

// INGESTION THREAD 
// Connects to Wikipedia SSE stream, parses edits, and routes to priority channels
fn ingestion_thread(
    tx_human: mpsc::SyncSender<WikiEdit>,
    tx_bot: mpsc::SyncSender<WikiEdit>,
    watchdog_state: WatchdogState,
) {
    println!("[{}] [THREADED] Ingestion started", timestamp());

    let response = ureq::get("https://stream.wikimedia.org/v2/stream/recentchange")
        .call()
        .expect("Failed to connect");

    println!("[{}] [THREADED] Connected!", timestamp());

    let reader = BufReader::new(response.into_reader());

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };

        if let Some(edit) = parse_edit(&line) {
            watchdog_state.heartbeat();

            let tx = if edit.is_bot { &tx_bot } else { &tx_human };
            match tx.try_send(edit) {
                Ok(_) => {}
                Err(mpsc::TrySendError::Full(dropped)) => {
                    println!(
                        "[OVERFLOW AT {}] [THREADED] Dropped {} - {}",
                        timestamp(),
                        dropped.domain,
                        dropped.user
                    );
                }
                Err(mpsc::TrySendError::Disconnected(_)) => break,
            }
        }
    }

    println!("[THREADED] Ingestion stream ended");
}

// PROCESSOR THREAD 
// Implements priority scheduling: drain human edits first, then bot edits.
// Enforces 2ms deadlines and manages degraded mode.
fn processor_thread(
    rx_human: mpsc::Receiver<WikiEdit>,
    rx_bot: mpsc::Receiver<WikiEdit>,
    mutex_lb: Arc<MutexLeaderboard>,
    rwlock_lb: Arc<RwLockLeaderboard>,
    atomic_lb: Arc<AtomicLeaderboard>,
    watchdog_state: WatchdogState,
) {
    println!("[{}] [THREADED] Processor started", timestamp());

    let mut total = 0u64;
    let mut deadline_misses = 0u64;
    let mut human_drift = Vec::new();
    let mut bot_drift = Vec::new();
    let mut benchmark = SyncBenchmark::new();
    let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);


    loop {
        // RIORITY SCHEDULING 
        // Try human first, then bot, then blocking recv on human
        let edit = match rx_human.try_recv() {
            Ok(edit) => edit,
            Err(_) => match rx_bot.try_recv() {
                Ok(edit) => edit,
                Err(_) => match rx_human.recv() {
                    Ok(edit) => edit,
                    Err(_) => break,
                },
            },
        };

        // simulate latency
        
        // thread::sleep(std::time::Duration::from_millis(100));

        // MICRO-DEADLINE CHECK (2ms) 
        let drift_ms = edit.enqueue_time.elapsed().as_micros() as f64 / 1000.0;

        if drift_ms > DEADLINE_MS {
            deadline_misses += 1;
            println!(
                "[DEADLINE MISS #{}] {:.3}ms > {}ms | {} | {}",
                deadline_misses, drift_ms, DEADLINE_MS, edit.domain, edit.user
            );
        }

        // JITTER TRACKING & DEGRADED MODE 
        jitter.record(drift_ms);

        if jitter.is_jitter_exceeded() && !watchdog_state.is_degraded() {
            println!(
                "\n[THREADED] DEGRADED MODE — avg jitter {:.2}ms exceeds {}ms threshold. \
                 Dropping bot edits to reduce load.",
                jitter.average_jitter(),
                JITTER_THRESH_MS
            );
            watchdog_state.degraded_mode
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        // In degraded mode, skip bot edits to protect human latency
        if watchdog_state.is_degraded() && edit.is_bot {
            continue;
        }

        // NETWORK RESET ACKNOWLEDGEMENT 
        if watchdog_state.reset_requested() {
            println!("[THREADED PROCESSOR] Network reset acknowledged");
            watchdog_state.clear_reset();
        }

        // LATENCY METRICS 
        if edit.is_bot {
            bot_drift.push(drift_ms);
        } else {
            human_drift.push(drift_ms);
        }

        // LEADERBOARDS (all three sync types) 
        let mutex_ns = mutex_lb.increment(&edit.domain);
        let rwlock_ns = rwlock_lb.increment(&edit.domain);
        let atomic_ns = atomic_lb.increment(&edit.domain);

        benchmark.record_mutex(mutex_ns);
        benchmark.record_rwlock(rwlock_ns);
        benchmark.record_atomic(atomic_ns);

        total += 1;
        let edit_type = if edit.is_bot { "BOT  " } else { "HUMAN" };

        println!(
            "[{}] [THREADED] [{}] #{} | drift: {:.3}ms | {} | {}",
            timestamp(),
            edit_type,
            total,
            drift_ms,
            edit.domain,
            edit.user
        );

        // PERIODIC SUMMARY EVERY 50 EDITS 
        if total % 50 == 0 {
            println!("\n── TOP 3 DOMAINS [THREADED] ────────────────");
            for (i, (domain, count)) in mutex_lb.top3().iter().enumerate() {
                println!("  {}. {} — {} edits", i + 1, domain, count);
            }
            println!("─────────────────────────────────────────────");
            benchmark.print_report();
            reports::print_drift_report("THREADED", &human_drift, &bot_drift);
            reports::print_deadline_report("THREADED", deadline_misses, total, DEADLINE_MS);

        }
    }

    // FINAL REPORTS 
    println!("\n[THREADED] Processing complete. Final reports:");
    reports::print_drift_report("THREADED", &human_drift, &bot_drift);
    reports::print_deadline_report("THREADED", deadline_misses, total, DEADLINE_MS);
}


pub fn run() {
    // WATCHDOG 
    let watchdog_state = WatchdogState::new();
    let watchdog_for_ingestion = watchdog_state.clone_state();
    let watchdog_for_processor = watchdog_state.clone_state();
    start_watchdog(watchdog_state, WATCHDOG_SECS, JITTER_THRESH_MS);

    // CHANNELS 
    let (tx_human, rx_human) = mpsc::sync_channel::<WikiEdit>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<WikiEdit>(CHANNEL_CAPACITY);

    // LEADERBOARDS 
    let mutex_lb = Arc::new(MutexLeaderboard::new());
    let rwlock_lb = Arc::new(RwLockLeaderboard::new());
    let atomic_lb = Arc::new(AtomicLeaderboard::new());

    let mutex_lb_proc = Arc::clone(&mutex_lb);
    let rwlock_lb_proc = Arc::clone(&rwlock_lb);
    let atomic_lb_proc = Arc::clone(&atomic_lb);

    // SPAWN THREADS 
    let ingestion = thread::spawn(move || {
        ingestion_thread(tx_human, tx_bot, watchdog_for_ingestion)
    });

    let processor = thread::spawn(move || {
        processor_thread(
            rx_human,
            rx_bot,
            mutex_lb_proc,
            rwlock_lb_proc,
            atomic_lb_proc,
            watchdog_for_processor,
        )
    });

    

    ingestion.join().unwrap();
    processor.join().unwrap();

}