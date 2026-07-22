// async_version.rs

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration,Instant,SystemTime};


use crossbeam::queue::ArrayQueue;
use serde::Deserialize;
use tokio::{task};

use crate::leaderboard::{AtomicLeaderboard, MutexLeaderboard, RwLockLeaderboard, SyncBenchmark};
use crate::watchdog::{JitterTracker, WatchdogState};
use crate::reports;
use crate::alloc;



const CHANNEL_CAPACITY: usize = 10;
const DEADLINE_MS:      f64   = 2.0;
const WATCHDOG_SECS:    u64   = 10;
const JITTER_WINDOW:    usize = 20;
const JITTER_THRESH_MS: f64   = 50.0;


// ZERO-COPY PARSE MODEL 
// Borrows directly from the JSON buffer — no heap allocations during parse.
#[derive(Deserialize)]
pub struct WikiEditRef<'a> {
    #[serde(rename = "server_name", borrow)]
    domain: &'a str,
    #[serde(borrow)]
    user: &'a str,
    bot: bool,
}

// OWNED MODEL
#[derive(Debug, Clone)]
pub struct WikiEdit {
    pub domain: Arc<str>,
    pub user: Arc<str>,
    pub is_bot: bool,
    pub enqueue_time: Instant,
}


// HELPERS 

fn timestamp() -> String {
    let now     = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let secs    = now.as_secs();
    let millis  = now.subsec_millis();
    format!("{:02}:{:02}:{:02}.{:03}",
        (secs % 86400) / 3600,
        (secs % 3600)  / 60,
         secs % 60,
         millis)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}


/// Non-zero-copy parse
pub fn parse_edit_nz(line: &str) -> Option<WikiEdit> {
    let json_str = line.strip_prefix("data: ")?;
    let parsed: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let domain = Arc::<str>::from(parsed["server_name"].as_str()?);
    let user = Arc::<str>::from(parsed["user"].as_str().unwrap_or("unknown"));
    Some(WikiEdit {
        domain,
        user,
        is_bot: parsed["bot"].as_bool().unwrap_or(false),
        enqueue_time: Instant::now(),
    })
}


/// Zero-copy parse: WikiEditRef borrows from `line`, then we allocate

pub fn parse_edit(line: &str) -> Option<WikiEdit> {
    let json_str = line.strip_prefix("data: ")?;
    let parsed: WikiEditRef = serde_json::from_str(json_str).ok()?;

    Some(WikiEdit {
        domain: Arc::<str>::from(parsed.domain),
        user: Arc::<str>::from(parsed.user),
        is_bot: parsed.bot,
        enqueue_time: Instant::now(),
    })
}



// DROP-OLDEST PUSH 
// crossbeam::ArrayQueue::force_push evicts the oldest item when full,
fn enqueue(queue: &ArrayQueue<WikiEdit>, edit: WikiEdit) {
    if queue.is_full() {
        println!("[OVERFLOW AT {}] [ASYNC] — Dropped {} - {}", timestamp(), edit.domain, edit.user);
    }
    // force_push drops the oldest item automatically if the queue is full.
    let _ = queue.force_push(edit);
}

// ── WATCHDOG TASK ────────────────────────────────────────────────────────────
async fn watchdog_task(state: WatchdogState) {
    println!("[ASYNC WATCHDOG] Started — timeout: {}s, jitter threshold: {}ms",
        WATCHDOG_SECS, JITTER_THRESH_MS);

    let timeout_ms = WATCHDOG_SECS * 1000;
    let mut tick   = 0u64;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        tick += 1;

        let now        = now_ms();
        let last       = state.last_heartbeat.load(Ordering::Relaxed);
        let silence_ms = now.saturating_sub(last);

        // ── Network timeout → trigger reset ───────────────────
        if silence_ms >= timeout_ms {
            println!("\n[ASYNC WATCHDOG] NETWORK RESET — no data for {}ms", silence_ms);
            state.reset_requested.store(true,    Ordering::Relaxed);
            state.last_heartbeat .store(now_ms(), Ordering::Relaxed);

        } else if silence_ms >= 3000 {
            println!("[ASYNC WATCHDOG] WARNING — no data for {}ms", silence_ms);
        }

        // ── Auto-recover from degraded mode ───────────────────
        if state.is_degraded() && silence_ms < 1000 {
            println!("[ASYNC WATCHDOG] Recovered — exiting degraded mode");
            state.degraded_mode.store(false, Ordering::Relaxed);
        }

        // ── Periodic status report ────────────────────────────
        if tick % 5 == 0 {
            let mode = if silence_ms >= timeout_ms   { "NETWORK TIMEOUT" }
                       else if state.is_degraded()   { "DEGRADED"        }
                       else if silence_ms >= 3000    { "WARNING"         }
                       else                          { "NORMAL"          };
            println!("[ASYNC WATCHDOG] tick={} | status={} | silence={}ms",
                tick, mode, silence_ms);
        }
    }
}

// INGESTION TASK 
// Connects to the Wikipedia SSE stream, parses each line zero-copy,
// and routes edits into the correct ArrayQueue (human or bot).
async fn ingestion_task(
    human_queue:    Arc<ArrayQueue<WikiEdit>>,
    bot_queue:      Arc<ArrayQueue<WikiEdit>>,
    watchdog_state: WatchdogState,
) {
    println!("[{}] [ASYNC] Ingestion started", timestamp());

    // Connect on a blocking thread so we don't block the Tokio executor.
    let response = task::spawn_blocking(|| {
        ureq::get("https://stream.wikimedia.org/v2/stream/recentchange")
            .call()
            .expect("[ASYNC] Failed to connect to Wikipedia SSE stream")
    })
    .await
    .expect("spawn_blocking failed");

    println!("[{}] [ASYNC] Connected to Wikipedia stream", timestamp());

    // Bridge the blocking BufReader to async via a std mpsc channel.
    // The reader thread pushes raw lines; this task drains and processes them.
    let (line_tx, line_rx) = std::sync::mpsc::channel::<String>();

    task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader};
        let reader = BufReader::new(response.into_reader());
        for line in reader.lines() {
            match line {
                Ok(l)  => { if line_tx.send(l).is_err() { break; } }
                Err(_) => break,
            }
        }
    });

    // Wrap the receiver so it can be shared across spawn_blocking calls.
    let line_rx = Arc::new(std::sync::Mutex::new(line_rx));

    let mut parsed_count = 0u64;
    alloc::reset_alloc_stats();     
    
    loop {
        let rx = Arc::clone(&line_rx);
        let result = task::spawn_blocking(move || rx.lock().unwrap().recv())
            .await
            .unwrap();

        match result {
            Ok(line) => {
                if let Some(edit) = parse_edit(&line) {
                    // Reset watchdog heartbeat on every valid packet.
                    watchdog_state.heartbeat();

                    // Route by priority: humans → human_queue, bots → bot_queue.
                    let queue = if edit.is_bot { &bot_queue } else { &human_queue };
                    enqueue(queue, edit);

                    parsed_count += 1;

                    if parsed_count % 1000 == 0 {
                        alloc::print_alloc_stats("ZERO COPY PARSER");}
                    }
                }
            
            Err(_) => {
                println!("[ASYNC] Stream ended — ingestion shutting down");
                break;
            }
            
        }
    }
}

// ── PROCESSOR TASK ───────────────────────────────────────────────────────────
// Drains queues with strict human priority, enforces 2ms micro-deadlines,
// manages degraded mode, and updates all three leaderboard types.
async fn processor_task(
    human_queue:    Arc<ArrayQueue<WikiEdit>>,
    bot_queue:      Arc<ArrayQueue<WikiEdit>>,
    mutex_lb:       Arc<MutexLeaderboard>,
    rwlock_lb:      Arc<RwLockLeaderboard>,
    atomic_lb:      Arc<AtomicLeaderboard>,
    watchdog_state: WatchdogState,
) {
    println!("[{}] [ASYNC] Processor started", timestamp());

    let mut total           = 0u64;
    let mut deadline_misses = 0u64;
    let mut human_drift     = Vec::new();
    let mut bot_drift       = Vec::new();
    let mut benchmark       = SyncBenchmark::new();
    let mut jitter          = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);



    loop {
        // PRIORITY SCHEDULING 
        // Always drain human edits before falling back to bots.
        let edit = if let Some(e) = human_queue.pop() {
            e
        } else if let Some(e) = bot_queue.pop() {
            e
        } else {
            tokio::time::sleep(tokio::time::Duration::from_nanos(0)).await;
            continue;
        };

        // simulate latency
        // tokio::time::sleep(Duration::from_millis(100)).await;

        // MICRO-DEADLINE CHECK 2ms
        // Measures scheduling drift: time from enqueue to processing start.
        let drift_ms = edit.enqueue_time.elapsed().as_micros() as f64 / 1000.0;

        if drift_ms > DEADLINE_MS {
            deadline_misses += 1;
            println!(
                "[DEADLINE MISS #{}] {:.3}ms > {}ms | {} | {}",
                deadline_misses, drift_ms, DEADLINE_MS, edit.domain, edit.user
            );
        }

        // ── JITTER TRACKING & DEGRADED MODE ──────────────────
        jitter.record(drift_ms);

        if jitter.is_jitter_exceeded() && !watchdog_state.is_degraded() {
            println!(
                "[ASYNC] DEGRADED MODE — avg jitter {:.2}ms exceeds {}ms threshold. \
                 Dropping bot edits to reduce load.",
                jitter.average_jitter(), JITTER_THRESH_MS
            );
            watchdog_state.degraded_mode.store(true, Ordering::Relaxed);
        }

        // In degraded mode, discard bot edits entirely to protect human latency.
        if watchdog_state.is_degraded() && edit.is_bot {
            continue;
            
        }

        // ── NETWORK RESET ACKNOWLEDGEMENT ────────────────────
        if watchdog_state.reset_requested() {
            println!("[ASYNC PROCESSOR] Network reset acknowledged — resuming normal operation");
            watchdog_state.clear_reset();
        }

        // ── LATENCY METRICS ───────────────────────────────────
        if edit.is_bot {
            bot_drift.push(drift_ms);
        } else {
            human_drift.push(drift_ms);
        }

        // ── LEADERBOARD UPDATE (all three sync types) ─────────
        // Each increment() call returns nanoseconds spent inside the lock/op.
        let mutex_ns  = mutex_lb.increment(&edit.domain);
        let rwlock_ns = rwlock_lb.increment(&edit.domain);
        let atomic_ns = atomic_lb.increment(&edit.domain);

        benchmark.record_mutex(mutex_ns);
        benchmark.record_rwlock(rwlock_ns);
        benchmark.record_atomic(atomic_ns);

        // ── PER-EDIT LOG LINE ─────────────────────────────────
        total += 1;
        println!(
            "[{}] [ASYNC] [{}] #{} | drift: {:.3}ms | {} | {}",
            timestamp(),
            if edit.is_bot { "BOT" } else { "HUMAN" },
            total, drift_ms, edit.domain, edit.user
        );

        // ── PERIODIC SUMMARY EVERY 50 EDITS ──────────────────
        if total % 50 == 0 {
            println!("─────────────────────────────────────────");
            println!("\n── TOP 3 DOMAINS [ASYNC] ────────────────");
            for (i, (domain, count)) in atomic_lb.top3().iter().enumerate() {
                println!("  {}. {} — {} edits", i + 1, domain, count);
            }
            println!("─────────────────────────────────────────");
            benchmark.print_report();
            reports::print_drift_report("ASYNC",&human_drift, &bot_drift);
            reports::print_deadline_report("ASYNC", deadline_misses, total, DEADLINE_MS);

        }
    }
}


pub fn run() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async {
            // Shared bounded queues — drop-oldest on overflow (see enqueue()).
            let human_queue = Arc::new(ArrayQueue::<WikiEdit>::new(CHANNEL_CAPACITY));
            let bot_queue   = Arc::new(ArrayQueue::<WikiEdit>::new(CHANNEL_CAPACITY));

            let mutex_lb  = Arc::new(MutexLeaderboard::new());
            let rwlock_lb = Arc::new(RwLockLeaderboard::new());
            let atomic_lb = Arc::new(AtomicLeaderboard::new());

            // Clone WatchdogState — cheap because all fields are Arc<Atomic*>.
            let watchdog_state = WatchdogState::new();
            let ws_watchdog    = watchdog_state.clone_state();
            let ws_ingestion   = watchdog_state.clone_state();
            let ws_processor   = watchdog_state.clone_state();

            tokio::join!(
                watchdog_task(ws_watchdog),
                ingestion_task(
                    Arc::clone(&human_queue),
                    Arc::clone(&bot_queue),
                    ws_ingestion,
                ),
                processor_task(
                    Arc::clone(&human_queue),
                    Arc::clone(&bot_queue),
                    Arc::clone(&mutex_lb),
                    Arc::clone(&rwlock_lb),
                    Arc::clone(&atomic_lb),
                    ws_processor,
                ),
            );
        });
}



