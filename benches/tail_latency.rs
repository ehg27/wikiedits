// benches/tail_latency.rs
//
// Run:  cargo bench --bench tail_latency
// HTML: open target/criterion/report/index.html
//


#![allow(dead_code)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam::queue::ArrayQueue;
use std::{
    sync::{Arc, mpsc},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use print::leaderboard::{MutexLeaderboard, RwLockLeaderboard, AtomicLeaderboard, SyncBenchmark};
use print::watchdog::{WatchdogState, JitterTracker};
use print::async_version::WikiEdit;

// ── Production constants (match your source files) ────────────────────────────
const DEADLINE_MS:      f64     = 2.0;
const JITTER_WINDOW:    usize   = 20;
const JITTER_THRESH_MS: f64     = 50.0;
const SPIKE_SIZES:      &[usize] = &[50, 200, 1_000];
const SPIKE_BACKLOG_MS: u64     = 5;   // aged past DEADLINE_MS → forces real drift
const BOT_RATIO:        f64     = 0.70;

// ── WikiEdit constructors ─────────────────────────────────────────────────────
fn fresh_edit(is_bot: bool) -> WikiEdit {
    WikiEdit { domain: Arc::from("en.wikipedia.org"), user: Arc::from("BenchUser"),
               is_bot, enqueue_time: Instant::now() }
}

fn aged_edit(is_bot: bool) -> WikiEdit {
    WikiEdit { domain: Arc::from("en.wikipedia.org"), user: Arc::from("BenchUser"),
               is_bot, enqueue_time: Instant::now() - Duration::from_millis(SPIKE_BACKLOG_MS) }
}

// ── Spike builders ────────────────────────────────────────────────────────────
fn build_async_spike(size: usize) -> Arc<ArrayQueue<WikiEdit>> {
    let q = Arc::new(ArrayQueue::new(size + 64));
    for i in 0..size { let _ = q.force_push(aged_edit((i as f64 / size as f64) < BOT_RATIO)); }
    q
}

fn build_threaded_spike(size: usize) -> (mpsc::Receiver<WikiEdit>, mpsc::Receiver<WikiEdit>) {
    let human_n = ((1.0 - BOT_RATIO) * size as f64) as usize;
    let bot_n   = size - human_n;
    let (tx_h, rx_h) = mpsc::sync_channel::<WikiEdit>(human_n + 1);
    let (tx_b, rx_b) = mpsc::sync_channel::<WikiEdit>(bot_n   + 1);
    for _ in 0..human_n { tx_h.try_send(aged_edit(false)).ok(); }
    for _ in 0..bot_n   { tx_b.try_send(aged_edit(true )).ok(); }
    (rx_h, rx_b)
}

// ── Percentile + summary ──────────────────────────────────────────────────────
fn percentile_ns(samples: &mut Vec<u64>, p: f64) -> u64 {
    samples.sort_unstable();
    let idx = ((p / 100.0 * samples.len() as f64).ceil() as usize)
        .saturating_sub(1).min(samples.len() - 1);
    samples[idx]
}

fn print_tail_summary(label: &str, samples: &mut Vec<u64>) {
    let p50  = percentile_ns(samples, 50.0);
    let p95  = percentile_ns(samples, 95.0);
    let p99  = percentile_ns(samples, 99.0);
    let p999 = percentile_ns(samples, 99.9);
    let max  = *samples.last().unwrap_or(&0);
    eprintln!("\n  ┌─ {label}");
    eprintln!("  │  n     : {}", samples.len());
    eprintln!("  │  p50   : {}ns", p50);
    eprintln!("  │  p95   : {}ns", p95);
    eprintln!("  │  p99   : {}ns  ← tail", p99);
    eprintln!("  │  p99.9 : {}ns", p999);
    eprintln!("  │  max   : {}ns", max);
    eprintln!("  └─────────────────────────────────");
}

// ── Shared processor hot-loop (identical logic, different queue type) ──────────
#[inline(always)]
fn process_one(
    edit: WikiEdit, ml: &MutexLeaderboard, rl: &RwLockLeaderboard,
    al: &AtomicLeaderboard, bench: &mut SyncBenchmark,
    jitter: &mut JitterTracker, w: &WatchdogState, misses: &mut u64,
) -> u64 {
    let t0       = Instant::now();
    let drift_ms = edit.enqueue_time.elapsed().as_micros() as f64 / 1000.0;

    if drift_ms > DEADLINE_MS { *misses += 1; }

    jitter.record(drift_ms);
    if jitter.is_jitter_exceeded() && !w.is_degraded() {
        w.degraded_mode.store(true, Ordering::Relaxed);
    }
    if w.is_degraded() && edit.is_bot { return t0.elapsed().as_nanos() as u64; }
    if w.reset_requested()            { w.clear_reset(); }

    bench.record_mutex (ml.increment(black_box(&edit.domain)));
    bench.record_rwlock(rl.increment(black_box(&edit.domain)));
    bench.record_atomic(al.increment(black_box(&edit.domain)));

    t0.elapsed().as_nanos() as u64
}

// ── Benchmark 1: single-item baseline ────────────────────────────────────────
fn bench_single_item_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("tail_latency/baseline");
    group.throughput(Throughput::Elements(1));

    group.bench_function("async", |b| {
        let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
        let w = WatchdogState::new();
        let mut bench = SyncBenchmark::new();
        let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
        let mut misses = 0u64;
        let q: Arc<ArrayQueue<WikiEdit>> = Arc::new(ArrayQueue::new(8));

        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let _ = q.force_push(fresh_edit(false));
                if let Some(edit) = q.pop() {
                    total += Duration::from_nanos(
                        process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                    );
                }
            }
            total
        });
    });

    group.bench_function("threaded", |b| {
        let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
        let w = WatchdogState::new();
        let mut bench = SyncBenchmark::new();
        let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
        let mut misses = 0u64;
        let (tx, rx) = mpsc::sync_channel::<WikiEdit>(8);

        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                tx.try_send(fresh_edit(false)).ok();
                if let Ok(edit) = rx.try_recv() {
                    total += Duration::from_nanos(
                        process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                    );
                }
            }
            total
        });
    });

    group.finish();
}

// ── Benchmark 2: spike drain p99 (THE main comparison) ───────────────────────
fn bench_spike_drain_p99(c: &mut Criterion) {
    for &spike_size in SPIKE_SIZES {
        let mut group = c.benchmark_group(format!("tail_latency/spike_drain/size_{spike_size}"));
        group.throughput(Throughput::Elements(spike_size as u64));
        group.sample_size(if spike_size >= 1000 { 30 } else { 50 });

        group.bench_function("async", |b| {
            let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
            let w = WatchdogState::new();

            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut all: Vec<u64> = Vec::with_capacity(iters as usize * spike_size);

                for _ in 0..iters {
                    w.degraded_mode.store(false, Ordering::Relaxed);
                    let mut bench  = SyncBenchmark::new();
                    let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
                    let mut misses = 0u64;
                    let q = build_async_spike(spike_size);
                    while let Some(edit) = q.pop() {
                        let ns = process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses);
                        all.push(ns);
                        total += Duration::from_nanos(ns);
                    }
                }
                print_tail_summary(&format!("ASYNC  spike={spike_size} ({iters}x)"), &mut all);
                total
            });
        });

        group.bench_function("threaded", |b| {
            let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
            let w = WatchdogState::new();

            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut all: Vec<u64> = Vec::with_capacity(iters as usize * spike_size);

                for _ in 0..iters {
                    w.degraded_mode.store(false, Ordering::Relaxed);
                    let mut bench  = SyncBenchmark::new();
                    let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
                    let mut misses = 0u64;
                    let (rx_h, rx_b) = build_threaded_spike(spike_size);
                    loop {
                        let edit = match rx_h.try_recv().or_else(|_| rx_b.try_recv()) {
                            Ok(e)  => e,
                            Err(_) => break,
                        };
                        let ns = process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses);
                        all.push(ns);
                        total += Duration::from_nanos(ns);
                    }
                }
                print_tail_summary(&format!("THREAD spike={spike_size} ({iters}x)"), &mut all);
                total
            });
        });

        group.finish();
    }
}

// ── Benchmark 3: sustained velocity (live concurrent producer) ────────────────
fn bench_sustained_velocity(c: &mut Criterion) {
    use std::thread;
    let mut group = c.benchmark_group("tail_latency/sustained_velocity");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);

    group.bench_function("async", |b| {
        let q: Arc<ArrayQueue<WikiEdit>> = Arc::new(ArrayQueue::new(10));
        let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
        let w = WatchdogState::new();
        let mut bench = SyncBenchmark::new();
        let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
        let mut misses = 0u64;
        let q_prod: Arc<ArrayQueue<WikiEdit>> = Arc::clone(&q);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let producer = thread::spawn(move || {
            let mut i = 0usize;
            while !stop2.load(Ordering::Relaxed) { let _ = q_prod.force_push(fresh_edit(i % 10 < 7)); i += 1; }
        });
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let edit = loop { if let Some(e) = q.pop() { break e; } std::hint::spin_loop(); };
                total += Duration::from_nanos(
                    process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                );
            }
            total
        });
        stop.store(true, Ordering::Relaxed);
        producer.join().ok();
    });

    group.bench_function("threaded", |b| {
        let (tx_h, rx_h) = mpsc::sync_channel::<WikiEdit>(10);
        let (tx_b, rx_b) = mpsc::sync_channel::<WikiEdit>(10);
        let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
        let w = WatchdogState::new();
        let mut bench = SyncBenchmark::new();
        let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
        let mut misses = 0u64;
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let producer = thread::spawn(move || {
            let mut i = 0usize;
            while !stop2.load(Ordering::Relaxed) {
                if i % 10 < 7 { tx_b.try_send(fresh_edit(true )).ok(); }
                else           { tx_h.try_send(fresh_edit(false)).ok(); }
                i += 1;
            }
        });
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let edit = loop {
                    if let Ok(e) = rx_h.try_recv() { break e; }
                    if let Ok(e) = rx_b.try_recv() { break e; }
                    std::hint::spin_loop();
                };
                total += Duration::from_nanos(
                    process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                );
            }
            total
        });
        stop.store(true, Ordering::Relaxed);
        producer.join().ok();
    });

    group.finish();
}

// ── Benchmark 4: degraded-mode gate cost ──────────────────────────────────────
fn bench_degraded_mode_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("tail_latency/degraded_mode_gate");
    group.throughput(Throughput::Elements(1));

    for &degraded in &[false, true] {
        let label = if degraded { "degraded_on" } else { "degraded_off" };

        group.bench_with_input(BenchmarkId::new("async", label), &degraded, |b, &deg| {
            let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
            let w = WatchdogState::new();
            w.degraded_mode.store(deg, Ordering::Relaxed);
            let mut bench = SyncBenchmark::new();
            let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
            let mut misses = 0u64;
            let q: Arc<ArrayQueue<WikiEdit>> = Arc::new(ArrayQueue::new(8));
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let _ = q.force_push(fresh_edit(true));
                    if let Some(edit) = q.pop() {
                        total += Duration::from_nanos(
                            process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                        );
                    }
                }
                total
            });
        });

        group.bench_with_input(BenchmarkId::new("threaded", label), &degraded, |b, &deg| {
            let (ml, rl, al) = (MutexLeaderboard::new(), RwLockLeaderboard::new(), AtomicLeaderboard::new());
            let w = WatchdogState::new();
            w.degraded_mode.store(deg, Ordering::Relaxed);
            let mut bench = SyncBenchmark::new();
            let mut jitter = JitterTracker::new(JITTER_WINDOW, JITTER_THRESH_MS);
            let mut misses = 0u64;
            let (tx, rx) = mpsc::sync_channel::<WikiEdit>(8);
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    tx.try_send(fresh_edit(true)).ok();
                    if let Ok(edit) = rx.try_recv() {
                        total += Duration::from_nanos(
                            process_one(black_box(edit), &ml, &rl, &al, &mut bench, &mut jitter, &w, &mut misses)
                        );
                    }
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(
    tail_latency_benches,
    bench_single_item_latency,
    bench_spike_drain_p99,
    bench_sustained_velocity,
    bench_degraded_mode_overhead,
);
criterion_main!(tail_latency_benches);