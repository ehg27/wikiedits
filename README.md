# RTS Wikipedia Edit Stream Monitor

Real-time Wikipedia edit stream processing system built in **Rust**, comparing two concurrent processing architectures — **Tokio async** and **std::thread**-based — under strict real-time constraints.

**Course:** CT078-3-3-RTS — Real Time Systems (APU)
**Author:** Loh Ehung (TP079311)

For the full documentation, please refer to 'Real Time Systems Proj.pdf'.

---

## Overview

The system connects to the live Wikimedia SSE stream
(`https://stream.wikimedia.org/v2/stream/recentchange`) and processes incoming edit events through a three-stage pipeline:

1. **Ingestion** — zero-copy JSON parsing of incoming edits
2. **Prioritization** — human edits routed ahead of bot edits via separate bounded channels
3. **Processing** — drift/deadline tracking, leaderboard updates, synchronization benchmarking

A strict **2.0 ms micro-deadline** is enforced per packet, with full support for backpressure handling, fault recovery (watchdog), and a degraded-mode safety interlock.

## Usage

```bash
cargo run            # Threaded architecture (default)
cargo run async       # Async (Tokio) architecture
cargo bench           # Criterion.rs tail-latency benchmarks
```

## Architecture

| | Threaded (`std::thread`) | Async (`tokio`) |
|---|---|---|
| Scheduler | OS-managed, preemptive | Cooperative, Tokio runtime |
| Channels | `mpsc::sync_channel` | `crossbeam::ArrayQueue` (lock-free) |
| Tasks | `ingestion_thread`, `processor_thread`, watchdog thread | `ingestion_task`, `processor_task`, `watchdog_task` via `tokio::join!` |

Both architectures share:
- Priority draining (human queue before bot queue)
- Drop-oldest backpressure on overflow (`CHANNEL_CAPACITY = 10`)
- 2.0 ms scheduling-drift deadline checks
- Watchdog fault recovery (10 s silence → network reset)
- Jitter-based degraded mode (avg jitter > 50 ms → drop bot edits)
- Zero-copy vs. non-zero-copy parsing comparison via a custom `CountingAlloc` global allocator
- Mutex / RwLock / Atomic leaderboard synchronization benchmarking

## Key Components

| Component | Description |
|---|---|
| `main.rs` | Entry point; selects architecture via CLI arg |
| `threaded.rs` | std::thread-based pipeline |
| `async_version.rs` | Tokio-based pipeline |
| `leaderboard.rs` | Mutex / RwLock / Atomic domain leaderboards |
| `watchdog.rs` | Heartbeat monitoring, network reset, degraded mode |
| `metrics.rs` | Drift, deadline, and allocation reporting |

## Constants

```rust
CHANNEL_CAPACITY   = 10
DEADLINE_MS        = 2.0
WATCHDOG_SECS      = 10
JITTER_WINDOW      = 20
JITTER_THRESH_MS   = 50.0
```

## Results Summary

| Metric | Async | Threaded |
|---|---|---|
| Deadline miss rate | **1.80%** | 46.80% |
| Human p99 drift | 1.612 ms | 3.407 ms |
| Bot p99 drift | 2.889 ms | 610.153 ms |
| Criterion p99 (hot-loop) | **375 ns** | 500 ns |
| Criterion max (hot-loop) | **10,958 ns** | 5,304,208 ns |
| Zero-copy allocation reduction | 72.8% fewer allocations, 46.5% fewer bytes | — |

**Synchronization primitives (avg / max latency):** RwLock performs best overall; Mutex has the highest average latency; Atomic has the lowest-to-mid average but the highest maximum latency spikes.

**Takeaway:** The async (Tokio) architecture provides more predictable worst-case latency, a far lower deadline-miss rate, and more balanced human/bot priority enforcement. The threaded architecture achieves the lowest human-edit latency in absolute terms but at the cost of severe bot starvation and unpredictable tail spikes caused by OS-level thread preemption.

## Report Sections

The full report (`RTS_TP079311.pdf`) covers:
- Literature review (Rust memory safety, performance, fearless concurrency; two related Rust systems)
- System design (Components A–E: architecture, optimization/priority, drift measurement, shared resources/metrics, watchdog)
- Advanced integration (zero-copy memory optimization, Criterion.rs benchmarking)
- Results and discussion across all measured dimensions
- Conclusion and references

## References

See Section 6.0 of the full report for the complete reference list (Rust language history, memory safety, related Rust streaming systems, Criterion/thread-scheduling literature).
