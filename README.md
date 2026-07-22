# Real-Time Wikipedia Edit Stream Processing System (Rust)

A high-performance, real-time Wikipedia edit stream processing pipeline built in **Rust**. 
This project ingests live data from the Wikimedia Server-Sent Events (SSE) stream, processes and 
classifies edits with strict micro-deadline guarantees, and compares two concurrent architecture paradigms: 
**Asynchronous (`tokio`)** vs. **Multi-Threaded (`std::thread`)**.

---

## 📌 Project Overview

Real-time systems require predictable, deterministic execution under variable and high-velocity workloads. 
This system connects to the live Wikimedia SSE stream (`https://stream.wikimedia.org/v2/stream/recentchange`) to process a 
mixture of human-authored and bot-generated edits.

### Key Features
* **Dual Concurrency Models:** Implements both a cooperative async pipeline using `tokio` and an OS-managed thread pipeline using `std::thread`.
* **Priority Scheduling:** Prioritizes human edits over bot edits, preferentially draining the human queue to minimize human scheduling drift.
* **2.0 ms Micro-Deadline Enforcement:** Tracks packet scheduling drift (enqueue-to-process duration) and logs deadline misses exceeding 2.0 ms.
* **Backpressure Management:** Bounded channels (`capacity = 10`) drop the oldest incoming packets during queue overflow conditions.
* **Zero-Copy JSON Parsing:** Employs Rust lifetimes (`WikiEditRef<'a>`) to borrow directly from the input string buffer, drastically reducing heap allocations.
* **Watchdog & Degraded Mode Subsystem:** Monitors system health via heartbeat timestamps; auto-triggers a network reset on 10s stream silence and enters **Degraded Mode** (dropping bot edits) when rolling jitter exceeds 50 ms.
* **Synchronization Benchmarking:** Measures lock contention and execution latency across `Mutex`, `RwLock`, and `Atomic` operations on a shared domain leaderboard.

---

## 🏗 System Architecture

```text
                 [ Wikimedia Live SSE Stream ]
                              │
                    ( Zero-Copy Parsing )
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
   [ Human Edit Queue ]               [ Bot Edit Queue ]
   (High Priority)                    (Low Priority)
             │                                 │
             └────────────────┬────────────────┘
                              ▼
                     [ Priority Router ]
                  (Drains Human First)
                              │
             ┌────────────────┴────────────────┐
             ▼                                 ▼
   [ Micro-Deadline Check ]          [ Leaderboard Updates ]
     (Enforces < 2.0 ms)             (Mutex / RwLock / Atomic)
             │                                 │
             └────────────────┬────────────────┘
                              ▼
                    [ Watchdog Monitor ]
           (Jitter Tracking & Network Resets)





