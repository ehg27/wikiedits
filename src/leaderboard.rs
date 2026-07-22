use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// 1. MUTEX 
pub struct MutexLeaderboard {
    pub data: Arc<Mutex<HashMap<String, u64>>>,
}

impl MutexLeaderboard {
    pub fn new() -> Self {
        Self { data: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub fn increment(&self, domain: &str) -> u64 {
        let start = Instant::now();
        let mut map = self.data.lock().unwrap();
        let count = map.entry(domain.to_string()).or_insert(0);
        *count += 1;
        start.elapsed().as_nanos() as u64
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let map = self.data.lock().unwrap();
        let mut entries: Vec<(String, u64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(3);
        entries
    }
}

// RWLOCK 
pub struct RwLockLeaderboard {
    pub data: Arc<RwLock<HashMap<String, u64>>>,
}

impl RwLockLeaderboard {
    pub fn new() -> Self {
        Self { data: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn increment(&self, domain: &str) -> u64 {
        let start = Instant::now();
        let mut map = self.data.write().unwrap();
        let count = map.entry(domain.to_string()).or_insert(0);
        *count += 1;
        start.elapsed().as_nanos() as u64
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let map = self.data.read().unwrap();
        let mut entries: Vec<(String, u64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(3);
        entries
    }
}

// ATOMIC 
pub struct AtomicLeaderboard {
    pub total: Arc<AtomicU64>,
    pub data:  Arc<Mutex<HashMap<String, u64>>>,
}

impl AtomicLeaderboard {
    pub fn new() -> Self {
        Self {
            total: Arc::new(AtomicU64::new(0)),
            data:  Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn increment(&self, domain: &str) -> u64 {
        let start = Instant::now();
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut map = self.data.lock().unwrap();
        let count = map.entry(domain.to_string()).or_insert(0);
        *count += 1;
        start.elapsed().as_nanos() as u64
    }

    pub fn get_total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn top3(&self) -> Vec<(String, u64)> {
        let map = self.data.lock().unwrap();
        let mut entries: Vec<(String, u64)> = map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(3);
        entries
    }
}

// BENCHMARK 
pub struct SyncBenchmark {
    pub mutex_times:  Vec<u64>,
    pub rwlock_times: Vec<u64>,
    pub atomic_times: Vec<u64>,
}

impl SyncBenchmark {
    pub fn new() -> Self {
        Self {
            mutex_times:  Vec::new(),
            rwlock_times: Vec::new(),
            atomic_times: Vec::new(),
        }
    }

    pub fn record_mutex(&mut self, nanos: u64)  { self.mutex_times.push(nanos);  }
    pub fn record_rwlock(&mut self, nanos: u64) { self.rwlock_times.push(nanos); }
    pub fn record_atomic(&mut self, nanos: u64) { self.atomic_times.push(nanos); }

    pub fn print_report(&self) {
        println!("\n╔════════════════════════════════════════╗");
        println!("║      SYNCHRONISATION BENCHMARK         ║");
        println!("╠════════════════════════════════════════╣");
        println!("║ {:^38} ║", "Mutex");
        println!("║   avg: {:>8.0}ns  max: {:>8.0}ns   ║",
            avg_f64(&self.mutex_times), max_u64(&self.mutex_times));
        println!("╠════════════════════════════════════════╣");
        println!("║ {:^38} ║", "RwLock");
        println!("║   avg: {:>8.0}ns  max: {:>8.0}ns   ║",
            avg_f64(&self.rwlock_times), max_u64(&self.rwlock_times));
        println!("╠════════════════════════════════════════╣");
        println!("║ {:^38} ║", "Atomic");
        println!("║   avg: {:>8.0}ns  max: {:>8.0}ns   ║",
            avg_f64(&self.atomic_times), max_u64(&self.atomic_times));
        println!("╚════════════════════════════════════════╝\n");
    }
}

fn avg_f64(values: &[u64]) -> f64 {
    if values.is_empty() { return 0.0; }
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

fn max_u64(values: &[u64]) -> f64 {
    *values.iter().max().unwrap_or(&0) as f64
}