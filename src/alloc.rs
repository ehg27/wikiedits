use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct CountingAlloc;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

pub fn reset_alloc_stats() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    DEALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES_ALLOCATED.store(0, Ordering::Relaxed);
}

pub fn print_alloc_stats(label: &str) {
    println!("\n=== ALLOCATION REPORT [{}] ===", label);
    println!(
        "allocations   : {}",
        ALLOCATIONS.load(Ordering::Relaxed)
    );
    println!(
        "deallocations : {}",
        DEALLOCATIONS.load(Ordering::Relaxed)
    );
    println!(
        "bytes allocated : {}",
        BYTES_ALLOCATED.load(Ordering::Relaxed)
    );
    println!("=================================\n");
}