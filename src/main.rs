pub mod async_version;
pub mod thread_version;
pub mod leaderboard;
pub mod watchdog;
pub mod reports;
pub mod alloc;



#[global_allocator]
static GLOBAL: alloc::CountingAlloc = alloc::CountingAlloc;


fn main() {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("async") => {
            println!("Running Async Version");
            async_version::run();
        }   
        _ => {
            println!("Running Thread Version");
            thread_version::run(); 
        }
    }
}




