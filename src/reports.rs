// DRIFT REPORT 
// Displays latency percentiles (p50, p90, p99) for human vs bot edits
// and shows priority scheduling effectiveness

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = (((p / 100.0) * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

fn average(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

fn max_val(v: &[f64]) -> f64 {
    v.iter().cloned().fold(0.0_f64, f64::max)
}

pub fn print_drift_report(architecture: &str, human: &[f64], bot: &[f64]) {
    println!(
        "\n╔═══════════════════════════════════════════════════════════╗"
    );
    println!(
        "║         [{}] DRIFT ANALYSIS REPORT                    ║",
        architecture
    );
    println!(
        "╠═══════════════════════════════════════════════════════════╣"
    );
    println!("║ HUMAN EDITS ({:>5} samples)                              ║", human.len());
    println!(
        "║   Average:  {:>8.3}ms                                   ║",
        average(human)
    );
    println!(
        "║   p50:      {:>8.3}ms                                   ║",
        percentile(human, 50.0)
    );
    println!(
        "║   p90:      {:>8.3}ms                                   ║",
        percentile(human, 90.0)
    );
    println!(
        "║   p99:      {:>8.3}ms  <- tail latency                  ║",
        percentile(human, 99.0)
    );
    println!(
        "║   Max:      {:>8.3}ms                                   ║",
        max_val(human)
    );
    println!(
        "╠═══════════════════════════════════════════════════════════╣"
    );
    println!("║ BOT EDITS   ({:>5} samples)                              ║", bot.len());
    println!(
        "║   Average:  {:>8.3}ms                                   ║",
        average(bot)
    );
    println!(
        "║   p50:      {:>8.3}ms                                   ║",
        percentile(bot, 50.0)
    );
    println!(
        "║   p90:      {:>8.3}ms                                   ║",
        percentile(bot, 90.0)
    );
    println!(
        "║   p99:      {:>8.3}ms  <- tail latency                  ║",
        percentile(bot, 99.0)
    );
    println!(
        "║   Max:      {:>8.3}ms                                   ║",
        max_val(bot)
    );
    println!(
        "╠═══════════════════════════════════════════════════════════╣"
    );

    let hp99 = percentile(human, 99.0);
    let bp99 = percentile(bot, 99.0);
    if hp99 > 0.0 && bp99 > 0.0 {
        let diff = (bp99 - hp99).abs();
        let pct = (diff / hp99.min(bp99)) * 100.0;
        println!(
            "║  HUMAN p99 is {:.1}% {} than BOT p99               ║",
            pct,
            if hp99 < bp99 { "BETTER" } else { "WORSE" }
        );
    }
    println!(
        "╚═══════════════════════════════════════════════════════════╝\n"
    );
}

pub fn print_deadline_report(architecture: &str, deadline_misses: u64, total: u64, deadline_ms: f64) {
    if total == 0 {
        return;
    }

    let miss_rate = (deadline_misses as f64 / total as f64) * 100.0;

    if deadline_misses == 0 {
        println!(
            "\n╔═══════════════════════════════════════════════════════════╗"
        );
        println!(
            "║     [{}] ALL PACKETS MET {:.1}MS DEADLINE         ║",
            architecture, deadline_ms
        );
        println!(
            "║     100% compliance achieved!                             ║"
        );
        println!(
            "╚═══════════════════════════════════════════════════════════╝\n"
        );
    } else {
        println!(
            "\n╔═══════════════════════════════════════════════════════════╗"
        );
        println!(
            "║    [{}] DEADLINE VIOLATIONS ({:.1}ms threshold)        ║",
            architecture, deadline_ms
        );
        println!(
            "╠═══════════════════════════════════════════════════════════╣"
        );
        println!(
            "║ Total violations: {}                                   ║",
            deadline_misses
        );
        println!("║ Miss rate: {:.2}%                                        ║", miss_rate);
        println!(
            "║ Out of {} total packets processed                      ║",
            total
        );
        println!(
            "╚═══════════════════════════════════════════════════════════╝\n"
        );
    }
}