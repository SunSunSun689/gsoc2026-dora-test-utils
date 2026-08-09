//! dora-test-utils Demo — Record/Replay regression testing with a real DORA example.
//!
//! Records the DORA `rust-dataflow` example (rust-node → rust-status-node)
//! using our `test-sink` in record mode, then replays to detect regressions.
//!
//! ## Usage
//!
//! ```bash
//! # Build dora example packages + our binaries
//! cargo build -p rust-dataflow-example-node -p rust-dataflow-example-status-node --manifest-path dora/Cargo.toml
//! cargo build --example demo_replay --bin test-sink
//!
//! # Run the demo
//! cargo run --example demo_replay
//! cargo run --example demo_replay -- --dora ./dora/target/debug/dora
//! ```

use dora_test_utils::record::{RecordSession, ReplaySession};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

// ── CLI ────────────────────────────────────────────────────

#[derive(Default)]
struct Args {
    dora: Option<PathBuf>,
    dataflow: Option<PathBuf>,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = Args::default();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--dora" => {
                i += 1;
                opts.dora = Some(PathBuf::from(&args[i]));
            }
            "--dataflow" => {
                i += 1;
                opts.dataflow = Some(PathBuf::from(&args[i]));
            }
            other => {
                eprintln!("Unknown flag: {other}");
                eprintln!("Usage: demo_replay [--dora <path>] [--dataflow <path>]");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    opts
}

// ── Helpers ────────────────────────────────────────────────

fn resolve_dora(args: &Args) -> Option<PathBuf> {
    if let Some(ref d) = args.dora {
        return if d.exists() { Some(d.clone()) } else { None };
    }
    for profile in &["debug", "release"] {
        let local = Path::new("dora/target").join(profile).join("dora");
        if local.exists() {
            return Some(local);
        }
    }
    Command::new("dora")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| PathBuf::from("dora"))
}

fn check_bin(name: &str) -> PathBuf {
    let path = PathBuf::from("target/debug").join(name);
    if !path.exists() {
        let release = PathBuf::from("target/release").join(name);
        if release.exists() {
            return release;
        }
        eprintln!(
            "ERROR: binary '{}' not found at {} or {}\n\
             Build: cargo build --bin {}",
            name,
            path.display(),
            release.display(),
            name,
        );
        std::process::exit(1);
    }
    path
}

fn section(title: &str) {
    println!("\n═══ {} ═══\n", title);
}

fn step(msg: &str) {
    println!("▸ {}", msg);
}

fn ok(msg: &str) {
    println!("  ✅ {}", msg);
}

fn fail(msg: &str) -> ! {
    eprintln!("  ❌ {}", msg);
    std::process::exit(1);
}

// ── Main ───────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    // ── Preflight ──────────────────────────────────────
    section("dora-test-utils — Record/Replay Demo");
    println!("  Uses DORA's rust-dataflow example (unmodified)");
    println!("  rust-node → rust-status-node → test-sink (record)");
    println!();

    let _dora = match resolve_dora(&args) {
        Some(d) => {
            step(&format!("dora CLI: {}", d.display()));
            d
        }
        None => {
            eprintln!(
                "SKIP: dora CLI not found.\n\
                 Build: PYO3_NO_PYTHON=1 cargo build --bin dora \\\n    \
                 --manifest-path dora/binaries/cli/Cargo.toml\n\
                 Or: --dora <path>"
            );
            return Ok(());
        }
    };

    let _sink_bin = check_bin("test-sink");
    ok("binaries found");

    // ── Setup ──────────────────────────────────────────
    let tmp = tempfile::TempDir::new()?;
    #[allow(deprecated)]
    let tmp_path = tmp.into_path();
    let sink_output = tmp_path.join("sink_output.json");
    let baseline_path = tmp_path.join("baseline.json");

    let yaml_path = args.dataflow.unwrap_or_else(|| PathBuf::from("demo/rust-dataflow.yml"));
    let mutated_yaml = PathBuf::from("demo/rust-dataflow-mutated.yml");

    // ── Step 1: Record baseline ────────────────────────
    section("Step 1 — Record baseline");

    step(&format!("Dataflow: {}", yaml_path.display()));
    step("Running dora run --stop-after 10s ...");

    let recording = RecordSession::attach(&yaml_path)?
        .record_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    recording.save(&baseline_path)?;
    ok(&format!("baseline saved → {}", baseline_path.display()));
    println!();
    println!("  ── Recording metadata ──");
    println!("  dataflow:       {}", recording.metadata.dataflow_yaml);
    println!("  recorded at:    {}", recording.metadata.recorded_at_unix);
    println!("  timeout:        {}s", recording.metadata.timeout_secs);
    println!("  dora version:   {}", recording.metadata.dora_version);
    println!(
        "  sinks recorded: {:?}",
        recording.sinks.keys().collect::<Vec<_>>()
    );

    // Show sample of recorded data
    if let Some(sink_data) = recording.sinks.get("test-sink") {
        println!();
        println!("  ── Recorded sink data (sample) ──");
        if let Some(items) = sink_data.as_array() {
            for item in items.iter().take(3) {
                let pretty = serde_json::to_string_pretty(item)?;
                for line in pretty.lines() {
                    println!("  {}", line);
                }
            }
            if items.len() > 3 {
                println!("  ... ({} entries total)", items.len());
            }
        }
    }

    // ── Step 2: Clean replay ───────────────────────────
    section("Step 2 — Replay (same YAML, no regression)");

    step("Replaying with identical dataflow...");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    ok(&format!("is_clean() = {}", result.is_clean()));
    result.assert_no_regression();
    ok("assert_no_regression() — no panic");

    // ── Step 3: Mutated YAML ───────────────────────────
    section("Step 3 — Switch to mutated dataflow (timer 100ms→50ms)");

    step(&format!("Dataflow: {}", mutated_yaml.display()));
    step("rust-status-node now ticks every 50ms instead of 100ms");
    step("→ produces more outputs in the same time window");
    ok("mutated YAML ready");

    // ── Step 4: Regression detected ────────────────────
    section("Step 4 — Replay (regression detected)");

    step("Replaying with mutated dataflow...");
    let _result = RecordSession::attach(&mutated_yaml)?
        .record_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    let clean = result.is_clean();
    if clean {
        fail("BUG: is_clean() = true — regression was NOT detected!");
    }
    ok(&format!("is_clean() = {}  ← regression detected", clean));

    // Print the structured diff
    let diff = result.diff();
    println!();
    println!("{}", diff);

    // assert_no_regression should panic here
    step("Verifying assert_no_regression() panics on regression...");
    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        result.assert_no_regression();
    }));
    match panic_result {
        Err(_) => ok("assert_no_regression() panicked — correct"),
        Ok(()) => fail("BUG: assert_no_regression() did NOT panic on regression!"),
    }

    // ── Summary ────────────────────────────────────────
    section("Summary");
    println!("  ✅ Record baseline    — {} bytes", baseline_path.metadata()?.len());
    println!("  ✅ Replay (clean)     — is_clean() = true");
    println!("  ✅ Replay (regression)— is_clean() = false, DiffReport generated");
    println!("  ✅ assert_no_regression() — panics on regression");
    println!();
    println!("  Working directory: {}", tmp_path.display());
    println!("  How this helps the DORA community:");
    println!("    • rust-node and rust-status-node are UNMODIFIED dora examples");
    println!("    • Only added: test-sink in record mode (1 YAML entry)");
    println!("    • Any existing dora dataflow gets regression testing by adding");
    println!("      1 sink node — no code changes to your existing nodes");

    Ok(())
}
