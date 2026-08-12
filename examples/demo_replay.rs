//! dora-test-utils Demo — Record/Replay regression testing with DORA's
//! official rust-dataflow example.
//!
//! Records rust-node output (deterministic UInt64, seed=42) and status-node
//! output (non-deterministic String) from a real DORA pipeline, then replays
//! to detect regressions.
//!
//! Demonstrates:
//!   - Non-invasive: DORA example nodes are NOT modified
//!   - ignore_paths: skips .count field (deterministic bookkeeping)
//!   - ignore_sink: skips status-node output (non-deterministic)
//!   - Regression detection: mutated tick rate → array length mismatch

use dora_test_utils::record::{RecordSession, ReplaySession};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

// ── CLI ────────────────────────────────────────────────────

#[derive(Default)]
struct Args {
    dora: Option<PathBuf>,
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
            other => {
                eprintln!("Unknown flag: {other}");
                eprintln!("Usage: demo_replay [--dora <path>]");
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

fn bin_path(name: &str) -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("target")
        .join(profile)
        .join(name)
}

fn dora_bin_path(name: &str) -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("dora/target")
        .join(profile)
        .join(name)
}

fn check_bin(label: &str, path: &Path) {
    if !path.exists() {
        eprintln!(
            "ERROR: {} not found at {}\n  Build: cargo build --bin {}",
            label,
            path.display(),
            label,
        );
        std::process::exit(1);
    }
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

/// Generate a temp YAML for the rust-dataflow pipeline with absolute paths.
fn generate_rust_dataflow_yaml(
    tmp: &Path,
    name: &str,
    rust_node_tick_ms: u64,
    rust_status_node_tick_ms: u64,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let rust_node_bin = dora_bin_path("rust-dataflow-example-node");
    let status_node_bin = dora_bin_path("rust-dataflow-example-status-node");
    let test_sink_bin = bin_path("test-sink");
    let random_output = tmp.join("sink_random_output.json");
    let status_output = tmp.join("sink_status_output.json");

    check_bin("rust-dataflow-example-node", &rust_node_bin);
    check_bin("rust-dataflow-example-status-node", &status_node_bin);
    check_bin("test-sink", &test_sink_bin);

    let yaml = format!(
        r#"nodes:
  - id: rust-node
    path: {rust_node_bin}
    inputs:
      tick: dora/timer/millis/{rust_node_tick_ms}
    outputs:
      - random

  - id: rust-status-node
    path: {status_node_bin}
    inputs:
      tick: dora/timer/millis/{rust_status_node_tick_ms}
      random: rust-node/random
    outputs:
      - status

  - id: test-sink-random
    path: {test_sink_bin}
    inputs:
      random: rust-node/random
    args: "--output-file {random_output} --record-mode"

  - id: test-sink-status
    path: {test_sink_bin}
    inputs:
      status: rust-status-node/status
    args: "--output-file {status_output} --record-mode"
"#,
        rust_node_bin = rust_node_bin.display(),
        status_node_bin = status_node_bin.display(),
        test_sink_bin = test_sink_bin.display(),
        random_output = random_output.display(),
        status_output = status_output.display(),
        rust_node_tick_ms = rust_node_tick_ms,
        rust_status_node_tick_ms = rust_status_node_tick_ms,
    );

    let yaml_path = tmp.join(name);
    let mut f = std::fs::File::create(&yaml_path)?;
    f.write_all(yaml.as_bytes())?;
    Ok(yaml_path)
}

// ── Main ───────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    section("dora-test-utils — Record/Replay Demo");
    println!("  DORA rust-dataflow example (unmodified upstream nodes)");
    println!("  Demonstrates ignore_paths + ignore_sink filtering");
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

    ok("prerequisites met");

    // ── Setup ──────────────────────────────────────────
    let tmp = tempfile::TempDir::new()?;
    #[allow(deprecated)]
    let tmp_path = tmp.into_path();
    let baseline_path = tmp_path.join("baseline.json");

    let baseline_yaml = generate_rust_dataflow_yaml(&tmp_path, "baseline.yml", 10, 100)?;
    let mutated_yaml = generate_rust_dataflow_yaml(&tmp_path, "mutated.yml", 200, 100)?;

    let random_output = tmp_path.join("sink_random_output.json");
    let status_output = tmp_path.join("sink_status_output.json");

    // ── Step 1: Record baseline ────────────────────────
    section("Step 1 — Record baseline");

    step(&format!("Dataflow: {}", baseline_yaml.display()));
    step("Running dora run --stop-after 10s ...");
    step("Recording: test-sink-random (rust-node UInt64) + test-sink-status (String)");

    let recording = RecordSession::attach(&baseline_yaml)?
        .record_sink("test-sink-random", &random_output)
        .record_sink("test-sink-status", &status_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    recording.save(&baseline_path)?;
    ok(&format!("baseline saved → {}", baseline_path.display()));

    // Show recorded data sample
    println!();
    println!("  ── Recording metadata ──");
    println!("  dora version:   {}", recording.metadata.dora_version);
    println!(
        "  sinks recorded: {:?}",
        recording.sinks.keys().collect::<Vec<_>>()
    );
    if let Some(random_data) = recording.sinks.get("test-sink-random") {
        if let Some(count) = random_data.get("count") {
            println!("  random events:  {count} (exactly 100 — upstream rust-node caps its loop at 100 events)");
        }
    }
    if let Some(status_data) = recording.sinks.get("test-sink-status") {
        if let Some(arr) = status_data.get("data").and_then(|d| d.as_array()) {
            if let Some(first) = arr.first().and_then(|v| v.as_str()) {
                println!("  status sample:  {first}");
            }
        }
    }

    // ── Step 2: Clean replay ───────────────────────────
    section("Step 2 — Replay (same YAML, no regression)");

    step("Using ignore_paths(&[\"count\"]) — count is deterministic (exactly 100 in both runs)");
    step("ignore_paths is still exercised; regressions are caught via data.length, not masked");
    step("Using ignore_sink(\"test-sink-status\") to skip non-deterministic status output");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink-random", &random_output)
        .replay_sink("test-sink-status", &status_output)
        .ignore_paths(&["count"])
        .ignore_sink("test-sink-status")
        .with_timeout(Duration::from_secs(10))
        .run()?;

    ok(&format!("is_clean() = {}", result.is_clean()));
    result.assert_no_regression();
    ok("assert_no_regression() — no panic");

    // ── Step 3: Mutated dataflow ───────────────────────
    section("Step 3 — Switch to mutated dataflow (rust-node tick: 10ms → 200ms)");

    step("At 200ms tick, only ~50 ticks arrive in the 10s window — under the 100-event loop cap");
    step("→ ~50 random events instead of exactly 100 (upstream node exits after 100 events)");
    step("Same ignore_paths + ignore_sink filters applied");
    ok("mutated dataflow ready");

    // ── Step 4: Regression detected ────────────────────
    section("Step 4 — Replay (regression detected)");

    step("Replaying with mutated dataflow...");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink-random", &random_output)
        .replay_sink("test-sink-status", &status_output)
        .dataflow(&mutated_yaml)
        .ignore_paths(&["count"])
        .ignore_sink("test-sink-status")
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
    println!("{diff}");

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
    println!("  ✅ Record baseline         — 2 sinks (random UInt64 + status String)");
    println!("  ✅ Replay (clean)          — is_clean() = true (count ignored, status skipped)");
    println!("  ✅ Replay (regression)     — is_clean() = false, array length mismatch detected");
    println!("  ✅ assert_no_regression()  — panics on regression");
    println!();
    println!("  How this helps the DORA community:");
    println!("    • DORA rust-dataflow example nodes are UNMODIFIED");
    println!("    • Only added: 2 test-sink nodes (2 YAML entries) for regression coverage");
    println!("    • ignore_paths filters deterministic bookkeeping (event count) without masking data diffs");
    println!("    • ignore_sink handles non-deterministic output (debug strings)");
    println!("    • Any deterministic DORA dataflow can benefit from the same pattern");
    println!("    • Filtering makes Record/Replay practical for real pipelines");

    Ok(())
}
