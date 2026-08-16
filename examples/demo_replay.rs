//! dora-test-utils Demo — Record/Replay regression testing (Layer 3).
//!
//! GEN72 joint-space motion control scenario: a trajectory node linearly
//! interpolates the 7 joints toward two target configurations.  The demo
//! records the resulting trajectory, replays it unchanged (clean), then
//! replays a MUTATED dataflow — the interpolation resolution changed from
//! 10 to 5 steps (a real motion-control regression: someone edited the
//! trajectory parameter) — and the regression is detected.
//!
//! Runs the static dataflow files in `demo/` — the same way a real user
//! would point the tool at their own YAML:
//!   - `demo/trajectory-baseline.yml` — interpolation --steps 10
//!   - `demo/trajectory-mutated.yml`  — interpolation --steps 5
//!
//! The trajectory output is pure computation, so it is fully
//! deterministic and needs no filtering.  For real pipelines with
//! non-deterministic noise (timestamps, tick counts, debug logs),
//! `ReplaySession::ignore_paths` / `ignore_sink` skip those fields —
//! see README and tests/e2e_replay.rs.
//!
//! Run from the repo root (demo-final.sh does this automatically).

use dora_test_utils::record::{RecordSession, ReplaySession};
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

fn section(title: &str) {
    println!("\n═══ {} ═══\n", title);
}
fn step(msg: &str) {
    println!("▸ {msg}");
}
fn ok(msg: &str) {
    println!("  ✅ {msg}");
}
fn fail(msg: &str) -> ! {
    eprintln!("  ❌ {msg}");
    std::process::exit(1);
}

/// Print the diff report, truncated for readability in the demo.
fn print_diff_trimmed(diff: &dora_test_utils::DiffReport, max_lines: usize) {
    let text = diff.to_string();
    let lines: Vec<&str> = text.lines().collect();
    let shown = lines.len().min(max_lines);
    for line in &lines[..shown] {
        println!("    {line}");
    }
    if lines.len() > max_lines {
        println!("    … ({} more lines)", lines.len() - max_lines);
    }
}

// ── Main ───────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    section("dora-test-utils — Record/Replay Demo (Layer 3: regression testing)");
    println!("  GEN72 joint-space motion control — trajectory interpolation");
    println!("  Static dataflow files under demo/ — no YAML generation");
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

    // Static dataflow files. dora resolves their relative paths against
    // the YAML file's own directory, so no generation is needed.
    let baseline_yaml = PathBuf::from("demo/trajectory-baseline.yml");
    let mutated_yaml = PathBuf::from("demo/trajectory-mutated.yml");

    // Output path must match the test-sink args in the static YAMLs.
    let sink_output = PathBuf::from("demo/sink_trajectory.json");

    // ── Step 1: Record baseline ────────────────────────
    section("Step 1 — Record baseline (trajectory with --steps 10)");

    step(&format!("Dataflow: {}", baseline_yaml.display()));
    step("2 targets × 10 interpolation steps × 7 joints = 140 trajectory values");
    step("Running dora run --stop-after 10s ...");

    let recording = RecordSession::attach(&baseline_yaml)?
        .record_sink("test-sink", &sink_output)
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
    if let Some(traj) = recording.sinks.get("test-sink") {
        if let Some(count) = traj.get("count") {
            println!("  trajectory values: {count} (expected 140)");
        }
        if let Some(arr) = traj.get("data").and_then(|d| d.as_array()) {
            let head: Vec<String> = arr.iter().take(7).map(|v| v.to_string()).collect();
            println!("  first step (J1..J7): [{}]", head.join(", "));
        }
    }

    // ── Step 2: Clean replay ───────────────────────────
    section("Step 2 — Replay (same YAML, no regression)");

    step("The trajectory output is pure computation — fully deterministic,");
    step("so no filtering is needed. Real pipelines with timing noise use");
    step("ignore_paths / ignore_sink to skip non-deterministic fields.");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    ok(&format!("is_clean() = {}", result.is_clean()));
    result.assert_no_regression();
    ok("assert_no_regression() — no panic");

    // ── Step 3: Mutated dataflow ───────────────────────
    section("Step 3 — Switch to mutated dataflow (interpolation --steps 10 → 5)");

    step(&format!("Dataflow: {}", mutated_yaml.display()));
    step("The motion controller now interpolates with HALF the resolution —");
    step("a real regression: someone changed the trajectory parameter.");
    step("→ 2 × 5 × 7 = 70 trajectory values instead of 140, and every");
    step("   shared interpolation point differs.");
    ok("mutated dataflow ready");

    // ── Step 4: Regression detected ────────────────────
    section("Step 4 — Replay (regression detected)");

    step("Replaying with mutated dataflow...");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .dataflow(&mutated_yaml)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    let clean = result.is_clean();
    if clean {
        fail("BUG: is_clean() = true — regression was NOT detected!");
    }
    ok(&format!("is_clean() = {}  ← regression detected", clean));

    // Print the structured diff (trimmed for readability)
    step("DiffReport:");
    print_diff_trimmed(result.diff(), 14);

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
    println!("  ✅ Record baseline         — 140 trajectory values (steps 10)");
    println!("  ✅ Replay (clean)          — is_clean() = true (deterministic output)");
    println!("  ✅ Replay (regression)     — is_clean() = false, 140 → 70 values");
    println!("  ✅ assert_no_regression()  — panics on regression");
    println!();
    println!("  How this helps the DORA community:");
    println!("    • GEN72 motion-control scenario — a realistic regression test");
    println!("    • Static YAML files — exactly how you'd use the tool on your own dataflow");
    println!("    • The mutation is a parameter change in the dataflow, not in the tool");
    println!("    • Non-deterministic noise? ignore_paths / ignore_sink handle it");

    Ok(())
}
