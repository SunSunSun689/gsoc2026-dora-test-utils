//! dora-test-utils Demo — Record/Replay regression testing showcase.
//!
//! Demonstrates the full RecordSession → ReplaySession workflow:
//!   1. Record a baseline from an echo pipeline (source → echo → sink)
//!   2. Replay → verify `is_clean() = true`
//!   3. Mutate the source data (simulate a regression)
//!   4. Replay → structured DiffReport with per-sink field-level diffs
//!
//! ## Usage
//!
//! ```bash
//! cargo build --example demo_replay --bin test-source --bin test-sink --bin echo-node
//! cargo run  --example demo_replay
//! cargo run  --example demo_replay -- --dora ./dora/target/debug/dora
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

/// Locate a binary compiled by cargo.  Prefers the `CARGO_BIN_EXE_<name>`
/// env var (set by `cargo run --example` for sibling bins), falls back to
/// `target/<profile>/<name>`.
fn find_bin(name: &str) -> PathBuf {
    let env_key = format!("CARGO_BIN_EXE_{}", name.to_uppercase().replace('-', "_"));
    if let Ok(p) = std::env::var(&env_key) {
        let path = PathBuf::from(p);
        if path.exists() {
            return path;
        }
    }
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    Path::new(&target).join(profile).join(name)
}

/// Resolve the dora CLI binary.  Order: `--dora` flag → local dora build →
/// `$PATH`.
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
    // Fall back to PATH
    Command::new("dora")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| PathBuf::from("dora"))
}

/// Check that every required binary exists; print a build hint if not.
fn check_bin(name: &str) -> PathBuf {
    let path = find_bin(name);
    if !path.exists() {
        eprintln!(
            "ERROR: binary '{}' not found at {}\n\
             Build it first:\n  cargo build --bin {}",
            name,
            path.display(),
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
    println!("  {}", env!("CARGO_PKG_NAME"));
    println!("  version: {}", env!("CARGO_PKG_VERSION"));
    println!();

    let _dora = match resolve_dora(&args) {
        Some(d) => {
            step(&format!("dora CLI: {}", d.display()));
            d
        }
        None => {
            eprintln!(
                "SKIP: dora CLI not found.\n\
                 Build it with:\n  PYO3_NO_PYTHON=1 cargo build --bin dora \\\n    \
                 --manifest-path dora/binaries/cli/Cargo.toml\n\
                 Or pass --dora <path>"
            );
            return Ok(());
        }
    };

    let source_bin = check_bin("test-source");
    let echo_bin = check_bin("echo-node");
    let sink_bin = check_bin("test-sink");
    ok("all binaries found");

    // ── Setup ──────────────────────────────────────────
    let tmp = tempfile::TempDir::new()?;
    #[allow(deprecated)]
    let tmp_path = tmp.into_path(); // keep dir for manual inspection
    let source_file = tmp_path.join("source.json");
    let sink_output = tmp_path.join("sink_output.json");
    let baseline_path = tmp_path.join("baseline.json");

    let yaml_path = match args.dataflow {
        Some(ref p) => p.clone(),
        None => {
            let yaml = format!(
                r#"nodes:
  - id: test-source
    path: {}
    args: "--output-id data --data-file {}"
    outputs:
      - data
  - id: echo-node
    path: {}
    inputs:
      data: test-source/data
    outputs:
      - data
  - id: test-sink
    path: {}
    inputs:
      data: echo-node/data
    args: "--output-file {} --record-mode"
"#,
                source_bin.display(),
                source_file.display(),
                echo_bin.display(),
                sink_bin.display(),
                sink_output.display(),
            );
            let p = tmp_path.join("echo.yml");
            std::fs::write(&p, &yaml)?;
            p
        }
    };

    // ── Step 1: Record baseline ────────────────────────
    section("Step 1 — Record baseline");

    let baseline_data = serde_json::json!({
        "data": [1, 2, 3, 4, 5],
        "data_type": "Int32"
    });
    step(&format!(
        "Writing source data: {}",
        serde_json::to_string(&baseline_data)?
    ));
    std::fs::write(&source_file, serde_json::to_string_pretty(&baseline_data)?)?;
    ok(&format!("source → {}", source_file.display()));

    step(&format!("Dataflow: {}", yaml_path.display()));
    step("dora run --stop-after 10s ...");

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

    // Show the recorded sink data
    if let Some(sink_data) = recording.sinks.get("test-sink") {
        println!();
        println!("  ── Recorded sink data ──");
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
    section("Step 2 — Replay (clean — no regression)");

    step("Replaying against baseline with identical source data...");
    // Re-write the same source data (Step 3 modifies it)
    std::fs::write(&source_file, serde_json::to_string_pretty(&baseline_data)?)?;

    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    ok(&format!("is_clean() = {}", result.is_clean()));
    result.assert_no_regression();
    ok("assert_no_regression() — no panic");

    // ── Step 3: Mutate source ──────────────────────────
    section("Step 3 — Mutate source data (simulate regression)");

    let mutated = serde_json::json!({
        "data": [1, 2, 99, 4, 5],    // ← third element changed: 3 → 99
        "data_type": "Int32"
    });
    step("Changed: [1, 2, 3, 4, 5] → [1, 2, 99, 4, 5]");
    std::fs::write(&source_file, serde_json::to_string_pretty(&mutated)?)?;
    ok("mutated source written");

    // ── Step 4: Regression detected ────────────────────
    section("Step 4 — Replay (regression detected)");

    step("Replaying with mutated source data...");
    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    let clean = result.is_clean();
    if clean {
        fail("BUG: is_clean() = true — regression was NOT detected!");
    }
    ok(&format!(
        "is_clean() = {}  ← correctly detected regression",
        clean
    ));

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
        Err(_) => ok("assert_no_regression() panicked — correct behavior"),
        Ok(()) => fail("BUG: assert_no_regression() did NOT panic on regression!"),
    }

    // ── Summary ────────────────────────────────────────
    section("Summary");
    println!(
        "  ✅ Record baseline        — {} bytes",
        baseline_path.metadata()?.len()
    );
    println!("  ✅ Replay (clean)         — is_clean() = true");
    println!("  ✅ Replay (regression)    — is_clean() = false, DiffReport generated");
    println!("  ✅ assert_no_regression() — panics on regression, no-op on clean");
    println!();
    println!("  Working directory: {}", tmp_path.display());
    println!("  (kept for inspection — delete manually when done)");

    Ok(())
}
