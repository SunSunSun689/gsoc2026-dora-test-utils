//! dora-test-utils Demo — Record/Replay regression testing.
//!
//! Records a deterministic echo pipeline (test-source → echo-node → test-sink)
//! in record mode, then replays to detect regressions when source data changes.
//!
//! ## Usage
//!
//! ```bash
//! # Build our binaries + echo-node
//! cargo build --bin test-source --bin test-sink --bin echo-node --example demo_replay
//!
//! # Run the demo
//! cargo run --example demo_replay
//! cargo run --example demo_replay -- --dora ./dora/target/debug/dora
//! ```

use dora_test_utils::record::{RecordSession, ReplaySession};
use std::io::Write;
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

fn bin_path(name: &str) -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    cwd.join("target").join(profile).join(name)
}

fn check_bin(name: &str) -> PathBuf {
    let path = bin_path(name);
    if path.exists() {
        return path;
    }
    eprintln!(
        "ERROR: binary '{}' not found at {}\n\
         Build: cargo build --bin {}",
        name,
        path.display(),
        name,
    );
    std::process::exit(1);
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

/// Generate a temp YAML for an echo pipeline with absolute paths.
///
/// This is necessary because dora spawns node processes from a different
/// working directory, so all file paths in the YAML must be absolute.
fn generate_yaml(
    tmp: &Path,
    name: &str,
    source_file: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let source_bin = check_bin("test-source");
    let echo_bin = check_bin("echo-node");
    let sink_bin = check_bin("test-sink");
    let output_file = tmp.join("sink_output.json");

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {source_bin}
    args: "--output-id data --data-file {source_file}"
    outputs:
      - data

  - id: echo-node
    path: {echo_bin}
    inputs:
      data: test-source/data
    outputs:
      - data

  - id: test-sink
    path: {sink_bin}
    inputs:
      data: echo-node/data
    args: "--output-file {output_file} --record-mode"
"#,
        source_bin = source_bin.display(),
        source_file = source_file.display(),
        echo_bin = echo_bin.display(),
        sink_bin = sink_bin.display(),
        output_file = output_file.display(),
    );

    let yaml_path = tmp.join(name);
    let mut f = std::fs::File::create(&yaml_path)?;
    f.write_all(yaml.as_bytes())?;
    Ok(yaml_path)
}

// ── Main ───────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    // ── Preflight ──────────────────────────────────────
    section("dora-test-utils — Record/Replay Demo");
    println!("  Deterministic echo pipeline (test-source → echo-node → test-sink)");
    println!("  Regression trigger: extra data element in mutated source");
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

    // Pre-flight: verify required binaries exist
    let _ = check_bin("test-source");
    let _ = check_bin("echo-node");
    let _ = check_bin("test-sink");
    ok("binaries found");

    // ── Setup ──────────────────────────────────────────
    let tmp = tempfile::TempDir::new()?;
    #[allow(deprecated)]
    let tmp_path = tmp.into_path();
    let baseline_path = tmp_path.join("baseline.json");

    // Resolve absolute paths for source data files.
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let baseline_data = cwd.join("demo/demo-source-baseline.json");
    let mutated_data = cwd.join("demo/demo-source-mutated.json");

    // Generate YAML files with absolute paths (dora spawns nodes from
    // a different working directory, so relative paths don't work).
    let yaml_path = if let Some(ref df) = args.dataflow {
        df.clone()
    } else {
        generate_yaml(&tmp_path, "baseline.yml", &baseline_data)?
    };
    let mutated_yaml = generate_yaml(&tmp_path, "mutated.yml", &mutated_data)?;

    let sink_output = tmp_path.join("sink_output.json");

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
    section("Step 3 — Switch to mutated dataflow (extra data element)");

    step(&format!("Dataflow: {}", mutated_yaml.display()));
    step("test-source reads demo-source-mutated.json (4 elements instead of 3)");
    step("→ produces one extra output in the same pipeline");
    ok("mutated YAML ready");

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
    println!("    • test-source, echo-node, and test-sink are reusable test harness nodes");
    println!("    • Record once, replay anytime — catch regressions automatically");
    println!("    • Any deterministic dora dataflow can benefit from the same pattern");
    println!("    • Only added: 1 test-sink node (1 YAML entry) for regression coverage");

    Ok(())
}
