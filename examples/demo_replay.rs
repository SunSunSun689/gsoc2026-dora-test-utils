//! Week 12 Demo — Record/Replay regression testing showcase.
//!
//! Demonstrates the full RecordSession → ReplaySession workflow:
//!   1. Record a baseline from an echo pipeline
//!   2. Replay → assert_no_regression() (clean)
//!   3. Mutate the source data
//!   4. Replay → detect regression with structured diff

use dora_test_utils::record::{RecordSession, ReplaySession};
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

fn bin_path(name: &str) -> PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    PathBuf::from(&target_dir).join(profile).join(name)
}

fn dora_binary() -> PathBuf {
    for profile in &["debug", "release"] {
        let local = PathBuf::from("dora/target").join(profile).join("dora");
        if local.exists() {
            return local;
        }
    }
    PathBuf::from("dora")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("══════════════════════════════════════════════");
    println!("  dora-test-utils — Week 12 Demo");
    println!("  Record/Replay Regression Testing");
    println!("══════════════════════════════════════════════\n");

    // ── 0. Check dora CLI ─────────────────────────────
    let dora = dora_binary();
    let has_dora = Command::new(&dora)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !has_dora {
        eprintln!("SKIP: dora CLI not available");
        eprintln!("Build with: PYO3_NO_PYTHON=1 cargo build --bin dora --manifest-path dora/binaries/cli/Cargo.toml");
        return Ok(());
    }
    println!("▶ dora CLI: {}\n", dora.display());

    // ── 1. Setup ─────────────────────────────────────
    let tmp = tempfile::TempDir::new()?;
    let source_file = tmp.path().join("source.json");
    let sink_output = tmp.path().join("sink_output.json");

    // Write test data: [1, 2, 3]
    let baseline_data = serde_json::json!({
        "data": [1, 2, 3],
        "data_type": "Int32"
    });
    std::fs::write(&source_file, serde_json::to_string_pretty(&baseline_data)?)?;

    // Generate echo dataflow YAML
    let source_bin = bin_path("test-source");
    let echo_bin = bin_path("echo-node");
    let sink_bin = bin_path("test-sink");

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
    let yaml_path = tmp.path().join("echo.yml");
    std::fs::write(&yaml_path, &yaml)?;

    // ── 2. Record baseline ───────────────────────────
    println!("═══ Step 1: Record baseline ═══");
    println!("  Data:     {}", serde_json::to_string(&baseline_data)?);
    println!("  Dataflow: {}", yaml_path.display());
    println!("  → Running dora run --stop-after 10s ...\n");

    let recording = RecordSession::attach(&yaml_path)?
        .record_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    let baseline_path = tmp.path().join("baseline.json");
    recording.save(&baseline_path)?;
    println!("  ✅ Baseline saved: {}\n", baseline_path.display());
    println!("  Metadata:");
    println!("    dataflow: {}", recording.metadata.dataflow_yaml);
    println!("    timeout:  {}s", recording.metadata.timeout_secs);
    println!("    dora:     {}", recording.metadata.dora_version);
    println!(
        "    sinks:    {:?}\n",
        recording.sinks.keys().collect::<Vec<_>>()
    );

    // ── 3. Replay — clean (no regression) ────────────
    println!("═══ Step 2: Replay — verify no regression ═══");
    println!("  → Replaying against baseline...\n");

    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    result.assert_no_regression();
    println!("  ✅ is_clean() = true");
    println!("  No regressions detected.\n");

    // ── 4. Mutate source data ────────────────────────
    println!("═══ Step 3: Mutate source data ─════");
    let mutated_data = serde_json::json!({
        "data": [1, 2, 99],  // ← was [1, 2, 3]
        "data_type": "Int32"
    });
    std::fs::write(&source_file, serde_json::to_string_pretty(&mutated_data)?)?;
    println!("  Changed: [1, 2, 3] → [1, 2, 99]\n");

    // ── 5. Replay — regression detected ───────────────
    println!("═══ Step 4: Replay — regression detected ═══");
    println!("  → Replaying with mutated data...\n");

    let result = ReplaySession::load(&baseline_path)?
        .replay_sink("test-sink", &sink_output)
        .with_timeout(Duration::from_secs(10))
        .run()?;

    println!("  is_clean() = {}", result.is_clean());
    println!();
    println!("  Diff report:");
    println!("{}", result.diff());

    if !result.is_clean() {
        println!("  ✅ Regression correctly detected!");
    } else {
        eprintln!(
            "  ❌ FAIL: Regression NOT detected — mutated data should have caused a Mismatch!"
        );
        std::process::exit(1);
    }

    println!("\n══════════════════════════════════════════════");
    println!("  Demo complete — all paths verified.");
    println!("══════════════════════════════════════════════");
    Ok(())
}
