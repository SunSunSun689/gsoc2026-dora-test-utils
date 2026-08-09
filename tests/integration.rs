//! Integration tests for the test-source → echo-node → test-sink pipeline.
//!
//! Each test generates a temporary YAML dataflow, runs it via `dora run`,
//! and asserts on the `SinkResult` written by test-sink.
//!
//! ## Prerequisites
//!
//! - `dora` CLI must be installed and on `$PATH`
//! - Port 6013 must be free
//! - Run with `--test-threads=1` (the `#[serial]` attribute enforces this)

use dora_test_utils::sink::SinkResult;
use serial_test::serial;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// Check whether `dora` CLI is available.
fn dora_available() -> bool {
    let dora = dora_binary();
    Command::new(&dora)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Require `dora` CLI on `PATH` before running an integration test.
///
/// In CI (`CI=true`), panics if unavailable — prevents silent pass.
/// Locally, prints a warning and returns `false` so the caller can skip.
fn require_dora() -> bool {
    if dora_available() {
        return true;
    }
    let in_ci = std::env::var("CI").is_ok();
    if in_ci {
        panic!(
            "dora CLI not found on PATH — required by integration tests in CI.\n\
             The CI workflow should install dora before running these tests."
        );
    }
    eprintln!(
        "⚠️  SKIP: dora CLI not found on PATH — integration tests will be skipped.\n\
         Install dora or run `cargo test --lib` for unit tests only."
    );
    false
}

/// Locate a compiled binary under `target/<profile>/<name>`.
fn bin_path(name: &str) -> PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    Path::new(&target_dir).join(profile).join(name)
}

/// Ensure all required binaries are built (cached — only builds once per process).
fn build_binaries() {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let mut args = vec![
            "build",
            "--bin",
            "test-source",
            "--bin",
            "test-sink",
            "--bin",
            "echo-node",
            "--bin",
            "classifier-node",
        ];
        if !cfg!(debug_assertions) {
            args.push("--release");
        }
        let status = Command::new("cargo")
            .args(&args)
            .status()
            .expect("cargo build should succeed");
        assert!(status.success(), "cargo build failed");
    });
}

/// Directory containing compiled test binaries.
fn bin_dir() -> PathBuf {
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target"));
    if cfg!(debug_assertions) {
        target.join("debug")
    } else {
        target.join("release")
    }
}

/// Find the dora CLI binary.
fn dora_binary() -> PathBuf {
    // Check the local dora workspace build first — try both
    // debug and release profiles (debug is more common in dev).
    for profile in &["debug", "release"] {
        let local = Path::new("dora/target").join(profile).join("dora");
        if local.exists() {
            return local;
        }
    }
    // Fall back to PATH.
    PathBuf::from("dora")
}

/// Run a test-source → echo-node → test-sink pipeline and return the result.
///
/// Generates a temporary YAML dataflow with the given source and expected
/// data, runs it via `dora run --stop-after`, and parses the `SinkResult`
/// from the output file.
fn run_echo_pipeline(
    source_data: &serde_json::Value,
    expected_output: &serde_json::Value,
    source_extra_args: &[&str],
    sink_extra_args: &[&str],
) -> eyre::Result<SinkResult> {
    let tmp = tempfile::TempDir::new()?;

    // ── Write data files ──────────────────────────────────────
    let source_file = tmp.path().join("source.json");
    let expected_file = tmp.path().join("expected.json");
    let output_file = tmp.path().join("result.json");

    std::fs::write(&source_file, serde_json::to_string_pretty(source_data)?)?;
    std::fs::write(
        &expected_file,
        serde_json::to_string_pretty(expected_output)?,
    )?;

    // ── Generate YAML dataflow with absolute paths ────────────
    let source_bin = bin_path("test-source");
    let echo_bin = bin_path("echo-node");
    let sink_bin = bin_path("test-sink");

    let source_extra = if source_extra_args.is_empty() {
        String::new()
    } else {
        format!(" {}", source_extra_args.join(" "))
    };

    let sink_extra = if sink_extra_args.is_empty() {
        String::new()
    } else {
        format!(" {}", sink_extra_args.join(" "))
    };

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {source_bin}
    args: "--output-id data --data-file {source_file}{source_extra}"
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
    args: "--expected-file {expected_file} --output-file {output_file}{sink_extra}"
"#,
        source_bin = source_bin.display(),
        echo_bin = echo_bin.display(),
        sink_bin = sink_bin.display(),
        source_file = source_file.display(),
        expected_file = expected_file.display(),
        output_file = output_file.display(),
        source_extra = source_extra,
        sink_extra = sink_extra,
    );

    let yaml_file = tmp.path().join("dataflow.yml");
    std::fs::write(&yaml_file, yaml)?;

    // ── Run dora run ──────────────────────────────────────────
    let dora = dora_binary();
    let output = Command::new(&dora)
        .args([
            "run",
            yaml_file
                .to_str()
                .expect("temp dir path must be valid UTF-8"),
            "--stop-after",
            "10s",
        ])
        .output()
        .map_err(|e| eyre::eyre!("failed to run dora ({}): {e}", dora.display()))?;

    // ── Try to parse result.json even on dora run failure ─────
    // test-sink writes result.json before exiting, so structured
    // Difference data is available even when fail_on_mismatch
    // causes a non-zero exit.
    let result_json = std::fs::read_to_string(&output_file);
    let sink_result: Option<SinkResult> =
        result_json.ok().and_then(|s| serde_json::from_str(&s).ok());

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Augment the error with SinkResult details when available.
        if let Some(ref sr) = sink_result {
            eyre::bail!(
                "dora run failed with status {}\n\
                 SinkResult: {sr:#?}\n\
                 --- dora stdout ---\n{stdout}\n--- dora stderr ---\n{stderr}",
                output.status
            );
        }
        eyre::bail!(
            "dora run failed with status {}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
            output.status
        );
    }

    let result =
        sink_result.ok_or_else(|| eyre::eyre!("result.json missing after successful dora run"))?;

    Ok(result)
}

// ═══════════════════════════════════════════════════════════════
// Test cases
// ═══════════════════════════════════════════════════════════════

#[test]
#[serial]
fn echo_pipeline_exact_match_int64() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let source = serde_json::json!({
        "data": [42, 99, -1],
        "data_type": "Int64"
    });
    let expected = serde_json::json!({
        "data": [42, 99, -1],
        "data_type": "Int64"
    });

    let result =
        run_echo_pipeline(&source, &expected, &[], &[]).expect("echo pipeline should succeed");

    assert!(result.r#match, "expected match=true, got {result:#?}");
    assert_eq!(result.expected_count, 3);
    assert_eq!(result.received_count, 3);
    assert!(
        result.differences.is_empty(),
        "expected no differences, got {:?}",
        result.differences
    );
}

#[test]
#[serial]
fn echo_pipeline_semantic_int32_tolerates_int64() {
    if !require_dora() {
        return;
    }
    build_binaries();

    // Source emits with the default Int64 inference (no data_type hint),
    // but the expected file declares Int32.  Semantic comparison should
    // still match because the numeric values [1, 2, 3] are within Int32
    // range and the cast Int64→Int32 succeeds.
    let source = serde_json::json!({
        "data": [1, 2, 3]
    });
    let expected = serde_json::json!({
        "data": [1, 2, 3],
        "data_type": "Int32"
    });

    let result =
        run_echo_pipeline(&source, &expected, &[], &[]).expect("echo pipeline should succeed");

    assert!(
        result.r#match,
        "semantic mode should tolerate compatible types, got {result:#?}"
    );
    assert_eq!(result.differences.len(), 0);
}

#[test]
#[serial]
fn echo_pipeline_ten_elements() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let values: Vec<i64> = (0..10).collect();
    let source = serde_json::json!({
        "data": values,
        "data_type": "Int64"
    });
    let expected = serde_json::json!({
        "data": (0..10).collect::<Vec<i64>>(),
        "data_type": "Int64"
    });

    let result =
        run_echo_pipeline(&source, &expected, &[], &[]).expect("echo pipeline should succeed");

    assert!(result.r#match, "expected match=true, got {result:#?}");
    assert_eq!(result.expected_count, 10);
    assert_eq!(result.received_count, 10);
    assert!(result.differences.is_empty());
}

#[test]
#[serial]
fn echo_pipeline_string_data() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let source = serde_json::json!({
        "data": ["hello", "world"],
        "data_type": "LargeUtf8"
    });
    let expected = serde_json::json!({
        "data": ["hello", "world"],
        "data_type": "LargeUtf8"
    });

    let result =
        run_echo_pipeline(&source, &expected, &[], &[]).expect("echo pipeline should succeed");

    assert!(result.r#match, "expected match=true, got {result:#?}");
    assert_eq!(result.expected_count, 2);
    assert_eq!(result.received_count, 2);
    assert!(result.differences.is_empty());
}

// ═══════════════════════════════════════════════════════════════
// Multi-output echo pipeline
// ═══════════════════════════════════════════════════════════════

/// Run a multi-output echo pipeline and return the two sink results.
fn run_multi_echo_pipeline(
    source_a: &serde_json::Value,
    source_b: &serde_json::Value,
    expected_a: &serde_json::Value,
    expected_b: &serde_json::Value,
) -> eyre::Result<(SinkResult, SinkResult)> {
    let tmp = tempfile::TempDir::new()?;
    let tmp_path = tmp.path();

    let source_a_file = tmp_path.join("source_a.json");
    let source_b_file = tmp_path.join("source_b.json");
    let expected_a_file = tmp_path.join("expected_a.json");
    let expected_b_file = tmp_path.join("expected_b.json");
    std::fs::write(&source_a_file, serde_json::to_string_pretty(source_a)?)?;
    std::fs::write(&source_b_file, serde_json::to_string_pretty(source_b)?)?;
    std::fs::write(&expected_a_file, serde_json::to_string_pretty(expected_a)?)?;
    std::fs::write(&expected_b_file, serde_json::to_string_pretty(expected_b)?)?;

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {bin_dir}/test-source
    args: "--output data_a:{src_a} --output data_b:{src_b}"
    outputs:
      - data_a
      - data_b
  - id: echo-a
    path: {bin_dir}/echo-node
    inputs:
      data_a: test-source/data_a
    outputs:
      - data_a
  - id: echo-b
    path: {bin_dir}/echo-node
    inputs:
      data_b: test-source/data_b
    outputs:
      - data_b
  - id: test-sink-a
    path: {bin_dir}/test-sink
    inputs:
      data_a: echo-a/data_a
    args: "--expected-file {exp_a} --output-file {res_a}"
  - id: test-sink-b
    path: {bin_dir}/test-sink
    inputs:
      data_b: echo-b/data_b
    args: "--expected-file {exp_b} --output-file {res_b}"
"#,
        bin_dir = bin_dir().display(),
        src_a = source_a_file.display(),
        src_b = source_b_file.display(),
        exp_a = expected_a_file.display(),
        exp_b = expected_b_file.display(),
        res_a = tmp_path.join("result_a.json").display(),
        res_b = tmp_path.join("result_b.json").display(),
    );

    let yaml_file = tmp_path.join("multi-echo.yml");
    std::fs::write(&yaml_file, &yaml)?;

    let status = Command::new(dora_binary())
        .args(["run", yaml_file.to_str().unwrap(), "--stop-after", "15s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .status()?;

    assert!(status.success(), "dora run failed with status: {status}");

    let result_a: SinkResult =
        serde_json::from_reader(std::fs::File::open(tmp_path.join("result_a.json"))?)?;
    let result_b: SinkResult =
        serde_json::from_reader(std::fs::File::open(tmp_path.join("result_b.json"))?)?;

    Ok((result_a, result_b))
}

#[test]
#[serial]
fn multi_echo_pipeline_two_outputs() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let source_a = serde_json::json!({"data": [1, 2, 3], "data_type": "Int64"});
    let source_b = serde_json::json!({"data": [10, 20, 30], "data_type": "Int64"});
    let expected_a = serde_json::json!({"data": [1, 2, 3], "data_type": "Int64"});
    let expected_b = serde_json::json!({"data": [10, 20, 30], "data_type": "Int64"});

    let (result_a, result_b) =
        run_multi_echo_pipeline(&source_a, &source_b, &expected_a, &expected_b)
            .expect("multi-echo pipeline should succeed");

    assert!(result_a.r#match, "output A mismatch: {result_a:#?}");
    assert!(result_b.r#match, "output B mismatch: {result_b:#?}");
}

// ═══════════════════════════════════════════════════════════════
// Classifier pipeline
// ═══════════════════════════════════════════════════════════════

fn run_classifier_pipeline(
    source: &serde_json::Value,
    expected_high: &serde_json::Value,
    expected_low: &serde_json::Value,
) -> eyre::Result<(SinkResult, SinkResult)> {
    let tmp = tempfile::TempDir::new()?;
    let tmp_path = tmp.path();

    let source_file = tmp_path.join("source.json");
    let expected_high_file = tmp_path.join("expected_high.json");
    let expected_low_file = tmp_path.join("expected_low.json");
    std::fs::write(&source_file, serde_json::to_string_pretty(source)?)?;
    std::fs::write(
        &expected_high_file,
        serde_json::to_string_pretty(expected_high)?,
    )?;
    std::fs::write(
        &expected_low_file,
        serde_json::to_string_pretty(expected_low)?,
    )?;

    let yaml = format!(
        r#"nodes:
  - id: test-source
    path: {bin_dir}/test-source
    args: "--output raw-data:{src}"
    outputs:
      - raw-data
  - id: classifier
    path: {bin_dir}/classifier-node
    inputs:
      raw-data: test-source/raw-data
    outputs:
      - high
      - low
  - id: test-sink-high
    path: {bin_dir}/test-sink
    inputs:
      high: classifier/high
    args: "--expected-file {exp_high} --output-file {res_high}"
  - id: test-sink-low
    path: {bin_dir}/test-sink
    inputs:
      low: classifier/low
    args: "--expected-file {exp_low} --output-file {res_low}"
"#,
        bin_dir = bin_dir().display(),
        src = source_file.display(),
        exp_high = expected_high_file.display(),
        exp_low = expected_low_file.display(),
        res_high = tmp_path.join("result_high.json").display(),
        res_low = tmp_path.join("result_low.json").display(),
    );

    let yaml_file = tmp_path.join("classifier.yml");
    std::fs::write(&yaml_file, &yaml)?;

    let status = Command::new(dora_binary())
        .args(["run", yaml_file.to_str().unwrap(), "--stop-after", "15s"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .status()?;

    assert!(status.success(), "dora run failed with status: {status}");

    let result_high: SinkResult =
        serde_json::from_reader(std::fs::File::open(tmp_path.join("result_high.json"))?)?;
    let result_low: SinkResult =
        serde_json::from_reader(std::fs::File::open(tmp_path.join("result_low.json"))?)?;

    Ok((result_high, result_low))
}

#[test]
#[serial]
fn classifier_pipeline_basic() {
    if !require_dora() {
        return;
    }
    build_binaries();

    let source =
        serde_json::json!({"data": [10, 25, 60, 90, 45, 75, 30, 100, 50], "data_type": "Int64"});
    let expected_high = serde_json::json!({"data": [60, 90, 75, 100], "data_type": "Int64"});
    let expected_low = serde_json::json!({"data": [10, 25, 45, 30, 50], "data_type": "Int64"});

    let (result_high, result_low) = run_classifier_pipeline(&source, &expected_high, &expected_low)
        .expect("classifier pipeline should succeed");

    assert!(
        result_high.r#match,
        "high output mismatch: {result_high:#?}"
    );
    assert!(result_low.r#match, "low output mismatch: {result_low:#?}");
}
