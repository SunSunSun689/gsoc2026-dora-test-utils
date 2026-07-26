//! TestSink binary — CLI wrapper around `dora_test_utils::sink::run_test_sink`.
//!
//! Receives inputs from a DORA dataflow, compares them against expected
//! data from a file, and writes the result to `result.json`.

use std::path::PathBuf;

use clap::Parser;
use dora_test_utils::sink::{run_test_sink, SinkConfig};

#[derive(Parser, Debug)]
#[command(
    name = "test-sink",
    about = "Capture DORA outputs and compare with expected data"
)]
struct Cli {
    /// Path to the expected output file (DORA JSON format).
    /// Not required when --record-mode is set.
    #[arg(long, default_value = "./expected.json")]
    expected_file: PathBuf,

    /// Path to write comparison result (default: ./result.json).
    #[arg(long, default_value = "./result.json")]
    output_file: PathBuf,

    /// Do not exit with non-zero on mismatch.
    #[arg(long)]
    no_fail_on_mismatch: bool,

    /// Use exact JSON string comparison instead of Arrow semantic comparison.
    #[arg(long)]
    strict: bool,

    /// Record mode: skip comparison, write raw received data to output_file.
    #[arg(long)]
    record_mode: bool,
}

fn main() {
    let cli = Cli::parse();

    let fail_on_mismatch = !cli.no_fail_on_mismatch;

    let config = SinkConfig {
        expected_file: cli.expected_file,
        output_file: cli.output_file,
        fail_on_mismatch,
        strict: cli.strict,
        record_mode: cli.record_mode,
    };

    match run_test_sink(config) {
        Ok(result) => {
            if !result.r#match {
                // fail_on_mismatch was already handled by run_test_sink() —
                // if it had been true and there was a mismatch, we would be
                // in the Err branch below.  But with fail_on_mismatch=false
                // we still print the differences for visibility.
                eprintln!(
                    "mismatch: {} differences found (expected {} items, got {})",
                    result.differences.len(),
                    result.expected_count,
                    result.received_count,
                );
                for diff in &result.differences {
                    eprintln!("  - {}", diff.message);
                }
            }
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            std::process::exit(1);
        }
    }
}
