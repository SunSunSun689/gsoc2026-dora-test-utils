//! dora-test-utils Demo — NodeHarness unit testing (Layer 1).
//!
//! Shows how to test a single DORA node inside a plain `main`/`#[test]`
//! function — no daemon, no YAML, no dataflow.  The harness drives the
//! node's event stream in memory.
//!
//! Scenario: a "detector" node receives sensor frames and emits a result.
//! The test injects three frames, drives the node's event loop, emits the
//! node's result through the harness, and captures + verifies it.
//!
//! Run: cargo run --example harness_demo
//! (No dora CLI needed — this demo is fully in-memory.)

use arrow::array::Int32Array;
use dora_node_api::Event;
use dora_test_utils::NodeHarness;

fn section(title: &str) {
    println!("\n═══ {} ═══\n", title);
}
fn step(msg: &str) {
    println!("▸ {msg}");
}
fn ok(msg: &str) {
    println!("  ✅ {msg}");
}
fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("  ❌ {msg}");
    std::process::exit(1);
}

fn main() {
    section("dora-test-utils — NodeHarness Demo (Layer 1: unit testing)");
    println!("  Test a single node without a daemon, YAML, or dataflow");
    println!();

    // ── Step 1: Create the harness ──────────────────────
    step("Create harness (no daemon started)");
    let mut harness = NodeHarness::new().expect("harness creation failed");
    ok("harness ready");

    // ── Step 2: Inject synthetic inputs ─────────────────
    step("Inject 3 synthetic sensor frames");
    harness.send_data("frame", serde_json::json!({"width": 640, "height": 480}));
    harness.send_data("frame", serde_json::json!({"width": 1280, "height": 720}));
    harness.send_data("frame", serde_json::json!({"width": 1920, "height": 1080}));
    harness.send_stop();
    ok("3 frames + Stop buffered");

    // ── Step 3: Drive the node's event loop ─────────────
    step("Drive the event loop (tick per event)");
    let mut frames_seen = 0;
    loop {
        match harness.tick() {
            Some(Event::Input { id, data, .. }) => {
                assert_eq!(id.as_str(), "frame", "unexpected input id");
                frames_seen += 1;
                println!("    frame {frames_seen}: {} bytes", data.0.len());
            }
            Some(Event::Stop(..)) => {
                println!("    stop event — event stream drained");
                break;
            }
            Some(other) => println!("    (other event: {other:?})"),
            None => break,
        }
    }
    if frames_seen != 3 {
        fail(format!("expected 3 frames, node saw {frames_seen}"));
    }
    ok("node received all 3 frames");

    // ── Step 4: Node emits its result ───────────────────
    step("Node processes frames and emits 'frame_count'");
    let result = Int32Array::from(vec![frames_seen]);
    harness
        .send_output("frame_count", result)
        .expect("send_output failed");
    ok("output emitted");

    // ── Step 5: Capture and assert ──────────────────────
    step("Capture the output and assert");
    let outputs = harness
        .recv_output("frame_count")
        .ok_or("no output captured")
        .unwrap_or_else(|e| fail(e));
    println!("    captured: {outputs:?}");

    // Captured shape: {"id": ..., "data": Array [Number(..)], "data_type": ...}
    let value = outputs
        .first()
        .and_then(|m| m.get("data"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_i64())
        .ok_or("output missing expected structure")
        .unwrap_or_else(|e| fail(e));
    if value != 3 {
        fail(format!(
            "assertion failed: frame_count = {value}, expected 3"
        ));
    }
    ok("assertion passed: frame_count == 3");

    // ── Summary ─────────────────────────────────────────
    section("Summary");
    println!("  ✅ NodeHarness::new()      — no daemon, no YAML");
    println!("  ✅ send_data()             — inject synthetic inputs (JSON or Arrow)");
    println!("  ✅ tick()                  — drive the event loop event-by-event");
    println!("  ✅ send_output()           — the node emits its result");
    println!("  ✅ recv_output()           — capture outputs in memory");
    println!("  ✅ assert!                 — plain Rust assertions");
    println!();
    println!("  This is exactly what a #[test] looks like — see README Layer 1.");
    println!("  A real node test replaces the injected/emitted data with the");
    println!("  node's actual inputs and expected outputs.");
}
