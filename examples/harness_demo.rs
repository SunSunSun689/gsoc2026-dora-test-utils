//! dora-test-utils Demo — NodeHarness unit testing (Layer 1).
//!
//! Shows the recommended structure for testing a DORA node:
//!
//!   1. Node logic lives in a LIBRARY module (below, `node_logic`) — this
//!      represents the `lib.rs` of the node's own crate.
//!   2. Pure logic is tested with plain asserts — no harness needed.
//!   3. The event-loop integration (receiving events, emitting outputs) is
//!      tested with NodeHarness — the harness drives the loop, and the loop
//!      calls THE SAME logic functions. Nothing is copied.
//!
//! Scenario: a "temperature alarm" node receives temperature readings
//! (one number per event) and emits an alarm for every reading above 30°C.
//!
//! Run: cargo run --example harness_demo
//! (No dora CLI needed — this demo is fully in-memory.)

use arrow::array::{Array, Int64Array};
use dora_node_api::Event;
use dora_test_utils::NodeHarness;

// ── 1. The node's logic ──────────────────────────────────────
// In a real project this module lives in the node's own crate
// (e.g. `my-node/src/lib.rs`) and is shared by the binary and the tests.

mod node_logic {
    /// Business logic: does this temperature trigger an alarm?
    pub fn should_alarm(temp: i64) -> bool {
        temp > 30
    }
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
fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("  ❌ {msg}");
    std::process::exit(1);
}

/// Read a single Int64 value out of an incoming Arrow payload.
fn read_temp(arr: &arrow::array::ArrayRef) -> i64 {
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .expect("temperature payload should be Int64")
        .value(0)
}

fn main() {
    section("dora-test-utils — NodeHarness Demo (Layer 1: unit testing)");
    println!("  Recommended structure: logic in a library module,");
    println!("  tested directly AND through the harness — no copying.");
    println!();

    // ── Part A: pure-logic tests (no harness needed) ─────────
    section("Part A — Test the logic directly (plain asserts)");

    step("node_logic::should_alarm(25)");
    println!("    = {}", node_logic::should_alarm(25));
    if node_logic::should_alarm(25) {
        fail("25°C should NOT alarm");
    }
    ok("correct");

    step("node_logic::should_alarm(35)");
    println!("    = {}", node_logic::should_alarm(35));
    if !node_logic::should_alarm(35) {
        fail("35°C SHOULD alarm");
    }
    ok("correct");

    println!();
    println!("  (These asserts call the SAME function the node uses in");
    println!("   production — nothing is rewritten for the test.)");

    // ── Part B: event-loop integration (NodeHarness) ─────────
    section("Part B — Test the event loop with NodeHarness");

    step("Create harness (no daemon started)");
    let mut harness = NodeHarness::new().expect("harness creation failed");
    ok("harness ready");

    step("Inject 3 synthetic temperature readings");
    harness.send_data("temperature", serde_json::json!(25)); // no alarm
    harness.send_data("temperature", serde_json::json!(35)); // alarm!
    harness.send_data("temperature", serde_json::json!(40)); // alarm!
    harness.send_stop();
    ok("3 readings + Stop buffered");

    step("Drive the event loop (the node's shell, adapted to the harness)");
    let mut alarm_count = 0;
    loop {
        match harness.tick() {
            Some(Event::Input { id, data, .. }) => {
                assert_eq!(id.as_str(), "temperature", "unexpected input id");
                // The node's event handler — calling THE SAME logic function.
                let temp = read_temp(&data.0);
                let alarmed = node_logic::should_alarm(temp); // ← same function
                println!("    {temp}°C → alarm: {alarmed}");
                if alarmed {
                    alarm_count += 1;
                }
            }
            Some(Event::Stop(..)) => {
                println!("    stop event — event stream drained");
                break;
            }
            Some(other) => println!("    (other event: {other:?})"),
            None => break,
        }
    }
    ok("node processed all readings through the same logic function");

    step("Node emits its result: 'alarm_count'");
    let result = Int64Array::from(vec![alarm_count]);
    harness
        .send_output("alarm_count", result)
        .expect("send_output failed");
    ok("output emitted");

    step("Capture the output and assert");
    let outputs = harness
        .recv_output("alarm_count")
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
    if value != 2 {
        fail(format!(
            "assertion failed: alarm_count = {value}, expected 2 (35°C and 40°C alarm)"
        ));
    }
    ok("assertion passed: alarm_count == 2");

    // ── Summary ─────────────────────────────────────────
    section("Summary");
    println!("  ✅ node_logic module        — the node's business logic (its lib.rs)");
    println!("  ✅ Part A                   — pure logic tested with plain asserts");
    println!("  ✅ Part B                   — event loop driven by NodeHarness, calling");
    println!("                                 the SAME logic functions (no copy)");
    println!();
    println!("  This is the recommended structure for testing a DORA node:");
    println!("    lib.rs  — the logic, written once");
    println!("    main.rs — the shell: real daemon in, logic called, outputs out");
    println!("    tests   — plain asserts on the logic + NodeHarness for the loop");
}
