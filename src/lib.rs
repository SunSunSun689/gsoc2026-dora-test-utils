//! # dora-test-utils
//!
//! Testing utilities for the [DORA](https://dora-rs.ai/) dataflow framework.
//!
//! This crate provides three layers of testing support, from lightweight
//! unit tests to full integration and regression testing:
//!
//! ## 1. Unit testing — [`NodeHarness`]
//!
//! Drive a single DORA node with synthetic inputs and assert its outputs
//! **without** starting the DORA daemon or coordinator.  Works inside
//! standard `#[test]` functions (NOT `#[tokio::test]` — `init_testing()`
//! uses `blocking_recv` internally, which panics inside a tokio runtime).
//!
//! ```ignore
//! use dora_test_utils::NodeHarness;
//!
//! #[test]
//! fn test_classifier_node() {
//!     let mut harness = NodeHarness::new()
//!         .expect("failed to create harness");
//!
//!     // Inject data via the convenience method.
//!     harness.send_data("image", serde_json::json!([1, 2, 3]));
//!
//!     // Drive to completion (auto-injects Stop, auto-closes input).
//!     let events = harness.run_to_completion();
//!     assert!(events.iter().any(|e| matches!(e, dora_node_api::Event::Input { .. })));
//!
//!     // Send and assert outputs.
//!     harness.send_output("label", arrow::array::Int32Array::from(vec![42]))
//!         .expect("send_output should succeed");
//!     let outputs = harness.recv_output("label");
//!     assert!(outputs.is_some());
//! }
//! ```
//!
//! For full control, use [`send_input`](NodeHarness::send_input) with
//! [`TimedIncomingEvent`] directly:
//!
//! ```ignore
//! use dora_node_api::integration_testing::integration_testing_format::{
//!     IncomingEvent, TimedIncomingEvent,
//! };
//!
//! harness.send_input(TimedIncomingEvent {
//!     time_offset_secs: 0.0,
//!     event: IncomingEvent::Input { /* ... */ },
//! });
//! ```
//!
//! ## 2. Integration testing — TestSource / TestSink
//!
//! Reusable binary nodes that emit test data from files and capture + assert
//! outputs.  Drop them into a real YAML dataflow alongside the node under
//! test. *(Library + CLI: Week 5; integration tests: Week 6–8)*
//!
//! ```ignore
//! use dora_test_utils::source::{run_test_source, SourceConfig};
//! use dora_test_utils::sink::{run_test_sink, SinkConfig};
//!
//! // Source: emit test data on one or more outputs.
//! let config = SourceConfig::single(
//!     "data".into(),
//!     serde_json::json!({"data": [42, 99], "data_type": "Int32"}),
//! );
//! run_test_source(config)?;
//!
//! // Sink: capture and compare with expected output.
//! let config = SinkConfig {
//!     expected_file: "expected.json".into(),
//!     output_file: "result.json".into(),
//!     fail_on_mismatch: true,
//!     strict: false,
//! };
//! let result = run_test_sink(config)?;
//! assert!(result.r#match);
//! ```
//!
//! ## 3. Regression testing — Record / Replay
//!
//! Record real dataflow I/O to disk and replay it later to detect behavioral
//! regressions. *(Extended scope — Week 13–17)*
//!
//! ## Implementation Status
//!
//! | Component | Status |
//! |-----------|--------|
//! | [`NodeHarness`] (struct + `new()`) | Implemented — wraps [`DoraNode::init_testing()`][init] with live [`TestingInput::Channel`] + [`TestingOutput::ToChannel`] |
//! | [`NodeHarness::send_input()`] | Implemented — pushes [`TimedIncomingEvent`] through live tokio mpsc channel |
//! | [`NodeHarness::send_data()`] | Implemented — convenience: inject data by ID (accepts [`serde_json::Value`] and [`arrow::array::ArrayData`]) |
//! | [`NodeHarness::send_stop()`] | Implemented — convenience wrapper around `send_input` for Stop events |
//! | [`NodeHarness::send_output()`] | Implemented — delegates to [`DoraNode::send_output`]; safe after [`close_input`](NodeHarness::close_input) or [`run_to_completion`](NodeHarness::run_to_completion) |
//! | [`NodeHarness::tick()`] | Implemented — synchronous, polls real [`EventStream`], collects outputs |
//! | [`NodeHarness::recv_output()`] | Implemented — drains output buffers; returns `Option<Vec<Map<String, Value>>>` |
//! | [`NodeHarness::close_input()`] | Implemented — drops input sender to unblock daemon thread for safe `send_output` |
//! | [`NodeHarness::run_to_completion()`] | Implemented — loops tick() until Stop/None, auto-calls close_input(), returns Vec<Event> |
//! | E2E tests | Implemented — `tests/e2e.rs`: 5 tests covering input pipeline, output path, run_to_completion, full pipeline, Arrow data |
//! | [`MockEventStream`] | Fully implemented + 3 tests |
//! | [`MockOutputSender`] / [`OutputCollector`] | Fully implemented + 3 tests |
//! | [`TestSource`][crate::source] library + CLI binary | Implemented — JSON→Arrow with `data_type` hint (Int8–UInt64, Float32/64, LargeUtf8); CLI: `--data-file`/`--inline-data` |
//! | [`TestSink`][crate::sink] library + CLI binary | Implemented — strict (JSON round-trip) + semantic (Arrow equality) comparison; CLI: `--expected-file`/`--strict`/`--no-fail-on-mismatch` |
//! | Integration tests | Week 6–8 |
//! | Record / Replay | Week 13–17 (extended) |
//!
//! ## Relationship to upstream DORA
//!
//! This crate extends the foundation in `dora-node-api`'s
//! [`integration_testing`][dora-it] module ([`DoraNode::init_testing()`][init]).
//! It adds the **runtime event injection** that `init_testing()` currently
//! lacks — via a new [`TestingInput::Channel`] variant added upstream — and
//! the output-capture + assertion helpers.
//!
//! The harness uses live [`tokio::sync::mpsc`] channels for both directions:
//! input events flow from test code to the node through
//! [`TestingInput::Channel`]; outputs flow back through
//! [`TestingOutput::ToChannel`].  No file I/O or daemon connection required.
//!
//! For pure-mock testing (no real node), the standalone mock types
//! ([`MockEventStream`], [`MockOutputSender`]) use
//! [`tokio::sync::mpsc`] channels.
//!
//! [init]: https://docs.rs/dora-node-api/latest/dora_node_api/struct.DoraNode.html#method.init_testing
//! [dora-it]: https://docs.rs/dora-node-api/latest/dora_node_api/integration_testing/

pub mod harness;
pub mod mock;
pub mod record;
pub mod sink;
pub mod source;
pub mod traits;

// Re-export the key types for convenience.
pub use harness::NodeHarness;
pub use mock::event_stream::MockEventStream;
pub use mock::output::{MockOutputSender, OutputCollector};
pub use record::{RecordSession, Recording};
pub use traits::IntoInputData;
