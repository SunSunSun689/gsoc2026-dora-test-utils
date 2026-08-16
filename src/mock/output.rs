//! Mock replacement for `DoraNode::send_output()` (`dora-node-api`).
//!
//! The real `send_output()` (in `apis/rust/node/src/node/mod.rs`) sends
//! data to the DORA daemon over a socket.  This mock replaces that socket
//! with a [`tokio::sync::mpsc::Sender`], so test code can capture and
//! assert on node outputs.

use std::collections::HashMap;
use tokio::sync::mpsc;

pub type OutputMessage = (String, arrow::array::ArrayData);

/// Captures calls to `send_output` in memory instead of sending them to
/// the daemon.
///
/// # Example
///
/// ```ignore
/// let (sender, mut collector) = MockOutputSender::new();
/// sender.send("label".into(), arrow_array).await;
/// collector.collect_pending().await;
/// let outputs = collector.drain("label");
/// assert_eq!(outputs.len(), 1);
/// ```
pub struct MockOutputSender {
    tx: mpsc::UnboundedSender<OutputMessage>,
}

/// Accumulator that collects and indexes outputs by ID for assertion.
///
/// This is the receiver side of a `MockOutputSender` pair.
pub struct OutputCollector {
    rx: mpsc::UnboundedReceiver<OutputMessage>,
    /// All outputs received so far, keyed by output ID.
    ///
    /// Each `send_output(id, data)` call appends `data` to the vector
    /// for that ID.
    pub buffers: HashMap<String, Vec<arrow::array::ArrayData>>,
}

impl MockOutputSender {
    /// Create a new mock output sender and a corresponding collector.
    ///
    /// The channel is unbounded, matching the real `DoraNode`'s
    /// `TestingOutput::ToChannel` (unbounded tokio mpsc) — a node emitting
    /// more outputs than a bounded buffer would hold must not deadlock the
    /// test.  Keep the returned collector alive: it is the receiver side.
    pub fn new() -> (Self, OutputCollector) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self { tx },
            OutputCollector {
                rx,
                buffers: HashMap::new(),
            },
        )
    }

    /// Send an output — mirrors the real `DoraNode::send_output(id, data)`.
    ///
    /// Unbounded, so it never blocks and never fails on backpressure;
    /// it only errors when the paired [`OutputCollector`] has been dropped.
    /// Kept `async` for API compatibility with existing callers.
    pub async fn send(
        &self,
        output_id: String,
        data: arrow::array::ArrayData,
    ) -> Result<(), String> {
        self.tx
            .send((output_id, data))
            .map_err(|_| "output receiver closed".to_string())
    }
}

impl OutputCollector {
    /// Drain and collect all pending outputs from the channel into buffers.
    ///
    /// This should be called after each `tick()` to collect outputs that
    /// were produced by the node.
    pub async fn collect_pending(&mut self) {
        while let Ok((output_id, data)) = self.rx.try_recv() {
            self.buffers.entry(output_id).or_default().push(data);
        }
    }

    /// Drain and return all outputs for the given `output_id`, or `None`
    /// if no outputs were recorded.
    pub fn drain(&mut self, output_id: &str) -> Option<Vec<arrow::array::ArrayData>> {
        self.buffers.remove(output_id)
    }

    /// Return the number of distinct output IDs recorded.
    pub fn len(&self) -> usize {
        self.buffers.len()
    }

    /// Return `true` if no outputs have been recorded.
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};

    fn create_test_array() -> arrow::array::ArrayData {
        Int32Array::from(vec![1, 2, 3]).into_data()
    }

    #[tokio::test]
    async fn test_mock_output_sender_single_output() {
        let (sender, mut collector) = MockOutputSender::new();

        let data1 = create_test_array();
        sender.send("output1".into(), data1).await.unwrap();
        collector.collect_pending().await;

        let outputs = collector.drain("output1");
        assert!(outputs.is_some());
        assert_eq!(outputs.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_mock_output_collector_multiple_outputs() {
        let (sender, mut collector) = MockOutputSender::new();

        // Send to different output IDs
        sender
            .send("out_a".into(), create_test_array())
            .await
            .unwrap();
        sender
            .send("out_b".into(), create_test_array())
            .await
            .unwrap();
        sender
            .send("out_a".into(), create_test_array())
            .await
            .unwrap();

        collector.collect_pending().await;

        // Verify grouping
        assert_eq!(collector.len(), 2);
        let out_a = collector.drain("out_a");
        assert!(out_a.is_some());
        assert_eq!(out_a.unwrap().len(), 2);

        let out_b = collector.drain("out_b");
        assert!(out_b.is_some());
        assert_eq!(out_b.unwrap().len(), 1);

        assert!(collector.is_empty());
    }

    #[tokio::test]
    async fn test_mock_output_sender_error_on_closed() {
        let (sender, collector) = MockOutputSender::new();
        drop(collector); // Close receiver

        let result = sender.send("output".into(), create_test_array()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_output_sender_many_outputs_no_deadlock() {
        // Unbounded channel: emitting far more outputs than a bounded
        // buffer would hold must not block or fail.
        let (sender, mut collector) = MockOutputSender::new();
        for _ in 0..1000 {
            sender
                .send("bulk".into(), create_test_array())
                .await
                .unwrap();
        }
        collector.collect_pending().await;
        let outputs = collector.drain("bulk");
        assert_eq!(outputs.unwrap().len(), 1000);
    }
}
