//! Extension traits for converting test data into DORA input formats.
//!
//! The [`IntoInputData`] trait allows [`NodeHarness::send_data`] to
//! accept multiple data types — currently [`serde_json::Value`] and
//! [`arrow::array::ArrayData`].

use dora_node_api::integration_testing::integration_testing_format::InputData;

/// Convert test data into an [`InputData`] variant.
///
/// Implementations produce [`InputData`] values suitable for injection
/// through [`NodeHarness::send_data`](crate::NodeHarness::send_data).
pub trait IntoInputData {
    fn into_input_data(self) -> InputData;
}

impl IntoInputData for serde_json::Value {
    fn into_input_data(self) -> InputData {
        InputData::JsonObject {
            data: self,
            data_type: None,
        }
    }
}

impl IntoInputData for arrow::array::ArrayData {
    fn into_input_data(self) -> InputData {
        assert!(
            !self.is_empty(),
            "IntoInputData: empty ArrayData is not supported — \
             empty data causes the daemon thread to deadlock in tick()"
        );
        use arrow::array::RecordBatch;
        use arrow::datatypes::{Field, Schema};
        use arrow_json::writer::{JsonArray, Writer};
        use std::sync::Arc;

        let data_type = self.data_type().clone();

        // Build a single-column RecordBatch wrapping this array.
        let array_ref = arrow::array::make_array(self);
        let schema = Schema::new(vec![Field::new("data", data_type, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array_ref])
            .expect("IntoInputData: failed to create RecordBatch from ArrayData");

        // Serialize the batch to JSON array format.
        let mut buf = Vec::new();
        let mut writer = Writer::<_, JsonArray>::new(&mut buf);
        writer
            .write(&batch)
            .expect("IntoInputData: Arrow -> JSON write failed");
        writer
            .finish()
            .expect("IntoInputData: Arrow -> JSON finish failed");

        // Parse JSON directly from the buffer (known-valid UTF-8, skip re-validation).
        // The output is a JSON array of row objects;
        // DORA's JSON->Arrow converter handles this correctly.
        let value: serde_json::Value =
            serde_json::from_slice(&buf).expect("IntoInputData: Arrow JSON output is valid JSON");

        // NOTE: data_type is kept as None because DORA's read_input_data wraps
        // each element in {"inner": ...} before applying the schema, and the
        // schema's field name must match "inner" (not "data").  Computing the
        // correct two-level Struct type that accounts for both the RecordBatch
        // wrapping ("data") and DORA's internal wrapping ("inner") is fragile
        // and depends on DORA internals.  A proper fix should use
        // InputData::ArrowFile instead of InputData::JsonObject to preserve
        // exact Arrow types without JSON round-tripping.
        InputData::JsonObject {
            data: value,
            data_type: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_input_data_for_json_value() {
        let value = serde_json::json!({"key": "value"});
        let input_data = value.into_input_data();
        // Should produce JsonObject variant
        match input_data {
            InputData::JsonObject { data, data_type } => {
                assert_eq!(data, serde_json::json!({"key": "value"}));
                assert!(data_type.is_none());
            }
            _ => panic!("Expected JsonObject variant"),
        }
    }

    #[test]
    fn test_into_input_data_for_arrow_array_data() {
        use arrow::array::{Array, Int32Array};

        let arr = Int32Array::from(vec![1, 2, 3]);
        let array_data = arr.into_data();
        let input_data = array_data.into_input_data();
        // Should produce JsonObject variant with correct serialized data
        match input_data {
            InputData::JsonObject { data, data_type } => {
                // The serialized output is a JSON array of row objects
                // e.g. [{"data":1},{"data":2},{"data":3}]
                assert!(data.is_array(), "Expected JSON array, got {data:?}");
                let rows = data.as_array().unwrap();
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0], serde_json::json!({"data": 1}));
                assert_eq!(rows[1], serde_json::json!({"data": 2}));
                assert_eq!(rows[2], serde_json::json!({"data": 3}));
                assert!(data_type.is_none());
            }
            _ => panic!("Expected JsonObject variant"),
        }
    }

    #[test]
    fn test_into_input_data_for_arrow_string_array() {
        use arrow::array::{Array, StringArray};

        let arr = StringArray::from(vec!["hello", "world"]);
        let array_data = arr.into_data();
        let input_data = array_data.into_input_data();
        match input_data {
            InputData::JsonObject { data, data_type } => {
                let rows = data.as_array().unwrap();
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], serde_json::json!({"data": "hello"}));
                assert_eq!(rows[1], serde_json::json!({"data": "world"}));
                assert!(data_type.is_none());
            }
            _ => panic!("Expected JsonObject variant"),
        }
    }

    #[test]
    #[should_panic(expected = "empty ArrayData is not supported")]
    fn test_into_input_data_empty_arraydata_panics() {
        use arrow::array::{Array, Int32Array};
        let arr = Int32Array::from(Vec::<i32>::new());
        let _ = arr.into_data().into_input_data();
    }
}
