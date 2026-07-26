//! RecordSession — run a DORA dataflow and capture sink outputs for regression testing.
//!
//! ```ignore
//! use dora_test_utils::record::RecordSession;
//! use std::time::Duration;
//!
//! let recording = RecordSession::attach("dataflow.yml")
//!     .unwrap()
//!     .record_sink("test-sink", "result.json")
//!     .with_timeout(Duration::from_secs(10))
//!     .run()
//!     .unwrap();
//! recording.save("recording.json").unwrap();
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Error type for RecordSession operations.
#[derive(Debug)]
pub enum RecordError {
    DoraNotFound(String),
    RunFailed { status: String, stderr: String },
    DataflowNotFound(PathBuf),
    NoSinksConfigured,
    SinkOutputMissing { sink_id: String, path: PathBuf },
    SinkReadError { sink_id: String, error: String },
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordError::DoraNotFound(path) => write!(f, "dora CLI not found at '{path}'"),
            RecordError::RunFailed { status, stderr } => {
                write!(f, "dora run failed with status {status}: {stderr}")
            }
            RecordError::DataflowNotFound(path) => {
                write!(f, "dataflow YAML file not found: '{}'", path.display())
            }
            RecordError::NoSinksConfigured => {
                write!(f, "no sinks configured — call record_sink() before run()")
            }
            RecordError::SinkOutputMissing { sink_id, path } => {
                write!(
                    f,
                    "sink '{sink_id}' output file not found at '{}'",
                    path.display()
                )
            }
            RecordError::SinkReadError { sink_id, error } => {
                write!(f, "failed to read sink '{sink_id}' output: {error}")
            }
            RecordError::Io(e) => write!(f, "I/O error: {e}"),
            RecordError::Json(e) => write!(f, "JSON error: {e}"),
        }
    }
}

impl std::error::Error for RecordError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RecordError::Io(e) => Some(e),
            RecordError::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for RecordError {
    fn from(e: std::io::Error) -> Self {
        RecordError::Io(e)
    }
}

impl From<serde_json::Error> for RecordError {
    fn from(e: serde_json::Error) -> Self {
        RecordError::Json(e)
    }
}

/// A recording session for a DORA dataflow.
///
/// Created via [`RecordSession::attach`], configured with sinks and timeout,
/// then executed via [`run`](RecordSession::run).
#[derive(Debug)]
pub struct RecordSession {
    dataflow_yaml: PathBuf,
    sinks: Vec<(String, PathBuf)>,
    timeout: Duration,
}

/// A completed recording containing sink outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recording {
    pub metadata: RecordingMetadata,
    pub sinks: HashMap<String, serde_json::Value>,
}

/// Metadata about a recording.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingMetadata {
    /// Absolute path to the dataflow YAML used.
    pub dataflow_yaml: String,
    /// Unix timestamp when the recording was made.
    pub recorded_at_unix: u64,
    /// Timeout duration in seconds (sub-second precision).
    pub timeout_secs: f64,
    /// Version string from `dora --version`.
    pub dora_version: String,
}

impl RecordSession {
    /// Create a new session for the given dataflow YAML.
    ///
    /// The YAML must exist and be readable. Call [`record_sink`](Self::record_sink)
    /// to register sinks whose outputs should be captured.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::DataflowNotFound`] if the YAML file doesn't exist.
    pub fn attach(dataflow_yaml: impl Into<PathBuf>) -> Result<Self, RecordError> {
        let yaml_path: PathBuf = dataflow_yaml.into();
        if !yaml_path.exists() {
            return Err(RecordError::DataflowNotFound(yaml_path));
        }
        Ok(Self {
            dataflow_yaml: yaml_path,
            sinks: Vec::new(),
            timeout: Duration::from_secs(30),
        })
    }

    /// Register a sink whose output file should be captured.
    ///
    /// `sink_id` is the node ID in the dataflow YAML. `output_file` is the
    /// path where the TestSink writes its output (set via `--output-file`).
    pub fn record_sink(
        mut self,
        sink_id: impl Into<String>,
        output_file: impl Into<PathBuf>,
    ) -> Self {
        self.sinks.push((sink_id.into(), output_file.into()));
        self
    }

    /// Set the timeout for `dora run --stop-after`.
    ///
    /// Default: 30 seconds.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Execute the dataflow and collect all sink outputs.
    ///
    /// Runs `dora run <yaml> --stop-after <N>s`, waits for completion,
    /// then reads each registered sink's output file.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError`] if:
    /// - No sinks configured
    /// - dora CLI not found
    /// - dora run exits non-zero
    /// - Any sink output file is missing or unreadable
    pub fn run(self) -> Result<Recording, RecordError> {
        if self.sinks.is_empty() {
            return Err(RecordError::NoSinksConfigured);
        }

        // ── 1. Locate dora binary ──────────────────────────────
        let dora = find_dora_binary();

        // ── 2. Get dora version ────────────────────────────────
        let dora_version = get_dora_version(&dora).unwrap_or_else(|_| "unknown".to_string());

        // ── 3. Run dora run ────────────────────────────────────
        let timeout_secs = self.timeout.as_secs_f64().max(0.1);
        let stop_after = format!("{}s", timeout_secs);
        let yaml_str = self.dataflow_yaml.to_str().ok_or_else(|| {
            RecordError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "dataflow YAML path is not valid UTF-8: {}",
                    self.dataflow_yaml.display()
                ),
            ))
        })?;

        let output = Command::new(&dora)
            .args(["run", yaml_str, "--stop-after", &stop_after])
            .output()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    RecordError::DoraNotFound(dora.display().to_string())
                } else {
                    RecordError::Io(e)
                }
            })?;

        if !output.status.success() {
            return Err(RecordError::RunFailed {
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        // ── 4. Collect sink outputs ────────────────────────────
        let mut sinks = HashMap::new();
        for (sink_id, output_file) in &self.sinks {
            if !output_file.exists() {
                return Err(RecordError::SinkOutputMissing {
                    sink_id: sink_id.clone(),
                    path: output_file.clone(),
                });
            }

            let contents =
                std::fs::read_to_string(output_file).map_err(|e| RecordError::SinkReadError {
                    sink_id: sink_id.clone(),
                    error: e.to_string(),
                })?;

            let value: serde_json::Value =
                serde_json::from_str(&contents).map_err(|e| RecordError::SinkReadError {
                    sink_id: sink_id.clone(),
                    error: format!("invalid JSON: {e}"),
                })?;

            sinks.insert(sink_id.clone(), value);
        }

        // ── 5. Build recording ─────────────────────────────────
        Ok(Recording {
            metadata: RecordingMetadata {
                dataflow_yaml: self.dataflow_yaml.to_string_lossy().to_string(),
                recorded_at_unix: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                timeout_secs,
                dora_version,
            },
            sinks,
        })
    }
}

impl Recording {
    /// Save the recording to a JSON file.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or file write fails.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<(), RecordError> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path.as_ref(), json)?;
        Ok(())
    }

    /// Load a recording from a JSON file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, RecordError> {
        let contents = std::fs::read_to_string(path.as_ref())?;
        let recording: Self = serde_json::from_str(&contents)?;
        Ok(recording)
    }
}

// ── Private helpers ────────────────────────────────────────────────

/// Locate the dora CLI binary.
fn find_dora_binary() -> PathBuf {
    // Check vendored dora workspace build first (debug, then release).
    for profile in &["debug", "release"] {
        let vendored = Path::new("dora/target").join(profile).join("dora");
        if vendored.exists() {
            return vendored;
        }
    }
    // Fall back to PATH.
    PathBuf::from("dora")
}

/// Get dora CLI version string.
fn get_dora_version(dora_binary: &Path) -> Result<String, RecordError> {
    let output = Command::new(dora_binary)
        .arg("--version")
        .output()
        .map_err(|e| RecordError::Io(e))?;
    if output.status.success() {
        let v = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(if v.is_empty() {
            "unknown".to_string()
        } else {
            v
        })
    } else {
        Ok("unknown".to_string())
    }
}
