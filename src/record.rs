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

use crate::sink;

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

/// Error type for ReplaySession operations.
#[derive(Debug)]
pub enum ReplayError {
    /// The Recording file could not be loaded.
    LoadFailed(Box<RecordError>),
    /// The dataflow YAML file was not found.
    DataflowNotFound(PathBuf),
    /// The dora CLI binary was not found.
    DoraNotFound(String),
    /// dora run exited with a non-zero status.
    RunFailed { status: String, stderr: String },
    /// No sinks were registered via replay_sink() before run().
    NoSinksConfigured,
    /// A sink registered via replay_sink() is not present in the baseline Recording.
    SinkNotInBaseline(String),
    /// A registered sink's output file was not found after dora run.
    SinkOutputMissing { sink_id: String, path: PathBuf },
    /// Failed to read or parse a sink's output file.
    SinkReadError { sink_id: String, error: String },
    /// I/O error.
    Io(std::io::Error),
    /// JSON (de)serialization error.
    Json(serde_json::Error),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::LoadFailed(e) => write!(f, "failed to load recording: {e}"),
            ReplayError::DataflowNotFound(path) => {
                write!(f, "dataflow YAML not found: '{}'", path.display())
            }
            ReplayError::DoraNotFound(path) => write!(f, "dora CLI not found at '{path}'"),
            ReplayError::RunFailed { status, stderr } => {
                write!(f, "dora run failed with status {status}: {stderr}")
            }
            ReplayError::NoSinksConfigured => {
                write!(f, "no sinks configured — call replay_sink() before run()")
            }
            ReplayError::SinkNotInBaseline(id) => {
                write!(
                    f,
                    "sink '{id}' registered via replay_sink() but not found in baseline recording"
                )
            }
            ReplayError::SinkOutputMissing { sink_id, path } => {
                write!(
                    f,
                    "sink '{sink_id}' output file not found at '{}'",
                    path.display()
                )
            }
            ReplayError::SinkReadError { sink_id, error } => {
                write!(f, "failed to read sink '{sink_id}' output: {error}")
            }
            ReplayError::Io(e) => write!(f, "I/O error: {e}"),
            ReplayError::Json(e) => write!(f, "JSON error: {e}"),
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReplayError::LoadFailed(e) => Some(e.as_ref()),
            ReplayError::Io(e) => Some(e),
            ReplayError::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ReplayError {
    fn from(e: std::io::Error) -> Self {
        ReplayError::Io(e)
    }
}

impl From<serde_json::Error> for ReplayError {
    fn from(e: serde_json::Error) -> Self {
        ReplayError::Json(e)
    }
}

impl From<RecordError> for ReplayError {
    fn from(e: RecordError) -> Self {
        ReplayError::LoadFailed(Box::new(e))
    }
}

/// Overall comparison status for a single sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum DiffStatus {
    /// Sink outputs match baseline exactly.
    Match,
    /// Sink outputs differ from baseline.
    Mismatch,
    /// Sink present in baseline but missing in replay.
    Missing,
    /// Sink present in replay but not in baseline.
    Extra,
}

/// A single field-level difference.
#[derive(Debug, Clone, Serialize)]
pub struct FieldDiff {
    /// JSON path to the differing field, e.g. "data[2]" or "count".
    pub path: String,
    /// Value in the baseline recording.
    pub baseline: serde_json::Value,
    /// Value in the current (replay) run.
    pub current: serde_json::Value,
}

/// Comparison result for one sink.
#[derive(Debug, Clone, Serialize)]
pub struct SinkDiff {
    pub sink_id: String,
    pub status: DiffStatus,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub differences: Vec<FieldDiff>,
}

/// Complete regression report.
#[derive(Debug, Clone, Serialize)]
pub struct DiffReport {
    pub regressions: Vec<SinkDiff>,
}

impl std::fmt::Display for DiffReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.regressions.is_empty()
            || self
                .regressions
                .iter()
                .all(|r| r.status == DiffStatus::Match)
        {
            write!(f, "No regressions detected.")?;
            let matched = self
                .regressions
                .iter()
                .filter(|r| r.status == DiffStatus::Match)
                .count();
            if matched > 0 {
                write!(
                    f,
                    " ({matched} sink{} match)",
                    if matched == 1 { "" } else { "s" }
                )?;
            }
            return Ok(());
        }

        let bad: Vec<_> = self
            .regressions
            .iter()
            .filter(|r| r.status != DiffStatus::Match)
            .collect();
        writeln!(
            f,
            "Regressions detected ({}/{} sinks affected):",
            bad.len(),
            self.regressions.len()
        )?;

        for reg in bad {
            write!(
                f,
                "\n  [{}] {}",
                reg.sink_id,
                match reg.status {
                    DiffStatus::Mismatch =>
                        format!("MISMATCH ({} differences)", reg.differences.len()),
                    DiffStatus::Missing =>
                        "MISSING — present in baseline but not in replay".to_string(),
                    DiffStatus::Extra =>
                        "EXTRA — present in replay but not in baseline".to_string(),
                    DiffStatus::Match => unreachable!(),
                }
            )?;

            for d in &reg.differences {
                write!(f, "\n    {}: {:?} -> {:?}", d.path, d.baseline, d.current)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Result of a replay run: baseline sinks, current sinks, and comparison report.
#[derive(Debug, Clone, Serialize)]
pub struct ReplayResult {
    /// Metadata from the baseline Recording.
    pub metadata: RecordingMetadata,
    /// Baseline sink outputs.
    pub baseline_sinks: HashMap<String, serde_json::Value>,
    /// Replay sink outputs.
    pub current_sinks: HashMap<String, serde_json::Value>,
    /// Diff report.
    pub report: DiffReport,
}

impl ReplayResult {
    /// Returns `true` if no regressions were found.
    pub fn is_clean(&self) -> bool {
        self.report.regressions.is_empty()
            || self
                .report
                .regressions
                .iter()
                .all(|r| r.status == DiffStatus::Match)
    }

    /// Returns a reference to the diff report.
    pub fn diff(&self) -> &DiffReport {
        &self.report
    }

    /// Panics if any regressions are detected.
    ///
    /// # Panics
    ///
    /// Panics with a formatted regression report when regressions exist.
    pub fn assert_no_regression(&self) {
        if !self.is_clean() {
            panic!("Regression detected:\n{}", self.report);
        }
    }
}

/// A replay session for regression testing a DORA dataflow.
///
/// Created via [`ReplaySession::load`], configured with sinks and optional
/// overrides, then executed via [`run`](ReplaySession::run).
#[derive(Debug)]
pub struct ReplaySession {
    recording: Recording,
    sinks: Vec<(String, PathBuf)>,
    dataflow_override: Option<PathBuf>,
    timeout_override: Option<Duration>,
}

impl ReplaySession {
    /// Load a previously saved Recording and prepare for replay.
    ///
    /// Call [`replay_sink`](Self::replay_sink) for each sink whose output
    /// should be captured and compared, then [`run`](Self::run).
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ReplayError> {
        let recording = Recording::load(path).map_err(ReplayError::from)?;
        Ok(Self {
            recording,
            sinks: Vec::new(),
            dataflow_override: None,
            timeout_override: None,
        })
    }

    /// Register a sink whose output should be captured and compared.
    ///
    /// `sink_id` must match a sink in the baseline Recording.
    /// `output_file` is where the TestSink writes its output (passed via
    /// `--output-file` CLI arg in the YAML).
    pub fn replay_sink(
        mut self,
        sink_id: impl Into<String>,
        output_file: impl Into<PathBuf>,
    ) -> Self {
        self.sinks.push((sink_id.into(), output_file.into()));
        self
    }

    /// Override the dataflow YAML path from the Recording.
    pub fn dataflow(mut self, path: impl Into<PathBuf>) -> Self {
        self.dataflow_override = Some(path.into());
        self
    }

    /// Override the timeout from the Recording.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_override = Some(timeout);
        self
    }

    /// Execute the replay and compare against the baseline.
    ///
    /// Runs `dora run <yaml> --stop-after <N>s`, collects sink outputs,
    /// then compares each against the baseline Recording.
    pub fn run(self) -> Result<ReplayResult, ReplayError> {
        if self.sinks.is_empty() {
            return Err(ReplayError::NoSinksConfigured);
        }

        // Validate that all registered sinks exist in the baseline.
        for (sink_id, _) in &self.sinks {
            if !self.recording.sinks.contains_key(sink_id) {
                return Err(ReplayError::SinkNotInBaseline(sink_id.clone()));
            }
        }

        // Resolve YAML path.
        let yaml_path = if let Some(ref p) = self.dataflow_override {
            p.clone()
        } else {
            PathBuf::from(&self.recording.metadata.dataflow_yaml)
        };
        if !yaml_path.exists() {
            return Err(ReplayError::DataflowNotFound(yaml_path));
        }

        // Resolve timeout.
        let timeout = self.timeout_override.unwrap_or(Duration::from_secs_f64(
            self.recording.metadata.timeout_secs.max(0.1),
        ));

        // Locate dora binary.
        let dora = find_dora_binary();

        // Run dora run.
        let timeout_secs = timeout.as_secs_f64().max(0.1);
        let stop_after = format!("{}s", timeout_secs);
        let yaml_str = yaml_path.to_str().ok_or_else(|| {
            ReplayError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("YAML path is not valid UTF-8: {}", yaml_path.display()),
            ))
        })?;

        let output = Command::new(&dora)
            .args(["run", yaml_str, "--stop-after", &stop_after])
            .output()
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    ReplayError::DoraNotFound(dora.display().to_string())
                } else {
                    ReplayError::Io(e)
                }
            })?;

        if !output.status.success() {
            return Err(ReplayError::RunFailed {
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        // Collect current sink outputs.
        let mut current_sinks = HashMap::new();
        for (sink_id, output_file) in &self.sinks {
            if !output_file.exists() {
                return Err(ReplayError::SinkOutputMissing {
                    sink_id: sink_id.clone(),
                    path: output_file.clone(),
                });
            }
            let contents =
                std::fs::read_to_string(output_file).map_err(|e| ReplayError::SinkReadError {
                    sink_id: sink_id.clone(),
                    error: e.to_string(),
                })?;
            let value: serde_json::Value =
                serde_json::from_str(&contents).map_err(|e| ReplayError::SinkReadError {
                    sink_id: sink_id.clone(),
                    error: format!("invalid JSON: {e}"),
                })?;
            current_sinks.insert(sink_id.clone(), value);
        }

        // Compare.
        let report = compare_recordings(&self.recording.sinks, &current_sinks);

        Ok(ReplayResult {
            metadata: self.recording.metadata.clone(),
            baseline_sinks: self.recording.sinks.clone(),
            current_sinks,
            report,
        })
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
    // Check local dora workspace build first (debug, then release).
    for profile in &["debug", "release"] {
        let local = Path::new("dora/target").join(profile).join("dora");
        if local.exists() {
            return local;
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
        .map_err(RecordError::Io)?;
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

// ── Replay comparison helpers ─────────────────────────────────────────

/// Compare baseline sink outputs against current (replay) outputs.
///
/// Layer 1: fast JSON structural comparison.
/// Layer 2: for `data` arrays that differ in layer 1, attempt Arrow semantic
///          comparison (tolerates Int32→Int64 etc.).
fn compare_recordings(
    baseline: &HashMap<String, serde_json::Value>,
    current: &HashMap<String, serde_json::Value>,
) -> DiffReport {
    let mut regressions = Vec::new();

    // Check baseline sinks present in current.
    for (sink_id, baseline_value) in baseline {
        match current.get(sink_id) {
            Some(current_value) => {
                let differences = compare_sink_outputs(sink_id, baseline_value, current_value);
                regressions.push(SinkDiff {
                    sink_id: sink_id.clone(),
                    status: if differences.is_empty() {
                        DiffStatus::Match
                    } else {
                        DiffStatus::Mismatch
                    },
                    differences,
                });
            }
            None => {
                regressions.push(SinkDiff {
                    sink_id: sink_id.clone(),
                    status: DiffStatus::Missing,
                    differences: Vec::new(),
                });
            }
        }
    }

    // Check for extra sinks in current not in baseline.
    for sink_id in current.keys() {
        if !baseline.contains_key(sink_id) {
            regressions.push(SinkDiff {
                sink_id: sink_id.clone(),
                status: DiffStatus::Extra,
                differences: Vec::new(),
            });
        }
    }

    DiffReport { regressions }
}

/// Compare two sink outputs. Returns empty Vec if they match.
fn compare_sink_outputs(
    _sink_id: &str,
    baseline: &serde_json::Value,
    current: &serde_json::Value,
) -> Vec<FieldDiff> {
    let mut diffs = Vec::new();
    json_diff("", baseline, current, &mut diffs);

    // If JSON diff found differences in a "data" array, try semantic
    // comparison as a second pass.
    let has_data_diff = diffs.iter().any(|d| d.path.starts_with(".data"));
    if has_data_diff {
        if let (Some(baseline_data), Some(current_data)) =
            (baseline.get("data"), current.get("data"))
        {
            diffs.retain(|d| !d.path.starts_with(".data"));
            let semantic_diffs = compare_data_semantic(baseline_data, current_data);
            diffs.extend(semantic_diffs);
        }
    }

    diffs
}

/// Recursive JSON field-by-field comparison.
fn json_diff(
    prefix: &str,
    baseline: &serde_json::Value,
    current: &serde_json::Value,
    diffs: &mut Vec<FieldDiff>,
) {
    let path = |suffix: &str| {
        if prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{prefix}{suffix}")
        }
    };

    match (baseline, current) {
        (serde_json::Value::Object(b), serde_json::Value::Object(c)) => {
            let mut keys: Vec<&String> = b.keys().chain(c.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                match (b.get(key), c.get(key)) {
                    (Some(bv), Some(cv)) => json_diff(&path(&format!(".{key}")), bv, cv, diffs),
                    (Some(_), None) => diffs.push(FieldDiff {
                        path: path(&format!(".{key}")),
                        baseline: serde_json::Value::String("<present>".into()),
                        current: serde_json::Value::String("<missing>".into()),
                    }),
                    (None, Some(_)) => diffs.push(FieldDiff {
                        path: path(&format!(".{key}")),
                        baseline: serde_json::Value::String("<missing>".into()),
                        current: serde_json::Value::String("<present>".into()),
                    }),
                    (None, None) => {}
                }
            }
        }
        (serde_json::Value::Array(b), serde_json::Value::Array(c)) => {
            let len = b.len().max(c.len());
            for i in 0..len {
                match (b.get(i), c.get(i)) {
                    (Some(bv), Some(cv)) => json_diff(&path(&format!("[{i}]")), bv, cv, diffs),
                    (Some(_), None) => diffs.push(FieldDiff {
                        path: path(&format!("[{i}]")),
                        baseline: serde_json::Value::String("<present>".into()),
                        current: serde_json::Value::String("<missing>".into()),
                    }),
                    (None, Some(_)) => diffs.push(FieldDiff {
                        path: path(&format!("[{i}]")),
                        baseline: serde_json::Value::String("<missing>".into()),
                        current: serde_json::Value::String("<present>".into()),
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => {
            if baseline != current {
                diffs.push(FieldDiff {
                    path: path(""),
                    baseline: baseline.clone(),
                    current: current.clone(),
                });
            }
        }
    }
}

/// Semantic comparison for `data` arrays — tolerates arrow type differences.
fn compare_data_semantic(
    baseline: &serde_json::Value,
    current: &serde_json::Value,
) -> Vec<FieldDiff> {
    // Extract baseline elements as &serde_json::Value references.
    let baseline_arr = match baseline.as_array() {
        Some(arr) => arr,
        None => {
            // Can't parse as array — keep JSON-level diffs.
            let mut diffs = Vec::new();
            json_diff("data", baseline, current, &mut diffs);
            return diffs;
        }
    };

    // Symmetric check: current must also be an array before attempting Arrow conversion.
    if current.as_array().is_none() {
        let mut diffs = Vec::new();
        json_diff("data", baseline, current, &mut diffs);
        return diffs;
    }

    let baseline_refs: Vec<&serde_json::Value> = baseline_arr.iter().collect();

    // Convert current to Arrow arrays.
    let c_arrays = match json_to_arrow_arrays(current) {
        Ok(a) => a,
        Err(_) => {
            // Can't parse as Arrow — keep JSON-level diffs.
            let mut diffs = Vec::new();
            json_diff("data", baseline, current, &mut diffs);
            return diffs;
        }
    };

    let b_len = baseline_refs.len();
    let c_len = c_arrays.len();

    if b_len != c_len {
        return vec![FieldDiff {
            path: "data.length".into(),
            baseline: serde_json::json!(b_len),
            current: serde_json::json!(c_len),
        }];
    }

    let mut diffs = Vec::new();
    for i in 0..b_len {
        let result = sink::compare_semantic(&[baseline_refs[i]], &c_arrays[i..i + 1], None);
        if !result.r#match {
            for d in &result.differences {
                diffs.push(FieldDiff {
                    path: format!("data[{i}]"),
                    baseline: serde_json::json!(d.message),
                    current: serde_json::json!(""),
                });
            }
        }
    }
    diffs
}

/// Convert a JSON `data` array value to Arrow ArrayRefs.
fn json_to_arrow_arrays(value: &serde_json::Value) -> Result<Vec<arrow::array::ArrayRef>, String> {
    use std::sync::Arc;

    let arrays = match value.as_array() {
        Some(arr) => arr,
        None => return Err("data is not an array".into()),
    };
    if arrays.is_empty() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();
    for elem in arrays {
        let data_val = elem.get("data").unwrap_or(elem);
        let arr: arrow::array::ArrayRef = match data_val {
            serde_json::Value::Number(n) => {
                // Try Float64 first (preserves fractional parts),
                // then fall back to Int64 for whole numbers.
                if let Some(f) = n.as_f64() {
                    Arc::new(arrow::array::Float64Array::from(vec![f]))
                } else if let Some(i) = n.as_i64() {
                    Arc::new(arrow::array::Int64Array::from(vec![i]))
                } else {
                    return Err("non-numeric value in data array".into());
                }
            }
            _ => return Err("unsupported data type for semantic comparison".into()),
        };
        result.push(arr);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_identical() {
        let baseline: serde_json::Value = serde_json::json!({"data": [1, 2, 3], "count": 3});
        let current = baseline.clone();
        let diffs = compare_sink_outputs("test", &baseline, &current);
        assert!(diffs.is_empty(), "identical outputs should have no diffs");
    }

    #[test]
    fn test_compare_count_diff() {
        let baseline = serde_json::json!({"data": [1, 2, 3], "count": 3});
        let current = serde_json::json!({"data": [1, 2, 3], "count": 4});
        let diffs = compare_sink_outputs("test", &baseline, &current);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].path.contains("count"));
    }

    #[test]
    fn test_compare_data_diff() {
        let baseline = serde_json::json!({"data": [1, 2, 3], "count": 3});
        let current = serde_json::json!({"data": [1, 2, 99], "count": 3});
        let diffs = compare_sink_outputs("test", &baseline, &current);
        assert!(!diffs.is_empty(), "data difference should be detected");
    }

    #[test]
    fn test_replay_result_is_clean() {
        let mut sinks = HashMap::new();
        sinks.insert("s1".into(), serde_json::json!({"count": 1}));
        let meta = RecordingMetadata {
            dataflow_yaml: "dummy.yml".into(),
            recorded_at_unix: 0,
            timeout_secs: 10.0,
            dora_version: "test".into(),
        };
        let result = ReplayResult {
            metadata: meta,
            baseline_sinks: sinks.clone(),
            current_sinks: sinks,
            report: DiffReport {
                regressions: vec![],
            },
        };
        assert!(result.is_clean());
    }

    #[test]
    fn test_replay_result_assert_panics_on_regression() {
        let meta = RecordingMetadata {
            dataflow_yaml: "dummy.yml".into(),
            recorded_at_unix: 0,
            timeout_secs: 10.0,
            dora_version: "test".into(),
        };
        let mut baseline = HashMap::new();
        baseline.insert("s1".into(), serde_json::json!({"count": 1}));
        let current = HashMap::new();
        let report = DiffReport {
            regressions: vec![SinkDiff {
                sink_id: "s1".into(),
                status: DiffStatus::Missing,
                differences: vec![],
            }],
        };
        let result = ReplayResult {
            metadata: meta,
            baseline_sinks: baseline,
            current_sinks: current,
            report,
        };
        assert!(!result.is_clean());
    }

    #[test]
    fn test_diffreport_display_clean() {
        let report = DiffReport {
            regressions: vec![],
        };
        let display = report.to_string();
        assert!(display.contains("No regressions"));
    }

    #[test]
    fn test_diffreport_display_mismatch() {
        let report = DiffReport {
            regressions: vec![SinkDiff {
                sink_id: "s1".into(),
                status: DiffStatus::Mismatch,
                differences: vec![FieldDiff {
                    path: "count".into(),
                    baseline: serde_json::json!(3),
                    current: serde_json::json!(4),
                }],
            }],
        };
        let display = report.to_string();
        assert!(display.contains("Regressions detected"));
        assert!(display.contains("MISMATCH"));
        assert!(display.contains("count"));
    }
}
