# ReplaySession Design

> Based on proposal §5.4, mentor Discussions #20/#28, brainstorming session 2026-07-27.

## Purpose

ReplaySession provides regression testing for DORA dataflows. It loads a
previously saved `Recording` (produced by `RecordSession`), reruns the same
dataflow to collect fresh sink outputs, and compares them against the baseline.
If outputs have changed, it produces a detailed diff and optionally panics.

## API

```rust
pub struct ReplaySession {
    recording: Recording,
    dataflow_override: Option<PathBuf>,
    timeout_override: Option<Duration>,
}

pub struct ReplayResult {
    pub metadata: RecordingMetadata,
    pub baseline_sinks: HashMap<String, serde_json::Value>,
    pub current_sinks: HashMap<String, serde_json::Value>,
    report: DiffReport,
}
```

### Constructors

| Method | Signature | |
|--------|-----------|----|
| `load` | `fn load(path: impl AsRef<Path>) -> Result<Self, ReplayError>` | Load saved Recording JSON |

### Configuration

| Method | Signature | |
|--------|-----------|----|
| `dataflow` | `fn dataflow(mut self, path: impl Into<PathBuf>) -> Self` | Override YAML path from Recording |
| `with_timeout` | `fn with_timeout(mut self, timeout: Duration) -> Self` | Override timeout from Recording |

### Execution

| Method | Signature | |
|--------|-----------|----|
| `run` | `fn run(self) -> Result<ReplayResult, ReplayError>` | Rerun dataflow, collect outputs, compare |

### Queries (on ReplayResult)

| Method | Returns | |
|--------|---------|----|
| `is_clean` | `bool` | true if all sinks match baseline |
| `diff` | `&DiffReport` | Structured diff for programmatic inspection |
| `assert_no_regression` | (panics) | `assert!`-style: panics with human-readable diff if regressions exist |

## DiffReport

```
DiffReport
  ├── regressions: Vec<SinkDiff>

SinkDiff
  ├── sink_id: String
  ├── status: DiffStatus    // Match | Mismatch | Missing | Extra
  └── differences: Vec<FieldDiff>

FieldDiff
  ├── path: String          // e.g. "data[2]" or "count"
  ├── baseline: Value
  └── current: Value
```

### Display output (example)

```
Regression in 'test-sink': MISMATCH
  data[2]: 3 -> 4

Regression in 'other-sink': MISSING
  (present in baseline, absent in current)
```

## Comparison strategy (two-layer)

1. **Fast pass**: recursive `serde_json::Value` field-by-field `==`.  If clean, done.
2. **Semantic fallback**: for `data` array fields that differ in layer 1,
   attempt Arrow semantic comparison (`compare_semantic` from `src/sink.rs`).
   This tolerates Int32→Int64, Float32→Float64, etc.  Only if semantic also
   fails is the difference reported.
3. **Sink presence**: baseline sink missing in current → `Missing`.  Extra
   sink in current not in baseline → `Extra`.

## YAML path resolution

1. Use `dataflow_override` if set.
2. Otherwise use `Recording.metadata.dataflow_yaml` (absolute path from record
   time).  If the file doesn't exist, return `ReplayError::DataflowNotFound`.

## Error type

Extend `RecordError` to cover replay scenarios, or add variants specific to
replay.  Tentative:

```
RecordError::DataflowNotFound(path)
RecordError::RunFailed { status, stderr }
RecordError::SinkOutputMissing { sink_id, path }
RecordError::SinkReadError { sink_id, error }
RecordError::Io(io::Error)
RecordError::Json(serde_json::Error)
```

`NoSinksConfigured` is not applicable to ReplaySession (baseline always has
sinks).  `DoraNotFound` is reused from RecordSession.

## File layout

```
src/record.rs         ← RecordSession + Recording + RecordError (existing)
                      ← ReplaySession + ReplayResult + DiffReport (new)

tests/e2e_replay.rs   ← new test file, 5-8 tests
```

## Tests (5-8)

| Test | Description |
|------|-------------|
| `replay_clean` | Run echo pipeline, save baseline, replay — clean |
| `replay_regression` | Modify source data, replay — detect mismatch |
| `replay_dataflow_not_found` | Recording with nonexistent YAML → error |
| `replay_load_invalid_json` | Load nonexistent/garbage file → error |
| `replay_override_dataflow` | `dataflow()` override works when original absent |
| `replay_timeout_override` | `with_timeout()` override works |
| `replay_sink_missing` | Baseline has sink, replay doesn't → Missing |
| `replay_sink_extra` | Replay produces extra sink → Extra |

## Not in scope

- Async `run()` — synchronous, consistent with RecordSession
- Replaying partial sinks — always replays all sinks in the recording
- Modifying the dataflow YAML between record and replay — this is a regression
  test, the same YAML should be used
