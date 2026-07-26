# Progress Log

## Completed

| Week | Content | Status |
|------|---------|--------|
| 1-2 | API design + scaffold | ✅ |
| 3-4 | NodeHarness core | ✅ |
| 5 | TestSource + TestSink | ✅ |
| 6 | Echo pipeline + integration tests | ✅ |
| 7 | Edge cases + CI | ✅ |
| 8 | Multi-output + classifier + 3 pipelines | ✅ |
| 9 前半 | flume→tokio mpsc migration | ✅ |
| 9 后半 | `RecordSession` implementation | ✅ |

## Remaining Plan (Adjusted 2026-07-26)

| Week | Dates (China, Mon–Sun) | Deliverable |
|------|------|------|
| 9 后半 | 7/22–7/27 | `RecordSession::attach()` + `run()` + `save()` + 4 tests |
| 10 | 7/28–8/3 | `ReplaySession::load()` + `run()` + `assert_no_regression()` + diff + 5-8 tests |
| 11 | 8/4–8/10 | Upstream PR (a): `ToChannel` flume→tokio + regression test examples + integration |
| 12 | 8/11–8/17 | Debug + edge cases + docs polish |
| 13 | 8/18–8/24 | Demo prep + final submission (Coding Phase 2 deadline) |

## Week 9 后半 (2026-07-21): PR #35 merge conflicts + CI fix

### Merge conflicts resolved (upstream/main → week9)
- **5 conflicts across 5 files**, all manually resolved with rationale:
  - `.github/workflows/ci.yml` — kept HEAD (simplified, flume→tokio eliminated workarounds)
  - `docs/PROGRESS.md` — kept HEAD (deleted, `docs/` is in `.gitignore`)
  - `src/harness.rs` — kept HEAD (tokio comments, no spinlock sleep in Drop)
  - `tests/fixtures/classifier-dataflow.yml` — kept upstream (`tests/fixtures/` prefix)
  - `tests/fixtures/multi-echo-dataflow.yml` — hybrid: upstream paths + HEAD expected file

### CI deadlock diagnosed + fixed
- **Symptom**: `cargo test --test e2e` timed out after 30min in CI
- **Root cause**: Parallel NodeHarness tests (>2 threads) cause intermittent
  deadlock. DORA's daemon simulation uses `tokio::sync::mpsc::channel(5)`
  internally; with multiple concurrent harnesses, the daemon threads can't
  consume requests fast enough on low-CPU runners, the bounded channel fills,
  and `blocking_send` blocks permanently.
- **Fix**: `--test-threads=1` for e2e + smoke tests (adds ~3s, eliminates flakiness)
- **Also**: Added `dora/target` cache to `check` + `clippy` jobs (previously only
  `test` + `integration-test` had it)

### Commits
| Commit | Description |
|--------|-------------|
| `866c117` | Merge upstream/main into week9 — resolve 5 conflicts |
| `73af426` | perf(ci): add dora/target cache to check and clippy jobs |
| `3e9790e` | fix(ci): run e2e/smoke tests with --test-threads=1 to prevent deadlock |

### Verification
- `cargo check` ✅
- `cargo test --lib` ✅
- `cargo test --test e2e -- --test-threads=1` ✅ (4.23s)
- `cargo fmt` ✅

## Week 9 后半 (2026-07-26): RecordSession implementation

### RecordSession + Recording (new `src/record.rs`)
- **RecordSession**: builder-pattern API — `attach(yaml)` → `record_sink(id, output_file)` → `with_timeout(dur)` → `run()`
- **Recording**: `save(path)` / `load(path)` JSON persistence with metadata
- **RecordingMetadata**: dataflow_yaml, recorded_at_unix, timeout_secs, dora_version
- **RecordError**: 8-variant error enum with Display + Error + From impls
- Uses `dora run --stop-after <N>s` via `std::process::Command`

### SinkConfig record_mode
- `record_mode: bool` field added to `SinkConfig` (default false)
- When enabled, TestSink writes raw received data as `{"data": [...], "data_type": "...", "count": N}` instead of comparison result
- `--record-mode` CLI flag added to test-sink binary
- `write_record_output()` helper reuses existing Arrow→JSON serialization

### Bug fix
- `run_test_sink` reordered: record-mode early return now happens before expected file loading (previously `--record-mode` would fail if expected.json didn't exist)

### Tests (4 e2e tests)
| Test | Description |
|------|-------------|
| `record_echo_pipeline` | Full record-mode dataflow, verify metadata + sink data |
| `record_save_and_load_roundtrip` | save() → load() JSON round-trip fidelity |
| `record_dataflow_not_found` | Error on nonexistent YAML |
| `record_no_sinks_configured` | Error when no sinks registered |

### Commits
| Commit | Description |
|--------|-------------|
| `35d40f1` | feat(sink): add record_mode to SinkConfig for raw data capture |
| `f7d74e0` | feat(record): add RecordSession and Recording types |
| `0c0d4fb` | fix(record): sub-second timeout precision + empty version handling |
| `5c8dc44` | test(record): add e2e tests for RecordSession |
| `abc986f` | fix(record): resolve clippy redundant_closure warning |

### Verification
- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅
- `cargo test --lib` ✅ (46/46 pass)
- `cargo test --test e2e_record -- --test-threads=1` ✅ (4/4 pass in 4.12s)
- Existing e2e test `e2e_send_output_and_recv` hangs (pre-existing, unrelated)

### Files changed
| File | Change |
|------|--------|
| `src/record.rs` | NEW — RecordSession + Recording + RecordError |
| `src/sink.rs` | Add record_mode + write_record_output + unit test |
| `src/bin/test-sink.rs` | Add --record-mode CLI flag |
| `src/lib.rs` | pub mod record + re-exports |
| `tests/e2e_record.rs` | NEW — 4 e2e tests |

## Deferred / Post-Submission

- Upstream PR (b): `TestingInput::Channel` API proposal (follow-up)
- Python bindings (stretch goal)
- Investigate parallel NodeHarness deadlock root cause in DORA upstream
  (`tokio::sync::mpsc::channel(5)` in `init_with_options`)

