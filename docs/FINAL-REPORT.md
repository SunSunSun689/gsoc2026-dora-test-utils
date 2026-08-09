# GSoC 2026 Final Report — dora-test-utils

> **Student:** SunSunSun689 | **Mentor:** bobdingAI
> **Period:** May–Aug 2026 | **Path:** Standard (2026-08-24)
> **Branch:** `week11` | **Repo:** [SunSunSun689/gsoc2026-dora-test-utils](https://github.com/SunSunSun689/gsoc2026-dora-test-utils)

---

## 1. Milestone Progress

| Week | Milestone | Delivered | Status |
|------|-----------|-----------|--------|
| 1–2 | API design + crate scaffold; DORA commit locked (`45436aad`, mentor-confirmed) | CLAUDE.md, design specs, milestone scaffolding | ✅ |
| 3–4 | NodeHarness core: `send_input` / `send_data` / `send_output` / `tick` / `recv_output` / `close_input` / `run_to_completion`; `IntoInputData` trait; Mock types | Complete | ✅ |
| 5 | TestSource / TestSink — library + CLI binaries | Complete (ahead of schedule; binaries were planned for Weeks 6–8) | ✅ |
| 6 | Echo pipeline + 4 integration tests + midterm demo script | Complete | ✅ |
| 7 | Edge-case tests + CI integration (PR #34; 4 CI issues diagnosed + fixed) | Complete | ✅ |
| 8 | Multi-output TestSource fix (Issue #3), `classifier-node`, 3-pipeline demo, CI deadlock root-cause diagnosis (flume spinlock) | Complete | ✅ |
| 9 | flume→tokio mpsc migration in crate (6 CI workarounds removed, 62 tests parallel-safe); vendored DORA patch removed (deferred-init model); PR #35 merge (5 conflicts resolved); `RecordSession` + `record_mode` | Complete | ✅ |
| 10 | `ReplaySession` + two-layer comparison + `DiffReport` (7 unit + 11 e2e tests); 2 code-review rounds (28 findings, 24 fixed); Weekly Sync posts | Complete | ✅ |
| 11 | DORA dep upgrade `45436aad` → `1fba721` (flume→tokio mpsc upstream, arrow 58→59), `flume` removed; integration tests no longer silently pass; demo polish | Complete | ✅ |
| 12 | Demo script (`demo-week12.sh`), README overhaul, +28 edge-case unit tests (52→80), docs polish for DORA upgrade | Complete (early) | ✅ |
| 13 | Final submission: demo prep + final report | In progress | 🚧 |

**Verdict: On schedule (Standard path, 2026-08-24).** TestSource/TestSink binaries were
delivered in Week 5 (ahead of plan), Record/Replay — the proposal's extended scope — were
delivered in Weeks 9–10, and Weeks 11–12 were spent on the DORA dependency upgrade,
edge-case hardening, and docs/demo polish. 291 commits on the `week11` branch.

---

## 2. Modules Delivered

### 2.1 NodeHarness (`src/harness.rs` — 481 lines) — **Stable**

Unit-test driver for single DORA nodes. No daemon required; runs inside plain `#[test]`
functions via the deferred-init model (see Decision 1).

| Method | Purpose |
|--------|---------|
| `new()` | Create harness, init output channel (node created lazily on first drive) |
| `send_input()` | Buffer a raw `TimedIncomingEvent` |
| `send_data()` | Convenience: inject data by ID (JSON or Arrow via `IntoInputData`) |
| `send_stop()` | Convenience: buffer a Stop event |
| `send_output()` | Send output from the node (triggers deferred init, auto-closes input) |
| `tick()` | Drive node to process one event, collect outputs |
| `recv_output()` | Retrieve captured outputs by ID |
| `close_input()` | No-op (kept for API compatibility — no live channel) |
| `run_to_completion()` | Auto-inject Stop, loop `tick()` until terminal event, return all events |

### 2.2 Mock Types (`src/mock/` — 298 lines) — **Stable**

Pure in-memory simulation — no real DORA node needed.

| Type | Purpose |
|------|---------|
| `MockEventStream` | Simulated event stream with multi-producer injection |
| `MockOutputSender` | Simulated output sender |
| `OutputCollector` | Collects outputs by ID for assertions |

### 2.3 IntoInputData Trait (`src/traits.rs` — 145 lines) — **Stable**

Format conversion for `NodeHarness::send_data()`: `serde_json::Value` → `InputData::JsonObject`,
`arrow::array::ArrayData` → JSON array → `InputData::JsonObject`.

### 2.4 TestSource (`src/source.rs` — 735 lines + `src/bin/test_source.rs`) — **Stable**

Injects test data into DORA dataflows from JSON files (`--data-file` / `--inline-data`).

- **Multi-output**: single shared `DoraNode` for all outputs (Issue #3 fix — one `Register`
  message, no daemon rejection)
- Int8–UInt64, Float32/64, LargeUtf8 type hints; each JSON element → separate Arrow array
- Library: `run_test_source(SourceConfig) -> Result<()>`

### 2.5 TestSink (`src/sink.rs` — 778 lines + `src/bin/test-sink.rs`) — **Stable**

Captures DORA outputs and compares with expected data.

- Two comparison modes: `strict` (JSON round-trip) and `semantic` (Arrow equality)
- `record_mode`: writes raw received data (`{"data": [...], "data_type": ..., "count": N}`)
  for RecordSession baselines
- Respects `data_type` hint from expected file; Library: `run_test_sink(SinkConfig) -> Result<SinkResult>`

### 2.6 Record / Replay (`src/record.rs` — 1,371 lines) — **Experimental**

The proposal's extended-scope regression-testing layer, delivered Weeks 9–10.

| Type | Purpose |
|------|---------|
| `RecordSession` | Builder API: `attach(yaml)` → `record_sink(id, file)` → `with_timeout(dur)` → `run()`; drives `dora run --stop-after Ns` |
| `Recording` / `RecordingMetadata` | JSON persistence (`save` / `load`) with dataflow YAML, timestamp, timeout, dora version |
| `ReplaySession` | Builder API: `load(baseline)` → `replay_sink(id, file)` → `with_timeout(dur)` → `run()` |
| `ReplayResult` | `is_clean()` / `diff()` / `assert_no_regression()` |
| `DiffReport` / `SinkDiff` / `FieldDiff` | Structured regression report — `Match` / `Mismatch` / `Missing` / `Extra`; `Display` + `Serialize` |
| `RecordError` (8) / `ReplayError` (10) | Typed error enums with `Display` + `Error` + `From` impls |

### 2.7 Demo & Fixtures

```
tests/fixtures/
├── echo-node.rs            # Pass-through node (also a [[bin]] target)
├── echo-dataflow.yml       # echo pipeline: test-source → echo-node → test-sink
├── multi-echo-dataflow.yml # multi-output echo (3 outputs)
├── classifier-dataflow.yml # test-source → classifier-node → test-sink
├── source-data.json / expected-output.json
examples/demo_replay.rs     # Record → Replay → regression detection demo
scripts/demo-week12.sh      # One-command build + demo + full test suite
```

---

## 3. Test Coverage

**109 tests total** (Week 11 final counts):

| Category | Count | Location | Notes |
|----------|-------|----------|-------|
| Library unit tests | 80 | `src/*.rs` | comparison logic, DiffReport, NodeHarness, TestSource/TestSink edge cases |
| E2E (NodeHarness) | 5 | `tests/e2e.rs` | input pipeline, output path, run_to_completion, full pipeline, Arrow data |
| Record e2e | 4 | `tests/e2e_record.rs` | record-mode dataflow, save/load round-trip, error paths (needs dora CLI) |
| Replay e2e | 11 | `tests/e2e_replay.rs` | clean replay, mismatch, Missing/Extra sinks, timeout/dataflow overrides (needs dora CLI) |
| Integration | 6 | `tests/integration.rs` | echo / multi-echo / classifier pipelines, type tolerance, 10-element + string data (needs dora CLI) |
| Smoke | 3 | `tests/smoke.rs` | binary availability |
| **Total** | **109** | | |

```
✅ cargo fmt -- --check                  — passes
✅ cargo clippy -- -D warnings           — zero warnings
✅ cargo test --lib                      — 80 passed
✅ cargo test --test e2e                 — 5 passed (parallel-safe)
✅ cargo test --test e2e_record          — 4 passed (--test-threads=1)
✅ cargo test --test e2e_replay          — 11 passed (--test-threads=1)
✅ cargo test --test integration         — 6 passed (--test-threads=1, dora CLI)
✅ cargo test --test smoke               — 3 passed
```

**CI** (`.github/workflows/ci.yml`, 5 jobs — all green on `main`):
`check`, `test` (lib + e2e + smoke, default parallelism), `clippy`, `fmt`,
`integration-test` (builds dora CLI, runs integration + record/replay e2e).

### Code Review History

| Phase | Findings | Fixed | Deferred |
|-------|----------|-------|----------|
| Weeks 4–6 (2 rounds) | 12 | 12 | 0 |
| Week 8 (Issues #3–#5) | 3 | 3 | 0 |
| Week 10 Round 1 | 15 | 13 | 2 |
| Week 10 Round 2 | 13 | 11 | 2 |
| **Total** | **43** | **39** | **4** |

Deferred items are documented with upgrade paths (see Section 6).

### Code Metrics

| Metric | Value |
|--------|-------|
| Commits (`week11` branch) | 291 |
| Rust source files (src/ + bins) | 12 |
| Library lines (src/, incl. bins) | ~4,190 |
| Test lines (tests/) | ~1,550 |

---

## 4. Key Technical Decisions

### 1. Deferred-init model (baked events) — `src/harness.rs`

Mentor's explicit Option 1 (Discussion #20, confirmed #28): drop live
runtime injection (`TestingInput::Channel`), buffer all events in a `Vec`,
create the node lazily with `TestingInput::Input` on first `tick` /
`run_to_completion` / `send_output`.

| Metric | Before | After |
|--------|--------|-------|
| e2e serial runtime | 4.23s (2/10 hangs) | 0.01s (0 hangs) |
| e2e parallel | deadlock | 0.00s — no `--test-threads` needed |
| Clean build | needs vendored dora clone + patch | `cargo fetch` only |
| CI | 4 jobs clone + patch dora | 0 (only `integration-test` clones dora for the CLI) |

This eliminated the daemon-thread deadlock (dora-rs/dora#2855), removed the
vendored DORA patch, and removed 6 CI workarounds (yield_now, Drop sleep,
retry×5, timeout, `#[ignore]`, `continue-on-error`).

### 2. Two-layer comparison — `src/record.rs`

`ReplaySession::run()` compares baseline vs current with:
- **Layer 1**: fast recursive JSON structural diff (`json_diff`) — field paths + values
- **Layer 2**: Arrow semantic comparison (`compare_data_semantic`) — tolerates
  type differences (e.g. Int32 vs Int64)

An empty Layer-2 result falls back to Layer-1 diffs, preventing false `Match`
results. Numbers compare via `as_f64()` (`3` vs `3.0` is a match).

### 3. Record / Replay architecture

Record once (`dora run --stop-after Ns` + `record_mode` sink writes raw data),
persist as JSON baseline with metadata; replay anytime and diff. Builder-pattern
APIs (`attach → record_sink → with_timeout → run`) mirror the natural test flow;
`assert_no_regression()` panics with a formatted diff for use in CI.

### 4. DORA dependency upgrade: `45436aad` → `1fba721` (Week 11)

Upstream dora-rs/dora migrated `TestingOutput::ToChannel` from `flume::Sender`
to `tokio::sync::mpsc::UnboundedSender` (commit `1fba721`, 2026-08-04) — the
exact change we had planned as Upstream PR (a). Week 11 consumed it:

- `Cargo.toml`: rev `45436aad` → `1fba721`; arrow 58 → 59; **`flume = "0.10"` removed**
- `harness.rs`: `flume::unbounded()` → `unbounded_channel()`; `UnboundedSender`/`UnboundedReceiver`
- 6 integration tests no longer silently pass when the dora CLI is missing:
  `require_dora()` panics in CI (`CI=true`), prints a visible warning locally

---

## 5. Upstream Contributions

### PR (a): `TestingOutput::ToChannel` flume → tokio mpsc — ✅ consumed

A complete, ready-to-submit PR plan was prepared (`docs/upstream-pr-plan.md`:
exact diffs for `integration_testing.rs`, `node_integration_testing.rs`,
`node/mod.rs`; commit message; verification steps; branch
`SunSunSun689/dora:testing-output-tokio-mpsc`).

**Outcome:** upstream dora-rs/dora merged the identical migration themselves
(`1fba721`, 2026-08-04). No PR submission needed — Week 11 upgraded the dep
and removed `flume` from our `Cargo.toml`.

### PR (b): `TestingInput::Channel` API proposal — ⏸ deferred

Runtime event-injection variant (`tokio::sync::mpsc::Receiver`-based) for
interactive harnesses, fuzzers, property-based tests. Requires a maintainer
discussion first (4 open design questions documented). Mentor-provided escape
hatch (baked events + deferred init) fully satisfies `NodeHarness` today, so
this is a general-ecosystem contribution to make post-submission.

**Upstream tracking:** dora-rs/dora#1603 (flume migration precedent), #2855
(daemon-thread deadlock).

---

## 6. Future Work

| Item | Notes |
|------|-------|
| Upstream PR (b): `TestingInput::Channel` | Full design in `docs/upstream-pr-plan.md`; needs maintainer discussion |
| `traits.rs` data_type fidelity | Passing Arrow types through requires two-level Struct wrapping; documented with `InputData::ArrowFile` upgrade path |
| `sink.rs` receive timeout | `EventStream` has no timeout API; dataflows use `--stop-after` via Record/ReplaySession |
| Python bindings | Stretch goal from proposal |
| GitHub Actions CI template | Reusable testing template for DORA node developers |
| Parallel NodeHarness root cause | Upstream `tokio::sync::mpsc::channel(5)` in `init_with_options` — bounded-channel pressure on low-CPU runners |
| Merge into dora-rs/dora | Final home of this crate per proposal |

---

## 7. Lessons Learned

### Technical

- **flume spinlock vs tokio mpsc.** flume 0.10's internal spinlock deadlocks on
  preemptive kernels when the lock holder is preempted (2-vCPU CI runners,
  near-100% failure; local machines ~30%). Tokio mpsc (`std::sync::Mutex`-based)
  eliminates it. Root-causing this (Week 8) and aligning with the upstream
  migration (Weeks 9–11) removed every CI workaround.
- **Deferred init is simpler and more reliable.** Baked events + lazy node
  creation removed live channels, Drop ordering, sleeps, and an entire class of
  test flakiness — with no API cost to users.
- **Two-layer comparison beats one.** Fast structural diff for debuggability,
  semantic Arrow layer for type tolerance — and fallbacks that prevent false
  matches were the exact bugs code review found.
- **Code review finds real bugs.** Two Week-10 rounds surfaced 28 findings:
  over-broad `.data` prefix matching, dead `as_f64` branches, NaN panics in
  timeout parsing, false Match on empty semantic results, tests with zero CI
  coverage (e2e_replay was missing from CI entirely).
- **Edge cases are where data-conversion bugs live.** +28 tests in Week 12
  caught UInt16 overflow, fractional→Int64 rejection, heterogeneous-array
  rejection, Null conversion errors, and boolean arrays.
- **CI is the canary.** The deadlock appeared on CI days before local machines;
  silent-skip guards (Issue #2) let tests pass without running — fixed by
  `require_dora()` which panics in CI.

### Process

- **Mentor alignment pays off.** Weekly syncs (Discussions #17/#20/#28) drove
  the architecture — the deferred-init pivot, the vendored-patch removal, and
  the upstream PR strategy all came from mentor direction.
- **Spec before code.** The ReplaySession design spec (`docs/`) fixed gaps
  (stop-after precision, stale-file deletion, metadata overrides) before
  implementation, and the two review rounds still found more.
- **Progress recording discipline.** `docs/PROGRESS.md` as a single source of
  truth (milestones, test counts, commits, decisions) made this report — and
  the midterm one — nearly mechanical to produce.
- **Test counts as contract.** 109 tests across 6 categories with a README table
  makes progress measurable week over week.

---

*Report generated 2026-08-09. Final submission deadline: 2026-08-24 (Standard path).*
