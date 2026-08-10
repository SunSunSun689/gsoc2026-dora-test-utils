# Progress Log

## Week 11 后半 (2026-08-10): Demo bug fix — 3 bugs in demo_replay.rs

### Context

`scripts/demo-final.sh` Step 4 未检测到回归，`is_clean()` 始终返回 `true`。

### Root cause (3 bugs)

| # | Bug | Impact |
|---|-----|--------|
| 1 | Step 4 未调用 `.dataflow(&mutated_yaml)` — ReplaySession 始终使用 baseline metadata 中的原始 YAML | 永远比较 baseline vs. baseline → `is_clean() = true` |
| 2 | `rust-dataflow` example 输出非确定性 — `status-node` 输出嵌入了 `after N ticks`，N 取决于两次独立 `dora run` 的 timer 启动时机（毫秒级抖动） | Step 2 clean replay 也报假 regression |
| 3 | YAML 中使用相对路径 — dora spawn 节点的工作目录不是 repo root，`--data-file demo/xxx.json` 找不到文件 | dataflow 启动失败 |

### Fix

- **Bug 1**: 在 Step 4 的 ReplaySession 链中添加 `.dataflow(&mutated_yaml)`
- **Bug 2**: Demo 从 `rust-dataflow` example 切换到**确定性 echo pipeline**（test-source → echo-node → test-sink）。test-source 从固定 JSON 文件读取数据，每次输出完全一致
- **Bug 3**: `demo_replay.rs` 动态生成临时 YAML，所有路径使用**绝对路径**（与 integration tests 一致）

### Files changed

| File | Change |
|------|--------|
| `examples/demo_replay.rs` | 重写：`generate_yaml()` 生成绝对路径 YAML + echo pipeline + `.dataflow()` fix |
| `scripts/demo-final.sh` | dora example build 替换为 echo-node build |
| `demo/demo-source-baseline.json` | NEW — 确定性源数据 `[10, 20, 30]` |
| `demo/demo-source-mutated.json` | NEW — 变异源数据 `[10, 20, 30, 999]` |
| `demo/demo-baseline.yml` | 改为参考文件（运行时生成绝对路径版本） |
| `demo/demo-mutated.yml` | 同上 |

### Verification

```
Step 1 (Record baseline):       ✅ baseline json saved
Step 2 (Replay same YAML):      ✅ is_clean() = true, assert_no_regression() passed
Step 3 (Show mutated YAML):     ✅ mutated YAML ready
Step 4 (Replay mutated YAML):   ✅ is_clean() = false
    DiffReport: .count: Number(3) -> Number(4)
                data.length: Number(3) -> Number(4)
    assert_no_regression() panics correctly ✅
```

- `cargo build --example demo_replay` ✅
- Full lib + e2e test suite unchanged (109 tests)

### Design note

Demo 中使用确定性 echo pipeline 而非 DORA `rust-dataflow` example 的原因是：两个独立的 `dora run` 之间 timer 启动有毫秒级抖动，`status-node` 输出中的 `after N ticks` 不稳定（N=0 或 1）。保留 `demo/rust-dataflow.yml` 和 `demo/rust-dataflow-mutated.yml` 作为参考文件。工具同样适用于任何确定性 DORA 数据流。

## DORA Version

**Pinned**: `1fba7214b79d8488229f6cc2027b9760dec4d6df` (was `45436aad`)
**Updated**: 2026-08-09 — flume→tokio migration merged upstream; no upstream PR needed
**Confirmed**: 2026-06-01 Weekly Sync (Discussion #17), mentor ZhangHanDong (original `45436aad`)
**Reason**: `1fba721` is the commit that migrated `TestingOutput::ToChannel` from flume to tokio mpsc
**Dependency**: `dora-node-api = { git = "...", rev = "1fba721..." }` (git dep, no vendored clone)
**Upstream tracking**: [dora-rs/dora#2855](https://github.com/dora-rs/dora/issues/2855)

> On 2026-07-27 we evaluated upgrading to v1.0.0-rc.4 and latest main.
> The patch applies cleanly to both, but rc.4 upgrades arrow 58→59
> and latest main adds `dora-examples` workspace member.  Decision: stay
> on mentor-confirmed `45436aad` until mentor approves a version bump.

## Week 9 后半 (2026-07-27): Removed vendored DORA patch (mentor Option 1)

### Context

Mentor ZhangHanDong recommended in Discussion #20 (Week 3) and confirmed in
Discussion #28 (Week 7): drop `TestingInput::Channel` live-runtime-injection,
use baked events (`TestingInput::Input`) with deferred node construction.
This eliminates the daemon-thread deadlock, removes the vendored dora fork,
and lets the crate build from a clean `git clone` + `cargo test`.

### Changes

- **`Cargo.toml`**: `dora-node-api` from `path = "dora/apis/rust/node"` to
  `git = "https://github.com/dora-rs/dora.git", rev = "45436aad..."`.
  Added `flume = "0.10"` for output channel (upstream default for
  `TestingOutput::ToChannel`).
- **`src/harness.rs`**: Complete refactor to deferred-init model.
  `new()` no longer creates a DoraNode — it only creates the output channel.
  `send_data`/`send_stop`/`send_input` buffer events into a `Vec`.
  `tick`/`run_to_completion`/`send_output` call `ensure_init()` which creates
  the node lazily with `TestingInput::Input(IntegrationTestInput::new(...))`.
  Removed `input_tx` (no live channel), removed Drop impl (no cleanup needed),
  removed 500ms sleep in `send_input` (not needed without live channel).
  `close_input()` is now a no-op (kept for API compatibility).
- **`.github/workflows/ci.yml`**: Removed `git clone dora` + `git apply` +
  `dora/target` cache from `check`, `test`, `clippy` jobs.  Only
  `integration-test` keeps the dora clone (needs CLI binary).  e2e/smoke
  tests now run with default parallelism — no `--test-threads=1` needed.
- **`dora-patches/`**: Deleted.  The vendored patch is no longer needed.
- **`src/lib.rs`**: Updated docs to reflect deferred-init model.

### Results

| Metric | Before | After |
|--------|--------|-------|
| dora dep | vendored path + patch | git dep, clean checkout |
| e2e serial | 4.23s (2/10 hang) | 0.01s (0/10 hang) |
| e2e parallel | hangs (deadlock) | 0.00s (no --test-threads needed) |
| Clean build | ❌ (needs dora clone + patch) | ✅ (cargo fetch only) |
| CI steps | 4 jobs clone+patch dora | 0 (only integration-test clones dora) |

### Mentor alignment

This refactor follows the mentor's explicit direction from Discussion #20
(Option 1 — baked events + deferred init) and Discussion #28 (remove
vendored Channel patch, submit upstream PR for ToChannel flume→tokio
migration separately).

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
| 10 | `ReplaySession` implementation | ✅ |

## Week 10 (2026-07-27): ReplaySession implementation

### ReplaySession (new in `src/record.rs`)
- **ReplaySession**: builder-pattern API — `load(json)` → `replay_sink(id, file)` → `with_timeout(dur)` → `run()`
- **ReplayResult**: `is_clean()` / `diff()` / `assert_no_regression()`
- **DiffReport**: structured regression report with `SinkDiff`/`FieldDiff`/`DiffStatus`
- **ReplayError**: 10-variant error enum with Display + Error + From impls
- **Two-layer comparison**: Layer 1: fast JSON structural diff; Layer 2: Arrow semantic comparison for `data` arrays (tolerates type differences)
- Reuses RecordSession's `find_dora_binary()` and `dora run` infrastructure

### Tests (7 unit + 11 e2e)
| Type | Count | Location |
|------|-------|----------|
| Unit tests (comparison logic) | 7 | `src/record.rs` |
| E2E tests | 11 | `tests/e2e_replay.rs` |

### Commits
| Commit | Description |
|--------|-------------|
| `c81efec` | feat(replay): add ReplayError type |
| `b1a4f8a` | feat(replay): add DiffReport and supporting types |
| `93a4439` | feat(replay): add ReplaySession struct and builder methods |
| `2922e03` | feat(replay): implement ReplaySession::run() with two-layer comparison |
| `b7d74fe` | fix(replay): address C1, C2, I3, M1 in comparison logic |
| `11ac7d6` | test(replay): add unit tests for DiffReport and comparison logic |
| `3234bf2` | test(replay): add 8 e2e tests for ReplaySession |
| `8ce3915` | chore: fmt, update PROGRESS.md + API stability table |
| `6075132` | fix(replay): critical — starts_with(".data") for Arrow semantic fallback |
| `5462bc8` | test(replay): add timeout override, Missing, Extra e2e tests |

### Verification
- `cargo check` ✅
- `cargo test --lib` ✅ (52/52 pass)
- `cargo test --test e2e` ✅ (5/5 pass)
- `cargo test --test e2e_replay -- --test-threads=1` ✅ (11/11 pass)
- `cargo test --test smoke` ✅ (3/3 pass)
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅

## Week 10 后半 (2026-08-01): Code review fixes (13 issues)

Code review of the Week 10 ReplaySession implementation found 15 issues.
13 were fixed, 2 deferred (see below).

### Fixes applied

| # | File | Fix |
|---|------|-----|
| 1 | `record.rs:757-779` | `.data` prefix → exact match (`.data` / `.data[`) — no longer matches `.data_type` etc. |
| 2 | `record.rs:921-933` | `json_to_arrow_arrays`: `as_i64()` before `as_f64()` — Int64 branch no longer dead code |
| 3 | `record.rs:862-866` | `compare_data_semantic`: unwrap `.data` from baseline elements symmetrically with current |
| 4 | `record.rs:408-410, 566-570` | `--stop-after` uses `.ceil() as u64` whole seconds — dora CLI parser rejects decimals |
| 5 | `harness.rs:324-329, 289-294` | `ensure_init` clears pending_events when node already initialized; `run_to_completion` skips Stop injection when already init |
| 6 | `record.rs:382-388` | `SinkNotInBaseline` hard error removed — unknown sinks flow through as Extra in DiffReport |
| 7 | `record.rs:762-787` | Capture json-level data diffs before `retain`, enrich semantic diffs with actual values |
| 8 | `.github/workflows/ci.yml:87` | Add `cargo test --test e2e_replay` to CI (was missing — 11 tests had zero CI coverage) |
| 10 | `sink.rs:396-406` | `write_record_output`: use `serde_json::to_value` instead of `format!("{:?}")` for data_type |
| 11 | `source.rs:243-253` | `number_to_arrow_array` no-hint branch: try `as_u64()` before `as_f64()` |
| 13 | `record.rs:388-393, 566-570` | Delete stale output files before `dora run` in both RecordSession and ReplaySession |
| 14 | `record.rs:637-643` | `dataflow_yaml` canonicalized via `.canonicalize()` before storing in metadata |
| 15 | `tests/e2e_replay.rs:342` | Dead `replay_timeout_override` test replaced with meaningful API verification |

### Deferred

| # | Issue | Reason |
|---|-------|--------|
| 9 | `traits.rs` data_type loss | Passing Arrow type through requires two-level Struct wrapping to match DORA internals (`{"inner": ...}`) — documented with `InputData::ArrowFile` upgrade path |
| 12 | `sink.rs` receive timeout | EventStream has no timeout API; dataflows always use `--stop-after` via Record/ReplaySession |

### Test renamed

`replay_sink_not_in_baseline` → `replay_sink_not_in_baseline_reported_as_extra` (behavior changed: no longer a hard error, reported as Extra in DiffReport)

### Commits

| Commit | Description |
|--------|-------------|
| `dfa071a` | fix: 13 code-review findings — comparison correctness, CI coverage, type fidelity |

### Verification

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅ (zero warnings)
- `cargo test --lib` ✅ (52/52 pass)
- `cargo test --test e2e` ✅ (5/5 pass)
- `cargo test --test e2e_record -- --test-threads=1` ✅ (4/4 pass)
- `cargo test --test e2e_replay -- --test-threads=1` ✅ (11/11 pass)
- `cargo test --test smoke -- --test-threads=1` ✅ (3/3 pass)

## Week 10 后半 (2026-08-03): Weekly Sync discussion posts

### Changes

- **`docs/discussions/weekly-sync-posts.md`**: 4 weekly sync discussion posts for GitHub Discussions (Week 8/9/10/12), covering all work since mid-July

### Verification

- Posts are ready to copy-paste to GitHub Discussions, Category: `Weekly Sync`

---

## Week 12 (2026-08-02): Demo script, README, edge-case tests

### Changes

- **`examples/demo_replay.rs`**: Record→Replay→regression detection showcase
- **`scripts/demo-week12.sh`**: full demo script (build + demo + test suite)
- **`README.md`**: Updated API docs, Record/Replay examples, test counts (52→80), API stability table
- **Edge-case tests**: +28 unit tests (80 total) covering compare_recordings, json_diff, compare_data_semantic, DiffReport, NodeHarness, TestSink, TestSource

### Commits

| Commit | Description |
|--------|-------------|
| `98d1f24` | docs: Week 12 — demo script, README update, edge-case tests |
| `5abca01` | docs: annotate upstream flume->tokio-mpsc migration plan |

### Verification

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅
- `cargo test --lib` ✅ (80/80 pass)
- `cargo test --test e2e` ✅ (5/5 pass)

### Week 12 后半 (2026-08-09): Docs polish — reflect DORA upgrade

Updated all docs to reflect Week 11 changes:

| File | Changes |
|------|---------|
| `README.md` | Test counts 81→109, Week 11 status, CI description |
| `docs/ISSUES-FOR-MENTOR.md` | Issue 1 (flume) → Resolved; Issue 2 (silent test) → Fixed; Summary table updated |
| `docs/upstream-pr-plan.md` | PR (a) status banner — upstream already merged |
| `docs/mentor-checkpoints-week10.md` | DORA version updated from `45436aad` to `1fba721` |
| `docs/WEEKLY_REPORT-week10-12.md` | Q4: Upstream PR (a) resolved |
| `docs/CI-DEADLOCK-FIX.md` | Resolution note at top |
| `docs/discussions/weekly-sync-posts.md` | Q3 resolved, Upstream PR references updated |

### Commits

| Commit | Description |
|--------|-------------|
| `a63a38b` | docs: Week 12 — update all docs for DORA upgrade and resolved issues |

### Verification

- `cargo test --lib` ✅ (80/80 pass)

## Week 11 (2026-08-09): DORA upgrade + integration test fix + demo polish

### 1. DORA dep upgrade — flume→tokio already migrated upstream

Upstream dora-rs/dora **main** already migrated `TestingOutput::ToChannel`
from `flume::Sender` to `tokio::sync::mpsc::UnboundedSender` (commit `1fba721`,
2026-08-04).  Week 11's planned Upstream PR (a) was not needed.

| File | Change |
|------|--------|
| `Cargo.toml` | DORA rev: `45436aad` → `1fba721`; arrow: 58 → 59; removed `flume = "0.10"` |
| `src/harness.rs` | `flume::unbounded()` → `unbounded_channel()`; types → `UnboundedSender`/`UnboundedReceiver` |
| `src/lib.rs` | Updated doc comment about output channel |

### 2. Fix: integration tests no longer silently pass (Issue #2)

6 integration tests in `tests/integration.rs` silently returned green when
`dora` CLI was missing.  Replaced `dora_available()` guard with `require_dora()`:
panics in CI (`CI=true`), prints visible ⚠️ warning locally and skips.

### 3. Demo polish

Rewrote `examples/demo_replay.rs` for final submission quality:
- `--dora` and `--dataflow` CLI flags
- `CARGO_BIN_EXE_*` env var for bin discovery
- Precondition checks with build hints
- Recording metadata + sink data preview
- `assert_no_regression()` panic verification
- Structured 4-step output with ✅/❌ markers

Also updated `scripts/demo-week12.sh` for week11 branch.

### Commits

| Commit | Description |
|--------|-------------|
| `169680e` | feat: upgrade DORA dep 45436aad → 1fba721, remove flume |
| `c1897ee` | fix: integration tests no longer silently pass when dora CLI is missing |
| `99d42ea` | docs(demo): enhance demo_replay — CLI args, structured output, metadata display |

### Verification

- `cargo check` ✅
- `cargo fmt --check` ✅
- `cargo clippy --lib` ✅ (zero warnings)
- `cargo test --lib` ✅ (80/80 pass)
- `cargo test --test e2e -- --test-threads=1` ✅ (5/5 pass)
- `cargo test --test e2e_record -- --test-threads=1` ✅ (4/4 pass)
- `cargo test --test e2e_replay -- --test-threads=1` ✅ (11/11 pass)
- `cargo test --test smoke -- --test-threads=1` ✅ (3/3 pass)
- `cargo test --test integration -- --test-threads=1` ✅ (6/6 pass)

### Resolved

- ~~Upstream PR (a): ToChannel flume→tokio~~ — upstream already did it
- ~~`flume = "0.10"` dependency~~ — removed
- ~~`harness.rs` TODO comment~~ — resolved
- ~~Integration tests silent pass (Issue #2)~~ — CI panics, local warns

## Remaining Plan (Adjusted 2026-08-09)

| Week | Dates (China, Mon–Sun) | Deliverable |
|------|------|------|
| 9 后半 | 7/22–7/27 | `RecordSession::attach()` + `run()` + `save()` + 4 tests |
| 10 | 7/28–8/3 | `ReplaySession::load()` + `run()` + `assert_no_regression()` + diff + 5-8 tests |
| 11 | 8/4–8/10 | ~~Upstream PR (a)~~ → Update DORA rev to post-migration commit + switch harness to tokio mpsc |
| 12 | 8/11–8/17 | ~~Debug + edge cases + docs polish~~ → docs done (2026-08-09), edge cases done (Week 10) |
| 13 | 8/18–8/24 | ~~Demo prep + final submission~~ → ✅ 已提前完成 (2026-08-09) — report + demo + CI 全部就绪 |

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

## Week 13 (2026-08-09): Final submission

Week 13 (8/18–8/24) is the final coding phase week — Coding Phase 2 ends
2026-08-24 23:59 UTC. All final submission deliverables prepared.

### Changes

- **`docs/FINAL-REPORT.md`**: GSoC 2026 final report — project summary, 109-test
  coverage contract, API stability table, deferred items (296 lines)
- **`scripts/demo-final.sh`**: final submission demo script — build + Record/Replay
  regression demo + full test suite (163 lines)
- **`demo/rust-dataflow.yml`**: DORA 官方 rust-dataflow example + test-sink 录制
- **`demo/rust-dataflow-mutated.yml`**: 同上，timer 100ms→50ms（触发回归）
- **`examples/demo_replay.rs`**: 重写 — 使用静态 YAML 文件，不再动态生成
- **`.github/workflows/ci.yml`**: dora clone pin 45436aad → 1fba721

### Demo 设计

Demo 从 trivial echo pipeline 升级为 DORA 官方 `rust-dataflow` example：
- rust-node 和 rust-status-node **一行不改**（证明工具的非侵入性）
- 只加一个 test-sink 节点（1 行 YAML）= 回归测试能力
- 回归触发：timer 100ms→50ms，输出频率翻倍 → DiffReport 检测到 count 差异

- **`docs/WEEKLY_REPORT-week13.md`**: mentor 周报 — Week 11–13 成果总结

### Commits

| Commit | Description |
|--------|-------------|
| `7a0201a` | docs: add GSoC 2026 final report |
| `5a82d31` | feat: add final submission demo script |
| `76d5173` | fix(ci): update dora clone pin 45436aad → 1fba721 |
| `53f0c1b` | fix(demo): add dora clone prerequisite check and cd guard |
| `6881771` | docs: update FINAL-REPORT.md commit count |
| `b7b09d5` | docs: fix PROGRESS.md Week 13 — add missing commit, fix demo description |
| `e20f472` | feat(demo): add rust-dataflow YAML with test-sink for recording |
| `d0ab9f8` | feat(demo): add mutated rust-dataflow YAML (timer 100→50ms) |
| `751b0a9` | feat(demo): use DORA rust-dataflow example for Record/Replay demo |
| `ee01ca7` | fix(demo): build dora example packages in demo-final.sh |
| `37007c5` | fix(demo): correct YAML paths for demo/ directory |
| `96ff23e` | docs: update PROGRESS.md — Week 13 demo redesign + full commit log |

### Test counts

- Unchanged: **109 tests** across 6 categories (80 lib + 5 e2e + 4 e2e_record +
  11 e2e_replay + 6 integration + 3 smoke) — same as Week 11 final counts

## Deferred / Post-Submission

- ~~Upstream PR (a): `ToChannel` flume→tokio~~ — ✅ done (upstream `1fba721`, consumed 2026-08-09)
- Upstream PR (b): `TestingInput::Channel` API proposal (follow-up)
- Python bindings (stretch goal)
- Investigate parallel NodeHarness deadlock root cause in DORA upstream
  (`tokio::sync::mpsc::channel(5)` in `init_with_options`)

