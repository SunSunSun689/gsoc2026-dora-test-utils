# demo/ — Record/Replay Regression Testing Demo

This directory holds the dataflow files the demo runs. It shows how to add
regression testing to a real DORA pipeline **without modifying the nodes
under test**.

## The pipeline

```
timer(10ms)  ──tick──▶ rust-node ──random──┬──▶ test-sink-random → sink_random_output.json
                                           │
timer(100ms) ──tick──▶ rust-status-node ◄─┘
                              │
                              └──status──▶ test-sink-status → sink_status_output.json
```

| Node | Origin | What it does |
|------|--------|--------------|
| `rust-node` | DORA official example, **unmodified** | Receives a timer tick every 10ms; emits a random UInt64 per tick. Uses `fastrand::seed(42)`, so the sequence is identical on every run. Exits after 100 events (upstream loop cap). |
| `rust-status-node` | DORA official example, **unmodified** | Receives the random values; reports each one as a status string. The string embeds a tick count that varies slightly between runs (timing jitter) — this is *non-deterministic output*, not a regression. |
| `test-sink-random` | **Ours** — the only addition | Sits at `rust-node`'s output and writes every received value to `sink_random_output.json` (record mode). |
| `test-sink-status` | **Ours** — the only addition | Sits at `rust-status-node`'s output and records the status strings. |

Adding regression coverage to the whole pipeline = adding the two
`test-sink-*` nodes (2 YAML entries). Nothing else changes.

## How the test works

Record once, replay always, compare:

1. **Record** (`demo_replay.rs` Step 1) — run `demo/rust-dataflow.yml` for
   10s; `test-sink-random` records ~100 deterministic random values. The
   recording is saved as the baseline ("known-good standard").
2. **Clean replay** (Step 2) — run the same YAML again and compare.
   Identical → `is_clean() = true`, no regression. Comparison skips the
   `count` field (`ignore_paths`) and the whole status sink (`ignore_sink`),
   because those are timing noise, not behavior.
3. **Mutate** (Step 3) — switch to `demo/rust-dataflow-mutated.yml`, which
   changes only `rust-node`'s tick: 10ms → 200ms. Now only ~48 ticks arrive
   in the 10s window (under the 100-event cap), so the sink records ~48
   values instead of ~100.
4. **Regression detected** (Step 4) — replay the mutated dataflow and
   compare: `data.length: 100 -> 48` → `is_clean() = false`, DiffReport
   shows the mismatch, `assert_no_regression()` panics.

This is exactly how you would use the tool on your own dataflow: keep a
static YAML, record a baseline once, replay it in CI whenever the code or
configuration changes.

## Files

| File | Role |
|------|------|
| `rust-dataflow.yml` | **Baseline** — the file the demo runs in Steps 1-2 |
| `rust-dataflow-mutated.yml` | **Mutated** — same pipeline, rust-node tick 200ms; run in Steps 3-4 |
| `sink_*.json` | Recorded sink outputs (generated at runtime, git-ignored) |

Relative paths in the YAMLs resolve against this directory (dora behavior),
so both files work as-is from the repo root:

```bash
dora run demo/rust-dataflow.yml --stop-after 10s
```

## Run the full demo

```bash
./scripts/demo-final.sh
```

This clones/pins dora, builds everything (including the unmodified upstream
example nodes), runs the 4-step Record/Replay demo above, then runs the
full 116-test suite.
