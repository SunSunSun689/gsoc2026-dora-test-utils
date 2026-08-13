#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# dora-test-utils — Final Submission Demo
# ─────────────────────────────────────────────────────────────
# Showcases RecordSession → ReplaySession regression testing:
#   1. Record a baseline from DORA's rust-dataflow example (upstream nodes unmodified)
#   2. Replay → verify no regression (clean; ignore_paths + ignore_sink)
#   3. Mutate the dataflow (rust-node tick 10ms → 200ms)
#   4. Replay → detect regression with structured diff report
#
# Also runs the full test suite (116 tests).
# ─────────────────────────────────────────────────────────────
set -euo pipefail

# Run from the repo root regardless of the calling directory
cd "$(dirname "$0")/.."

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

banner() {
    echo ""
    echo -e "${CYAN}${BOLD}═══ $1 ═══${NC}"
    echo ""
}

step() {
    echo -e "${GREEN}▶ $1${NC}"
}

warn() {
    echo -e "${RED}⚠ $1${NC}"
}

ok() {
    echo -e "${GREEN}✔ $1${NC}"
}

# ─── 0. Prerequisites ────────────────────────────────────
banner "0. Check prerequisites"

step "Check Rust toolchain..."
cargo --version
rustc --version

step "Check dora source..."
if [ ! -f dora/binaries/cli/Cargo.toml ]; then
    echo "dora source not found — cloning..."
    git clone https://github.com/dora-rs/dora.git dora
    git -C dora checkout 1fba7214b79d8488229f6cc2027b9760dec4d6df
    ok "dora cloned and checked out at 1fba721"
else
    ok "dora source found"
    PIN=$(git -C dora rev-parse --short=7 HEAD 2>/dev/null || true)
    if [ "$PIN" != "1fba721" ]; then
        warn "dora checkout is at $PIN, expected pinned commit 1fba721 (flume→tokio mpsc, arrow 59)"
        warn "Demo behavior is not guaranteed on a different dora commit."
        warn "Re-clone, or checkout the pin: git -C dora checkout 1fba7214b79d8488229f6cc2027b9760dec4d6df"
    else
        ok "dora checkout verified at pinned commit 1fba721"
    fi
fi

# ─── 1. Build everything ─────────────────────────────────
banner "1. Build all binaries"

BUILD_LOG=$(mktemp)
trap "rm -f $BUILD_LOG" EXIT

step "Build test-source, test-sink, echo-node, classifier-node..."
if cargo build --bin test-source --bin test-sink --bin echo-node --bin classifier-node > "$BUILD_LOG" 2>&1; then
    tail -1 "$BUILD_LOG"
else
    warn "Build failed! Last 20 lines:"
    tail -20 "$BUILD_LOG"
    exit 1
fi

step "Build dora CLI..."
if PYO3_NO_PYTHON=1 cargo build --bin dora --manifest-path dora/binaries/cli/Cargo.toml > "$BUILD_LOG" 2>&1; then
    tail -1 "$BUILD_LOG"
else
    warn "dora CLI build failed! Last 20 lines:"
    tail -20 "$BUILD_LOG"
    exit 1
fi

step "Build rust-dataflow example nodes (DORA upstream, unmodified)..."
if PYO3_NO_PYTHON=1 cargo build -p rust-dataflow-example-node -p rust-dataflow-example-status-node --manifest-path dora/Cargo.toml > "$BUILD_LOG" 2>&1; then
    tail -1 "$BUILD_LOG"
else
    warn "rust-dataflow example build failed! Last 20 lines:"
    tail -20 "$BUILD_LOG"
    exit 1
fi

step "Build demo_replay example..."
if cargo build --example demo_replay > "$BUILD_LOG" 2>&1; then
    tail -1 "$BUILD_LOG"
else
    warn "demo_replay build failed! Last 20 lines:"
    tail -20 "$BUILD_LOG"
    exit 1
fi

# ─── 2. Record/Replay demo ───────────────────────────────
banner "2. Record/Replay regression testing demo"

DORA_BIN="dora/target/debug/dora"
if [ ! -f "$DORA_BIN" ]; then
    DORA_BIN="dora/target/release/dora"
fi

DEMO="target/debug/examples/demo_replay"
if [ -f "$DEMO" ]; then
    step "Running Record/Replay demo..."
    echo ""
    set +e
    if [ -f "$DORA_BIN" ]; then
        "$DEMO" --dora "$DORA_BIN" 2>&1
    else
        "$DEMO" 2>&1
    fi
    DEMO_EXIT=$?
    set -e
    echo ""
    if [ $DEMO_EXIT -ne 0 ]; then
        warn "demo_replay exited with code $DEMO_EXIT"
        exit 1
    fi
else
    warn "$DEMO not found"
    exit 1
fi

# ─── 3. Library unit tests ───────────────────────────────
banner "3. Library unit tests (85)"

step "Running cargo test --lib..."
cargo test --lib

# ─── 4. E2E tests ────────────────────────────────────────
banner "4. E2E tests (5)"

step "Running cargo test --test e2e..."
cargo test --test e2e -- --test-threads=1

# ─── 5. Record/Replay E2E tests ──────────────────────────
banner "5. Record/Replay e2e tests (17)"

step "Running e2e_record tests (4)..."
timeout 120 cargo test --test e2e_record -- --test-threads=1

step "Running e2e_replay tests (13)..."
timeout 120 cargo test --test e2e_replay -- --test-threads=1

# ─── 6. Integration tests ────────────────────────────────
banner "6. Integration tests (6)"

step "Running cargo test --test integration (dora run pipelines)..."
timeout 120 cargo test --test integration -- --test-threads=1

# ─── 7. Smoke tests ──────────────────────────────────────
banner "7. Smoke tests (3)"

step "Running cargo test --test smoke..."
cargo test --test smoke -- --test-threads=1

# ─── Done ────────────────────────────────────────────────
banner "Demo Complete"

echo -e "${GREEN}${BOLD}Summary:${NC}"
echo "  • RecordSession: baseline from DORA rust-dataflow example (upstream nodes unmodified)"
echo "  • Demo uses rust-dataflow-example-node + rust-dataflow-example-status-node → 2 test-sink nodes"
echo "  • Sinks recorded: test-sink-random (UInt64) + test-sink-status (String)"
echo "  • ReplaySession (clean): ignore_paths([count]) + ignore_sink(status) → is_clean() = true"
echo "  • ReplaySession (regression): tick 10ms → 200ms → array length mismatch → DiffReport"
echo "  • Full suite: 116 tests green (85 unit + 5 e2e + 4 record + 13 replay + 6 integration + 3 smoke)"
echo ""
echo -e "${CYAN}Repo:${NC} https://github.com/SunSunSun689/gsoc2026-dora-test-utils"
echo -e "${CYAN}Branch:${NC} week11"
echo -e "${CYAN}DORA dep:${NC} 1fba721 (flume→tokio mpsc, arrow 59)"
