#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# dora-test-utils — Final Submission Demo
# ─────────────────────────────────────────────────────────────
# Showcases RecordSession → ReplaySession regression testing:
#   1. Record a baseline from an echo pipeline
#   2. Replay → verify no regression (clean)
#   3. Mutate source data
#   4. Replay → detect regression with structured diff report
#
# Also runs the full test suite (109 tests).
# ─────────────────────────────────────────────────────────────
set -euo pipefail

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

# ─── 0. Prerequisites ────────────────────────────────────
banner "0. Check prerequisites"

step "Check Rust toolchain..."
cargo --version
rustc --version

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
banner "3. Library unit tests (80)"

step "Running cargo test --lib..."
cargo test --lib

# ─── 4. E2E tests ────────────────────────────────────────
banner "4. E2E tests (5)"

step "Running cargo test --test e2e..."
cargo test --test e2e -- --test-threads=1

# ─── 5. Record/Replay E2E tests ──────────────────────────
banner "5. Record/Replay e2e tests (15)"

step "Running e2e_record tests (4)..."
timeout 120 cargo test --test e2e_record -- --test-threads=1

step "Running e2e_replay tests (11)..."
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
echo "  • RecordSession: baseline captured with metadata"
echo "  • ReplaySession (clean): assert_no_regression() passed"
echo "  • ReplaySession (regression): structured DiffReport with MISMATCH"
echo "  • DiffReport: Display format shows field-level differences"
echo "  • Full suite: 109 tests green (80 unit + 5 e2e + 4 record + 11 replay + 6 integration + 3 smoke)"
echo ""
echo -e "${CYAN}Repo:${NC} https://github.com/SunSunSun689/gsoc2026-dora-test-utils"
echo -e "${CYAN}Branch:${NC} week11"
echo -e "${CYAN}DORA dep:${NC} 1fba721 (flume→tokio mpsc, arrow 59)"
