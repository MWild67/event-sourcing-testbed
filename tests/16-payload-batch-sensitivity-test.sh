#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 16 — Payload and Batch-Shape Sensitivity
#
# Sweeps payload size and batch size across all three backends, then reports
# whether backend throughput ranking changes with workload shape.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DIRECT="${DIRECT:-1}"
TESTBED_BIN="${TESTBED_BIN:-}"
BACKENDS="${BACKENDS:-kurrentdb mongodb postgres}"
PAYLOAD_SIZES="${PAYLOAD_SIZES:-256 1024 4096}"
BATCH_SIZES="${BATCH_SIZES:-1 8}"

TARGET_RATE="${TARGET_RATE:-5000}"
CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-12}"
EVENT_STORE_MODE="${EVENT_STORE_MODE:-0}"

KURRENT_URL_DIRECT="${KURRENT_URL_DIRECT:-kurrentdb://localhost:2113?tls=false}"
MONGO_URL_DIRECT="${MONGO_URL_DIRECT:-mongodb://localhost:27017/?directConnection=true}"
POSTGRES_URL_DIRECT="${POSTGRES_URL_DIRECT:-postgres://postgres:postgres@localhost:5432/eventbench}"

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "'$1' not found in PATH"
}

resolve_testbed_bin() {
  local candidate

  if [[ -n "$TESTBED_BIN" && -x "$TESTBED_BIN" ]]; then
    echo "$TESTBED_BIN"
    return 0
  fi

  for candidate in \
    rust-app/target/release/testbed \
    rust-app/target/debug/testbed \
    rust-app/target/x86_64-unknown-linux-gnu/release/testbed \
    rust-app/target/x86_64-unknown-linux-gnu/debug/testbed \
    rust-app/target/release/testbed.exe \
    rust-app/target/debug/testbed.exe
  do
    if [[ -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done

  fail "testbed binary not found/executable. Build with: cargo build --manifest-path rust-app/Cargo.toml"
}

extract_json_last() {
  local text="$1"
  echo "$text" | grep '^{' | tail -1
}

run_case() {
  local backend="$1"
  local payload_bytes="$2"
  local batch_size="$3"
  local output json

  case "$backend" in
    kurrentdb)
      output=$("$TESTBED_BIN" \
        --kurrentdb-url "$KURRENT_URL_DIRECT" \
        kurrentdb-bench \
        --target-rate "$TARGET_RATE" \
        --duration-secs "$DURATION_SECS" \
        --concurrency "$CONCURRENCY" \
        --batch-size "$batch_size" \
        --payload-bytes "$payload_bytes" \
        --json 2>&1) || true
      ;;
    mongodb)
      output=$("$TESTBED_BIN" \
        --mongodb-url "$MONGO_URL_DIRECT" \
        mongo-bench \
        --target-rate "$TARGET_RATE" \
        --duration-secs "$DURATION_SECS" \
        --concurrency "$CONCURRENCY" \
        --batch-size "$batch_size" \
        --payload-bytes "$payload_bytes" \
        $( [[ "$EVENT_STORE_MODE" == "1" ]] && echo "--event-store-mode" ) \
        --json 2>&1) || true
      ;;
    postgres)
      output=$("$TESTBED_BIN" \
        --postgres-url "$POSTGRES_URL_DIRECT" \
        pg-bench \
        --target-rate "$TARGET_RATE" \
        --duration-secs "$DURATION_SECS" \
        --concurrency "$CONCURRENCY" \
        --batch-size "$batch_size" \
        --payload-bytes "$payload_bytes" \
        $( [[ "$EVENT_STORE_MODE" == "1" ]] && echo "--event-store-mode" ) \
        --json 2>&1) || true
      ;;
    *)
      fail "unsupported backend: $backend"
      ;;
  esac

  json=$(extract_json_last "$output")
  [[ -n "$json" ]] || fail "no JSON result for backend=$backend payload=$payload_bytes batch=$batch_size"

  local rate p99
  rate=$(echo "$json" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(float(d.get("actual_rate_eps", 0.0)))')
  p99=$(echo "$json" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("p99_us", 0)))')

  echo "$backend,$payload_bytes,$batch_size,$rate,$p99" >> "$RESULTS_CSV"
  pass "$backend payload=${payload_bytes}B batch=${batch_size}: rate=${rate} ev/s p99=${p99} us"
}

step "Payload and batch-shape sensitivity"
echo "  Direct mode               : $DIRECT"
echo "  Backends                  : $BACKENDS"
echo "  Payload sizes (bytes)     : $PAYLOAD_SIZES"
echo "  Batch sizes               : $BATCH_SIZES"
echo "  Target rate               : $TARGET_RATE ev/s"
echo "  Concurrency               : $CONCURRENCY"
echo "  Duration                  : ${DURATION_SECS}s"

[[ "$DIRECT" == "1" ]] || fail "this test currently supports DIRECT=1 only"
TESTBED_BIN="$(resolve_testbed_bin)"

if [[ "$TESTBED_BIN" == *.exe && "$(uname -s)" == "Linux" ]]; then
  fail "found Windows binary ($TESTBED_BIN) on Linux. Build a Linux binary first: cargo build --manifest-path rust-app/Cargo.toml --target x86_64-unknown-linux-gnu"
fi

require_cmd python3

RESULTS_CSV="$(mktemp)"
trap 'rm -f "$RESULTS_CSV"' EXIT
echo "backend,payload_bytes,batch_size,actual_rate_eps,p99_us" > "$RESULTS_CSV"

for payload_bytes in $PAYLOAD_SIZES; do
  for batch_size in $BATCH_SIZES; do
    step "Running shape payload=${payload_bytes}B batch=${batch_size}"
    for backend in $BACKENDS; do
      run_case "$backend" "$payload_bytes" "$batch_size"
    done
  done
done

step "Ranking analysis"
python3 - "$RESULTS_CSV" <<'PY'
import csv
import json
import sys

path = sys.argv[1]
rows = []
with open(path, newline="") as f:
    for r in csv.DictReader(f):
        r["payload_bytes"] = int(r["payload_bytes"])
        r["batch_size"] = int(r["batch_size"])
        r["actual_rate_eps"] = float(r["actual_rate_eps"])
        r["p99_us"] = int(r["p99_us"])
        rows.append(r)

shapes = {}
for r in rows:
    key = (r["payload_bytes"], r["batch_size"])
    shapes.setdefault(key, []).append(r)

def ranking(entries):
    ordered = sorted(entries, key=lambda e: e["actual_rate_eps"], reverse=True)
    return [e["backend"] for e in ordered], ordered

ordered_keys = sorted(shapes.keys())
baseline_rank, _ = ranking(shapes[ordered_keys[0]])
ranking_changed = False
shape_reports = []

for key in ordered_keys:
    rank, entries = ranking(shapes[key])
    if rank != baseline_rank:
        ranking_changed = True
    shape_reports.append(
        {
            "payload_bytes": key[0],
            "batch_size": key[1],
            "ranking": rank,
            "results": [
                {
                    "backend": e["backend"],
                    "actual_rate_eps": round(e["actual_rate_eps"], 1),
                    "p99_us": e["p99_us"],
                }
                for e in entries
            ],
        }
    )

for shape in shape_reports:
    print()
    print(
        f"shape payload={shape['payload_bytes']}B batch={shape['batch_size']} -> "
        + " > ".join(shape["ranking"])
    )
    for item in shape["results"]:
        print(
            f"  {item['backend']}: rate={item['actual_rate_eps']} ev/s p99={item['p99_us']} us"
        )

summary = {
    "ranking_changed": ranking_changed,
    "baseline_ranking": baseline_rank,
    "shapes": shape_reports,
}

print()
print(json.dumps(summary))
PY

pass "payload/batch-shape sensitivity test completed"
