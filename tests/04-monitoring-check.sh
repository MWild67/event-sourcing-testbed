#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Test 04 — Monitoring Integration Check
#
# Verifies:
#   1. Prometheus is scraping node-exporter, KurrentDB, and RabbitMQ targets.
#   2. The four key metric families are present:
#        • node_cpu_seconds_total{mode="iowait"}   — Disk I/O Wait
#        • node_disk_reads_completed_total          — Read IOPS
#        • node_disk_writes_completed_total         — Write IOPS
#        • up{job="kurrentdb"}                     — Storage cluster health
#   3. Grafana is running and the "Event Store Namespace" dashboard is loaded.
#
# Usage: ./tests/04-monitoring-check.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NS="event-store"
PROM_PORT=9090
GRAFANA_PORT=3000
TIMEOUT=30

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*" >&2; exit 1; }
step() { echo; echo "▶ $*"; }

require_cmd() { command -v "$1" &>/dev/null || fail "'$1' not found in PATH"; }
require_cmd kubectl
require_cmd curl

# ── Port-forward Prometheus ───────────────────────────────────────────────────
step "Port-forwarding Prometheus (localhost:${PROM_PORT})"
kubectl port-forward svc/prometheus "$PROM_PORT:$PROM_PORT" -n "$NS" &
PROM_PF_PID=$!
trap "kill $PROM_PF_PID $GRAFANA_PF_PID 2>/dev/null || true" EXIT
sleep 3

PROM_BASE="http://localhost:${PROM_PORT}"

# ── Check targets ─────────────────────────────────────────────────────────────
step "Checking Prometheus scrape targets"

wait_for_prom() {
    local attempt=0
    until curl -sf "${PROM_BASE}/-/ready" > /dev/null 2>&1; do
        (( attempt++ ))
        [[ $attempt -lt $TIMEOUT ]] || fail "Prometheus did not become ready within ${TIMEOUT}s"
        sleep 1
    done
}
wait_for_prom
pass "Prometheus is ready"

TARGETS=$(curl -sf "${PROM_BASE}/api/v1/targets" | grep -oP '"job":"[^"]+"' | sort -u)
echo "  Active jobs:"
echo "$TARGETS" | sed 's/^/    /'

for JOB in node-exporter kurrentdb rabbitmq; do
    echo "$TARGETS" | grep -q "\"job\":\"$JOB\"" \
      || fail "Prometheus target '$JOB' not found — check annotations and scrape config"
    pass "Target '$JOB' is present"
done

# ── Check metric families ─────────────────────────────────────────────────────
step "Checking required metric families"

check_metric() {
    local metric="$1" description="$2"
    local count
    count=$(curl -sf "${PROM_BASE}/api/v1/query?query=${metric}" \
              | grep -oP '"result":\[.*?\]' \
              | grep -c '"value"' || echo "0")
    [[ "$count" -gt 0 ]] \
      || fail "Metric '$metric' not found in Prometheus ($description)"
    pass "Metric '$metric' present ($description)"
}

check_metric 'node_cpu_seconds_total{mode="iowait"}'  "Disk I/O Wait"
check_metric 'node_disk_reads_completed_total'         "Read IOPS"
check_metric 'node_disk_writes_completed_total'        "Write IOPS"
check_metric 'up{job="kurrentdb"}'                    "KurrentDB cluster health"
check_metric 'up{job="rabbitmq"}'                      "RabbitMQ health"

# ── Check Grafana dashboard ───────────────────────────────────────────────────
step "Port-forwarding Grafana (localhost:${GRAFANA_PORT})"
kubectl port-forward svc/grafana "$GRAFANA_PORT:$GRAFANA_PORT" -n "$NS" &
GRAFANA_PF_PID=$!
sleep 3

GRAFANA_BASE="http://admin:admin@localhost:${GRAFANA_PORT}"

wait_for_grafana() {
    local attempt=0
    until curl -sf "${GRAFANA_BASE}/api/health" > /dev/null 2>&1; do
        (( attempt++ ))
        [[ $attempt -lt $TIMEOUT ]] || fail "Grafana did not become ready within ${TIMEOUT}s"
        sleep 1
    done
}
wait_for_grafana
pass "Grafana is ready"

DASHBOARDS=$(curl -sf "${GRAFANA_BASE}/api/search?type=dash-db" 2>/dev/null || echo "[]")
echo "  Loaded dashboards:"
echo "$DASHBOARDS" | grep -oP '"title":"[^"]+"' | sed 's/^/    /' || echo "    (none)"

echo "$DASHBOARDS" | grep -q "event-store\|Event Store" \
  || fail "Event Store dashboard not found in Grafana — check ConfigMap grafana-dashboard-event-store"
pass "Event Store dashboard is loaded in Grafana"

# ── Final summary ─────────────────────────────────────────────────────────────
echo
echo "  Access the dashboard locally while port-forwards are active:"
echo "    Prometheus : http://localhost:${PROM_PORT}"
echo "    Grafana    : http://localhost:${GRAFANA_PORT}  (admin / admin)"
echo
echo "══════════════════════════════════════════════"
echo "  Test 04 — Monitoring Integration: PASS"
echo "══════════════════════════════════════════════"
