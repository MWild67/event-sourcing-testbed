# ─────────────────────────────────────────────────────────────────────────────
# Makefile — Event Sourcing Testbed
# ─────────────────────────────────────────────────────────────────────────────
.PHONY: help up down build push deploy undeploy test-all \
	test-all-extended \
        test-storage test-bench test-failover test-monitoring \
	test-mongodb test-postgres test-rehydration \
	test-hot-cache test-hot-cold-view test-projection test-search test-scale \
	test-rate-ramp test-replay-under-write test-replay-preflight test-failover-impact \
	test-hot-stream-contention test-payload-batch-sensitivity \
	test-failover-impact-local \
        logs-es logs-rmq pf-grafana pf-prom pf-es

NAMESPACE     := event-store
IMAGE         := localhost/event-sourcing-testbed
IMAGE_TAG     := latest
REGISTRY      ?=                   # e.g. "myregistry.io/" (with trailing slash)
FULL_IMAGE    := $(REGISTRY)$(IMAGE):$(IMAGE_TAG)
TESTBED_IMAGE ?= $(FULL_IMAGE)

# ── Compose command and container runtime ────────────────────────────────────
# Defaults to docker (devcontainer uses Docker).
# Override for Podman: make up COMPOSE="podman compose" RUNTIME=podman
COMPOSE   ?= docker compose
RUNTIME   ?= docker

# ── Environment detection ────────────────────────────────────────────────────
# In the devcontainer, DIRECT=1 is already set via devcontainer.json remoteEnv.
# In K8s CI mode, leave DIRECT unset (or 0) to run tests as Kubernetes Jobs.
DIRECT ?= 0

# ── Thresholds ────────────────────────────────────────────────────────────────
# Production targets (K8s mode)    : 10 000 ev/s, p99 < 2 ms
# Devcontainer/direct targets      : achievable on a shared VM / laptop
ifeq ($(DIRECT),1)
  KURRENT_RATE   ?= 500
  KURRENT_P99_US ?= 200000
  MONGO_RATE     ?= 500
  MONGO_P99_MS   ?= 200
  PG_RATE        ?= 500
  PG_P99_MS      ?= 200
else
  KURRENT_RATE   ?= 10000
  KURRENT_P99_US ?= 2000
  MONGO_RATE     ?= 10000
  MONGO_P99_MS   ?= 2
  PG_RATE        ?= 10000
  PG_P99_MS      ?= 2
endif

# ── Help ─────────────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-24s\033[0m %s\n",$$1,$$2}'

# ── Local dev (devcontainer services) ────────────────────────────────────────
up: ## Start all services locally via docker compose
	$(COMPOSE) -f docker-compose.yml up -d
	@echo "KurrentDB UI : http://localhost:2113/web"
	@echo "RabbitMQ UI  : http://localhost:15672  (guest/guest)"
	@echo "Grafana      : http://localhost:3000   (admin/admin)"

down: ## Stop and remove local containers
	$(COMPOSE) -f docker-compose.yml down -v

# ── Build & publish Rust image ────────────────────────────────────────────────
build: ## Build the Rust benchmark image
	$(RUNTIME) build -t $(FULL_IMAGE) rust-app/

push: build ## Build and push image to REGISTRY
	$(RUNTIME) push $(FULL_IMAGE)

# ── Kubernetes deploy ─────────────────────────────────────────────────────────
deploy: ## Apply all Kubernetes manifests in order
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/01-storageclass.yaml
	kubectl apply -f k8s/02-kurrentdb/
	kubectl apply -f k8s/03-rabbitmq/
	kubectl apply -f k8s/04-monitoring/
	kubectl apply -f k8s/05-mongodb/
	kubectl apply -f k8s/06-postgres/
	@echo ""
	@echo "Waiting for KurrentDB to be ready (this may take ~60s)..."
	kubectl rollout status statefulset/kurrentdb -n $(NAMESPACE) --timeout=180s
	@echo "Waiting for RabbitMQ to be ready..."
	kubectl rollout status statefulset/rabbitmq -n $(NAMESPACE) --timeout=180s
	@echo ""
	@echo "All components deployed."

undeploy: ## Remove all Kubernetes resources in the event-store namespace
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

# ── Tests ─────────────────────────────────────────────────────────────────────
# When DIRECT=1 (devcontainer default): tests run against the local services
# and use relaxed thresholds appropriate for a shared dev VM.
# When DIRECT=0 (K8s mode): tests run as Kubernetes Jobs against the cluster
# and enforce the production SLAs (10 000 ev/s, p99 < 2 ms).
#
# Usage in devcontainer:  make test-all          (DIRECT=1 already in env)
# Usage against K8s:      make test-all DIRECT=0

test-all: test-storage test-bench test-failover test-monitoring test-mongodb test-postgres test-rehydration ## Run core 7 test suites
	@echo ""
	@echo "════════════════════════════════════════"
	@echo "  Core test suite completed."
	@echo "════════════════════════════════════════"

test-all-extended: test-all test-hot-cache test-hot-cold-view test-projection test-search test-scale test-rate-ramp test-replay-under-write test-hot-stream-contention test-payload-batch-sensitivity test-failover-impact ## Run core + advanced tests (long)
	@echo ""
	@echo "════════════════════════════════════════"
	@echo "  Extended test suite completed."
	@echo "════════════════════════════════════════"

test-storage: ## Test 01: StorageClass validation (skips without kubectl)
	bash tests/01-validate-storage.sh

test-bench: ## Test 02: KurrentDB performance benchmark
	DIRECT=$(DIRECT) \
	TARGET_RATE=$(KURRENT_RATE) MAX_P99_US=$(KURRENT_P99_US) \
	TESTBED_IMAGE=$(TESTBED_IMAGE) \
	  bash tests/02-stress-test.sh

test-failover: ## Test 03: Automated failover (skips without kubectl)
	bash tests/03-failover-test.sh

test-monitoring: ## Test 04: Prometheus + Grafana check (skips without kubectl)
	bash tests/04-monitoring-check.sh

test-mongodb: ## Test 05: MongoDB write-latency stress test
	DIRECT=$(DIRECT) \
	TARGET_RATE=$(MONGO_RATE) P99_LIMIT_MS=$(MONGO_P99_MS) \
	TESTBED_IMAGE=$(TESTBED_IMAGE) \
	  bash tests/05-mongodb-stress-test.sh

test-postgres: ## Test 07: PostgreSQL write-latency stress test
	DIRECT=$(DIRECT) \
	TARGET_RATE=$(PG_RATE) P99_LIMIT_MS=$(PG_P99_MS) \
	TESTBED_IMAGE=$(TESTBED_IMAGE) \
	  bash tests/07-postgres-stress-test.sh

test-rehydration: ## Test 06: Event rehydration/replay (KurrentDB, MongoDB, PostgreSQL)
	bash tests/06-rehydration-replay-test.sh

test-hot-cache: ## Test 08: Hot-tail-cache benchmark
	bash tests/08-hot-cache-bench.sh

test-hot-cold-view: ## Test 17: KurrentDB hot/cold views via onboard features
	bash tests/17-hot-cold-view-bench.sh --json

test-projection: ## Test 09: Projection/subscription-lag benchmark
	bash tests/09-projection-bench.sh

test-search: ## Test 10: Search/index projection benchmark
	bash tests/10-search-bench.sh

test-scale: ## Test 11: Scale benchmark
	bash tests/11-scale-bench.sh

test-rate-ramp: ## Test 12: Rate ramp knee-point test (BACKEND=kurrentdb|mongodb|postgres)
	DIRECT=$(DIRECT) \
	BACKEND=$(BACKEND) \
	RATE_STEPS="$(if $(RATE_STEPS),$(RATE_STEPS),1000 3000 5000 8000 10000)" \
	CONCURRENCY=$(if $(CONCURRENCY),$(CONCURRENCY),64) \
	BATCH_SIZE=$(if $(BATCH_SIZE),$(BATCH_SIZE),1) \
	DURATION_SECS=$(if $(DURATION_SECS),$(DURATION_SECS),20) \
	EVENT_STORE_MODE=$(if $(EVENT_STORE_MODE),$(EVENT_STORE_MODE),0) \
	TESTBED_IMAGE=$(TESTBED_IMAGE) \
	  bash tests/12-rate-ramp-test.sh

test-replay-under-write: ## Test 13: Replay-under-write (write latency regression during concurrent replay)
	DIRECT=$(DIRECT) \
	BACKEND=$(if $(BACKEND),$(BACKEND),kurrentdb) \
	SEED_EVENTS=$(if $(SEED_EVENTS),$(SEED_EVENTS),100000) \
	DURATION_SECS=$(if $(DURATION_SECS),$(DURATION_SECS),30) \
	CONCURRENCY=$(if $(CONCURRENCY),$(CONCURRENCY),64) \
	BATCH_SIZE=$(if $(BATCH_SIZE),$(BATCH_SIZE),1) \
	TARGET_RATE=$(if $(TARGET_RATE),$(TARGET_RATE),10000) \
	EVENT_STORE_MODE=$(if $(EVENT_STORE_MODE),$(EVENT_STORE_MODE),0) \
	TESTBED_IMAGE=$(TESTBED_IMAGE) \
	  bash tests/13-replay-under-write-test.sh

test-replay-preflight: ## Fast local replay-under-write smoke test for all backends (DIRECT=1)
	@set -e; \
	echo "Running replay preflight (kurrentdb, mongodb, postgres)"; \
	for backend in kurrentdb mongodb postgres; do \
	  echo ""; \
	  echo "[preflight] backend=$$backend"; \
	  DIRECT=1 \
	  BACKEND=$$backend \
	  SEED_EVENTS=$${SEED_EVENTS:-1000} \
	  DURATION_SECS=$${DURATION_SECS:-3} \
	  CONCURRENCY=$${CONCURRENCY:-4} \
	  TARGET_RATE=$${TARGET_RATE:-200} \
	  EVENT_STORE_MODE=$${EVENT_STORE_MODE:-0} \
	  TESTBED_IMAGE=$(TESTBED_IMAGE) \
	    bash tests/13-replay-under-write-test.sh; \
	done

test-failover-impact: ## Test 14: Short failover-impact (pause window, error spike, recovery time)
	RECOVERY_SLA_SECS=$(if $(RECOVERY_SLA_SECS),$(RECOVERY_SLA_SECS),60) \
	TARGET_RATE=$(if $(TARGET_RATE),$(TARGET_RATE),4000) \
	CONCURRENCY=$(if $(CONCURRENCY),$(CONCURRENCY),48) \
	BATCH_SIZE=$(if $(BATCH_SIZE),$(BATCH_SIZE),1) \
	BASELINE_DURATION_SECS=$(if $(BASELINE_DURATION_SECS),$(BASELINE_DURATION_SECS),12) \
	IMPACT_DURATION_SECS=$(if $(IMPACT_DURATION_SECS),$(IMPACT_DURATION_SECS),35) \
	SPIKE_WINDOW_MS=$(if $(SPIKE_WINDOW_MS),$(SPIKE_WINDOW_MS),5000) \
	bash tests/14-failover-impact-test.sh

test-failover-impact-local: ## Test 14 local CI-like repro using k3d (creates temporary cluster)
	bash tests/14-failover-impact-local-repro.sh

test-hot-stream-contention: ## Test 15: Hot-stream contention (conflicts/retries + tail-latency impact)
	DIRECT=$(DIRECT) \
	BACKEND=$(if $(BACKEND),$(BACKEND),kurrentdb) \
	TARGET_RATE=$(if $(TARGET_RATE),$(TARGET_RATE),6000) \
	BASELINE_DURATION_SECS=$(if $(BASELINE_DURATION_SECS),$(BASELINE_DURATION_SECS),12) \
	CONTENTION_DURATION_SECS=$(if $(CONTENTION_DURATION_SECS),$(CONTENTION_DURATION_SECS),18) \
	CONCURRENCY=$(if $(CONCURRENCY),$(CONCURRENCY),96) \
	HOT_STREAMS=$(if $(HOT_STREAMS),$(HOT_STREAMS),2) \
	COLD_STREAMS=$(if $(COLD_STREAMS),$(COLD_STREAMS),128) \
	HOT_RATIO=$(if $(HOT_RATIO),$(HOT_RATIO),0.95) \
	MAX_RETRIES=$(if $(MAX_RETRIES),$(MAX_RETRIES),10) \
	bash tests/15-hot-stream-contention-test.sh

test-payload-batch-sensitivity: ## Test 16: Payload + batch-shape sensitivity (ranking shift across shapes)
	DIRECT=$(DIRECT) \
	BACKENDS="$(if $(BACKENDS),$(BACKENDS),kurrentdb mongodb postgres)" \
	PAYLOAD_SIZES="$(if $(PAYLOAD_SIZES),$(PAYLOAD_SIZES),256 1024 4096)" \
	BATCH_SIZES="$(if $(BATCH_SIZES),$(BATCH_SIZES),1 8)" \
	TARGET_RATE=$(if $(TARGET_RATE),$(TARGET_RATE),5000) \
	CONCURRENCY=$(if $(CONCURRENCY),$(CONCURRENCY),64) \
	DURATION_SECS=$(if $(DURATION_SECS),$(DURATION_SECS),12) \
	EVENT_STORE_MODE=$(if $(EVENT_STORE_MODE),$(EVENT_STORE_MODE),0) \
	bash tests/16-payload-batch-sensitivity-test.sh

# ── Utility ───────────────────────────────────────────────────────────────────
logs-es: ## Tail KurrentDB logs
	kubectl logs -n $(NAMESPACE) -l app=kurrentdb -f --max-log-requests=3

logs-rmq: ## Tail RabbitMQ logs
	kubectl logs -n $(NAMESPACE) -l app=rabbitmq -f --max-log-requests=3

pf-grafana: ## Port-forward Grafana to localhost:3000
	kubectl port-forward svc/grafana -n $(NAMESPACE) 3000:3000

pf-prom: ## Port-forward Prometheus to localhost:9090
	kubectl port-forward svc/prometheus -n $(NAMESPACE) 9090:9090

pf-es: ## Port-forward KurrentDB to localhost:2113
	kubectl port-forward svc/kurrentdb -n $(NAMESPACE) 2113:2113
