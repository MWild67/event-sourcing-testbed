# ─────────────────────────────────────────────────────────────────────────────
# Makefile — Event Sourcing Testbed
# ─────────────────────────────────────────────────────────────────────────────
.PHONY: help up down build push deploy undeploy test-all \
        test-storage test-bench test-failover test-monitoring test-mongodb \
	bench-local mongo-bench-local \
        logs-es logs-rmq pf-grafana pf-prom

NAMESPACE   := event-store
IMAGE       := localhost/event-sourcing-testbed
IMAGE_TAG   := latest
REGISTRY    ?=                   # e.g. "myregistry.io/" (with trailing slash)
FULL_IMAGE  := $(REGISTRY)$(IMAGE):$(IMAGE_TAG)

# ── Compose command and container runtime ────────────────────────────────────
# Defaults to podman (works on Windows and Linux).
# Override for Docker: make up COMPOSE="docker compose" RUNTIME=docker
COMPOSE     ?= podman compose
RUNTIME     ?= podman

# ── Help ──────────────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n",$$1,$$2}'

# ── Local dev ─────────────────────────────────────────────────────────────────
up: ## Start all services locally
	$(COMPOSE) up -d
	@echo "KurrentDB UI : http://localhost:2113/web"
	@echo "RabbitMQ UI     : http://localhost:15672  (guest/guest)"
	@echo "Grafana         : http://localhost:3000   (admin/admin)"

down: ## Stop and remove local containers
	$(COMPOSE) down -v

bench-local: build ## Run the KurrentDB performance benchmark (requires 'make up' first)
	@$(RUNTIME) run --rm --network event-sourcing-testbed_event-net $(FULL_IMAGE) \
	  --kurrentdb-url esdb://eventstore-bench:2113?tls=false \
	  bench --target-rate 10000 --concurrency 20 --batch-size 1 --duration-secs 30

mongo-bench-local: build ## Run the MongoDB performance benchmark (requires 'make up' first)
	@$(RUNTIME) run --rm --network event-sourcing-testbed_event-net $(FULL_IMAGE) \
	  --mongodb-url mongodb://mongodb:27017 \
	  mongo-bench --target-rate 10000 --concurrency 64 --batch-size 1 --duration-secs 30 --p99-limit-ms 5

# ── Build & publish Rust image ────────────────────────────────────────────────
build: ## Build the Rust benchmark image
	$(RUNTIME) build -t $(FULL_IMAGE) rust-app/

push: build ## Build and push image to REGISTRY
	$(RUNTIME) push $(FULL_IMAGE)

# ── Kubernetes deploy ─────────────────────────────────────────────────────────
deploy: ## Apply all Kubernetes manifests in order
	kubectl apply -f k8s/00-namespace.yaml
	kubectl apply -f k8s/01-storageclass.yaml
	kubectl apply -f k8s/02-eventstore/
	kubectl apply -f k8s/03-rabbitmq/
	kubectl apply -f k8s/04-monitoring/
	@echo ""
	@echo "Waiting for KurrentDB to be ready (this may take ~60s)..."
	kubectl rollout status statefulset/eventstore -n $(NAMESPACE) --timeout=180s
	@echo "Waiting for RabbitMQ to be ready..."
	kubectl rollout status statefulset/rabbitmq    -n $(NAMESPACE) --timeout=180s
	@echo ""
	@echo "All components deployed."

undeploy: ## Remove all Kubernetes resources in the event-store namespace
	kubectl delete namespace $(NAMESPACE) --ignore-not-found

# ── Tests ─────────────────────────────────────────────────────────────────────
test-all: test-storage test-bench test-monitoring test-mongodb ## Run all test suites
	@echo ""
	@echo "All tests passed."

test-storage: ## Test 01: StorageClass WaitForFirstConsumer validation
	bash tests/01-validate-storage.sh

test-bench: ## Test 02: Performance benchmark (10k ev/s, p99 < 2ms)
	TESTBED_IMAGE=$(FULL_IMAGE) bash tests/02-stress-test.sh

test-bench-direct: ## Test 02: Run benchmark binary directly (requires local ES)
	DIRECT=1 bash tests/02-stress-test.sh

test-failover: ## Test 03: Automated failover (recovery < 60s)
	bash tests/03-failover-test.sh

test-monitoring: ## Test 04: Monitoring integration (Prometheus + Grafana)
	bash tests/04-monitoring-check.sh

test-mongodb: ## Test 05: MongoDB write-latency stress test (p99 < 5ms)
	TESTBED_IMAGE=$(FULL_IMAGE) bash tests/05-mongodb-stress-test.sh

test-mongodb-direct: ## Test 05: Run MongoDB benchmark directly (requires local MongoDB)
	DIRECT=1 bash tests/05-mongodb-stress-test.sh

# ── Utility ───────────────────────────────────────────────────────────────────
logs-es: ## Tail KurrentDB logs
	kubectl logs -n $(NAMESPACE) -l app=eventstore -f --max-log-requests=3

logs-rmq: ## Tail RabbitMQ logs
	kubectl logs -n $(NAMESPACE) -l app=rabbitmq -f --max-log-requests=3

pf-grafana: ## Port-forward Grafana to localhost:3000
	kubectl port-forward svc/grafana -n $(NAMESPACE) 3000:3000

pf-prom: ## Port-forward Prometheus to localhost:9090
	kubectl port-forward svc/prometheus -n $(NAMESPACE) 9090:9090

pf-es: ## Port-forward KurrentDB to localhost:2113
	kubectl port-forward svc/eventstore -n $(NAMESPACE) 2113:2113
