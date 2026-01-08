# Jira Tickets Reference

This document contains all planned Jira tickets for the Pedregal Spark Platform, organized by epic.

---

## Epic 1: Internal Spark Runtime

Build the internal Spark runtime infrastructure for the Pedregal Spark Platform including SK8, Spark Gateway, Spark Connect, Docker images, and observability.

### Theme 1: Spark on Kubernetes (SK8)

#### SK8-001: Deploy Spark Kubernetes Operator (SKO)

**Type:** Story | **Priority:** P0 | **Points:** 8

**Description:**
Deploy Apache Spark Kubernetes Operator to manage SparkApplication CRDs on dash-data cluster.

**Acceptance Criteria:**
- [ ] SKO deployed to dash-data cluster
- [ ] SKO can watch SparkApplication CRDs
- [ ] SKO creates driver/executor pods on CRD submission
- [ ] SKO reports job status back to CRD
- [ ] Health checks and metrics emitted
- [ ] Runbook created for operator troubleshooting

---

#### SK8-002: Create Domain-Specific Namespaces

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Implement domain-specific namespace model (`sjns-<domain>`) for team isolation.

**Acceptance Criteria:**
- [ ] Namespace naming convention: `sjns-<domain>-<environment>`
- [ ] ResourceQuota per namespace (CPU, memory limits)
- [ ] LimitRange for pod defaults
- [ ] NetworkPolicy for namespace isolation
- [ ] RBAC for team-based access
- [ ] Self-service secret management within namespace
- [ ] Documentation for namespace onboarding

---

#### SK8-003: Configure Dynamic Resource Allocation (DRA)

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Enable Spark Dynamic Resource Allocation for executor autoscaling.

**Acceptance Criteria:**
- [ ] DRA enabled in Spark configuration
- [ ] Min/max executor bounds configurable per job
- [ ] Executor scale-up latency < 30s
- [ ] Executor scale-down on idle
- [ ] Metrics for DRA events (scale up/down counts)
- [ ] Integration with Karpenter for node provisioning

---

#### SK8-004: Implement Shuffle Strategy (Phase 1: Local)

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Implement local shuffle storage with monitoring and budgets.

**Acceptance Criteria:**
- [ ] Local shuffle storage on executor nodes
- [ ] Per-job storage budgets enforced
- [ ] Monitoring for DiskPressure conditions
- [ ] Alerts for shuffle fetch failures
- [ ] Metrics: shuffle read/write bytes, fetch failure rate
- [ ] Documentation on shuffle limits

---

#### SK8-005: Multi-Cluster Routing via Cluster Proxy

**Type:** Story | **Priority:** P1 | **Points:** 8

**Description:**
Implement Cluster Proxy Service for multi-cluster routing and HA.

**Acceptance Criteria:**
- [ ] Cluster Proxy Service deployed
- [ ] Load balancing across healthy K8s clusters
- [ ] Health checks for cluster availability
- [ ] Failover to secondary cluster on outage
- [ ] Multi-region routing support (us-west-2, us-east-1, eu-west-1)
- [ ] Metrics: cluster health, routing latency

---

### Theme 2: Spark Gateway

#### GW-001: Build Spark Gateway Service

**Type:** Story | **Priority:** P0 | **Points:** 13

**Description:**
Build Spark Gateway as the single entry point for all Spark operations.

**Acceptance Criteria:**
- [ ] Pedregal Service deployed
- [ ] Receives SparkApplication CRD requests from Spark Runner
- [ ] Routes to Cluster Proxy → K8s API Server
- [ ] Enforces domain namespace model
- [ ] Job TTL management and cleanup
- [ ] Support for SK8 (primary) and EMR (fallback)
- [ ] gRPC API for job submission
- [ ] REST API for status/logs
- [ ] Metrics: requests, latency, errors by backend

---

#### GW-002: Implement Spark Connect Proxy in Gateway

**Type:** Story | **Priority:** P0 | **Points:** 8

**Description:**
Add gRPC proxy in Spark Gateway for Spark Connect traffic.

**Acceptance Criteria:**
- [ ] gRPC proxy for Spark Connect protocol (port 15002)
- [ ] Cluster discovery (find correct Driver pod)
- [ ] Authentication via Okta
- [ ] Routing by team/environment
- [ ] Hot cluster management (route to pre-warmed clusters)
- [ ] Load balancing across driver pool
- [ ] Session affinity for ongoing connections
- [ ] Metrics: connections, query latency

---

#### GW-003: Implement Authentication/Authorization

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Add Okta-based authentication and team-based authorization to Spark Gateway.

**Acceptance Criteria:**
- [ ] Okta SSO integration
- [ ] Team membership validation via Okta groups
- [ ] Authorization: teams can only access their namespaces
- [ ] Audit logging for all requests
- [ ] Service account support for CI/CD pipelines
- [ ] Documentation on auth setup

---

#### GW-004: Hot Cluster Management

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Implement hot cluster pool for fast session startup.

**Acceptance Criteria:**
- [ ] Pre-warmed cluster pool per team/environment
- [ ] Configurable pool size (e.g., prod: 3, staging: 1, dev: 0)
- [ ] Cluster warm-up on pool depletion
- [ ] Target session startup latency < 30s (vs 6-8 min cold start)
- [ ] Metrics: pool size, cluster utilization, wait times
- [ ] Cost optimization via idle cluster shutdown

---

### Theme 3: Spark Runner

#### SR-001: Build Spark Runner Graph

**Type:** Story | **Priority:** P0 | **Points:** 13

**Description:**
Build Spark Runner as a stateless Pedregal Graph with Submit/Check/Cancel primitives.

**Acceptance Criteria:**
- [ ] Pedregal Graph created
- [ ] `Submit(JobSpec, IdempotencyKey)` → (ExecutionID, VendorRunID)
- [ ] `Check(ExecutionID)` → ExecutionState
- [ ] `Cancel(ExecutionID, Team)` → Success/Failure
- [ ] ExecutionSnapshot written to Taulu on state changes
- [ ] Execution events emitted to Taulu event bus
- [ ] Idempotency token handling
- [ ] Integration tests

---

#### SR-002: Integrate APS (Auto Parameter Selection)

**Type:** Story | **Priority:** P1 | **Points:** 8

**Description:**
Integrate APS for pre-submit cluster sizing recommendations.

**Acceptance Criteria:**
- [ ] APS called before job submission (when enabled)
- [ ] Recommendations: driver/executor memory, cores, instances
- [ ] Historical execution data used for predictions
- [ ] User can override APS recommendations
- [ ] Metrics: APS accuracy, cost savings
- [ ] A/B testing framework for APS improvements

---

#### SR-003: Integrate AR (Auto-Retry/Remediation)

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Integrate AR for failure classification and retry recommendations.

**Acceptance Criteria:**
- [ ] Failure classification: user error vs system error
- [ ] Retry recommendations for transient failures
- [ ] Remediation suggestions (e.g., increase memory)
- [ ] Integration with orchestrator retry policies
- [ ] Metrics: failure categories, retry success rate

---

#### SR-004: Debug Utilities (Logs, Job Info)

**Type:** Story | **Priority:** P2 | **Points:** 3

**Description:**
Build debug utilities for job investigation.

**Acceptance Criteria:**
- [ ] Get logs by execution ID
- [ ] Get job info (config, status, timeline)
- [ ] Link to Spark UI (SHS)
- [ ] Link to ODIN logs
- [ ] MCP integration for AI-assisted debugging

---

### Theme 4: Docker Images

#### IMG-001: Build Base Spark Docker Image

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Create base Spark Docker image with layered architecture.

**Acceptance Criteria:**
- [ ] Layer 1: Base OS (Debian Slim) + JDK 17
- [ ] Layer 2: Spark 3.5 binaries + PySpark + Spark Connect server
- [ ] Layer 3: Connectors (S3, Iceberg, Kafka, Unity Catalog)
- [ ] Layer 4: CoreETL JARs + init scripts
- [ ] Image size optimized (< 2GB)
- [ ] Security scanning in CI
- [ ] Published to internal registry

---

#### IMG-002: Multi-Version Image Support

**Type:** Story | **Priority:** P1 | **Points:** 3

**Description:**
Support multiple Spark versions via image tags.

**Acceptance Criteria:**
- [ ] Image tags: `spark-platform:3.5-latest`, `spark-platform:4.0-latest`
- [ ] Version specified in DCP manifest (`engine.version`)
- [ ] Spark Runner resolves version to image tag
- [ ] Deprecation policy for old versions
- [ ] Documentation on version support matrix

---

#### IMG-003: Unity Catalog Connector Integration

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Pre-configure Unity Catalog access in Spark images.

**Acceptance Criteria:**
- [ ] Unity Catalog connector bundled in image
- [ ] Default catalog configuration (`pedregal`)
- [ ] Credential provider for UC authentication
- [ ] Support for multiple catalogs (pedregal, feature_store)
- [ ] Documentation on catalog access

---

### Theme 5: Cluster Provisioning

#### CP-001: Self-Service Cluster Provisioning API

**Type:** Story | **Priority:** P1 | **Points:** 8

**Description:**
Build self-service API for teams to request Spark Connect clusters.

**Acceptance Criteria:**
- [ ] `POST /api/v1/clusters` - Request new cluster
- [ ] `GET /api/v1/clusters/{id}` - Get cluster status
- [ ] `GET /api/v1/clusters` - List team clusters
- [ ] `DELETE /api/v1/clusters/{id}` - Delete cluster
- [ ] `POST /api/v1/clusters/{id}/scale` - Scale cluster
- [ ] `POST /api/v1/clusters/{id}/ttl` - Extend TTL
- [ ] Team permission validation (Okta groups)
- [ ] Quota enforcement per team
- [ ] Documentation

---

#### CP-002: Cluster Templates

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Create cluster templates for standardized configurations.

**Acceptance Criteria:**
- [ ] Size templates: small, medium, large, xlarge
- [ ] Pre-configured Unity Catalog access
- [ ] Environment-specific defaults (dev/staging/prod)
- [ ] Regional templates (us-west-2, us-east-1, eu-west-1)
- [ ] Custom template support for advanced users
- [ ] Documentation on template options

---

#### CP-003: DNS and Routing Setup

**Type:** Task | **Priority:** P1

**Description:**
Set up DNS CNAME records for team cluster endpoints.

**Acceptance Criteria:**
- [ ] Pattern: `sc://<team>-<env>.doordash.team` → Spark Gateway
- [ ] Regional endpoints: `sc://<team>-<env>-<region>.doordash.team`
- [ ] Automatic DNS record creation on cluster provisioning
- [ ] DNS cleanup on cluster deletion
- [ ] Documentation on endpoint conventions

---

### Theme 6: Observability

#### OBS-001: OTEL Metrics Integration

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Emit platform and job metrics to Chronosphere via OTel.

**Acceptance Criteria:**
- [ ] OTel collector sidecar on driver pods
- [ ] Platform metrics: job submissions, latency, success rate
- [ ] Spark metrics: task counts, shuffle stats, GC time
- [ ] Resource metrics: CPU, memory, disk usage
- [ ] Dashboards in Chronosphere
- [ ] Alerts for SLO breaches

---

#### OBS-002: Log Collection to ODIN

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Collect driver/executor logs and forward to ODIN.

**Acceptance Criteria:**
- [ ] Fluent Bit sidecar on all Spark pods
- [ ] Structured log format with job metadata
- [ ] Log correlation by execution ID
- [ ] Log retention policy (30 days default)
- [ ] ODIN queries documented

---

#### OBS-003: Spark History Server (SHS) Integration

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Deploy and integrate Spark History Server for Spark UI access.

**Acceptance Criteria:**
- [ ] SHS deployed to dash-data cluster
- [ ] Event logs written to S3 (shared location)
- [ ] SHS reads event logs from S3
- [ ] Spark UI accessible via URL in job status
- [ ] Authentication integrated with Okta
- [ ] Retention policy for event logs

---

#### OBS-004: Execution Events to Taulu

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Write execution events to Taulu for history and lineage.

**Acceptance Criteria:**
- [ ] Executions table: one row per execution with final state
- [ ] Execution Events table: append-only lifecycle events
- [ ] Events: SUBMIT, RUNNING, SUCCEEDED, FAILED, CANCELLED
- [ ] Metrics summary event on completion
- [ ] Query API for execution history
- [ ] Integration with lineage tracking

---

### Theme 7: Testing

#### TEST-001: TestContainers for Spark

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Create TestContainers module for local Spark testing.

**Acceptance Criteria:**
- [ ] `SparkContainer` class for Java/Kotlin tests
- [ ] Spark Connect enabled (port 15002)
- [ ] Configurable driver memory, Spark configs
- [ ] MinIO container for S3 mock
- [ ] Unity Catalog mock support
- [ ] Documentation and examples

---

#### TEST-002: Integration Test Suite

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Build integration test suite for Spark platform components.

**Acceptance Criteria:**
- [ ] Tests for Spark Runner (Submit/Check/Cancel)
- [ ] Tests for Spark Gateway routing
- [ ] Tests for Spark Connect proxy
- [ ] Tests run in CI pipeline
- [ ] Test coverage > 80%

---

## Epic 2: Unravel Recommendations Integration

Integrate Unravel's recommendation API with SJEM to enable pre-submit optimization recommendations for Spark jobs.

### Research

#### UNR-001: Document Unravel API for Pre-Submit Recommendations

**Type:** Task | **Priority:** P0

**Description:**
Obtain and document Unravel's API for fetching pre-submit recommendations.

**Acceptance Criteria:**
- [ ] API endpoint(s) identified for fetching recommendations
- [ ] Authentication method documented (API key, OAuth, etc.)
- [ ] Request format documented (what job metadata to send)
- [ ] Response format documented (what recommendations are returned)
- [ ] Rate limits and SLAs documented
- [ ] Internal wiki page created with findings

---

#### UNR-002: Map SJEM Job Metadata to Unravel Inputs

**Type:** Task | **Priority:** P1

**Description:**
Determine what SJEM job attributes can be sent to Unravel for recommendations.

**Acceptance Criteria:**
- [ ] List of available SJEM job metadata (name, cluster config, historical runs)
- [ ] Mapping to Unravel's expected input fields
- [ ] Identify gaps (data Unravel needs but SJEM doesn't have)
- [ ] Document in wiki

**Blocked By:** UNR-001

---

### Implementation

#### UNR-003: Create Unravel API Client

**Type:** Story | **Priority:** P0 | **Points:** 5

**Description:**
Build an HTTP client in SJEM to call Unravel's recommendation API.

**Acceptance Criteria:**
- [ ] HTTP client class created (`UnravelClient.java` or similar)
- [ ] Authentication via Vault secret (`UNRAVEL_API_KEY`)
- [ ] Retry logic with exponential backoff
- [ ] Circuit breaker for API failures
- [ ] Configurable timeout (default 5s)
- [ ] Unit tests with mocked responses
- [ ] Metrics emitted: `unravel_api_latency`, `unravel_api_errors`

**Blocked By:** UNR-001

---

#### UNR-004: Fetch Recommendations Before Job Submission

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Call Unravel API to fetch recommendations before creating DBR job.

**Acceptance Criteria:**
- [ ] Call Unravel API in `DBRProvider` before job creation (when enabled)
- [ ] Parse recommendation response into structured object
- [ ] Log recommendations for debugging
- [ ] Graceful fallback if API fails (proceed without recommendations)
- [ ] Only call for jobs with `opt_in_unravel_recommendations = true`
- [ ] Unit and integration tests

**Blocked By:** UNR-003

---

#### UNR-005: Apply Unravel Recommendations to Spark Config

**Type:** Story | **Priority:** P1 | **Points:** 5

**Description:**
Merge Unravel recommendations into Spark job configuration.

**Acceptance Criteria:**
- [ ] Apply recommended values: driver/executor memory, cores, instances
- [ ] Apply recommended Spark configs (shuffle partitions, etc.)
- [ ] Precedence: user-provided config overrides Unravel suggestions
- [ ] Controlled by user control: `unravel_recommendation_mode`
  - `off` = don't fetch
  - `suggest` = fetch and log only
  - `apply` = fetch and apply
- [ ] Log before/after config diff
- [ ] Unit tests for merge logic

**Blocked By:** UNR-004

---

#### UNR-006: Send Post-Run Feedback to Unravel

**Type:** Story | **Priority:** P2 | **Points:** 3

**Description:**
Send job completion metrics to Unravel to improve future recommendations.

**Acceptance Criteria:**
- [ ] Call Unravel feedback API after job completes
- [ ] Include: job ID, runtime, memory used, success/failure, cost
- [ ] Async call (don't block job completion flow)
- [ ] Graceful failure handling
- [ ] Metrics: `unravel_feedback_sent`, `unravel_feedback_errors`

**Blocked By:** UNR-003

---

### Configuration

#### UNR-007: Add User Controls for Unravel Recommendations

**Type:** Story | **Priority:** P1 | **Points:** 2

**Description:**
Add user control flags to enable/configure Unravel recommendations per job.

**Acceptance Criteria:**
- [ ] Add `OPT_IN_UNRAVEL_RECOMMENDATIONS` user control (default: false)
- [ ] Add `UNRAVEL_RECOMMENDATION_MODE` user control (values: off, suggest, apply)
- [ ] Document in user controls wiki
- [ ] Unit tests

---

#### UNR-008: Add Unravel API Configuration

**Type:** Task | **Priority:** P1

**Description:**
Add configuration for Unravel API endpoints and credentials.

**Acceptance Criteria:**
- [ ] Config key for Unravel API base URL (per environment)
- [ ] Vault path for API credentials added to SJEM config
- [ ] Feature flag: `unravel_recommendations_enabled` (global kill switch)
- [ ] Values populated for staging environment
- [ ] Values populated for prod environment

---

### Observability

#### UNR-009: Add Metrics and Logging for Recommendations

**Type:** Story | **Priority:** P2 | **Points:** 2

**Description:**
Add observability for Unravel recommendation flow.

**Acceptance Criteria:**
- [ ] Metrics emitted to Chronosphere:
  - `unravel_recommendations_fetched`
  - `unravel_recommendations_applied`
  - `unravel_recommendation_latency_ms`
  - `unravel_api_errors`
- [ ] Structured logging for recommendations (before/after configs)
- [ ] Dashboard created in Chronosphere

**Blocked By:** UNR-004

---

### Testing & Rollout

#### UNR-010: Integration Tests for Unravel Flow

**Type:** Story | **Priority:** P2 | **Points:** 3

**Description:**
Add integration tests for the Unravel recommendation flow.

**Acceptance Criteria:**
- [ ] Mock Unravel API in tests
- [ ] Test: recommendations fetched and applied correctly
- [ ] Test: fallback when API unavailable
- [ ] Test: user control modes (off, suggest, apply)
- [ ] Test: user config overrides Unravel suggestions
- [ ] Tests run in CI pipeline

---

#### UNR-011: Staged Rollout Plan

**Type:** Task | **Priority:** P2

**Description:**
Create and execute staged rollout plan for Unravel recommendations.

**Acceptance Criteria:**
- [ ] Rollout plan documented:
  - Phase 1: Staging - 2 test jobs (suggest mode)
  - Phase 2: Prod - 3 pilot jobs (suggest mode) for 1 week
  - Phase 3: Prod - pilot jobs (apply mode) for 1 week
  - Phase 4: Broader opt-in rollout
- [ ] Success criteria defined (latency, error rate, cost savings)
- [ ] Rollback plan documented
- [ ] Stakeholder approval

---

## Summary Tables

### Internal Spark Runtime - Story Points

| Theme | P0 | P1 | P2 | Total |
|-------|----|----|----|----|
| SK8 | 13 | 18 | - | 31 |
| Spark Gateway | 26 | 5 | - | 31 |
| Spark Runner | 13 | 13 | 3 | 29 |
| Docker Images | 10 | 3 | - | 13 |
| Cluster Provisioning | - | 13 | - | 13 |
| Observability | 10 | 10 | - | 20 |
| Testing | - | 10 | - | 10 |
| **Total** | **72** | **72** | **3** | **147** |

### Unravel Integration - Story Points

| Category | P0 | P1 | P2 | Total |
|----------|----|----|----|----|
| Research | - | - | - | - |
| Implementation | 5 | 10 | 3 | 18 |
| Configuration | - | 2 | - | 2 |
| Observability | - | - | 2 | 2 |
| Testing | - | - | 3 | 3 |
| **Total** | **5** | **12** | **8** | **25** |

### Suggested Phases

#### Internal Spark Runtime

| Phase | Focus | Tickets | Goal |
|-------|-------|---------|------|
| **Phase 1** | Core Infrastructure | SK8-001, SK8-002, GW-001, SR-001, IMG-001, IMG-003, OBS-001, OBS-002 | Basic job submission on SK8 |
| **Phase 2** | Spark Connect | GW-002, GW-003, GW-004, CP-001, CP-002 | Interactive sessions via Gateway |
| **Phase 3** | Optimization | SK8-003, SK8-004, SR-002, SR-003, SK8-005 | Autoscaling, multi-cluster HA |
| **Phase 4** | Polish | OBS-003, OBS-004, SR-004, TEST-001, TEST-002 | Observability, testing, debugging |

#### Unravel Integration

| Phase | Tickets | Goal |
|-------|---------|------|
| **Phase 1** | UNR-001, UNR-002, UNR-003 | Research + API client |
| **Phase 2** | UNR-004, UNR-005, UNR-007, UNR-008 | Core integration |
| **Phase 3** | UNR-006, UNR-009, UNR-010, UNR-011 | Feedback, observability, rollout |

---

*Last updated: January 2025*
