# Spark Connect Proxy Design

## Overview

This document describes the design for adding Spark Connect L7 proxy support to the existing Spark Gateway service. The proxy enables interactive Spark sessions via the `sc://` protocol while reusing the existing batch job infrastructure.

## Problem Statement

The current Spark Gateway only supports batch job submission:
- Creates SparkApplication CRDs via Cluster Proxy → K8s API
- Jobs run to completion and results are written to storage
- No support for interactive sessions

Spark Connect requires:
- Bidirectional gRPC streams between client and driver pod
- Session affinity (same session → same driver)
- Direct connectivity to driver pod IPs (not K8s API)
- Idle session detection and cleanup

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment model | Integrated (single binary) | Share K8s client, Vault auth, simpler ops |
| Session management | Hybrid (on-demand + BYOD) | Flexibility for notebooks and batch inspection |
| Pod discovery | Kubernetes Watch | Real-time updates, already have K8s client |
| Network connectivity | Direct pod IP | Gateway runs in cluster, simple and fast |
| Idle detection | Traffic timeout + Max TTL | Cost savings + runaway prevention |
| Authentication | None initially | Trust network boundary, add Okta later |
| Session routing | Explicit header | Simple, predictable, debuggable |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     ENHANCED SPARK GATEWAY ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   External Clients                                                              │
│        │                                                                        │
│        ├─── Batch API (port 50051) ──────────────────────┐                     │
│        │    SubmitSparkJob, GetStatus, Delete            │                     │
│        │                                                  │                     │
│        └─── Spark Connect (port 15002) ──────────────────┤                     │
│             sc://gateway:15002                            │                     │
│             Header: x-spark-session-id                    │                     │
│                                                           ▼                     │
│   ┌─────────────────────────────────────────────────────────────────────────┐  │
│   │                      SPARK GATEWAY (Single Binary)                       │  │
│   │                                                                         │  │
│   │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │  │
│   │  │  Batch Handler   │  │ Connect Proxy    │  │  Session Manager     │  │  │
│   │  │  (port 50051)    │  │ (port 15002)     │  │                      │  │  │
│   │  │                  │  │                  │  │  • Session registry  │  │  │
│   │  │  • SubmitJob     │  │  • gRPC proxy    │  │  • Idle tracker      │  │  │
│   │  │  • GetStatus     │  │  • Stream relay  │  │  • TTL enforcer      │  │  │
│   │  │  • DeleteJob     │  │  • Header routing│  │  • Pod watcher       │  │  │
│   │  └────────┬─────────┘  └────────┬─────────┘  └──────────┬───────────┘  │  │
│   │           │                     │                       │              │  │
│   │           └─────────────────────┴───────────────────────┘              │  │
│   │                                 │                                       │  │
│   │                    ┌────────────┴────────────┐                         │  │
│   │                    │      K8s Client         │                         │  │
│   │                    │  (via Cluster Proxy)    │                         │  │
│   │                    └────────────┬────────────┘                         │  │
│   └─────────────────────────────────┼───────────────────────────────────────┘  │
│                                     │                                          │
│              ┌──────────────────────┼──────────────────────┐                   │
│              │                      │                      │                   │
│              ▼                      ▼                      ▼                   │
│      ┌─────────────┐        ┌─────────────┐        ┌─────────────┐            │
│      │ Driver Pod  │        │ Driver Pod  │        │ Driver Pod  │            │
│      │ session-001 │        │ session-002 │        │ session-003 │            │
│      │ :15002      │        │ :15002      │        │ :15002      │            │
│      └─────────────┘        └─────────────┘        └─────────────┘            │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Connect Proxy (port 15002)

The Connect Proxy is an L7 gRPC proxy that:
- Listens on port 15002 for Spark Connect protocol
- Extracts `x-spark-session-id` header from incoming requests
- Looks up target pod IP from Session Manager
- Establishes bidirectional gRPC stream to driver pod
- Relays messages in both directions
- Reports activity timestamps to Session Manager

**Key interfaces:**

```go
// ConnectProxy handles Spark Connect gRPC streams
type ConnectProxy struct {
    sessionMgr  *SessionManager
    listener    net.Listener
    logger      *slog.Logger
}

// ServeConnect starts the Spark Connect proxy server
func (p *ConnectProxy) ServeConnect(ctx context.Context) error

// handleStream proxies a single gRPC stream to target driver
func (p *ConnectProxy) handleStream(ctx context.Context, conn net.Conn) error
```

### 2. Session Manager

The Session Manager maintains the mapping between session IDs and driver pods:

**Responsibilities:**
- Watch SparkApplication CRDs for driver pod IPs
- Maintain session registry (session_id → pod_ip, last_activity, created_at)
- Track activity timestamps per session
- Enforce idle timeout (default: 30 min)
- Enforce max TTL (default: 4 hours)
- Delete SparkApplications for expired sessions

**Key interfaces:**

```go
// Session represents an active Spark Connect session
type Session struct {
    ID           string
    Namespace    string
    AppName      string
    PodIP        string
    Team         string
    Environment  string
    User         string
    CreatedAt    time.Time
    LastActivity time.Time
}

// SessionManager manages Spark Connect sessions
type SessionManager struct {
    sessions     map[string]*Session  // session_id -> Session
    podToSession map[string]string    // pod_ip -> session_id
    k8sClient    dynamic.Interface
    mu           sync.RWMutex
    logger       *slog.Logger

    // Configurable timeouts
    idleTimeout  time.Duration  // Default: 30 min
    maxTTL       time.Duration  // Default: 4 hours
}

// RegisterSession creates or updates a session
func (m *SessionManager) RegisterSession(ctx context.Context, session *Session) error

// GetSession returns session by ID
func (m *SessionManager) GetSession(sessionID string) (*Session, bool)

// RecordActivity updates last activity timestamp
func (m *SessionManager) RecordActivity(sessionID string)

// StartWatcher begins watching SparkApplication CRDs
func (m *SessionManager) StartWatcher(ctx context.Context) error

// StartCleanupLoop periodically checks and cleans expired sessions
func (m *SessionManager) StartCleanupLoop(ctx context.Context) error
```

### 3. Pod Watcher

Watches SparkApplication CRDs to discover driver pod IPs:

```go
// PodWatcher watches for SparkApplication changes
type PodWatcher struct {
    k8sClient   dynamic.Interface
    sessionMgr  *SessionManager
    logger      *slog.Logger
}

// Watch starts watching SparkApplications across namespaces
func (w *PodWatcher) Watch(ctx context.Context) error

// handleAdd processes new SparkApplication
func (w *PodWatcher) handleAdd(obj *unstructured.Unstructured)

// handleUpdate processes SparkApplication updates (pod IP available)
func (w *PodWatcher) handleUpdate(oldObj, newObj *unstructured.Unstructured)

// handleDelete processes SparkApplication deletion
func (w *PodWatcher) handleDelete(obj *unstructured.Unstructured)
```

## Data Flow

### Flow 1: Connect to Existing Driver (BYOD)

```
1. Client already has a running SparkApplication (created via batch API)
2. Client connects: sc://gateway:15002
   Headers: x-spark-session-id: my-spark-app
            x-spark-namespace: sjns-data-control
3. Connect Proxy extracts session ID from header
4. Session Manager looks up pod IP for "my-spark-app" in "sjns-data-control"
5. Proxy establishes gRPC connection to pod_ip:15002
6. Bidirectional stream relay begins
7. Session Manager records activity on each message
```

### Flow 2: On-Demand Session Creation

```
1. Client connects: sc://gateway:15002
   Headers: x-spark-session-id: (empty or "new")
            x-doordash-team: feature-engineering
            x-doordash-environment: staging
2. Connect Proxy sees no session ID
3. Session Manager creates new SparkApplication:
   - Name: spark-connect-{team}-{random}
   - Namespace: sjns-{team}-{environment}
   - Spark Connect enabled
4. Wait for driver pod to be ready (poll status)
5. Session Manager registers session with pod IP
6. Proxy returns session ID to client (via gRPC metadata)
7. Proxy establishes stream to new driver
8. Client caches session ID for reconnection
```

### Flow 3: Idle Session Cleanup

```
1. Cleanup loop runs every 1 minute
2. For each session:
   a. Check if (now - last_activity) > idle_timeout (30 min)
   b. Check if (now - created_at) > max_ttl (4 hours)
3. If either condition met:
   a. Close any active connections to that session
   b. Delete SparkApplication CRD
   c. Remove session from registry
4. Log cleanup action for audit
```

## Configuration

Add to `config/prod.yml`:

```yaml
config:
  # Existing batch config
  cluster_proxy_url: "https://dash-data-usw2-001-data-01.proxy.cluster.doordash.com"
  vault_address: ""
  vault_approle_file_path: "secrets/approle.json"
  vault_mount_path: "secret"
  vault_token_secret_path: "spark-gateway/cluster-proxy-service/token"

  # New Spark Connect config
  spark_connect:
    enabled: true
    port: 15002
    idle_timeout_minutes: 30
    max_ttl_hours: 4
    default_namespace_pattern: "sjns-{team}-{environment}"

    # On-demand session defaults
    on_demand:
      enabled: true
      spark_version: "4.1.0"
      driver_cores: "2"
      driver_memory: "4g"
      executor_instances: 2
      executor_cores: "2"
      executor_memory: "4g"
```

## Client Usage

### Python (PySpark)

```python
from pyspark.sql import SparkSession

# Connect to existing session
spark = SparkSession.builder \
    .remote("sc://spark-gateway.spark-gateway.svc.cluster.local:15002") \
    .config("spark.connect.grpc.header.x-spark-session-id", "my-spark-app") \
    .config("spark.connect.grpc.header.x-spark-namespace", "sjns-data-control") \
    .getOrCreate()

# Or request on-demand session
spark = SparkSession.builder \
    .remote("sc://spark-gateway.spark-gateway.svc.cluster.local:15002") \
    .config("spark.connect.grpc.header.x-doordash-team", "feature-engineering") \
    .config("spark.connect.grpc.header.x-doordash-environment", "staging") \
    .getOrCreate()

# Use Spark as normal
df = spark.sql("SELECT * FROM catalog.schema.table")
df.show()
```

### Java

```java
SparkSession spark = SparkSession.builder()
    .remote("sc://spark-gateway.spark-gateway.svc.cluster.local:15002")
    .config("spark.connect.grpc.header.x-spark-session-id", "my-spark-app")
    .config("spark.connect.grpc.header.x-spark-namespace", "sjns-data-control")
    .getOrCreate();
```

## File Structure

```
services/spark-gateway/
├── cmd/server/
│   ├── main.go
│   └── fx.go              # Add ConnectProxy and SessionManager
├── internal/
│   ├── handler/
│   │   └── spark_gateway.go  # Existing batch handler
│   ├── connect/           # NEW
│   │   ├── proxy.go       # L7 gRPC proxy
│   │   ├── session.go     # Session management
│   │   └── watcher.go     # Pod watcher
│   ├── client/
│   │   └── k8s_client.go  # Existing K8s client
│   └── config/
│       └── config.go      # Add SparkConnectConfig
└── config/
    ├── local.yml
    ├── staging.yml
    └── prod.yml
```

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1)
1. Add SparkConnectConfig to config.go
2. Implement SessionManager with in-memory registry
3. Implement PodWatcher using existing K8s client
4. Unit tests for session management

### Phase 2: Connect Proxy (Week 2)
1. Implement ConnectProxy gRPC server on port 15002
2. Implement header extraction and routing
3. Implement bidirectional stream relay
4. Integration tests with local Spark Connect server

### Phase 3: Session Lifecycle (Week 3)
1. Implement on-demand session creation
2. Implement idle timeout detection
3. Implement max TTL enforcement
4. Implement cleanup loop

### Phase 4: Integration & Testing (Week 4)
1. Deploy to staging cluster
2. End-to-end testing with PySpark client
3. Load testing for concurrent sessions
4. Documentation and runbooks

## Metrics

Expose the following metrics:

```
spark_connect_sessions_active          # Gauge: current active sessions
spark_connect_sessions_created_total   # Counter: total sessions created
spark_connect_sessions_expired_total   # Counter: sessions expired (idle/ttl)
spark_connect_proxy_requests_total     # Counter: total proxy requests
spark_connect_proxy_errors_total       # Counter: proxy errors
spark_connect_session_duration_seconds # Histogram: session lifetimes
spark_connect_message_latency_seconds  # Histogram: proxy latency
```

## Future Enhancements

1. **Hot Pool**: Pre-warmed driver pods for instant session startup
2. **Okta Authentication**: Validate user identity via Okta JWT
3. **Team Quotas**: Limit concurrent sessions per team
4. **Session Persistence**: Store sessions in CRDB for HA
5. **Multi-Cluster**: Route to different clusters based on team/environment

## Appendix: Why Cluster Proxy Can't Do This

The existing Cluster Proxy:
- Routes REST/HTTP calls to K8s API server
- Handles authentication via Bearer tokens from Vault
- Cannot route application traffic to pod IPs

Spark Connect requires:
- Bidirectional gRPC streams (not REST)
- Routing to driver pod IPs (not K8s API)
- Session affinity based on gRPC headers
- Traffic monitoring for idle detection

These are fundamentally different routing requirements that need an L7 application proxy.
