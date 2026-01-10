# Spark Connect Platform for Pedregal

## Metadata

| Field | Value |
|-------|-------|
| **Author(s)** | Data Infrastructure Team, Laxman Mamidi |
| **Status** | Draft |
| **Origin** | New |
| **History** | Drafted: January 2026 |
| **Keywords** | Spark Connect, SK8, Kubernetes, Pedregal, Interactive Spark, Unity Catalog, gRPC |
| **References** | [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html), Pedregal Platform Documentation, SJEM Documentation |

## Reviewers

| Reviewer | Status | Notes |
|----------|--------|-------|
| TBD | Pending | |
| TBD | Pending | |
| TBD | Pending | |

---

## What?

This project implements **Spark Connect** as a unified interactive Spark access layer for DoorDash's Pedregal Spark Platform. Spark Connect enables remote, client-server architecture for Spark where clients (notebooks, services, pipelines) connect to a centralized Spark cluster via gRPC without running a local Spark driver.

The solution provides:
1. **Spark Gateway** - Central entry point with authentication, routing, and gRPC proxy
2. **Spark Connect Server** - Runs inside Driver pods on SK8 (Spark on Kubernetes)
3. **Client Libraries** - PySpark, Scala, Java, and Go clients for various use cases
4. **Unity Catalog Integration** - Unified data governance across all connections

**Key Benefit**: Static endpoints (e.g., `sc://spark-gateway.doordash.team:15002`) eliminate SSH tunneling and provide consistent access patterns across all client types.

---

## Why?

### Current Pain Points

1. **SSH Tunneling Overhead**: Current interactive Spark access requires SSH tunnels to EMR/Databricks clusters, adding latency and operational complexity.

2. **No Static Endpoints**: Cluster IPs change on restart, requiring users to reconfigure connections frequently.

3. **Fragmented Access Patterns**: Different teams use different methods (notebooks, scripts, services) with inconsistent authentication and routing.

4. **EU Latency Issues**: No EU-region Spark clusters for EU-based teams, causing cross-Atlantic latency.

5. **Limited Self-Service**: Teams cannot provision their own Spark clusters; depends on central platform team.

### User Stories

> "As a **data scientist**, I need a stable Spark endpoint that I can use in my Jupyter notebook without setting up SSH tunnels every time my cluster restarts."

> "As a **ML engineer** using Metaflow, I need my pipeline steps running on ML K8s clusters to access Spark for feature engineering without complex network configuration."

> "As a **backend service owner**, I need to query feature tables from my Python service with low latency using a direct API connection."

> "As a **platform engineer**, I need to run Spark integration tests in CI/CD pipelines against isolated test clusters."

### Benefits and Expected Outcomes

| Benefit | Expected Outcome |
|---------|------------------|
| **Static Endpoints** | Zero SSH tunneling; `sc://team-cluster.doordash.team:15002` works permanently |
| **Unified Access** | Single authentication flow (Okta/PAT) for all client types |
| **Low Latency** | EU clusters for EU teams; <100ms for interactive queries |
| **Self-Service** | Teams provision clusters via API (like Fabricator) |
| **Cost Efficiency** | Shared hot clusters for test workloads; dedicated clusters for production |

---

## Goals

1. **Deploy Spark Connect on SK8**: Enable Spark Connect Server on all SK8 Driver pods (port 15002)
2. **Build Spark Gateway**: Central gRPC proxy with authentication, routing, session affinity, and audit logging
3. **Static CNAME Endpoints**: Provide `sc://<team>-cluster.doordash.team:15002` for each team
4. **Unity Catalog Integration**: All Spark Connect sessions access UC-governed tables
5. **Multi-Region Support**: Deploy EU-region clusters for low-latency EU access
6. **Self-Service Provisioning**: API for teams to create/delete their own clusters
7. **Support 5 Client Patterns**: Direct API, Jupyter Notebooks, Metaflow, CI/CD, DCP Playground

---

## Non-Goals

1. **Replacing Batch Jobs**: Spark Runner remains the execution path for batch/scheduled jobs (DCP manifests)
2. **Spark Thrift Server**: Not implementing JDBC/ODBC interface; Spark Connect only
3. **Custom Spark Distributions**: Using standard Apache Spark 3.5+ with Unity Catalog plugin
4. **Real-time Streaming via Connect**: Structured Streaming jobs should use Spark Runner, not Connect
5. **Multi-tenant Session Isolation**: Initial release assumes one session per cluster; multi-tenant shared clusters are Phase 2

---

## Who?

| Role | DRI(s) / Developer(s) | Platform |
|------|----------------------|----------|
| DRI (Overall) | Data Infrastructure Team | Pedregal, SK8 |
| Spark Gateway | TBD | Go, gRPC |
| SK8 Integration | TBD | Kubernetes, Spark Operator |
| Client Libraries | TBD | Python, Scala, Go |
| Unity Catalog | TBD | UC, Iceberg |
| Observability | TBD | Chronosphere, ODIN |

---

## Design

### Introduction

Spark Connect decouples the Spark client from the Spark driver, enabling a thin client to send operations over gRPC to a remote Spark cluster. This architecture is ideal for:
- Interactive sessions from notebooks
- Programmatic access from backend services
- CI/CD pipeline testing
- ML pipelines requiring Spark for feature engineering

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                         SPARK CONNECT PLATFORM ARCHITECTURE                                          │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                     │
│   ┌─────────────────────────────────────┐                                                                           │
│   │          CLIENT LAYER               │                                                                           │
│   │                                     │                                                                           │
│   │  ┌─────────┐ ┌─────────┐ ┌────────┐ │                                                                           │
│   │  │Direct   │ │Metaflow │ │Jupyter │ │                                                                           │
│   │  │API      │ │Pipeline │ │Notebook│ │                                                                           │
│   │  └────┬────┘ └────┬────┘ └───┬────┘ │                                                                           │
│   │       │           │          │      │                                                                           │
│   │  ┌────┴───────────┴──────────┴────┐ │                                                                           │
│   │  │     SPARK CONNECT CLIENT       │ │                                                                           │
│   │  │  ┌─────────┐ ┌───────────────┐ │ │                                                                           │
│   │  │  │ PySpark │ │spark-connect- │ │ │                                                                           │
│   │  │  │         │ │  client-jvm   │ │ │                                                                           │
│   │  │  └─────────┘ └───────────────┘ │ │                                                                           │
│   │  │  ┌─────────┐ ┌───────────────┐ │ │                                                                           │
│   │  │  │  Scala  │ │spark-connect- │ │ │                                                                           │
│   │  │  │ client  │ │     go        │ │ │                                                                           │
│   │  │  └─────────┘ └───────────────┘ │ │                                                                           │
│   │  │                                │ │                                                                           │
│   │  │  • Serialize DataFrame ops    │ │                                                                           │
│   │  │  • gRPC channel management    │ │                                                                           │
│   │  │  • Arrow result conversion    │ │                                                                           │
│   │  └────────────────┬───────────────┘ │                                                                           │
│   │                   │                 │                                                                           │
│   └───────────────────┼─────────────────┘                                                                           │
│                       │                                                                                             │
│                       │ gRPC (:15002)                                                                               │
│                       ▼                                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                          SPARK GATEWAY                                                       │   │
│   │                                    spark-gateway.doordash.team                                               │   │
│   │                                                                                                              │   │
│   │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                                     │   │
│   │   │ Auth Handler │  │gRPC Listener │  │  Router/LB   │  │  gRPC Proxy  │                                     │   │
│   │   │              │  │              │  │              │  │              │                                     │   │
│   │   │• Okta/PAT    │  │• TLS term    │  │• Team-based  │  │• Forward to  │                                     │   │
│   │   │• Extract team│  │• Port 15002  │  │• Session     │  │  driver pod  │                                     │   │
│   │   │• Validate    │  │              │  │  affinity    │  │• Stream      │                                     │   │
│   │   └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘                                     │   │
│   │                                                                                                              │   │
│   │   Features:                                                                                                  │   │
│   │   • TLS termination           • Team-based routing          • Hot cluster management                         │   │
│   │   • Authentication (Okta/PAT) • Session affinity            • Automatic failover                             │   │
│   │   • Audit logging → Taulu     • Load balancing              • Health checking                                │   │
│   │                                                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                       │                                                                                             │
│                       │                          ┌────────────┐                                                     │
│                       │                          │   Taulu    │  (Audit Logs)                                       │
│                       │                          └────────────┘                                                     │
│                       │                                                                                             │
│                       │ gRPC                                                                                        │
│                       ▼                                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                       K8s CONTROL PLANE                                                      │   │
│   │                                                                                                              │   │
│   │   ┌──────────────────────┐  ┌──────────────────────┐                                                         │   │
│   │   │   SK8s Operator      │  │  Ingress/Proxy       │                                                         │   │
│   │   │   (Spark K8s Op)     │  │                      │                                                         │   │
│   │   │                      │  │  • Route to driver   │                                                         │   │
│   │   │   • Manage CRDs      │  │  • Service discovery │                                                         │   │
│   │   │   • Driver lifecycle │  │                      │                                                         │   │
│   │   │   • Executor scaling │  │                      │                                                         │   │
│   │   └──────────────────────┘  └──────────────────────┘                                                         │   │
│   │                                                                                                              │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                       │                                                                                             │
│                       ▼                                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                          K8 CLUSTER                                                          │   │
│   │                                                                                                              │   │
│   │   ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐   │   │
│   │   │                                      DRIVER POD                                                      │   │   │
│   │   │                                                                                                      │   │   │
│   │   │   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐   │   │   │
│   │   │   │                              SPARK CONNECT SERVER                                            │   │   │   │
│   │   │   │                                                                                              │   │   │   │
│   │   │   │   ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                │   │   │   │
│   │   │   │   │ ExecutePlan    │ │ AnalyzePlan    │ │ Config         │ │ Interrupt      │                │   │   │   │
│   │   │   │   │ Handler        │ │ Handler        │ │ Handler        │ │ Handler        │                │   │   │   │
│   │   │   │   │                │ │                │ │                │ │                │                │   │   │   │
│   │   │   │   │• Receive plan  │ │• Schema info   │ │• Get/Set conf  │ │• Cancel query  │                │   │   │   │
│   │   │   │   │• Invoke engine │ │• Explain plan  │ │• Spark props   │ │• Stop session  │                │   │   │   │
│   │   │   │   └────────────────┘ └────────────────┘ └────────────────┘ └────────────────┘                │   │   │   │
│   │   │   │                                         │                                                    │   │   │   │
│   │   │   └─────────────────────────────────────────┼────────────────────────────────────────────────────┘   │   │   │
│   │   │                                             │                                                        │   │   │
│   │   │                                             ▼                                                        │   │   │
│   │   │   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐   │   │   │
│   │   │   │                                   SPARK SESSION                                              │   │   │   │
│   │   │   │                                                                                              │   │   │   │
│   │   │   │   ┌────────────────┐ ┌────────────────┐ ┌────────────────┐                                   │   │   │   │
│   │   │   │   │   Analyzer     │ │    Catalyst    │ │   Physical     │                                   │   │   │   │
│   │   │   │   │                │ │   Optimizer    │ │   Planner      │                                   │   │   │   │
│   │   │   │   │• Resolve refs  │ │• Rule-based    │ │• Cost-based    │                                   │   │   │   │
│   │   │   │   │• Type checking │ │• Predicate     │ │• Join strategy │                                   │   │   │   │
│   │   │   │   │                │ │  pushdown      │ │                │                                   │   │   │   │
│   │   │   │   └────────────────┘ └────────────────┘ └────────────────┘                                   │   │   │   │
│   │   │   │                                                                                              │   │   │   │
│   │   │   │   ┌────────────────┐ ┌────────────────┐                                                      │   │   │   │
│   │   │   │   │ DAG Scheduler  │ │ Task Scheduler │   Unity Catalog Config:                              │   │   │   │
│   │   │   │   │                │ │                │   spark.sql.catalog.datalake = io.unitycatalog...    │   │   │   │
│   │   │   │   │• Stage DAG     │ │• Task assign   │                                                      │   │   │   │
│   │   │   │   │• Shuffle deps  │ │• Locality      │                                                      │   │   │   │
│   │   │   │   └────────────────┘ └────────────────┘                                                      │   │   │   │
│   │   │   │                                                                                              │   │   │   │
│   │   │   └─────────────────────────────────────────────────────────────────────────────────────────────┘   │   │   │
│   │   │                                                                                                      │   │   │
│   │   │   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                                         │   │   │
│   │   │   │   Spark UI     │  │ OTel Sidecar   │  │  Fluent Bit    │  (Sidecars)                             │   │   │
│   │   │   │   (:4040)      │  │                │  │                │                                         │   │   │
│   │   │   └────────────────┘  └────────────────┘  └────────────────┘                                         │   │   │
│   │   │                                                                                                      │   │   │
│   │   └──────────────────────────────────────────────────────────────────────────────────────────────────────┘   │   │
│   │                       │                                                                                      │   │
│   │                       ▼                                                                                      │   │
│   │   ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┐  │   │
│   │   │                                       EXECUTOR PODS                                                    │  │   │
│   │   │                                                                                                        │  │   │
│   │   │   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                                           │  │   │
│   │   │   │  Executor 1    │  │  Executor 2    │  │  Executor N    │                                           │  │   │
│   │   │   │                │  │                │  │                │                                           │  │   │
│   │   │   │  • Run tasks   │  │  • Run tasks   │  │  • Run tasks   │                                           │  │   │
│   │   │   │  • Cache data  │  │  • Cache data  │  │  • Cache data  │                                           │  │   │
│   │   │   └───────┬────────┘  └───────┬────────┘  └───────┬────────┘                                           │  │   │
│   │   │           │                   │                   │                                                    │  │   │
│   │   └───────────┼───────────────────┼───────────────────┼────────────────────────────────────────────────────┘  │   │
│   │               │                   │                   │                                                       │   │
│   └───────────────┼───────────────────┼───────────────────┼───────────────────────────────────────────────────────┘   │
│                   │                   │                   │                                                           │
│                   ▼                   ▼                   ▼                                                           │
│   ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                         UNITY CATALOG                                                          │   │
│   │                                                                                                                │   │
│   │   • Table metadata (schema, location, partitions)                                                              │   │
│   │   • Access control (ACLs, row/column security)                                                                 │   │
│   │   • Data lineage                                                                                               │   │
│   │                                                                                                                │   │
│   └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                   │                                                                                                   │
│                   ▼                                                                                                   │
│   ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                                       OBJECT STORE (S3/Taulu)                                                  │   │
│   │                                                                                                                │   │
│   │   s3://doordash-datalake/...                                                                                   │   │
│   │   • Iceberg tables                                                                                             │   │
│   │   • Parquet files                                                                                              │   │
│   │   • Delta tables                                                                                               │   │
│   │                                                                                                                │   │
│   └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Connection Patterns

The platform supports 5 client connection patterns:

#### Pattern 1: Direct API Connection

For backend services (feature services, ML inference, data APIs):

```
┌──────────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Service/Application │      │ Spark Connect   │      │  Spark Gateway  │      │ Spark Connect   │
│                      │      │ Client Library  │      │                 │      │ Server (Driver) │
│  SparkSession.builder│─────►│ • Serialize ops │─────►│ • Auth + Route  │─────►│ • Execute ops   │
│  .remote("sc://...")│      │ • gRPC channel  │      │                 │      │                 │
└──────────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

#### Pattern 2: Jupyter Notebook Connection

For interactive data exploration:

```
┌──────────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  JupyterHub/         │      │ Spark Connect   │      │  Spark Gateway  │      │  Team Cluster   │
│  JupyterLab          │      │ Client          │      │                 │      │                 │
│                      │      │                 │ gRPC │ • Auth + Route  │      │ • Namespace     │
│  • get spark object  │─────►│ • Serialize ops │─────►│ • Session       │─────►│ • Catalog       │
│  • DataFrame creation│      │                 │      │   affinity      │      │                 │
│  • pull to local     │      │                 │      │                 │      │                 │
└──────────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

#### Pattern 3: Metaflow Pipeline Connection

For ML pipelines accessing Spark for feature engineering:

```
┌──────────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────────┐
│  Metaflow            │      │  Metaflow       │      │  Spark Gateway  │      │ Spark Cluster       │
│  • featureflow       │─────►│  K8s Pod ML     │─────►│                 │─────►│ (Data Cluster)      │
│  • extract features  │      │                 │      │ Network policy: │      │                     │
│  • train             │      │                 │      │ Allow ingress   │      │ • Read features     │
│                      │      │                 │      │ from ML cluster │      │   from Unity Catalog│
│                      │      │                 │      │                 │      │ • Process via Spark │
│                      │      │                 │      │ Auth: Service   │      │ • Return results    │
│                      │      │                 │      │ account token   │      │   via Spark Connect │
└──────────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────────┘
```

#### Pattern 4: CI/CD Pipeline Connection

For integration testing in GitHub Actions:

```
┌──────────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  GitHub Actions:     │      │  Test code      │      │  Spark Gateway  │      │  Test Cluster   │
│  jobs:               │      │                 │      │                 │      │                 │
│    integration-test: │─────►│ • Build Spark   │─────►│ • Route to      │─────►│ • Namespace     │
│      env: test       │      │   Connect client│      │   dedicated     │      │ • Catalog       │
│      steps:          │      │                 │      │   test cluster  │      │                 │
│        run pytests   │      │                 │      │ • Isolated from │      │                 │
│                      │      │                 │      │   staging/prod  │      │                 │
│                      │      │                 │      │ • Auto-cleanup  │      │                 │
│                      │      │                 │      │   after TTL     │      │                 │
└──────────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

#### Pattern 5: DCP Playground (Interactive Sandbox)

For testing DCP manifests interactively:

```
┌──────────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  DCP Playground UI   │      │  Playground     │      │  Spark Gateway  │      │ Playground      │
│                      │      │  Service        │      │                 │      │ Cluster         │
│  • Edit manifest     │─────►│ • Parse SQL     │─────►│ • Route to user │─────►│ • User namespace│
│  • Click "Test"      │      │ • Create session│      │   playground    │      │ • Read prod data│
│  • View results      │      │                 │      │   cluster       │      │ • Write to      │
│                      │      │                 │      │                 │      │   playground    │
│                      │      │                 │      │                 │      │   tables only   │
└──────────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

### Spark Connect Internal Architecture

The Spark Connect Server runs inside the Driver Pod and handles gRPC requests:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              SPARK CONNECT REQUEST FLOW                                      │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                             │
│   CLIENT                                              SERVER (Driver Pod)                   │
│                                                                                             │
│   df = spark.sql("SELECT * FROM table")                                                     │
│        │                                                                                    │
│        │ 1. Serialize to Protobuf                                                           │
│        ▼                                                                                    │
│   ┌─────────────────────┐                                                                   │
│   │ Logical Plan (Proto)│                                                                   │
│   │ • SQL text          │                                                                   │
│   │ • DataFrame ops     │                                                                   │
│   └─────────┬───────────┘                                                                   │
│             │                                                                               │
│             │ 2. gRPC ExecutePlan                                                           │
│             ▼                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                         SPARK CONNECT SERVER                                         │   │
│   │                                                                                      │   │
│   │   ┌─────────────────────┐                                                            │   │
│   │   │ ExecutePlanHandler  │ ◄── Receives logical plan                                  │   │
│   │   └─────────┬───────────┘                                                            │   │
│   │             │                                                                        │   │
│   │             │ 3. Parse & Analyze                                                     │   │
│   │             ▼                                                                        │   │
│   │   ┌─────────────────────┐                                                            │   │
│   │   │      ANALYZER       │ • Resolve table/column references via Unity Catalog        │   │
│   │   │                     │ • Type checking and validation                             │   │
│   │   └─────────┬───────────┘                                                            │   │
│   │             │                                                                        │   │
│   │             │ 4. Optimize                                                            │   │
│   │             ▼                                                                        │   │
│   │   ┌─────────────────────┐                                                            │   │
│   │   │  CATALYST OPTIMIZER │ • Predicate pushdown                                       │   │
│   │   │                     │ • Column pruning                                           │   │
│   │   │                     │ • Join reordering                                          │   │
│   │   └─────────┬───────────┘                                                            │   │
│   │             │                                                                        │   │
│   │             │ 5. Physical Planning                                                   │   │
│   │             ▼                                                                        │   │
│   │   ┌─────────────────────┐                                                            │   │
│   │   │  PHYSICAL PLANNER   │ • Choose join strategies (broadcast, sort-merge)           │   │
│   │   │                     │ • Partition planning                                       │   │
│   │   └─────────┬───────────┘                                                            │   │
│   │             │                                                                        │   │
│   │             │ 6. Schedule                                                            │   │
│   │             ▼                                                                        │   │
│   │   ┌─────────────────────┐                                                            │   │
│   │   │    DAG SCHEDULER    │ • Create stage DAG                                         │   │
│   │   │    TASK SCHEDULER   │ • Assign tasks to executors                                │   │
│   │   └─────────┬───────────┘                                                            │   │
│   │             │                                                                        │   │
│   └─────────────┼────────────────────────────────────────────────────────────────────────┘   │
│                 │                                                                            │
│                 │ 7. Execute on Executors                                                    │
│                 ▼                                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                              EXECUTORS                                               │   │
│   │                                                                                      │   │
│   │   Read from S3 ──► Process ──► Return results to Driver                              │   │
│   │                                                                                      │   │
│   └─────────────────────────────────────────────────────────────────────────────────────┘   │
│                 │                                                                            │
│                 │ 8. Stream results as Arrow batches                                         │
│                 ▼                                                                            │
│   ┌─────────────────────┐                                                                   │
│   │ Arrow Batches       │ • Columnar format                                                 │
│   │ (streaming)         │ • Efficient serialization                                         │
│   └─────────┬───────────┘                                                                   │
│             │                                                                               │
│             │ 9. Deserialize to DataFrame                                                   │
│             ▼                                                                               │
│   ┌─────────────────────┐                                                                   │
│   │ pandas DataFrame    │                                                                   │
│   │ or PySpark DataFrame│                                                                   │
│   └─────────────────────┘                                                                   │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Component Impact

| Component Layer | Impact Summary | Details |
|----------------|----------------|---------|
| **Domain Layer** | New Spark Gateway service | Go service with gRPC proxy, auth, routing |
| **Network Layer** | gRPC traffic on port 15002 | TLS-terminated at gateway, internal plaintext |
| **Caching** | Session affinity in Gateway | Routes same client to same driver for session reuse |
| **Error Handling** | Graceful degradation | Client retries on transient failures; gateway health checks |
| **Other Key Changes** | SK8 operator config | Enable Spark Connect plugin on all Driver pods |

---

## Selected Approach

### Alternatives Considered

#### Approach 1: Databricks SQL Warehouse

Use Databricks SQL Endpoints for interactive queries.

**Pros:** Managed service, built-in optimization, serverless scaling.

**Cons:**
- Vendor lock-in to Databricks
- No integration with SK8/Pedregal
- Cost at scale (per-query pricing)
- Limited to SQL (no DataFrame API)

**Decision:** Rejected. Does not integrate with our SK8 strategy.

---

#### Approach 2: Spark Thrift Server (JDBC/ODBC)

Deploy Spark Thrift Server for SQL access via JDBC.

**Pros:** Standard JDBC interface, works with BI tools.

**Cons:**
- SQL-only (no DataFrame operations)
- No streaming results (full result set in memory)
- Single-threaded query execution
- Legacy protocol, not actively developed

**Decision:** Rejected. Spark Connect is the modern replacement with better performance.

---

#### Approach 3: Direct Driver Access (No Gateway)

Expose Driver pods directly to clients via K8s Service.

**Pros:** Simpler architecture, lower latency (one less hop).

**Cons:**
- No centralized authentication
- No audit logging
- No session affinity across driver restarts
- Difficult to implement team-based routing
- No hot cluster management

**Decision:** Rejected. Requires gateway for enterprise features.

---

#### Approach 4: Spark Connect via Spark Gateway (Selected)

Build Spark Gateway as a gRPC proxy with authentication, routing, and session management.

**Pros:**
- Centralized auth (Okta/PAT)
- Audit logging to Taulu
- Team-based routing
- Session affinity
- Hot cluster management
- Static endpoints (CNAME)
- Works with Unity Catalog

**Cons:**
- Additional hop (minor latency)
- Gateway is a new service to maintain

**Decision:** Selected. Best balance of enterprise features and Spark Connect benefits.

---

## API Changes

### Spark Gateway gRPC API

The Spark Gateway exposes the standard Spark Connect gRPC service:

```protobuf
// Proxied from Apache Spark Connect
service SparkConnectService {
  rpc ExecutePlan(ExecutePlanRequest) returns (stream ExecutePlanResponse);
  rpc AnalyzePlan(AnalyzePlanRequest) returns (AnalyzePlanResponse);
  rpc Config(ConfigRequest) returns (ConfigResponse);
  rpc Interrupt(InterruptRequest) returns (InterruptResponse);
  rpc AddArtifacts(stream AddArtifactsRequest) returns (AddArtifactsResponse);
}
```

### Custom Headers for Routing

Clients must include headers for team routing:

| Header | Required | Description |
|--------|----------|-------------|
| `x-doordash-team` | Yes | Team name for routing (e.g., `feature-engineering`) |
| `x-doordash-env` | Yes | Environment (`dev`, `staging`, `prod`) |
| `x-doordash-user` | No | User email (extracted from Okta if not provided) |
| `x-metaflow-run-id` | No | Metaflow run ID for correlation |

### Client Connection Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://spark-gateway.doordash.team:15002") \
    .config("spark.connect.grpc.header.x-doordash-team", "feature-engineering") \
    .config("spark.connect.grpc.header.x-doordash-env", "prod") \
    .appName("my-notebook") \
    .getOrCreate()

# Standard Spark operations work transparently
df = spark.sql("SELECT * FROM datalake.features.user_embeddings LIMIT 10")
df.show()
```

---

## Migration Path

This is a **greenfield deployment** - no migration from existing systems required.

### Phased Rollout

| Phase | Duration | Scope | Success Criteria |
|-------|----------|-------|------------------|
| Phase 1 | 2 weeks | Dev/Test clusters only | 10 users, <5s query latency |
| Phase 2 | 2 weeks | Staging + select teams | 50 users, 99% uptime |
| Phase 3 | 4 weeks | Production (25% teams) | 200 users, audit logging working |
| Phase 4 | Ongoing | Production (100% teams) | Full adoption, self-service |

### Backward Compatibility

Not applicable - new capability. Existing SSH-tunnel access remains available during transition.

---

## Testing Plan

### Unit Tests

- Spark Gateway routing logic
- Authentication handler (mock Okta)
- gRPC proxy message forwarding
- Session affinity implementation

### Integration Tests

- End-to-end: PySpark client → Gateway → SK8 Driver → S3
- Unity Catalog table access
- Multi-tenant routing (team A → cluster A, team B → cluster B)
- Connection pooling and session reuse

### Load Testing

| Scenario | Concurrent Sessions | Queries/sec | Expected Latency |
|----------|---------------------|-------------|------------------|
| Light | 10 | 5 | <100ms |
| Medium | 50 | 25 | <200ms |
| Heavy | 200 | 100 | <500ms |

### Manual QA

- Jupyter notebook connection flow
- Metaflow pipeline integration
- CI/CD test execution
- Error handling and retry behavior

---

## Rollout Plan

### Feature Flag

Controlled by Dynamic Value: `SPARK_CONNECT_ENABLED`

- `0%`: Disabled (default)
- `10%`: Early adopter teams
- `50%`: Expanded rollout
- `100%`: GA

### Rollback Plan

1. **Immediate**: Set `SPARK_CONNECT_ENABLED` to `0%`
2. **Gateway rollback**: Revert to previous Gateway deployment
3. **SK8 config**: Disable Spark Connect plugin in SparkApplication CRD

---

## Monitoring & Alerting

### Observability

| Metric | Attributes | Description |
|--------|------------|-------------|
| `spark_connect_requests_total` | team, env, status | Total gRPC requests |
| `spark_connect_latency_ms` | team, env, percentile | Request latency histogram |
| `spark_connect_active_sessions` | team, env | Gauge of active sessions |
| `spark_connect_errors_total` | team, env, error_type | Error counter |
| `spark_gateway_upstream_health` | cluster_id | Health check status |

### Alerting

| Alert | Condition | Severity |
|-------|-----------|----------|
| High Error Rate | Error rate >5% for 5 min | P2 |
| High Latency | p95 latency >1s for 10 min | P3 |
| Gateway Down | No healthy upstreams | P1 |
| Session Limit | Active sessions >90% capacity | P3 |

### Dashboards

- **Spark Connect Overview**: Request rate, latency, errors by team
- **Gateway Health**: Upstream health, connection pool stats
- **Per-Team Usage**: Query patterns, resource consumption

---

## Security

### Authentication

- **Okta SSO**: For interactive users (notebooks, playground)
- **Service Account Tokens**: For backend services and CI/CD
- **PAT (Personal Access Tokens)**: For individual developer access

### Authorization

- Unity Catalog ACLs enforce table-level access
- Team routing ensures isolation between teams
- Network policies restrict pod-to-pod communication

### Data Security

- TLS encryption for all client-gateway communication
- Internal traffic uses mTLS (service mesh)
- No PII in metrics or logs (user IDs only)

---

## Estimations and Risks

### Estimations

| Component | Estimation |
|-----------|------------|
| Spark Gateway (gRPC proxy, auth, routing) | 4 weeks |
| SK8 Spark Connect configuration | 2 weeks |
| Client library helpers (Python, Go) | 2 weeks |
| Unity Catalog integration testing | 2 weeks |
| Observability (metrics, dashboards, alerts) | 2 weeks |
| Documentation and onboarding | 1 week |
| **Total** | **13 weeks** |

### Risks

#### 1. Gateway Latency Overhead (MEDIUM)

**Issue:** Additional network hop through gateway adds latency.

**Impact:**
- Expected: 5-10ms additional latency
- Acceptable for interactive workloads
- May be noticeable for high-frequency API calls

**Mitigation:**
- Connection pooling in gateway
- Keep-alive connections
- Co-locate gateway with SK8 clusters
- Monitor p99 latency closely

---

#### 2. Session Affinity Failures (MEDIUM)

**Issue:** If gateway loses track of session-to-cluster mapping, clients may be routed to wrong cluster.

**Impact:**
- Query failures due to missing session state
- Users need to reconnect

**Mitigation:**
- Persistent session store (Redis)
- Graceful session recovery
- Client-side retry with backoff

---

#### 3. Hot Cluster Resource Contention (HIGH)

**Issue:** Shared hot clusters may become overloaded with concurrent users.

**Impact:**
- Slow query performance
- Query timeouts
- Poor user experience

**Mitigation:**
- Per-team cluster isolation for production
- Resource quotas per session
- Auto-scaling executor pods
- Queue management for burst traffic

---

#### 4. Unity Catalog Compatibility (LOW)

**Issue:** Spark Connect may have compatibility issues with Unity Catalog plugin.

**Impact:**
- Table access failures
- Metadata resolution errors

**Mitigation:**
- Test extensively in staging
- Pin compatible Spark + UC versions
- Fallback to direct S3 paths if needed

---

#### 5. Client Library Fragmentation (MEDIUM)

**Issue:** Multiple client libraries (PySpark, Scala, Go) may have different behaviors.

**Impact:**
- Inconsistent user experience
- Support burden

**Mitigation:**
- Standardize on PySpark for primary support
- Provide wrapper libraries with consistent config
- Document known limitations per client

---

## Open Questions

1. **EU Region Clusters**: Which teams need EU-region clusters? What's the latency requirement?
2. **Self-Service API**: Should cluster provisioning use existing Fabricator patterns or new API?
3. **Cost Attribution**: How to charge teams for Spark Connect usage?
4. **Multi-Tenant Isolation**: Phase 2 - how to safely share clusters between teams?

---

## References

- [Apache Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark Connect Architecture (SPARK-39375)](https://issues.apache.org/jira/browse/SPARK-39375)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- Pedregal Platform Documentation (internal)
- SJEM Documentation (internal)

---

*Last Updated: January 2026*
