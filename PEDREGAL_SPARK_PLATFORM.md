# Pedregal Spark Platform - Complete Reference Guide

This document consolidates all architectural information, design decisions, and implementation details for the Pedregal Spark Platform at DoorDash.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Component Deep Dive](#component-deep-dive)
4. [Spark on Kubernetes (SK8)](#spark-on-kubernetes-sk8)
5. [Spark Connect](#spark-connect)
6. [Client Entry Points](#client-entry-points)
7. [Job Execution Flows](#job-execution-flows)
8. [Docker Image Strategy](#docker-image-strategy)
9. [Testing Strategy](#testing-strategy)
10. [Observability](#observability)
11. [Glossary](#glossary)

---

## Executive Summary

The **Pedregal Spark Platform** is DoorDash's unified, Pedregal-native system for running Spark workloads. It provides:

- **Lower and more predictable cost** through Kubernetes-native execution
- **Tighter operational control** with domain-specific namespaces
- **Deep integration** with the Pedregal ecosystem (DCP, Graph Runner, Taulu)

### Key Goals

1. Replace the legacy SJEM (Spark Job Execution Manager) monolithic service
2. Enable Spark on Kubernetes (SK8) as the primary compute backend
3. Provide vendor-neutral abstractions (SK8 primary, EMR fallback)
4. Support both batch and streaming workloads
5. Enable interactive development via Spark Connect

### Migration Path

```
Current State                    Future State
─────────────────────────────    ─────────────────────────────
SJEM (monolithic)          →     Spark Runner (stateless graphs)
EMR/Databricks             →     SK8 (Spark on Kubernetes)
Per-account deployment     →     Centralized platform
Vendor-specific APIs       →     Unified Pedregal interface
```

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    CLIENT LAYER                                              │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────────────────┐  │
│  │  DCP Manifest   │    │ Jupyter Notebook │   │           Direct API Calls              │  │
│  │  (YAML/pretzel) │    │    (Interactive) │   │    (REST/gRPC Job Submission)           │  │
│  └────────┬────────┘    └────────┬────────┘    └──────────────────┬──────────────────────┘  │
│           │                      │                                │                          │
│           ▼                      ▼                                ▼                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                           DCP Plugin (CoreETL/Pretzel)                               │   │
│  │   - Parses manifest → JobSpec                                                        │   │
│  │   - Creates Orchestrator primitive (Airflow DAG) for triggered jobs                  │   │
│  │   - Creates ContinuousJob primitive for streaming                                    │   │
│  └─────────────────────────────────────────┬───────────────────────────────────────────┘   │
└─────────────────────────────────────────────┼───────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              CONTROL PLANE (Spark Runner)                                    │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                         Spark Runner Graph (Pedregal Graph)                           │   │
│  │  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐                        │   │
│  │  │    Submit()    │   │    Check()     │   │   Cancel()     │                        │   │
│  │  │  - Validate    │   │  - Get status  │   │  - Stop job    │                        │   │
│  │  │  - Create exec │   │  - Return state│   │  - Cleanup     │                        │   │
│  │  └───────┬────────┘   └───────┬────────┘   └───────┬────────┘                        │   │
│  │          │                    │                    │                                  │   │
│  │          └────────────────────┼────────────────────┘                                  │   │
│  │                               │                                                       │   │
│  │  ┌──────────────────────────────────────────────────────────────────────────────┐    │   │
│  │  │                        Supporting Components                                  │    │   │
│  │  │  ┌───────────┐  ┌────────────┐  ┌──────────────┐  ┌───────────────────────┐  │    │   │
│  │  │  │    APS    │  │     AR     │  │     DCR      │  │   Debug Utilities     │  │    │   │
│  │  │  │ (Auto     │  │ (Auto      │  │ (Dynamic     │  │   - Logs              │  │    │   │
│  │  │  │ Parameter │  │ Retry/     │  │ Cluster      │  │   - Job Info          │  │    │   │
│  │  │  │ Selection)│  │ Remediate) │  │ Resizing)    │  │   - MCP Integration   │  │    │   │
│  │  │  └───────────┘  └────────────┘  └──────────────┘  └───────────────────────┘  │    │   │
│  │  └──────────────────────────────────────────────────────────────────────────────┘    │   │
│  └───────────────────────────────────────┬──────────────────────────────────────────────┘   │
│                                          │                                                   │
│  ┌───────────────────────────────────────▼──────────────────────────────────────────────┐   │
│  │                              Spark Gateway Service                                    │   │
│  │   - Receives SparkApplication CRD requests from Spark Runner                         │   │
│  │   - Enforces domain-specific namespace model                                         │   │
│  │   - Routes to appropriate cluster via Cluster Proxy Service                          │   │
│  │   - Manages job TTL and cleanup                                                      │   │
│  │   - Supports SK8 (primary) and EMR (fallback)                                        │   │
│  └───────────────────────────────────────┬──────────────────────────────────────────────┘   │
│                                          │                                                   │
│  ┌───────────────────────────────────────▼──────────────────────────────────────────────┐   │
│  │                           Cluster Proxy Service                                       │   │
│  │   - Routes jobs across multiple K8s clusters                                         │   │
│  │   - Provides HA / load balancing                                                     │   │
│  │   - Handles multi-region routing                                                     │   │
│  └───────────────────────────────────────┬──────────────────────────────────────────────┘   │
└──────────────────────────────────────────┼──────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                            EXECUTION LAYER (Spark on K8s)                                    │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                        K8s API Server (SparkApplication CRD)                          │   │
│  └───────────────────────────────────────┬──────────────────────────────────────────────┘   │
│                                          │                                                   │
│  ┌───────────────────────────────────────▼──────────────────────────────────────────────┐   │
│  │                      Spark Kubernetes Operator (SKO)                                  │   │
│  │   - Watches SparkApplication CRDs                                                    │   │
│  │   - Creates driver/executor pods                                                      │   │
│  │   - Manages job lifecycle                                                             │   │
│  │   - Reports status back to control plane                                              │   │
│  └───────────────────────────────────────┬──────────────────────────────────────────────┘   │
│                                          │                                                   │
│  ┌───────────────────────────────────────▼──────────────────────────────────────────────┐   │
│  │                              Domain Namespace                                         │   │
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  │   │
│  │  │                        Spark Application Pods                                   │  │   │
│  │  │  ┌─────────────────────────────────────────────────────────────────────────┐   │  │   │
│  │  │  │                         Driver Pod                                       │   │  │   │
│  │  │  │  ┌───────────────────────────────────────────────────────────────────┐  │   │  │   │
│  │  │  │  │             Spark Connect Server (port 15002)                      │  │   │  │   │
│  │  │  │  │  - gRPC endpoint for client connections                           │  │   │  │   │
│  │  │  │  │  - Session management                                             │  │   │  │   │
│  │  │  │  │  - Query execution                                                │  │   │  │   │
│  │  │  │  └───────────────────────────────────────────────────────────────────┘  │   │  │   │
│  │  │  │  ┌───────────────────────────────────────────────────────────────────┐  │   │  │   │
│  │  │  │  │              OTel Collector Sidecar                               │  │   │  │   │
│  │  │  │  │  - Metrics, logs, traces collection                               │  │   │  │   │
│  │  │  │  └───────────────────────────────────────────────────────────────────┘  │   │  │   │
│  │  │  └─────────────────────────────────────────────────────────────────────────┘   │  │   │
│  │  │  ┌─────────────────────────────────────────────────────────────────────────┐   │  │   │
│  │  │  │                     Executor Pods (N instances)                         │   │  │   │
│  │  │  │  - Dynamic scaling via DRA                                              │   │  │   │
│  │  │  │  - Ephemeral (terminated on job completion)                             │   │  │   │
│  │  │  └─────────────────────────────────────────────────────────────────────────┘   │  │   │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              DATA & OBSERVABILITY LAYER                                      │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌─────────────────────────┐   │
│  │   S3/Iceberg   │  │     Kafka      │  │  Taulu/CRDB    │  │    Unity Catalog        │   │
│  │   (Data Lake)  │  │   (Streaming)  │  │   (Storage)    │  │   (Metastore)           │   │
│  └────────────────┘  └────────────────┘  └────────────────┘  └─────────────────────────┘   │
│                                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │                            Observability                                                │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────┐  ┌─────────────────────────────┐  │ │
│  │  │    ODIN    │  │    SHS     │  │ Chronosphere   │  │    Event Bus (Taulu)        │  │ │
│  │  │ (Logging)  │  │ (Spark UI) │  │  (Metrics)     │  │  (Execution Events)         │  │ │
│  │  └────────────┘  └────────────┘  └────────────────┘  └─────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Deep Dive

### 1. DCP (Data Control Plane)

DCP is the **primary entrypoint** for Spark in Pedregal. Users write declarative manifests that plugins decompose into primitives.

#### Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **DCP Plugin** | Parses manifest, creates primitives |
| **Orchestrator** | Data Plane - ensures processed data is up-to-date |
| **DCP Controller** | Control Plane - job configuration reflects desired state |

#### Primitives

1. **CoreETL Job Primitive**: Maps 1:1 to a Spark job, contains static job definition
2. **Orchestrator Primitive**: Static definition of an Airflow DAG for batch jobs
3. **ContinuousJob Primitive**: For streaming jobs that run continuously

#### Example DCP Manifest

```yaml
apiVersion: dcp.pedregal.doordash.com/v1
kind: SparkJob
metadata:
  name: example-data-transform
  namespace: platform
  team: data-infra
  owner: data-infra@doordash.com

spec:
  engine:
    version: "3.5"

  resources:
    driver:
      cores: 2
      memory: "4g"
    executors:
      instances: 5
      cores: 4
      memory: "8g"

  jobType: batch

  coreEtl:
    jarPath: "s3://core-etl-build-artifacts/jars/2.7.0/core-etl-driver-bundle.jar"
    mainClass: "com.doordash.coreetl.spark.Main"
    jobSpec:
      sources:
        - type: iceberg
          catalog: pedregal
          database: raw
          table: events
      transformations:
        - type: sql
          query: "SELECT * FROM source WHERE event_type = 'order'"
      sinks:
        - type: iceberg
          catalog: pedregal
          database: derived
          table: processed_events
          mode: append

  orchestration:
    schedule: "0 */6 * * *"
    retryPolicy:
      maxRetries: 3
```

### 2. Spark Runner

Spark Runner is a **stateless Pedregal graph** that provides three core primitives for job execution.

#### Core Primitives

```
┌─────────────────────────────────────────────────────────────────┐
│                      SPARK RUNNER API                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Submit(JobSpec, IdempotencyKey) → (ExecutionID, VendorRunID)  │
│    - Validates job specification                                │
│    - Generates execution ID (UUID)                              │
│    - Calls SparkGateway to submit to compute backend            │
│    - Writes ExecutionSnapshot (SUBMITTED) to Taulu              │
│    - Emits SUBMIT event                                         │
│                                                                 │
│  Check(ExecutionID) → ExecutionState                           │
│    - Read-only status check                                     │
│    - Returns: SUBMITTED | RUNNING | SUCCEEDED | FAILED | CANCELLED│
│    - Updates ExecutionSnapshot on state changes                 │
│                                                                 │
│  Cancel(ExecutionID, Team) → Success/Failure                   │
│    - Attempts to stop running execution                         │
│    - Updates state to CANCELLED                                 │
│    - Emits CANCEL event                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Supporting Components

| Component | Purpose |
|-----------|---------|
| **APS (Auto Parameter Selection)** | Pre-submit cluster sizing recommendations based on execution history |
| **AR (Auto-Retry/Remediation)** | Failure classification (user error vs retriable system error) |
| **DCR (Dynamic Cluster Resizing)** | Runtime resource adjustment while job is running |
| **Debug Utilities** | Job info retrieval, logs, MCP integration |

#### Why Replace SJEM?

| SJEM Problem | Spark Runner Solution |
|--------------|----------------------|
| Stateful monolith | Stateless graphs |
| Per-AWS-account deployment | Centralized platform |
| Hard to upgrade/scale | Easy horizontal scaling |
| Rigid retry logic | Composable orchestrator policies |
| Cannot handle 10x workloads | Designed for 100K+ jobs/day |

### 3. Spark Gateway

Spark Gateway is a **Pedregal service** that sits between Spark Runner and compute backends.

#### Responsibilities

- Receives SparkApplication CRD requests from Spark Runner
- Enforces domain-specific namespace model
- Routes to appropriate cluster via Cluster Proxy Service
- Manages job TTL and cleanup
- Supports SK8 (primary) and EMR (fallback)

#### Routing Logic

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK GATEWAY ROUTING                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Input: JobSpec from Spark Runner                               │
│                                                                 │
│  1. Determine target backend:                                   │
│     - SK8 (default) for Kubernetes-native execution             │
│     - EMR (fallback) when SK8 unavailable or specific needs     │
│                                                                 │
│  2. Select cluster:                                             │
│     - Multiple K8s clusters per region for HA                   │
│     - Load balancing across healthy clusters                    │
│     - Failover to other regions on outage                       │
│                                                                 │
│  3. Apply namespace:                                            │
│     - Domain-specific namespace (e.g., sjns-data-control)       │
│     - Team manages own secrets within namespace                 │
│                                                                 │
│  4. Submit SparkApplication CRD:                                │
│     - Set job TTL for cleanup                                   │
│     - Return vendor_run_id to Spark Runner                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Spark on Kubernetes (SK8)

SK8 is DoorDash's in-house Spark compute engine running on managed Kubernetes clusters.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SK8 EXECUTION FLOW                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. User submits job via Spark Runner / Notebooks               │
│                          │                                      │
│                          ▼                                      │
│  2. Platform validates, normalizes, creates SparkApplication CRD│
│                          │                                      │
│                          ▼                                      │
│  3. Spark Kubernetes Operator reconciles CRD                    │
│     - Creates driver pod                                        │
│     - Driver requests executor pods                             │
│                          │                                      │
│                          ▼                                      │
│  4. Logs/metrics emitted to observability systems               │
│     - ODIN for logs                                             │
│     - SHS for Spark UI                                          │
│     - Chronosphere for metrics                                  │
│                          │                                      │
│                          ▼                                      │
│  5. Completed CRDs cleaned up via TTL policy                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Spark Kubernetes Operator (SKO)

DoorDash chose **Apache spark-kubernetes-operator** over Kubeflow spark-operator:

| Criteria | Apache SKO | Kubeflow SO |
|----------|------------|-------------|
| Governance | Apache Spark project | Kubeflow org |
| CRDs | SparkApplication + SparkCluster | SparkApplication + ScheduledSparkApplication |
| Scheduling | External (orchestrators) | Built-in cron |
| Future-proofing | Designed for Spark 3.5+, 4.x | Supports Spark 2.x/3.x |

### Multitenancy

- **Domain-specific namespaces**: `sjns-<domain-name>` (e.g., `sjns-data-control`)
- **Self-serve secrets**: Teams create/rotate secrets within their namespace
- **Cost attribution**: Resource usage attributed at namespace/domain level
- **Quota controls**: Per-namespace ResourceQuota/LimitRange

### Autoscaling

**Batch Jobs:**
- Spark Dynamic Resource Allocation (DRA) for executor scaling
- Karpenter + dedicated Spark worker pool for node scaling
- Per-namespace quotas prevent noisy-neighbor issues

**Streaming Jobs:**
- DRA if workload supports it
- Controlled restart with updated resources (checkpoints for state recovery)

### Shuffle Strategy

**Phase 1 (Current):** Local shuffle with hardened executor storage
- Shuffle data written to node-local storage
- Per-job storage budgets enforced
- Monitoring for DiskPressure, fetch failures

**Phase 2 (Future):** Remote shuffle with Apache Celeborn
- Decouples shuffle from executor lifecycle
- Better tolerance to executor churn
- Enables aggressive autoscaling

---

## Spark Connect

Spark Connect enables **client-server separation** for Spark, allowing thin clients to connect to a Spark cluster without requiring a local Spark installation.

**Important:** Spark Connect is NOT a standalone service - it's a server process that runs **inside the Spark Driver Pod**. However, clients access it **through Spark Gateway**, which handles routing, authentication, and cluster discovery.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK CONNECT ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Clients                     Spark Gateway              Driver Pod         │
│   ┌─────────────┐            ┌─────────────┐         ┌─────────────────┐   │
│   │ PySpark     │            │             │         │ Spark Connect   │   │
│   │ Scala       │───────────▶│   Routes    │────────▶│ Server (15002)  │   │
│   │ Jupyter     │            │   Auth      │         │                 │   │
│   │ DCP Sandbox │            │   Discovery │         │ - Session mgmt  │   │
│   └─────────────┘            └─────────────┘         │ - Query exec    │   │
│                                                      └─────────────────┘   │
│                                                                             │
│   Flow: Client → Spark Gateway → Spark Connect Server (on Driver Pod)      │
│                                                                             │
│   Spark Gateway provides:                                                   │
│   • Cluster discovery (which driver to connect to)                          │
│   • Authentication/Authorization                                            │
│   • Routing to correct domain namespace                                     │
│   • Load balancing across hot clusters                                      │
│                                                                             │
│   Benefits:                                                                  │
│   ✓ No SSH tunneling required                                               │
│   ✓ Thin client (no local Spark installation)                               │
│   ✓ Language-agnostic (gRPC protocol)                                       │
│   ✓ Session isolation                                                       │
│   ✓ Interactive/sandbox development                                         │
│   ✓ Fast startup for DCP sandbox flows                                      │
│   ✓ Single entry point via Spark Gateway                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Connection Flow

```
1. Client requests interactive session from Spark Gateway
2. Spark Gateway either:
   a. Routes to existing hot cluster's Spark Connect server, OR
   b. Triggers creation of new cluster, then routes to it
3. Client receives connection details (sc://host:port)
4. Client connects to Spark Connect server via Gateway proxy
```

### Connection URL

```
sc://<gateway-host>:<port>

Example: sc://spark-gateway.service.prod.ddsd:15002
```

### Client Examples

**Python:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .appName("MyApp") \
    .getOrCreate()

df = spark.sql("SELECT * FROM my_table")
```

**Scala:**
```scala
val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .appName("MyApp")
  .getOrCreate()
```

---

## Client Entry Points

### Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CLIENT ENTRY POINTS                                  │
│                                                                             │
│  Option 1: DCP REST API (Recommended for external services)                 │
│  ───────────────────────────────────────────────────────────                │
│  REST Client → DCP Server → Spark Runner → Spark Gateway → SK8             │
│                                                                             │
│  Option 2: DCP Manifest (GitOps / Declarative)                              │
│  ───────────────────────────────────────────────────────────                │
│  YAML Manifest → DCP Plugin → Orchestrator → Spark Runner → SK8            │
│                                                                             │
│  Option 3: Spark Runner gRPC (Internal Pedregal graphs)                     │
│  ───────────────────────────────────────────────────────────                │
│  Graph Node → Spark Runner gRPC → Spark Gateway → SK8                      │
│                                                                             │
│  Option 4: CoreETL Library (In-Process)                                     │
│  ───────────────────────────────────────────────────────────                │
│  Pretzel Plugin → CoreETL Component → Spark Runner → SK8                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### When to Use Each

| Entry Point | Use Case | Who Uses It |
|-------------|----------|-------------|
| **DCP REST API** | External services, CI/CD pipelines | Any service needing programmatic job submission |
| **DCP Manifest** | GitOps workflows, declarative definitions | Teams defining jobs in config repos |
| **Spark Runner gRPC** | Internal Pedregal graphs | Custom orchestrators, DCP plugins |
| **CoreETL Library** | In-process Graph Runner nodes | Pretzel plugins, DCP controllers |
| **Jupyter Notebooks** | Interactive development | Data scientists, analysts |

### REST API Reference

#### POST /api/v1/jobs - Submit Job

```bash
curl -X POST http://dcp-spark:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-etl-job",
    "team": "data-infra",
    "user": "user@doordash.com",
    "config": {
      "spark_version": "3.5",
      "job_type": "BATCH",
      "core_etl": {
        "version": "2.7.0",
        "job_spec": {
          "sources": [...],
          "transformations": [...],
          "sinks": [...]
        }
      },
      "resources": {
        "driver": {"cores": 2, "memory": "4g"},
        "executor": {"instances": 5, "cores": 4, "memory": "8g"}
      }
    }
  }'
```

**Response:**
```json
{
  "job_id": "abc-123-def",
  "execution_id": "exec-456",
  "status": "SUBMITTED",
  "message": "Job submitted successfully"
}
```

#### GET /api/v1/jobs/{jobID} - Get Status

```bash
curl http://dcp-spark:8080/api/v1/jobs/abc-123-def
```

**Response:**
```json
{
  "job_id": "abc-123-def",
  "execution_id": "exec-456",
  "status": "RUNNING",
  "start_time": "2024-01-15T10:30:00Z",
  "spark_ui_url": "http://spark-ui.example.com/jobs/exec-456",
  "logs_url": "https://odin.doordash.team/logs?job_id=exec-456"
}
```

#### DELETE /api/v1/jobs/{jobID} - Cancel Job

```bash
curl -X DELETE http://dcp-spark:8080/api/v1/jobs/abc-123-def
```

#### GET /api/v1/jobs/{jobID}/logs - Get Logs

```bash
curl http://dcp-spark:8080/api/v1/jobs/abc-123-def/logs
```

---

## Job Execution Flows

### Triggered Jobs (Batch)

```
┌─────────────────────────────────────────────────────────────────┐
│                  TRIGGERED JOB FLOW                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. User deploys DCP manifest                                   │
│     └─► Plugin writes CoreETL Job + Orchestrator primitive      │
│                                                                 │
│  2. Airflow operator runs on schedule                           │
│     └─► Calls SR.Submit with idempotency token                  │
│     └─► Generates execution_id (UUID)                           │
│                                                                 │
│  3. SR.Submit → SparkGateway                                    │
│     └─► Gateway submits to SK8/EMR                              │
│     └─► Returns vendor_run_id                                   │
│     └─► SR writes ExecutionSnapshot (SUBMITTED)                 │
│     └─► Emits SUBMIT event to Taulu                             │
│                                                                 │
│  4. Airflow checks status via SR.Check (read-only)              │
│     └─► On state changes, writes lifecycle events to Taulu      │
│     └─► Updates ExecutionSnapshot                               │
│                                                                 │
│  5. On completion                                               │
│     └─► Airflow DAG writes to ACS (Asset Completion Service)    │
│     └─► Job marked as SUCCEEDED or FAILED                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Continuous Jobs (Streaming)

```
┌─────────────────────────────────────────────────────────────────┐
│                 CONTINUOUS JOB FLOW                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. User writes manifest                                        │
│     └─► Plugin creates ContinuousJob Primitive                  │
│                                                                 │
│  2. DCP controller reconciliation loop:                         │
│     a. Get current state from SR                                │
│     b. Get recommended actions from APS/AR                      │
│     c. Diff desired state vs actual state                       │
│     d. Apply changes if needed                                  │
│                                                                 │
│  3. Changes trigger job redeploy:                               │
│     └─► Checkpoints ensure state recovery                       │
│     └─► New job starts from last checkpoint                     │
│                                                                 │
│  4. Continuous monitoring:                                      │
│     └─► DCP controller watches for drift                        │
│     └─► Auto-remediation via AR recommendations                 │
│     └─► Auto-sizing via APS signals                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Docker Image Strategy

### Layer Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                  SPARK DOCKER IMAGE LAYERS                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  LAYER 4: Application Layer                                     │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  - CoreETL JAR files                                       │  │
│  │  - User application code                                   │  │
│  │  - Init scripts (coreetl_init.sh)                         │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  LAYER 3: Connectors & Dependencies                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  - Unity Catalog connector                                 │  │
│  │  - S3/GCS connectors (hadoop-aws)                         │  │
│  │  - Iceberg runtime                                         │  │
│  │  - Kafka client                                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  LAYER 2: Spark Runtime                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  - Spark 3.5.x / 4.x binaries                              │  │
│  │  - Spark Connect server JARs (port 15002)                  │  │
│  │  - PySpark (for Python jobs)                               │  │
│  │  - Default spark configurations                            │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  LAYER 1: Base OS + JVM                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  - Debian Slim / Amazon Linux 2                            │  │
│  │  - OpenJDK 17                                              │  │
│  │  - Python 3.x (for PySpark)                               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Versioning

Clients specify version in DCP Manifest:

```yaml
spec:
  engine:
    version: "3.5"  # Maps to spark-platform:3.5-latest
```

Supported versions:
- Spark 3.5.x
- Spark 4.0.x
- Spark 4.1.x (preview)

### Exposed Ports

| Port | Service |
|------|---------|
| 4040 | Spark UI |
| 7077 | Spark Master (standalone mode) |
| 15002 | Spark Connect gRPC |
| 18080 | History Server |

---

## Testing Strategy

### Local Testing with TestContainers

```java
@Container
SparkContainer spark = SparkContainer.builder()
    .withDriverMemory(2048)
    .withSparkConfig("spark.sql.shuffle.partitions", "4")
    .build();

// Connect via Spark Connect
SparkSession session = SparkSession.builder()
    .remote(spark.getSparkConnectUrl())  // sc://localhost:xxxxx
    .getOrCreate();

// Run tests
Dataset<Row> df = session.sql("SELECT 1 as id");
assertEquals(1, df.count());
```

### Integration Test Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TEST ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Test Code                                                      │
│       │                                                          │
│       ▼                                                          │
│   ┌───────────────────────────────────────────────────┐         │
│   │              TestContainers                        │         │
│   │   ┌─────────────────────────────────────────┐     │         │
│   │   │       Spark Docker Container             │     │         │
│   │   │  ┌─────────────────────────────────┐    │     │         │
│   │   │  │  Spark Connect Server (15002)   │◄───┼─────┼── Tests │
│   │   │  └─────────────────────────────────┘    │     │         │
│   │   └─────────────────────────────────────────┘     │         │
│   │                                                    │         │
│   │   ┌─────────────────────────────────────────┐     │         │
│   │   │       MinIO Container (S3 mock)         │     │         │
│   │   └─────────────────────────────────────────┘     │         │
│   │                                                    │         │
│   │   ┌─────────────────────────────────────────┐     │         │
│   │   │       Unity Catalog (mock)              │     │         │
│   │   └─────────────────────────────────────────┘     │         │
│   └───────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### Hot Clusters for Asset Testing

For DCP Sandbox testing:
- Pre-warmed clusters eliminate 6-8 minute cluster acquisition time
- Target: < 3 minutes latency for remote testing
- SK8 provides faster startup than EMR/Databricks

---

## Observability

### Components

| System | Purpose | Query Pattern |
|--------|---------|---------------|
| **ODIN** | Centralized logging | "What error happened? What did the app print?" |
| **SHS** | Spark History Server | "Why did Spark behave this way? Where was time spent?" |
| **Chronosphere** | Metrics (via OTel) | "How is the platform performing? SLO tracking" |
| **Taulu** | Execution history | "What jobs ran? What was the outcome?" |

### Taulu Tables

**Executions Table** (single row per execution):
```protobuf
message Execution {
  string execution_id;     // PK and idempotency
  string job_path;         // CoreETL job id
  string owner;
  string team;
  int64 start_time;
  int64 end_time;
  ExecutionState state;    // SUBMITTED/RUNNING/SUCCEEDED/FAILED/CANCELLED
  string vendor;           // EMR/DBR/SK8
  string vendor_run_id;
  string vendor_webui;
  string vendor_logs_link;
}
```

**Execution Events Table** (append-only):
```protobuf
message ExecutionEvent {
  string event_id;         // UUID
  string execution_id;
  EventType event_type;    // SUBMIT/RUNNING/TERMINAL/METRICS_SUMMARY
  int64 timestamp;
  bytes payload;           // Event-specific data
}
```

### Metrics

Platform metrics emitted via OTel:
- K8s cluster capacity and scheduling delays
- SKO health
- Job lifecycle SLOs
- Shuffle health (fetch failures, retries)

---

## Glossary

| Term | Definition |
|------|------------|
| **ACS** | Asset Completion Service - tracks when data assets are ready |
| **APS** | Auto Parameter Selection - cluster sizing recommendations |
| **AR** | Auto-Retry/Auto-Remediation - failure classification and retry logic |
| **CoreETL** | DoorDash's ETL framework built on Spark |
| **CRD** | Custom Resource Definition (Kubernetes) |
| **DCP** | Data Control Plane - Pedregal's declarative job management |
| **DCR** | Dynamic Cluster Resizing - runtime resource adjustment |
| **DRA** | Dynamic Resource Allocation - Spark executor autoscaling |
| **EB** | Event Bus - async event transport in Pedregal |
| **ODIN** | DoorDash's centralized logging system |
| **Pedregal** | DoorDash's data platform ecosystem |
| **SHS** | Spark History Server |
| **SJEM** | Spark Job Execution Manager (legacy) |
| **SK8** | Spark on Kubernetes |
| **SKO** | Spark Kubernetes Operator |
| **SR** | Spark Runner |
| **Taulu** | Pedregal's metadata storage system |

---

## References

- [RFC 12032] Spark Runner - A Pedregal-Native Spark Job Lifecycle Manager
- [RFC 130034] Pedregal Spark Runner
- [RFC 578] Graph Runner Control Plane
- [RFC 710] Graph Runner
- Pedregal Data Processing & Frameworks
- Apache Spark Kubernetes Operator: https://github.com/apache/spark-kubernetes-operator

---

## Architecture Diagram Analysis

Based on the official Spark Platform Architecture diagram, here is a detailed breakdown:

### Diagram Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           SPARK PLATFORM ARCHITECTURE                                    │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  dash-compute cluster                    dash-data cluster                              │
│  ┌─────────────────────┐                ┌──────────────────────────────────────────┐   │
│  │                     │                │                                          │   │
│  │  ┌───────┐          │                │  ┌─────────────┐    ┌──────────────────┐│   │
│  │  │ Taulu │          │                │  │spark-gateway│───▶│Cluster Proxy Svc ││   │
│  │  └───┬───┘          │                │  └─────────────┘    └────────┬─────────┘│   │
│  │      │              │                │                              │          │   │
│  │  ┌───▼───────┐      │                │                     ┌────────▼────────┐ │   │
│  │  │DCP/Graphs │──────┼───────────────▶│                     │  K8 API Server  │ │   │
│  │  └───────────┘      │                │                     └────────┬────────┘ │   │
│  │        │            │                │                              │          │   │
│  │        ▼            │                │  ┌───────────────────────────┼──────┐   │   │
│  │  ┌───────────┐      │                │  │                           ▼      │   │   │
│  │  │SparkRunner│──────┼───────────────▶│  │                  ┌──────────────┐│   │   │
│  │  └───────────┘      │                │  │                  │Spark K8      ││   │   │
│  │                     │                │  │                  │Operator (SKO)││   │   │
│  └─────────────────────┘                │  │                  └──────┬───────┘│   │   │
│                                         │  │                         │        │   │   │
│  cell-00x cluster                       │  │  ┌──────────────────────┼────────┤   │   │
│  ┌─────────────────────┐                │  │  │ Domain Namespace A   │        │   │   │
│  │  ┌────────────────┐ │                │  │  │  ┌────────────┬─────▼──────┐ │   │   │
│  │  │ Asgard Services│ │                │  │  │  │Spark Driver│Spark Exec  │ │   │   │
│  │  └────────────────┘ │                │  │  │  │(+Connect)  │(N pods)    │ │   │   │
│  └─────────────────────┘                │  │  │  └────────────┴────────────┘ │   │   │
│                                         │  │  └──────────────────────────────┘   │   │
│  data-01 cluster                        │  │                                     │   │
│  ┌─────────────────────┐                │  │  ┌──────────────────────────────┐   │   │
│  │  ┌─────────────┐    │                │  │  │ Domain Namespace Z           │   │   │
│  │  │  Notebooks  │────┼───────────────▶│  │  │  ┌────────────┬────────────┐ │   │   │
│  │  └─────────────┘    │                │  │  │  │Spark Driver│Spark Exec  │ │   │   │
│  └─────────────────────┘                │  │  │  └────────────┴────────────┘ │   │   │
│                                         │  │  └──────────────────────────────┘   │   │
│                                         │  └─────────────────────────────────────┘   │
│                                         │                                            │
│                                         │  ┌─────────────────┐                       │
│                                         │  │      EMR        │ (fallback)            │
│                                         │  └─────────────────┘                       │
│                                         └────────────────────────────────────────────┘   │
│                                                                                         │
│  Data Plane                              Observability Plane                            │
│  ┌──────┬───────┬────┬──────────────┐   ┌────────────┬──────────┬─────────────────┐   │
│  │Kafka │ Taulu │ S3 │Unity Catalog │   │OTel Metrics│ODIN Logs │Spark History Svr│   │
│  └──────┴───────┴────┴──────────────┘   └────────────┴──────────┴─────────────────┘   │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Notebook Interaction Modes

The diagram shows Notebooks as **Spark Platform Clients**. There are two interaction modes, and **both go through Spark Gateway**:

#### Mode 1: Job Submission (Batch/Streaming Jobs)

```
Notebooks → SparkRunner → Spark Gateway → Cluster Proxy → K8 API Server → SKO → Driver/Executors
```

This path is used when:
- Submitting new Spark jobs
- Running batch ETL workloads
- Starting streaming jobs

#### Mode 2: Interactive Sessions (via Spark Connect)

```
Notebooks → Spark Gateway → Spark Connect Server (on Driver Pod) → Executors
```

This path is used when:
- Running interactive queries
- Exploratory data analysis
- DCP Sandbox development
- Ad-hoc SQL queries

**Important:** Both paths go through **Spark Gateway** as the single entry point. The Gateway provides:
- **Cluster discovery** - finds the right Driver pod to connect to
- **Authentication** - validates user/team permissions
- **Routing** - directs to correct domain namespace
- **Hot cluster management** - routes to pre-warmed clusters for fast startup

**Note:** Spark Connect server runs **inside the Driver Pod** (not a separate service). The Gateway proxies connections to it.

### Key Observations from Diagram

| Component | Location | Color Code | Notes |
|-----------|----------|------------|-------|
| Notebooks | data-01 | Blue (Client) | Spark Platform Client |
| DCP/Graphs | dash-compute | Blue (Client) | Primary job submission path |
| SparkRunner | dash-compute | Blue (Client) | Control plane for job lifecycle |
| Taulu | dash-compute | Yellow (Co-owned) | Metadata/execution history |
| spark-gateway | dash-data | Pink (Platform) | Routes to compute backends |
| Spark K8 Operator | dash-data | Yellow (Co-owned) | Manages SparkApplication CRDs |
| Driver/Executor | Domain Namespaces | Pink (Platform) | Actual Spark execution |
| EMR | dash-data | Yellow (Co-owned) | Fallback compute backend |

### Recommended Diagram Enhancement

To make the notebook interaction clearer, consider updating the diagram to show:

1. **Spark Connect port (15002)** annotation on Driver pods
2. **Both paths going through Spark Gateway** (single entry point)
3. **Legend entry** for "Job Submission" vs "Interactive Session" paths

```
Corrected Notebook Flow (Both paths through Spark Gateway):

┌─────────────┐
│  Notebooks  │
└──────┬──────┘
       │
       ├───────────────────────────────────────┐
       │ (Job Submission)                      │ (Interactive Session)
       ▼                                       │
┌─────────────┐                                │
│ SparkRunner │                                │
└──────┬──────┘                                │
       │                                       │
       └──────────────┬────────────────────────┘
                      │
                      ▼
             ┌──────────────┐
             │spark-gateway │  ◄── Single entry point for ALL Spark access
             └──────┬───────┘
                    │
       ┌────────────┴────────────┐
       │ (Job Submission)        │ (Interactive)
       ▼                         ▼
┌──────────────┐        ┌──────────────────────────────┐
│Cluster Proxy │        │     Spark Connect Proxy      │
│     ↓        │        │            ↓                 │
│ K8 API Server│        │     Driver Pod:15002         │
│     ↓        │        └──────────────────────────────┘
│    SKO       │
│     ↓        │
│ Driver Pod   │
└──────────────┘
```

### Key Insight: Spark Gateway as Single Entry Point

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SPARK GATEWAY RESPONSIBILITIES                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  For Job Submission:                    For Interactive Sessions:           │
│  ─────────────────────                  ────────────────────────            │
│  • Receive SparkApplication CRD         • Route to existing hot cluster    │
│  • Route to Cluster Proxy               • Or trigger new cluster creation  │
│  • Forward to K8s API Server            • Proxy Spark Connect connections  │
│  • Return vendor_run_id                 • Handle authentication            │
│                                         • Load balance across clusters     │
│                                                                             │
│  Both flows benefit from:                                                   │
│  • Centralized authentication/authorization                                 │
│  • Domain namespace enforcement                                             │
│  • Observability and audit logging                                          │
│  • Multi-cluster routing and HA                                             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

*Last updated: January 2025*
*Generated from Pedregal Spark Platform documentation*
