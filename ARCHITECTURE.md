# Pedregal Spark Platform Architecture

This document describes the complete architecture for running Spark workloads in the Pedregal ecosystem, including testing strategies using Docker containers.

## Overview

The Pedregal Spark Platform provides a unified, Pedregal-native way to run Spark workloads on Kubernetes (SK8), with EMR as an interim fallback.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              ARCHITECTURE OVERVIEW                            │
│                                                                              │
│   DCP Manifest    Jupyter     Direct API                                     │
│       │          Notebook        │                                           │
│       │              │           │                                           │
│       ▼              ▼           ▼                                           │
│   ┌──────────────────────────────────────────┐                               │
│   │           DCP Plugin (CoreETL)            │                               │
│   │   - Parse manifest → JobSpec              │                               │
│   │   - Create orchestrator/continuous job    │                               │
│   └──────────────────────┬───────────────────┘                               │
│                          │                                                    │
│   ┌──────────────────────▼───────────────────┐                               │
│   │              Spark Runner                 │                               │
│   │   - Submit / Check / Cancel primitives    │                               │
│   │   - APS (auto-sizing)                     │                               │
│   │   - AR (auto-remediation)                 │                               │
│   └──────────────────────┬───────────────────┘                               │
│                          │                                                    │
│   ┌──────────────────────▼───────────────────┐                               │
│   │             Spark Gateway                 │                               │
│   │   - Routes to compute backend             │                               │
│   │   - Namespace/domain isolation            │                               │
│   │   - Cluster proxy for multi-cluster       │                               │
│   └──────────────────────┬───────────────────┘                               │
│                          │                                                    │
│   ┌──────────────────────▼───────────────────┐                               │
│   │     Spark on K8s (SK8) or Docker          │                               │
│   │   - SparkApplication CRD                  │                               │
│   │   - Driver + Executor pods                │                               │
│   │   - Spark Connect Server (port 15002)     │                               │
│   └──────────────────────────────────────────┘                               │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. DCP (Data Control Plane)

DCP is the primary entrypoint for Spark in Pedregal. Users write manifests that plugins decompose into primitives:

- **CoreETL Job Primitive**: Maps 1:1 to a Spark job, contains static job definition
- **Orchestrator Primitive**: Static definition of an Airflow DAG for batch jobs
- **ContinuousJob Primitive**: For streaming jobs that run continuously

### 2. Spark Runner

Spark Runner provides three core primitives:

```go
// Submit creates a new Spark execution
Submit(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error)

// Check retrieves the current execution state
Check(ctx context.Context, req *CheckRequest) (*CheckResponse, error)

// Cancel attempts to cancel a running execution
Cancel(ctx context.Context, req *CancelRequest) (*CancelResponse, error)
```

Supporting components:
- **APS (Auto Parameter Selection)**: Pre-submit cluster sizing recommendations
- **AR (Auto-Retry/Remediation)**: Failure classification and retry recommendations
- **DCR (Dynamic Cluster Resizing)**: Runtime resource adjustment

### 3. Spark Gateway

The gateway sits between Spark Runner and compute backends:

- Receives SparkApplication CRD requests from Spark Runner
- Enforces domain-specific namespace model
- Routes to appropriate cluster via Cluster Proxy Service
- Manages job TTL and cleanup
- Supports SK8 (primary) and EMR (fallback)

### 4. Spark on Kubernetes (SK8)

SK8 uses the Apache Spark Kubernetes Operator:

- Watches SparkApplication CRDs
- Creates driver/executor pods
- Manages job lifecycle
- Reports status back to control plane

### 5. Spark Connect

Spark Connect enables client-server separation. **Important:** Spark Connect is NOT a standalone service - it's a server that runs inside the Driver Pod. Clients access it **through Spark Gateway**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK CONNECT ARCHITECTURE                           │
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
│   • Hot cluster management for fast startup                                 │
│                                                                             │
│   Benefits:                                                                  │
│   ✓ No SSH tunneling required                                               │
│   ✓ Thin client (no local Spark installation)                               │
│   ✓ Language-agnostic (gRPC)                                                │
│   ✓ Session isolation                                                       │
│   ✓ Interactive/sandbox development                                         │
│   ✓ Single entry point via Spark Gateway                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Summary: Services vs Runtime Components

| Name | Type | Deployed As |
|------|------|-------------|
| **Spark Runner** | Service | Pedregal Graph |
| **Spark Gateway** | Service | Pedregal Service |
| **Spark Connect** | Runtime Component | Runs inside Driver Pod (not separate) |

## Testing Strategy

### Local Testing with TestContainers

For framework testing (CoreETL, etc.), use TestContainers with Docker:

```java
@Container
SparkContainer spark = SparkContainer.builder()
    .withDriverMemory(2048)
    .withSparkConfig("spark.sql.shuffle.partitions", "4")
    .build();

// Connect via Spark Connect
SparkSession session = SparkSession.builder()
    .remote(spark.getSparkConnectUrl())
    .getOrCreate();
```

### Integration Testing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    TEST ARCHITECTURE                             │
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
│   │   │  ┌─────────────────────────────────┐    │     │         │
│   │   │  │  Unity Catalog (mock)           │    │     │         │
│   │   │  └─────────────────────────────────┘    │     │         │
│   │   │  ┌─────────────────────────────────┐    │     │         │
│   │   │  │  S3/MinIO (local storage)       │    │     │         │
│   │   │  └─────────────────────────────────┘    │     │         │
│   │   └─────────────────────────────────────────┘     │         │
│   │                                                    │         │
│   │   ┌─────────────────────────────────────────┐     │         │
│   │   │       MinIO Container (S3 mock)         │     │         │
│   │   └─────────────────────────────────────────┘     │         │
│   └───────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

### Hot Clusters for Asset Testing

For asset/job testing via DCP Sandboxes:

- Pre-warmed clusters eliminate 6-8 minute cluster acquisition time
- Target: < 3 minutes latency for remote testing
- SK8 provides faster startup than EMR/Databricks

## Docker Image Strategy

```dockerfile
# Layer 1: Base OS + JVM
FROM openjdk:17-slim

# Layer 2: Spark Runtime
- Spark binaries
- Spark Connect server JARs
- PySpark

# Layer 3: Connectors
- Unity Catalog connector
- S3/Iceberg connectors
- Kafka client

# Layer 4: Application
- CoreETL JAR
- Init scripts
- Configuration
```

### Image Versioning

Clients specify version in DCP Manifest:

```yaml
spec:
  engine:
    version: "3.5"  # Maps to spark-platform:3.5-latest
```

Spark Runner resolves to specific Docker tag based on version.

## Execution Flows

### Triggered Jobs (Batch)

```
1. User deploys DCP manifest
2. Plugin writes CoreETL Job + Orchestrator primitive (Airflow DAG)
3. Airflow operator calls SR.Submit with idempotency token
4. SR.Submit → SparkGateway → SK8/EMR
5. Airflow checks status via SR.Check
6. On completion, Airflow writes to ACS (asset completion service)
```

### Continuous Jobs (Streaming)

```
1. User writes manifest → ContinuousJob Primitive
2. DCP controller gets current state from SR
3. DCP controller gets recommendations from APS/AR
4. DCP controller diffs and applies changes
5. Changes result in job redeploy if needed
```

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA LAYER                               │
│                                                                 │
│   ┌───────────┐    ┌───────────┐    ┌───────────────────┐      │
│   │  S3/GCS   │    │   Kafka   │    │  Iceberg Tables   │      │
│   │ (Raw Data)│    │ (Events)  │    │ (Lakehouse)       │      │
│   └─────┬─────┘    └─────┬─────┘    └─────────┬─────────┘      │
│         │                │                    │                 │
│         └────────────────┼────────────────────┘                 │
│                          │                                       │
│                          ▼                                       │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                   Unity Catalog                          │   │
│   │   - Table metadata                                       │   │
│   │   - Access control                                       │   │
│   │   - Lineage tracking                                     │   │
│   └─────────────────────────────────────────────────────────┘   │
│                          │                                       │
│                          ▼                                       │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Spark Application                       │   │
│   │   - Read from sources                                    │   │
│   │   - Transform data                                       │   │
│   │   - Write to sinks                                       │   │
│   └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Observability

All components emit to Pedregal-native observability:

- **Logs**: ODIN (centralized logging)
- **Metrics**: Chronosphere (via OTel)
- **Spark UI**: Spark History Server (SHS)
- **Execution Events**: Taulu tables

## Multi-Region Support

- Compute close to data principle
- Cluster Proxy Service handles routing
- Failover to available regions on outage
