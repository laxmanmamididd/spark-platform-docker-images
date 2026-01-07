# Pedregal Spark Platform Architecture

This document describes the complete architecture for running Spark workloads in the Pedregal ecosystem, including testing strategies using Docker containers.

## Overview

The Pedregal Spark Platform provides a unified, Pedregal-native way to run Spark workloads on Kubernetes (SK8), with EMR as an interim fallback.

**Two distinct access patterns exist:**
1. **Batch Jobs** - Submit via DCP → Spark Runner → Spark Gateway → SK8
2. **Interactive Sessions** - Connect via Spark Gateway → Spark Connect Server

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              ARCHITECTURE OVERVIEW                            │
│                                                                              │
│  ══════════════════════════════════════════════════════════════════════════  │
│  MODE 1: BATCH JOBS (Job Submission)                                         │
│  ══════════════════════════════════════════════════════════════════════════  │
│                                                                              │
│   DCP Manifest         Direct API                                            │
│       │                    │                                                 │
│       ▼                    ▼                                                 │
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
│   └──────────────────────────────────────────┘                               │
│                                                                              │
│  ══════════════════════════════════════════════════════════════════════════  │
│  MODE 2: INTERACTIVE SESSIONS (Spark Connect)                                │
│  ══════════════════════════════════════════════════════════════════════════  │
│                                                                              │
│   Jupyter Notebook                                                           │
│   DCP Sandbox                                                                │
│   PySpark Client                                                             │
│       │                                                                      │
│       │  Spark Connect Client (thin client)                                  │
│       │  - pyspark[connect] or spark-connect-client                          │
│       │  - No local Spark installation needed                                │
│       │                                                                      │
│       ▼                                                                      │
│   ┌──────────────────────────────────────────┐                               │
│   │             Spark Gateway                 │                               │
│   │   - Cluster discovery                     │                               │
│   │   - Authentication / Authorization        │                               │
│   │   - Routes to correct driver pod          │                               │
│   │   - Hot cluster management                │                               │
│   └──────────────────────┬───────────────────┘                               │
│                          │                                                    │
│   ┌──────────────────────▼───────────────────┐                               │
│   │   Spark Connect Server (on Driver Pod)    │                               │
│   │   - Runs inside Driver (port 15002)       │                               │
│   │   - Session management                    │                               │
│   │   - Query execution                       │                               │
│   │   - NOT a separate service                │                               │
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

Spark Connect enables client-server separation for interactive workloads. **Important:** Spark Connect is NOT a standalone service - it's a server that runs inside the Driver Pod. Clients access it **through Spark Gateway**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPARK CONNECT ARCHITECTURE                           │
│                                                                             │
│   Spark Connect Client         Spark Gateway              Driver Pod        │
│   ┌─────────────────┐         ┌─────────────┐         ┌─────────────────┐  │
│   │ PySpark Client  │         │             │         │ Spark Connect   │  │
│   │ (pyspark[conn]) │────────▶│   Routes    │────────▶│ Server (15002)  │  │
│   ├─────────────────┤         │   Auth      │         │                 │  │
│   │ Scala Client    │         │   Discovery │         │ - Session mgmt  │  │
│   │ (spark-connect) │         │             │         │ - Query exec    │  │
│   ├─────────────────┤         └─────────────┘         │ - DataFrame ops │  │
│   │ Jupyter Notebook│                                 └─────────────────┘  │
│   │ DCP Sandbox     │                                                      │
│   └─────────────────┘                                                      │
│                                                                             │
│   Flow: Spark Connect Client → Spark Gateway → Spark Connect Server        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Spark Connect Client

The Spark Connect Client is a thin client library that communicates with Spark Connect Server via gRPC:

**Python (PySpark Connect):**
```python
# Install: pip install pyspark[connect]
from pyspark.sql import SparkSession

# Connect through Spark Gateway
spark = SparkSession.builder \
    .remote("sc://spark-gateway.service.prod.ddsd:15002") \
    .getOrCreate()

# Use DataFrame API as normal
df = spark.sql("SELECT * FROM my_table")
df.show()
```

**Scala/Java:**
```scala
// Add dependency: org.apache.spark:spark-connect-client-jvm_2.12:3.5.0
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .remote("sc://spark-gateway.service.prod.ddsd:15002")
  .getOrCreate()
```

#### What Spark Connect Client Provides

| Feature | Description |
|---------|-------------|
| **Thin Client** | No local Spark installation required |
| **gRPC Protocol** | Language-agnostic communication |
| **DataFrame API** | Full DataFrame/Dataset operations |
| **SQL Support** | Execute SQL queries remotely |
| **Session Isolation** | Each client gets isolated session |

#### What Spark Connect Client Does NOT Do

- Does NOT execute Spark code locally
- Does NOT require Spark JARs on client machine
- Does NOT connect directly to executors
- Does NOT bypass Spark Gateway (all traffic routes through gateway)

#### Spark Gateway Role in Spark Connect

Spark Gateway is the **single entry point** for Spark Connect:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│   Spark Gateway provides:                                                   │
│   • Cluster discovery (which driver to connect to)                          │
│   • Authentication/Authorization                                            │
│   • Routing to correct domain namespace                                     │
│   • Hot cluster management for fast startup                                 │
│   • Load balancing across multiple drivers                                  │
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

| Name | Type | Deployed As | Used For |
|------|------|-------------|----------|
| **Spark Runner** | Service | Pedregal Graph | Batch job submission (Submit/Check/Cancel) |
| **Spark Gateway** | Service | Pedregal Service | Routes to compute + Spark Connect proxy |
| **Spark Connect Server** | Runtime Component | Runs inside Driver Pod | Interactive session execution |
| **Spark Connect Client** | Client Library | Installed in user environment | Connects to Spark Connect Server |

### Access Pattern Summary

| Mode | Entry Point | Flow |
|------|-------------|------|
| **Batch Jobs** | DCP Manifest / REST API | DCP → Spark Runner → Spark Gateway → SK8 |
| **Interactive** | Jupyter / PySpark | Spark Connect Client → Spark Gateway → Spark Connect Server |

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

## Multi-Tenant Spark Connect Architecture

This section describes how to provide standardized Spark Connect access for teams like Feature Engineering, ML Platform, and other stakeholders.

### Requirements Summary

| Requirement | Solution |
|-------------|----------|
| Static URL access (`sc://team-cluster.doordash.team`) | DNS CNAME → Spark Gateway |
| No SSH tunneling | Spark Gateway publicly routable with auth |
| Multi-region (US/EU) | Regional Spark Gateway + clusters |
| Self-service provisioning | Cluster Request API (like Fabricator) |
| Unity Catalog access | Cluster templates with catalog config |
| Environment separation | Namespace per environment (dev/staging/prod) |
| Backend agnostic | Spark Gateway abstracts DBR/EMR/K8s |
| ML K8s accessibility | Network policy allowing Metaflow → Gateway |

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    MULTI-TENANT SPARK CONNECT ARCHITECTURE                        │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                           CLIENT LAYER                                       │ │
│  │                                                                             │ │
│  │   Jupyter Notebooks    Metaflow Steps     Feature Pipelines    Ad-hoc      │ │
│  │        │                    │                   │              Queries      │ │
│  │        │                    │                   │                 │         │ │
│  │        └────────────────────┴───────────────────┴─────────────────┘         │ │
│  │                                    │                                         │ │
│  │                    Spark Connect Client (pyspark[connect])                  │ │
│  │                    - Thin client library                                     │ │
│  │                    - No local Spark needed                                   │ │
│  │                    - Uses sc:// protocol                                     │ │
│  │                                    │                                         │ │
│  └────────────────────────────────────┼─────────────────────────────────────────┘ │
│                                       │                                          │
│                                       ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         DNS / ROUTING LAYER                                  │ │
│  │                                                                             │ │
│  │   sc://feature-eng.doordash.team ──┐                                        │ │
│  │   sc://ml-platform.doordash.team ──┼──▶ CNAME → spark-gateway.doordash.team │ │
│  │   sc://data-science.doordash.team ─┘                                        │ │
│  │                                                                             │ │
│  │   Regional endpoints:                                                        │ │
│  │   sc://feature-eng-eu.doordash.team → spark-gateway-eu.doordash.team        │ │
│  │   sc://feature-eng-us.doordash.team → spark-gateway-us.doordash.team        │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                       │                                          │
│                                       ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                         SPARK GATEWAY (Control Plane)                        │ │
│  │                                                                             │ │
│  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │ │
│  │   │    Auth     │  │  Cluster    │  │   Routing   │  │    Hot      │       │ │
│  │   │   (Okta)    │  │  Discovery  │  │   Engine    │  │  Clusters   │       │ │
│  │   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │ │
│  │                                                                             │ │
│  │   Features:                                                                  │ │
│  │   • Team-based authentication (Okta groups)                                 │ │
│  │   • Cluster discovery by team/environment                                   │ │
│  │   • Load balancing across cluster pool                                      │ │
│  │   • Hot cluster management (pre-warmed)                                     │ │
│  │   • Automatic failover                                                      │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                       │                                          │
│           ┌───────────────────────────┼───────────────────────────┐              │
│           │                           │                           │              │
│           ▼                           ▼                           ▼              │
│  ┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐        │
│  │   US-WEST-2     │       │   US-EAST-1     │       │    EU-WEST-1    │        │
│  │   Cluster Pool  │       │   Cluster Pool  │       │   Cluster Pool  │        │
│  │                 │       │                 │       │                 │        │
│  │  ┌───────────┐  │       │  ┌───────────┐  │       │  ┌───────────┐  │        │
│  │  │  Driver   │  │       │  │  Driver   │  │       │  │  Driver   │  │        │
│  │  │  + Spark  │  │       │  │  + Spark  │  │       │  │  + Spark  │  │        │
│  │  │  Connect  │  │       │  │  Connect  │  │       │  │  Connect  │  │        │
│  │  │  Server   │  │       │  │  Server   │  │       │  │  Server   │  │        │
│  │  └───────────┘  │       │  └───────────┘  │       │  └───────────┘  │        │
│  │                 │       │                 │       │                 │        │
│  │  Backend: SK8   │       │  Backend: SK8   │       │  Backend: EMR   │        │
│  │  or DBR or EMR  │       │  or DBR or EMR  │       │  or DBR         │        │
│  └─────────────────┘       └─────────────────┘       └─────────────────┘        │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Cluster Provisioning Model

Teams request clusters through a self-service API (similar to Fabricator):

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         CLUSTER PROVISIONING FLOW                                 │
│                                                                                  │
│   1. Team Request                                                                │
│   ───────────────                                                                │
│   POST /api/v1/spark-connect-clusters                                            │
│   {                                                                              │
│     "team": "feature-engineering",                                               │
│     "environment": "prod",                                                       │
│     "region": "eu-west-1",                                                       │
│     "size": "medium",           // small/medium/large/xlarge                     │
│     "catalogs": ["pedregal", "feature_store"],                                   │
│     "ttl": "7d"                 // or "persistent"                               │
│   }                                                                              │
│                                                                                  │
│   2. Provisioning Service                                                        │
│   ───────────────────────                                                        │
│   • Validates team permissions (Okta group membership)                           │
│   • Creates namespace: sjns-feature-engineering-prod-eu                          │
│   • Provisions cluster with Unity Catalog config                                 │
│   • Registers cluster in Spark Gateway                                           │
│   • Creates DNS record: feature-eng-prod-eu.doordash.team                        │
│                                                                                  │
│   3. Response                                                                    │
│   ──────────                                                                     │
│   {                                                                              │
│     "cluster_id": "sc-feat-eng-prod-eu-001",                                     │
│     "endpoint": "sc://feature-eng-prod-eu.doordash.team",                        │
│     "status": "provisioning",                                                    │
│     "catalogs": ["pedregal", "feature_store"],                                   │
│     "expires_at": "2024-01-15T00:00:00Z"                                         │
│   }                                                                              │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Environment Separation

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         ENVIRONMENT NAMESPACES                                    │
│                                                                                  │
│   Team: feature-engineering                                                      │
│                                                                                  │
│   ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                 │
│   │      DEV        │  │    STAGING      │  │      PROD       │                 │
│   │                 │  │                 │  │                 │                 │
│   │ Namespace:      │  │ Namespace:      │  │ Namespace:      │                 │
│   │ sjns-feat-dev   │  │ sjns-feat-stg   │  │ sjns-feat-prod  │                 │
│   │                 │  │                 │  │                 │                 │
│   │ Endpoint:       │  │ Endpoint:       │  │ Endpoint:       │                 │
│   │ sc://feat-eng   │  │ sc://feat-eng   │  │ sc://feat-eng   │                 │
│   │ -dev.dd.team    │  │ -stg.dd.team    │  │ .doordash.team  │                 │
│   │                 │  │                 │  │                 │                 │
│   │ Unity Catalog:  │  │ Unity Catalog:  │  │ Unity Catalog:  │                 │
│   │ pedregal-dev    │  │ pedregal-stg    │  │ pedregal-prod   │                 │
│   │                 │  │                 │  │                 │                 │
│   │ Hot Clusters: 0 │  │ Hot Clusters: 1 │  │ Hot Clusters: 3 │                 │
│   └─────────────────┘  └─────────────────┘  └─────────────────┘                 │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Unity Catalog Configuration

Clusters are pre-configured with Unity Catalog access:

```python
# Cluster template configuration
spark_config = {
    # Unity Catalog configuration
    "spark.sql.catalog.pedregal": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.pedregal.type": "rest",
    "spark.sql.catalog.pedregal.uri": "https://unity-catalog.doordash.team",
    "spark.sql.catalog.pedregal.warehouse": "s3://doordash-lakehouse/warehouse",
    "spark.sql.catalog.pedregal.credential": "uc-credential-provider",

    # Feature store catalog
    "spark.sql.catalog.feature_store": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.feature_store.type": "rest",
    "spark.sql.catalog.feature_store.uri": "https://unity-catalog.doordash.team",

    # Default catalog
    "spark.sql.defaultCatalog": "pedregal",
}
```

### Client Usage Examples

**Metaflow Step (ML Pipeline):**
```python
from metaflow import FlowSpec, step
from pyspark.sql import SparkSession

class FeatureFlow(FlowSpec):
    @step
    def start(self):
        # Connect to team's Spark Connect cluster
        spark = SparkSession.builder \
            .remote("sc://feature-eng.doordash.team") \
            .getOrCreate()

        # Access Unity Catalog tables directly
        self.features = spark.table("pedregal.feature_store.user_features") \
            .filter("ds = '2024-01-01'") \
            .toPandas()

        self.next(self.train)
```

**Jupyter Notebook:**
```python
# No local Spark installation needed!
# Just: pip install pyspark[connect]

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://feature-eng-dev.doordash.team") \
    .getOrCreate()

# Explore data interactively
spark.sql("SHOW DATABASES IN pedregal").show()
spark.sql("SELECT * FROM pedregal.raw.events LIMIT 10").show()
```

**CI/CD Pipeline:**
```yaml
# .github/workflows/feature-tests.yaml
jobs:
  test-features:
    runs-on: ubuntu-latest
    steps:
      - name: Run feature tests
        env:
          SPARK_REMOTE: sc://feature-eng-ci.doordash.team
        run: |
          pip install pyspark[connect] pytest
          pytest tests/features/
```

### Network Access from ML K8s

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         NETWORK TOPOLOGY                                          │
│                                                                                  │
│   ML K8s Cluster                          Spark Infrastructure                   │
│   ┌─────────────────────┐                 ┌─────────────────────┐               │
│   │                     │                 │                     │               │
│   │  ┌───────────────┐  │   gRPC/15002    │  ┌───────────────┐  │               │
│   │  │ Metaflow Pod  │──┼─────────────────┼─▶│ Spark Gateway │  │               │
│   │  │ (Spark Client)│  │                 │  │               │  │               │
│   │  └───────────────┘  │                 │  └───────┬───────┘  │               │
│   │                     │                 │          │          │               │
│   │  ┌───────────────┐  │                 │          ▼          │               │
│   │  │ Jupyter Pod   │──┼─────────────────┼─▶ ┌─────────────┐   │               │
│   │  │ (Spark Client)│  │                 │   │ Driver Pod  │   │               │
│   │  └───────────────┘  │                 │   │ + SC Server │   │               │
│   │                     │                 │   └─────────────┘   │               │
│   └─────────────────────┘                 └─────────────────────┘               │
│                                                                                  │
│   Network Policy: Allow egress from ML namespace to Spark Gateway on port 15002 │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Standardization Benefits

| Aspect | Without Standardization | With Spark Gateway |
|--------|------------------------|-------------------|
| **Access** | SSH tunneling, port forwarding | `sc://team.doordash.team` |
| **Auth** | Manual credential management | Okta SSO integration |
| **Discovery** | "Ask around for cluster IP" | DNS-based discovery |
| **Provisioning** | Manual ticket to infra team | Self-service API |
| **Multi-region** | Separate process per region | Single API, regional routing |
| **Catalogs** | Manual Spark config | Pre-configured templates |
| **Environments** | Ad-hoc separation | Namespace-based isolation |
