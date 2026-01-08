# Internal Spark Runtime (SK8)

This document describes the Internal Spark Runtime architecture for Spark on Kubernetes (SK8) as part of the Pedregal Spark Platform.

## Overview

The Internal Spark Runtime is DoorDash's self-managed Spark execution environment running on Kubernetes. It replaces vendor-managed solutions (EMR/Databricks) with a fully controlled, cost-optimized runtime.

### Key Goals

1. **Lower cost** - EC2 spot instances vs vendor markups
2. **Faster startup** - ~30s pod scheduling vs 6-8 min cluster spin-up
3. **Full control** - Custom configurations, debugging, optimization
4. **Deep integration** - Native Pedregal ecosystem integration
5. **Multi-tenancy** - Namespace-based team isolation

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         INTERNAL SPARK RUNTIME (SK8)                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                           CONTROL PLANE                                        │  │
│  │                                                                               │  │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                      │  │
│  │   │Spark Runner │───▶│Spark Gateway│───▶│Cluster Proxy│                      │  │
│  │   │  (Graph)    │    │  (Service)  │    │  (Service)  │                      │  │
│  │   │             │    │             │    │             │                      │  │
│  │   │• Submit     │    │• Routing    │    │• Multi-K8s  │                      │  │
│  │   │• Check      │    │• Auth       │    │• HA/LB      │                      │  │
│  │   │• Cancel     │    │• SC Proxy   │    │• Failover   │                      │  │
│  │   │• APS/AR     │    │• Namespaces │    │             │                      │  │
│  │   └─────────────┘    └─────────────┘    └──────┬──────┘                      │  │
│  │                                                │                              │  │
│  └────────────────────────────────────────────────┼──────────────────────────────┘  │
│                                                   │                                  │
│                                                   ▼                                  │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                           EXECUTION PLANE (K8s)                                │  │
│  │                                                                               │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐ │  │
│  │   │                    Spark Kubernetes Operator (SKO)                       │ │  │
│  │   │                                                                         │ │  │
│  │   │   • Watches SparkApplication CRDs                                       │ │  │
│  │   │   • Creates/manages Driver + Executor pods                              │ │  │
│  │   │   • Reports status back to control plane                                │ │  │
│  │   │   • Handles pod failures and restarts                                   │ │  │
│  │   └─────────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                               │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐ │  │
│  │   │                    Domain Namespace (sjns-<team>)                        │ │  │
│  │   │                                                                         │ │  │
│  │   │   ┌─────────────────────────────────────────────────────────────────┐   │ │  │
│  │   │   │                      DRIVER POD                                  │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   │   ┌─────────────────┐  ┌─────────────────┐  ┌───────────────┐  │   │ │  │
│  │   │   │   │  Spark Driver   │  │  Spark Connect  │  │ OTel Sidecar  │  │   │ │  │
│  │   │   │   │                 │  │  Server (:15002)│  │               │  │   │ │  │
│  │   │   │   │  • DAG Sched    │  │                 │  │ • Metrics     │  │   │ │  │
│  │   │   │   │  • Task Sched   │  │  • Sessions     │  │ • Logs        │  │   │ │  │
│  │   │   │   │  • Block Mgr    │  │  • Query Exec   │  │ • Traces      │  │   │ │  │
│  │   │   │   └─────────────────┘  └─────────────────┘  └───────────────┘  │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   │   Spark UI (:4040)                                              │   │ │  │
│  │   │   └─────────────────────────────────────────────────────────────────┘   │ │  │
│  │   │                                                                         │ │  │
│  │   │   ┌─────────────────────────────────────────────────────────────────┐   │ │  │
│  │   │   │                    EXECUTOR PODS (N)                             │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   │   ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐   │   │ │  │
│  │   │   │   │ Executor 1│  │ Executor 2│  │ Executor 3│  │ Executor N│   │   │ │  │
│  │   │   │   │           │  │           │  │           │  │           │   │   │ │  │
│  │   │   │   │ • Tasks   │  │ • Tasks   │  │ • Tasks   │  │ • Tasks   │   │   │ │  │
│  │   │   │   │ • Shuffle │  │ • Shuffle │  │ • Shuffle │  │ • Shuffle │   │   │ │  │
│  │   │   │   │ • Cache   │  │ • Cache   │  │ • Cache   │  │ • Cache   │   │   │ │  │
│  │   │   │   └───────────┘  └───────────┘  └───────────┘  └───────────┘   │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   │   Dynamic Resource Allocation (DRA) - scales 1 to N             │   │ │  │
│  │   │   └─────────────────────────────────────────────────────────────────┘   │ │  │
│  │   │                                                                         │ │  │
│  │   │   ResourceQuota | LimitRange | NetworkPolicy | RBAC                     │ │  │
│  │   └─────────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                               │  │
│  └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                           NODE INFRASTRUCTURE                                  │  │
│  │                                                                               │  │
│  │   Spark Worker Node Pool (Karpenter-managed)                                  │  │
│  │   • EC2 Spot instances for cost optimization                                  │  │
│  │   • Local NVMe SSD for shuffle storage                                        │  │
│  │   • Auto-scales based on pending pods                                         │  │
│  │   • Terminates idle nodes after configurable TTL                              │  │
│  │                                                                               │  │
│  └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Control Plane

#### Spark Runner (Pedregal Graph)

Stateless graph providing core job lifecycle primitives:

```
Submit(JobSpec, IdempotencyKey) → (ExecutionID, VendorRunID)
Check(ExecutionID) → ExecutionState
Cancel(ExecutionID, Team) → Success/Failure
```

Supporting components:
- **APS (Auto Parameter Selection)**: Pre-submit cluster sizing based on history
- **AR (Auto-Retry/Remediation)**: Failure classification and retry logic
- **DCR (Dynamic Cluster Resizing)**: Runtime resource adjustment

#### Spark Gateway (Pedregal Service)

Single entry point for all Spark operations:
- Routes SparkApplication CRDs to K8s clusters
- Proxies Spark Connect gRPC traffic
- Enforces authentication (Okta) and authorization
- Manages domain namespaces
- Handles hot cluster pool for fast startup

#### Cluster Proxy Service

Multi-cluster routing and HA:
- Load balances across healthy K8s clusters
- Failover on cluster outages
- Multi-region support (us-west-2, us-east-1, eu-west-1)

### 2. Execution Plane

#### Spark Kubernetes Operator (SKO)

Apache spark-kubernetes-operator manages SparkApplication CRDs:

| Responsibility | Details |
|----------------|---------|
| Watch CRDs | Monitors SparkApplication resources |
| Create Pods | Provisions Driver pod, then Executor pods |
| Manage Lifecycle | Handles restarts, failures, completion |
| Report Status | Updates CRD status for control plane |

#### Driver Pod

| Component | Port | Purpose |
|-----------|------|---------|
| Spark Driver | - | DAG scheduling, task scheduling, block manager |
| Spark Connect Server | 15002 | Interactive sessions via gRPC |
| Spark UI | 4040 | Web UI for job monitoring |
| OTel Sidecar | - | Metrics, logs, traces to observability |

#### Executor Pods

- **Scaling**: Dynamic Resource Allocation (DRA) scales 1 to N
- **Tasks**: Execute Spark tasks assigned by driver
- **Storage**: Local shuffle data, cached RDDs/DataFrames
- **Lifecycle**: Ephemeral - terminated on job completion

### 3. Domain Namespaces

Team isolation via Kubernetes namespaces:

```
sjns-<team>-<environment>

Examples:
├── sjns-data-infra-prod
├── sjns-feature-eng-prod
├── sjns-feature-eng-staging
└── sjns-ml-platform-dev
```

Each namespace includes:

| Resource | Purpose |
|----------|---------|
| ResourceQuota | CPU/memory limits per team |
| LimitRange | Default pod resource requests/limits |
| NetworkPolicy | Isolation between namespaces |
| RBAC | Team-based access control |
| Secrets | Team-managed credentials (Unity Catalog, S3, etc.) |

## Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              JOB EXECUTION FLOW                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  1. Job Submission                                                                  │
│     ────────────────                                                                │
│     DCP/Airflow → Spark Runner.Submit(JobSpec)                                      │
│                        │                                                            │
│                        ▼                                                            │
│  2. APS Recommendation (optional)                                                   │
│     ─────────────────────────────                                                   │
│     Spark Runner → APS → Get sizing recommendation                                  │
│                        │                                                            │
│                        ▼                                                            │
│  3. Gateway Routing                                                                 │
│     ───────────────────                                                             │
│     Spark Runner → Spark Gateway → Cluster Proxy                                    │
│                        │                                                            │
│                        ▼                                                            │
│  4. CRD Creation                                                                    │
│     ────────────────                                                                │
│     Cluster Proxy → K8s API → SparkApplication CRD in sjns-<team>                   │
│                        │                                                            │
│                        ▼                                                            │
│  5. Operator Reconciliation                                                         │
│     ─────────────────────────                                                       │
│     SKO watches CRD → Creates Driver pod                                            │
│                        │                                                            │
│                        ▼                                                            │
│  6. Driver Startup                                                                  │
│     ──────────────────                                                              │
│     Driver pod starts → Initializes SparkContext                                    │
│                       → Starts Spark Connect server (if interactive)                │
│                       → Requests Executor pods from K8s                             │
│                        │                                                            │
│                        ▼                                                            │
│  7. Executor Scaling                                                                │
│     ────────────────────                                                            │
│     K8s scheduler → Places Executor pods                                            │
│     Karpenter → Provisions nodes if needed                                          │
│     DRA → Scales executors based on workload                                        │
│                        │                                                            │
│                        ▼                                                            │
│  8. Task Execution                                                                  │
│     ────────────────────                                                            │
│     Driver schedules tasks → Executors execute                                      │
│                            → Shuffle data exchanged                                 │
│                            → Results aggregated                                     │
│                        │                                                            │
│                        ▼                                                            │
│  9. Completion                                                                      │
│     ────────────                                                                    │
│     Job finishes → Driver reports to SKO                                            │
│                  → SKO updates CRD status                                           │
│                  → Spark Runner.Check() returns SUCCEEDED/FAILED                    │
│                        │                                                            │
│                        ▼                                                            │
│  10. Cleanup                                                                        │
│      ─────────                                                                      │
│      Executor pods terminated → Driver pod terminated                               │
│      TTL controller → Cleans up completed CRDs                                      │
│      Karpenter → Terminates idle nodes                                              │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Shuffle Strategy

### Phase 1: Local Shuffle (Current)

```
┌─────────────────────────────────────────────────────────────────┐
│                      LOCAL SHUFFLE                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────┐              ┌─────────────┐                  │
│   │ Executor 1  │              │ Executor 2  │                  │
│   │             │   Network    │             │                  │
│   │ ┌─────────┐ │◄────────────►│ ┌─────────┐ │                  │
│   │ │ Shuffle │ │   Fetch      │ │ Shuffle │ │                  │
│   │ │ (NVMe)  │ │              │ │ (NVMe)  │ │                  │
│   │ └─────────┘ │              │ └─────────┘ │                  │
│   └─────────────┘              └─────────────┘                  │
│                                                                 │
│   Characteristics:                                              │
│   • Shuffle data on node-local NVMe SSD                         │
│   • Per-job storage budgets enforced                            │
│   • DiskPressure monitoring and alerts                          │
│   • Fetch failure tracking and retry                            │
│                                                                 │
│   Limitations:                                                  │
│   • Executor failure loses shuffle data                         │
│   • Limits aggressive autoscaling                               │
│   • Node-local storage capacity constraints                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Phase 2: Remote Shuffle (Future - Celeborn)

```
┌─────────────────────────────────────────────────────────────────┐
│                     REMOTE SHUFFLE (CELEBORN)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────┐              ┌─────────────┐                  │
│   │ Executor 1  │              │ Executor 2  │                  │
│   └──────┬──────┘              └──────┬──────┘                  │
│          │                            │                          │
│          │  Write shuffle             │  Write shuffle           │
│          │                            │                          │
│          └────────────┬───────────────┘                          │
│                       │                                          │
│                       ▼                                          │
│          ┌─────────────────────────┐                             │
│          │    Celeborn Service     │                             │
│          │                         │                             │
│          │  • Remote shuffle store │                             │
│          │  • Replicated for HA    │                             │
│          │  • Decoupled from exec  │                             │
│          └─────────────────────────┘                             │
│                                                                 │
│   Benefits:                                                      │
│   • Executor failure doesn't lose shuffle                        │
│   • Enables aggressive executor autoscaling                      │
│   • Better resource utilization                                  │
│   • Shuffle data persists across executor restarts               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Autoscaling

### Three-Layer Autoscaling

| Layer | Component | Mechanism | Latency |
|-------|-----------|-----------|---------|
| **Executor** | Spark DRA | Scale executors based on pending tasks | Seconds |
| **Node** | Karpenter | Provision EC2 when pods pending | 1-2 minutes |
| **Cluster** | Cluster Proxy | Route across K8s clusters | Instant |

### Dynamic Resource Allocation (DRA)

```yaml
# Spark configuration for DRA
spark.dynamicAllocation.enabled: true
spark.dynamicAllocation.minExecutors: 1
spark.dynamicAllocation.maxExecutors: 100
spark.dynamicAllocation.initialExecutors: 2
spark.dynamicAllocation.executorIdleTimeout: 60s
spark.dynamicAllocation.schedulerBacklogTimeout: 1s
```

### Karpenter Node Provisioning

```yaml
# Karpenter provisioner for Spark workers
apiVersion: karpenter.sh/v1alpha5
kind: Provisioner
metadata:
  name: spark-workers
spec:
  requirements:
    - key: node.kubernetes.io/instance-type
      operator: In
      values: ["r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge"]
    - key: karpenter.sh/capacity-type
      operator: In
      values: ["spot", "on-demand"]
  limits:
    resources:
      cpu: 1000
      memory: 4000Gi
  ttlSecondsAfterEmpty: 300  # 5 min idle before termination
  labels:
    spark-worker: "true"
```

## Docker Image

### Layer Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPARK DOCKER IMAGE LAYERS                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Layer 4: Application                                       │  │
│  │ • CoreETL JAR files                                        │  │
│  │ • User application code                                    │  │
│  │ • Init scripts                                             │  │
│  │ • Job-specific configurations                              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Layer 3: Connectors & Dependencies                         │  │
│  │ • Unity Catalog connector                                  │  │
│  │ • S3/GCS connectors (hadoop-aws)                          │  │
│  │ • Iceberg runtime                                          │  │
│  │ • Kafka client                                             │  │
│  │ • Delta Lake (optional)                                    │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Layer 2: Spark Runtime                                     │  │
│  │ • Spark 3.5.x / 4.x binaries                               │  │
│  │ • Spark Connect server JARs                                │  │
│  │ • PySpark (for Python jobs)                               │  │
│  │ • Default Spark configurations                             │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Layer 1: Base OS + JVM                                     │  │
│  │ • Debian Slim / Amazon Linux 2                             │  │
│  │ • OpenJDK 17 (LTS)                                         │  │
│  │ • Python 3.11                                              │  │
│  │ • Essential utilities                                      │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Image Versioning

| Tag Pattern | Example | Description |
|-------------|---------|-------------|
| `<spark>-latest` | `3.5-latest` | Latest stable for Spark version |
| `<spark>-<date>` | `3.5-20240115` | Dated build for reproducibility |
| `<spark>-<coreetl>` | `3.5-coreetl-2.7.0` | With specific CoreETL version |

### Exposed Ports

| Port | Service | Purpose |
|------|---------|---------|
| 4040 | Spark UI | Web UI for job monitoring |
| 7077 | Spark Master | Standalone mode (not used in K8s) |
| 15002 | Spark Connect | gRPC for interactive sessions |
| 18080 | History Server | Historical Spark UI |

## Observability

### Observability Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                      OBSERVABILITY STACK                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│   │   Driver    │    │  Executor   │    │  Executor   │        │
│   │   Pod       │    │   Pod 1     │    │   Pod N     │        │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘        │
│          │                  │                  │                 │
│          └──────────────────┼──────────────────┘                 │
│                             │                                    │
│                             ▼                                    │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                   OTel Collector                         │   │
│   │   • Receives metrics, logs, traces from sidecars        │   │
│   │   • Enriches with job metadata                          │   │
│   │   • Routes to appropriate backends                      │   │
│   └─────────────────────────────────────────────────────────┘   │
│                             │                                    │
│          ┌──────────────────┼──────────────────┐                 │
│          │                  │                  │                 │
│          ▼                  ▼                  ▼                 │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │
│   │ Chronosphere│    │    ODIN     │    │    SHS      │        │
│   │  (Metrics)  │    │   (Logs)    │    │ (Spark UI)  │        │
│   └─────────────┘    └─────────────┘    └─────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Metrics (Chronosphere)

| Category | Metrics |
|----------|---------|
| **Job** | submissions, success_rate, duration, cost |
| **Task** | count, duration, failures, retries |
| **Shuffle** | read_bytes, write_bytes, fetch_failures |
| **Resource** | cpu_usage, memory_usage, disk_usage |
| **DRA** | executor_scale_up, executor_scale_down |

### Logs (ODIN)

| Source | Content |
|--------|---------|
| Driver | Application logs, Spark logs, errors |
| Executor | Task logs, shuffle logs |
| Init Scripts | Startup logs, configuration |

### Spark History Server

- **Purpose**: Historical Spark UI for completed jobs
- **Storage**: Event logs in S3
- **Access**: Authenticated via Okta
- **Retention**: 30 days default

### Execution History (Taulu)

```protobuf
// Executions table - one row per execution
message Execution {
  string execution_id = 1;     // Primary key
  string job_path = 2;         // CoreETL job identifier
  string owner = 3;
  string team = 4;
  int64 start_time = 5;
  int64 end_time = 6;
  ExecutionState state = 7;    // SUBMITTED/RUNNING/SUCCEEDED/FAILED/CANCELLED
  string vendor = 8;           // SK8/EMR/DBR
  string vendor_run_id = 9;
  string spark_ui_url = 10;
  string logs_url = 11;
}

// Execution Events table - append-only lifecycle events
message ExecutionEvent {
  string event_id = 1;
  string execution_id = 2;
  EventType event_type = 3;    // SUBMIT/RUNNING/TERMINAL/METRICS
  int64 timestamp = 4;
  bytes payload = 5;
}
```

## Comparison: SK8 vs Legacy

| Aspect | SJEM (EMR/DBR) | SK8 (Internal Runtime) |
|--------|----------------|------------------------|
| **Compute** | Vendor-managed clusters | Self-managed K8s pods |
| **Cost Model** | EMR/DBR pricing + EC2 | EC2 spot + K8s overhead |
| **Startup Time** | 6-8 minutes | ~30 seconds |
| **Scaling** | Vendor autoscaling | DRA + Karpenter |
| **Multitenancy** | Per-account isolation | Namespace isolation |
| **Control** | Limited customization | Full control |
| **Debugging** | Vendor UI | Direct pod access + SHS |
| **Integration** | API-based | Native Pedregal |

## Configuration

### SparkApplication CRD Example

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: example-etl-job
  namespace: sjns-data-infra-prod
spec:
  type: Scala
  mode: cluster
  image: spark-platform:3.5-latest
  imagePullPolicy: Always
  mainClass: com.doordash.coreetl.spark.Main
  mainApplicationFile: s3://core-etl-artifacts/jars/core-etl-driver.jar
  sparkVersion: "3.5.0"

  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark-driver
    labels:
      version: "3.5.0"

  executor:
    cores: 4
    instances: 5
    memory: "8g"
    labels:
      version: "3.5.0"

  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 1
    maxExecutors: 20

  sparkConf:
    spark.sql.catalog.pedregal: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.pedregal.type: rest
    spark.sql.catalog.pedregal.uri: https://unity-catalog.doordash.team
    spark.kubernetes.executor.deleteOnTermination: "true"
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: s3://spark-event-logs/

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 3
```

## References

- [PEDREGAL_SPARK_PLATFORM.md](PEDREGAL_SPARK_PLATFORM.md) - Complete platform reference
- [ARCHITECTURE.md](ARCHITECTURE.md) - Architecture overview
- [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator)
- [Karpenter](https://karpenter.sh/)
- [Apache Celeborn](https://celeborn.apache.org/) (Remote Shuffle)

---

*Last updated: January 2025*
