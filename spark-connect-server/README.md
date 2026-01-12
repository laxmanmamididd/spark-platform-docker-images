# DoorDash Spark Connect Server

This directory contains the **server-side** components for DoorDash's Spark Connect implementation.

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SPARK CONNECT SERVER ARCHITECTURE                                                 │
├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                                     │
│   CLIENT (standard pyspark)          SPARK GATEWAY                     SK8 CLUSTER                                  │
│   ┌─────────────────────────┐       ┌─────────────────────────┐       ┌─────────────────────────────────────────┐   │
│   │                         │       │                         │       │                                         │   │
│   │  spark = SparkSession   │ gRPC  │  1. Authenticate        │ gRPC  │   DRIVER POD                            │   │
│   │    .builder             │──────►│  2. Route by team       │──────►│   ┌─────────────────────────────────┐   │   │
│   │    .remote("sc://gw")   │       │  3. Proxy to driver     │       │   │  Spark Connect Server (:15002)  │   │   │
│   │    .getOrCreate()       │       │  4. Audit log           │       │   │  - ExecutePlanHandler           │   │   │
│   │                         │       │                         │       │   │  - AnalyzePlanHandler           │   │   │
│   │  df = spark.sql(...)    │◄──────│                         │◄──────│   │  - ConfigHandler                │   │   │
│   │                         │ Arrow │                         │       │   │  - InterruptHandler             │   │   │
│   │                         │       │                         │       │   └─────────────────────────────────┘   │   │
│   └─────────────────────────┘       └─────────────────────────┘       │                                         │   │
│                                                                       │   SparkSession                          │   │
│                                                                       │   - Unity Catalog                       │   │
│                                                                       │   - Analyzer, Optimizer, Scheduler      │   │
│                                                                       │                                         │   │
│                                                                       │   EXECUTOR PODS                         │   │
│                                                                       │   ┌─────┐ ┌─────┐ ┌─────┐               │   │
│                                                                       │   │ E1  │ │ E2  │ │ EN  │               │   │
│                                                                       │   └──┬──┘ └──┬──┘ └──┬──┘               │   │
│                                                                       └──────┼──────┼──────┼───────────────────┘   │
│                                                                              │      │      │                       │
│                                                                              ▼      ▼      ▼                       │
│                                                                       ┌─────────────────────────────────────────┐   │
│                                                                       │  S3 (Iceberg/Delta tables)              │   │
│                                                                       └─────────────────────────────────────────┘   │
│                                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Docker Image (`Dockerfile`)

The server-side Docker image containing:
- Apache Spark 3.5 with Spark Connect Server
- Unity Catalog Spark plugin
- AWS S3 connectors (hadoop-aws)
- Iceberg and Delta Lake support
- Python environment with pyarrow, pandas

**Build:**
```bash
cd spark-connect-server
docker build -t doordash-docker.jfrog.io/spark-platform/3.5-oss:latest .
```

### 2. Entrypoint Script (`entrypoint.sh`)

Handles different modes:
- `connect` - Start Spark Connect Server (for interactive sessions)
- `driver` - Start as Spark Driver (for batch jobs)
- `driver-connect` - Batch job with Spark Connect enabled
- `executor` - Start as Spark Executor

### 3. Kubernetes Resources (`k8s/`)

- `spark-connect-server.yaml` - SparkApplication CRD for long-running server
- Service, ConfigMap, ServiceAccount definitions

## How It Works

### 1. Gateway Instantiates Clusters

The Spark Gateway does NOT directly start the Spark Connect Server. Instead:

1. **Cluster Registry** discovers existing clusters via K8s API
2. **SparkApplication CRDs** are created by:
   - Platform team (for shared team clusters)
   - Self-service API (for on-demand clusters)
   - Playground service (for user playgrounds)

3. **Spark Operator** watches for SparkApplication CRDs and creates Driver pods

4. **Gateway routes** to discovered clusters based on team/environment

### 2. Cluster Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              CLUSTER LIFECYCLE                                               │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                             │
│   1. CREATION (how clusters get started)                                                    │
│   ────────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                             │
│   Option A: Pre-provisioned (team clusters)                                                 │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐                 │
│   │ Platform Team    │      │ Spark Operator   │      │ Driver Pod       │                 │
│   │ applies YAML     │─────►│ watches CRD      │─────►│ starts with      │                 │
│   │ to K8s           │      │ creates pods     │      │ Spark Connect    │                 │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘                 │
│                                                                                             │
│   Option B: On-demand (playgrounds, self-service)                                           │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐                 │
│   │ User requests    │      │ Provisioning API │      │ Spark Operator   │                 │
│   │ via pretzel CLI  │─────►│ creates CRD      │─────►│ creates pods     │                 │
│   │ or API           │      │                  │      │                  │                 │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘                 │
│                                                                                             │
│   Option C: Hot pool (fast assignment)                                                      │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐                 │
│   │ Gateway assigns  │      │ Pre-warmed       │      │ Cluster already  │                 │
│   │ from hot pool    │─────►│ cluster pool     │─────►│ running, instant │                 │
│   │                  │      │ (N ready)        │      │ assignment       │                 │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘                 │
│                                                                                             │
│   2. DISCOVERY (how Gateway finds clusters)                                                 │
│   ────────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                             │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐                 │
│   │ Gateway          │      │ K8s API          │      │ Service          │                 │
│   │ ClusterRegistry  │─────►│ List Services    │─────►│ spark-connect-   │                 │
│   │                  │      │ in sjns-* ns     │      │ server:15002     │                 │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘                 │
│                                                                                             │
│   3. ROUTING (how requests reach clusters)                                                  │
│   ────────────────────────────────────────────────────────────────────────────────────────  │
│                                                                                             │
│   ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐                 │
│   │ Client request   │      │ Gateway extracts │      │ Route to         │                 │
│   │ with Okta token  │─────►│ team from token  │─────►│ sjns-{team}-prod │                 │
│   │                  │      │ claims           │      │ :15002           │                 │
│   └──────────────────┘      └──────────────────┘      └──────────────────┘                 │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3. Gateway ↔ Server Communication

```go
// Gateway discovers cluster
cluster, _ := clusterRegistry.GetClusterForTeam(ctx, "feature-eng", "prod")
// cluster.Endpoint = "spark-connect-server.sjns-feature-eng-prod.svc.cluster.local:15002"

// Gateway proxies gRPC
conn, _ := grpc.Dial(cluster.Endpoint, ...)
backendClient := pb.NewSparkConnectServiceClient(conn)

// Forward ExecutePlan request
backendStream, _ := backendClient.ExecutePlan(ctx, req)
for {
    resp, _ := backendStream.Recv()
    stream.Send(resp)  // Forward to client
}
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SPARK_MODE` | Server mode: connect, driver, executor | `connect` |
| `SPARK_CONNECT_GRPC_PORT` | gRPC port for Spark Connect | `15002` |
| `UNITY_CATALOG_URI` | Unity Catalog server URL | `http://unity-catalog.doordash.team:8080` |
| `UNITY_CATALOG_TOKEN` | UC authentication token | (empty) |
| `AWS_REGION` | AWS region for S3 | `us-west-2` |

### Spark Configuration

Key settings in `spark-defaults.conf`:

```properties
# Enable Spark Connect
spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin
spark.connect.grpc.binding.port=15002

# Unity Catalog
spark.sql.catalog.datalake=io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.datalake.uri=http://unity-catalog.doordash.team:8080
spark.sql.defaultCatalog=datalake
```

## Deployment

### Deploy Team Cluster

```bash
# Create namespace
kubectl create namespace sjns-feature-eng-prod

# Apply SparkApplication
kubectl apply -f k8s/spark-connect-server.yaml

# Check status
kubectl get sparkapplication -n sjns-feature-eng-prod
```

### Verify Connectivity

```bash
# Port-forward for testing
kubectl port-forward -n sjns-feature-eng-prod svc/spark-connect-server 15002:15002

# Test with pyspark
python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
spark.sql('SELECT 1').show()
"
```

## Image Variants

| Image | Contents | Use Case |
|-------|----------|----------|
| `3.5-oss:latest` | Base Spark + Connect + UC | Interactive sessions |
| `3.5-oss-minimal:latest` | Minimal dependencies | Test clusters |
| `3.5-coreetl:2.7.0` | + CoreETL | Production batch jobs |
| `3.5-ml:latest` | + MLlib, XGBoost | ML workloads |

## Q&A

**Q: Does DoorDash need a `doordash-spark-connect` server library?**

A: No. The "server library" is just:
1. Apache Spark 3.5 (with built-in Spark Connect Server)
2. Unity Catalog plugin JAR
3. Cloud connectors (S3, Iceberg)

These are packaged into a Docker image, not a separate library.

**Q: What open source Spark library should we use?**

A: Use official Apache Spark 3.5+. Spark Connect Server is built-in:
- `apache/spark:3.5.3-java17` as base image
- Add `spark-connect_2.12-3.5.3.jar` if not included
- Add `unitycatalog-spark_2.12-0.2.0.jar` for Unity Catalog

**Q: How does Gateway instantiate/call the Spark Connect Server?**

A: Gateway does NOT instantiate servers. It:
1. Discovers existing clusters via K8s Service discovery
2. Routes requests to the appropriate cluster's `:15002` gRPC endpoint
3. Proxies gRPC streams bidirectionally

Cluster creation is handled by:
- Platform team (pre-provisioned team clusters)
- Self-service API (on-demand)
- Hot cluster pool (pre-warmed)

## References

- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Unity Catalog Spark Integration](https://docs.unitycatalog.io/integrations/unity-catalog-spark/)
- [Spark Kubernetes Operator](https://github.com/kubeflow/spark-operator)
- [Apache Spark Docker Images](https://hub.docker.com/r/apache/spark/)
