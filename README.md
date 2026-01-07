# Spark Platform Docker Images and Testing Framework

A comprehensive framework for Spark job submission, interactive Spark Connect sessions, and testing, designed to integrate with DoorDash's Pedregal Spark Platform architecture.

## Architecture Overview

This platform provides **two distinct access patterns**:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              ACCESS PATTERNS                                      │
│                                                                                  │
│  ═══════════════════════════════════════════════════════════════════════════════ │
│  MODE 1: BATCH JOBS                                                              │
│  ═══════════════════════════════════════════════════════════════════════════════ │
│                                                                                  │
│    DCP Manifest / REST API                                                       │
│           │                                                                      │
│           ▼                                                                      │
│    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐          │
│    │  DCP Plugin     │────▶│  Spark Runner   │────▶│  Spark Gateway  │──▶ SK8   │
│    │  (CoreETL)      │     │  Submit/Check   │     │  Routes/Auth    │          │
│    └─────────────────┘     └─────────────────┘     └─────────────────┘          │
│                                                                                  │
│  ═══════════════════════════════════════════════════════════════════════════════ │
│  MODE 2: INTERACTIVE (Spark Connect)                                             │
│  ═══════════════════════════════════════════════════════════════════════════════ │
│                                                                                  │
│    Jupyter / Metaflow / PySpark                                                  │
│           │                                                                      │
│           │  Spark Connect Client (pyspark[connect])                             │
│           ▼                                                                      │
│    ┌─────────────────┐     ┌─────────────────────────────────────────┐          │
│    │  Spark Gateway  │────▶│  Spark Connect Server (on Driver Pod)   │          │
│    │  gRPC Proxy     │     │  Session mgmt, Query execution          │          │
│    └─────────────────┘     └─────────────────────────────────────────┘          │
│                                                                                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Interactive Sessions (Spark Connect)

```python
# Install: pip install pyspark[connect]
from pyspark.sql import SparkSession

# Connect to your team's cluster through Spark Gateway
spark = SparkSession.builder \
    .remote("sc://feature-eng-prod.doordash.team:15002") \
    .getOrCreate()

# Access Unity Catalog tables
df = spark.table("pedregal.feature_store.user_features")
df.show()
```

### Using the Python Helper Library

```python
from spark_connect_client import SparkConnectClient

# Connect with team/environment/region
spark = SparkConnectClient.create_session(
    team="feature-engineering",
    environment="prod",
    region="us-west-2"
)

# Use Spark as normal
df = spark.sql("SELECT * FROM pedregal.raw.events LIMIT 10")
```

### Batch Job Submission (REST API)

```bash
curl -X POST http://dcp-spark:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-etl-job",
    "team": "data-infra",
    "config": {
      "spark_version": "3.5",
      "job_type": "BATCH",
      "core_etl": {
        "version": "2.7.0",
        "job_spec": {...}
      }
    }
  }'
```

## Components

### Services (Control Plane)

| Component | Type | Description |
|-----------|------|-------------|
| **Spark Runner** | Pedregal Graph | Submit/Check/Cancel primitives for batch jobs |
| **Spark Gateway** | Pedregal Service | Routes to compute, gRPC proxy for Spark Connect |
| **Cluster Provisioning** | API Service | Self-service cluster management |

### Runtime Components

| Component | Type | Description |
|-----------|------|-------------|
| **Spark Connect Server** | Runs on Driver Pod | Handles interactive sessions (port 15002) |
| **Spark Connect Client** | Client Library | Thin client for connecting to clusters |

## Directory Structure

```
.
├── ARCHITECTURE.md                    # Detailed architecture documentation
├── PEDREGAL_SPARK_PLATFORM.md         # Complete platform reference
├── README.md                          # This file
│
├── spark-connect-client/              # Spark Connect client libraries
│   ├── python/
│   │   ├── spark_connect_client.py    # Python helper library
│   │   └── requirements.txt
│   └── java/
│       ├── pom.xml
│       └── src/main/java/.../SparkConnectClient.java
│
├── spark-gateway/                     # Spark Gateway implementation
│   ├── gateway.go                     # Docker-based gateway
│   └── spark_connect_proxy.go         # gRPC proxy for Spark Connect
│
├── spark-runner/                      # Spark Runner implementation
│   ├── runner.go
│   └── runner_test.go
│
├── cluster-provisioning/              # Self-service cluster provisioning
│   ├── proto/
│   │   └── cluster_provisioning.proto # gRPC API definitions
│   └── api/
│       └── server.go                  # Provisioning server
│
├── client-examples/                   # DCP client examples
│   ├── proto/                         # gRPC definitions
│   ├── dcp-rest-client/               # Go REST client
│   ├── dcp-rest-server/               # Go REST server
│   └── java-client/                   # Java REST client
│
├── examples/                          # Usage examples
│   ├── metaflow/
│   │   └── feature_pipeline.py        # Metaflow feature engineering
│   ├── jupyter/
│   │   └── spark_connect_notebook.ipynb
│   └── ci-cd/
│       ├── feature_tests.py           # pytest tests
│       └── github-workflow.yaml       # CI/CD workflow
│
├── testcontainers/                    # Java TestContainers
│   ├── build.gradle.kts
│   └── src/
│
├── docker/                            # Spark Docker image
│   ├── Dockerfile
│   ├── build.sh
│   └── entrypoint.sh
│
└── dcp-example/                       # DCP manifest example
    └── manifest.yaml
```

## Spark Connect Client Usage

### Python

```python
# Install
pip install pyspark[connect]

# Option 1: Direct connection
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .remote("sc://feature-eng.doordash.team:15002") \
    .getOrCreate()

# Option 2: Using helper library
from spark_connect_client import get_spark_session
spark = get_spark_session("feature-engineering", "prod", "us-west-2")

# Option 3: From environment variables
# export SPARK_CONNECT_TEAM=feature-engineering
# export SPARK_CONNECT_ENV=prod
from spark_connect_client import SparkConnectClient
spark = SparkConnectClient.create_session_from_env()
```

### Java/Scala

```java
// Add dependency: org.apache.spark:spark-connect-client-jvm_2.12:3.5.0

// Option 1: Direct connection
SparkSession spark = SparkSession.builder()
    .remote("sc://feature-eng.doordash.team:15002")
    .getOrCreate();

// Option 2: Using helper library
SparkSession spark = SparkConnectClient.builder()
    .team("feature-engineering")
    .environment(Environment.PROD)
    .region(Region.US_WEST_2)
    .build();
```

## Cluster Provisioning

Teams can request clusters through the self-service API:

```bash
# Request a new cluster
curl -X POST http://cluster-provisioning:8080/api/v1/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "team": "feature-engineering",
    "environment": "prod",
    "region": "eu-west-1",
    "size": "medium",
    "catalogs": ["pedregal", "feature_store"],
    "ttl": "7d"
  }'

# Response
{
  "cluster_id": "sc-feateng-pro-euw1-abc123",
  "endpoint": "sc://feateng-prod-euwest1.doordash.team:15002",
  "status": "provisioning",
  "estimated_ready_seconds": 120
}
```

## Examples

### Metaflow Pipeline

```python
from metaflow import FlowSpec, step
from spark_connect_client import SparkConnectClient

class FeatureFlow(FlowSpec):
    @step
    def start(self):
        spark = SparkConnectClient.create_session(
            team="feature-engineering",
            environment="prod"
        )
        self.features = spark.table("pedregal.feature_store.user_features") \
            .filter("ds = '2024-01-01'") \
            .toPandas()
        self.next(self.train)
```

### CI/CD Testing

```yaml
# .github/workflows/feature-tests.yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: pip install pyspark[connect] pytest
      - run: pytest tests/feature_tests.py
        env:
          SPARK_CONNECT_TEAM: feature-engineering
          SPARK_CONNECT_ENV: ci
```

### Jupyter Notebook

```python
from spark_connect_client import SparkConnectClient

spark = SparkConnectClient.create_session(
    team="feature-engineering",
    environment="dev"
)

# Explore data
spark.sql("SHOW TABLES IN pedregal.raw").show()
spark.sql("SELECT * FROM pedregal.raw.events LIMIT 10").show()
```

## Multi-Tenant Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TEAM ISOLATION                                         │
│                                                                                 │
│  Feature Engineering          ML Platform              Data Science             │
│  ┌─────────────────┐        ┌─────────────────┐      ┌─────────────────┐       │
│  │ sc://feat-eng   │        │ sc://ml-plat    │      │ sc://data-sci   │       │
│  │ .doordash.team  │        │ .doordash.team  │      │ .doordash.team  │       │
│  └────────┬────────┘        └────────┬────────┘      └────────┬────────┘       │
│           │                          │                        │                 │
│           └──────────────────────────┼────────────────────────┘                 │
│                                      │                                          │
│                                      ▼                                          │
│                         ┌─────────────────────────┐                             │
│                         │     Spark Gateway       │                             │
│                         │  • Auth (Okta)          │                             │
│                         │  • Routing              │                             │
│                         │  • Discovery            │                             │
│                         └─────────────────────────┘                             │
│                                      │                                          │
│           ┌──────────────────────────┼──────────────────────────┐               │
│           ▼                          ▼                          ▼               │
│    ┌─────────────┐           ┌─────────────┐           ┌─────────────┐         │
│    │  US-WEST-2  │           │  US-EAST-1  │           │  EU-WEST-1  │         │
│    │  SK8/DBR    │           │  SK8/DBR    │           │  EMR/DBR    │         │
│    └─────────────┘           └─────────────┘           └─────────────┘         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Environment Separation

| Environment | Endpoint Pattern | Use Case |
|-------------|------------------|----------|
| **dev** | `sc://team-dev.doordash.team` | Local development, exploration |
| **staging** | `sc://team-staging.doordash.team` | Pre-production testing |
| **ci** | `sc://team-ci.doordash.team` | Automated CI/CD tests |
| **prod** | `sc://team.doordash.team` | Production workloads |

## TestContainers (Local Testing)

```java
@Container
SparkContainer spark = SparkContainer.builder()
    .withDriverMemory(2048)
    .withSparkConfig("spark.sql.shuffle.partitions", "4")
    .build();

SparkSession session = SparkSession.builder()
    .remote(spark.getSparkConnectUrl())  // sc://localhost:xxxxx
    .getOrCreate();
```

## API Reference

### Cluster Provisioning API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/clusters` | POST | Request new cluster |
| `/api/v1/clusters/{id}` | GET | Get cluster status |
| `/api/v1/clusters` | GET | List team clusters |
| `/api/v1/clusters/{id}` | DELETE | Delete cluster |
| `/api/v1/clusters/{id}/scale` | POST | Scale cluster |
| `/api/v1/clusters/{id}/ttl` | POST | Extend TTL |

### DCP REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/jobs` | POST | Submit batch job |
| `/api/v1/jobs/{id}` | GET | Get job status |
| `/api/v1/jobs/{id}` | DELETE | Cancel job |
| `/api/v1/jobs/{id}/logs` | GET | Get job logs |

## Related Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture with diagrams
- [PEDREGAL_SPARK_PLATFORM.md](PEDREGAL_SPARK_PLATFORM.md) - Complete platform reference
- [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
