# Spark Platform Docker Images and Testing Framework

A comprehensive framework for Spark job submission and testing, designed to integrate with DoorDash's Pedregal Spark Platform architecture.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         CLIENT ENTRY POINTS                                   │
│                                                                              │
│  Option 1: DCP REST API (Recommended for external services)                  │
│  ─────────────────────────────────────────────────────────────               │
│  REST Client → DCP Server → Spark Runner → Spark Gateway → SK8              │
│                                                                              │
│  Option 2: Spark Runner gRPC (Internal Pedregal graphs)                      │
│  ─────────────────────────────────────────────────────────────               │
│  gRPC Client → Spark Runner → Spark Gateway → SK8                           │
│                                                                              │
│  Option 3: DCP Manifest (GitOps / CI-CD)                                     │
│  ─────────────────────────────────────────────────────────────               │
│  YAML Manifest → DCP Plugin → Spark Runner → Spark Gateway → SK8            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Client Examples (`client-examples/`)
- **DCP REST Client (Go)** - Submit jobs via REST API
- **DCP REST Server (Go)** - Example DCP server implementation
- **Java Client** - Java client for DCP Spark API
- **Proto definitions** - gRPC service definitions

### 2. Spark Docker Image (`docker/`)
- Multi-layer Docker image with Spark + Spark Connect
- Configurable for different Spark versions
- Includes Unity Catalog and S3 connectors

### 3. Spark Runner (`spark-runner/`)
- Go implementation of the Spark Runner interface
- Submit/Check/Cancel primitives
- Integration with Spark Gateway

### 4. Spark Gateway (`spark-gateway/`)
- Routes jobs to containerized Spark
- Manages SparkApplication lifecycle
- Supports local Docker and K8s backends

### 5. TestContainers (`testcontainers/`)
- Java TestContainers modules
- Automatic container lifecycle management
- Port mapping and health checks

### 6. DCP Example (`dcp-example/`)
- Sample DCP manifest
- CoreETL job specification

## Quick Start

### Submit a Job via REST API (Go)

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"

    dcpclient "github.com/doordash/spark-platform-docker-images/client-examples/dcp-rest-client"
)

func main() {
    client := dcpclient.NewClient("http://dcp-spark.service.prod.ddsd:8080")

    // Define CoreETL job spec
    jobSpec := map[string]interface{}{
        "sources": []map[string]interface{}{
            {"type": "iceberg", "catalog": "pedregal", "database": "raw", "table": "events"},
        },
        "transformations": []map[string]interface{}{
            {"type": "sql", "query": "SELECT * FROM source WHERE event_type = 'order'"},
        },
        "sinks": []map[string]interface{}{
            {"type": "iceberg", "catalog": "pedregal", "database": "derived", "table": "orders"},
        },
    }
    jobSpecJSON, _ := json.Marshal(jobSpec)

    resp, err := client.SubmitJob(context.Background(), &dcpclient.SubmitJobRequest{
        Name: "my-etl-job",
        Team: "data-infra",
        User: "user@doordash.com",
        Config: dcpclient.SparkJobConfig{
            SparkVersion: "3.5",
            JobType:      "BATCH",
            CoreETL: &dcpclient.CoreETLConfig{
                Version: "2.7.0",
                JobSpec: jobSpecJSON,
            },
            Resources: dcpclient.ResourceConfig{
                Driver:   dcpclient.DriverConfig{Cores: 2, Memory: "4g"},
                Executor: dcpclient.ExecutorConfig{Instances: 5, Cores: 4, Memory: "8g"},
            },
        },
    })

    if err != nil {
        panic(err)
    }

    fmt.Printf("Job submitted: %s\n", resp.JobID)
}
```

### Submit a Job via REST API (Java)

```java
DCPSparkClient client = new DCPSparkClient("http://dcp-spark.service.prod.ddsd:8080");

SubmitJobRequest request = SubmitJobRequest.builder()
    .name("my-etl-job")
    .team("data-infra")
    .user("user@doordash.com")
    .config(SparkJobConfig.builder()
        .sparkVersion("3.5")
        .jobType("BATCH")
        .coreEtl(CoreETLConfig.builder()
            .version("2.7.0")
            .jobSpec(jobSpecJson)
            .build())
        .resources(ResourceConfig.builder()
            .driver(DriverConfig.builder().cores(2).memory("4g").build())
            .executor(ExecutorConfig.builder().instances(5).cores(4).memory("8g").build())
            .build())
        .build())
    .build();

SubmitJobResponse response = client.submitJob(request);
System.out.println("Job ID: " + response.getJobId());
```

### Submit via DCP Manifest (YAML)

```yaml
# dcp-example/manifest.yaml
apiVersion: dcp.pedregal.doordash.com/v1
kind: SparkJob
metadata:
  name: example-data-transform
  team: data-infra
spec:
  engine:
    version: "3.5"
  jobType: batch
  coreEtl:
    version: "2.7.0"
    jobSpec:
      sources:
        - type: iceberg
          catalog: pedregal
          database: raw
          table: events
      transformations:
        - type: sql
          query: "SELECT * FROM source"
      sinks:
        - type: iceberg
          catalog: pedregal
          database: derived
          table: processed_events
```

### Run Tests with TestContainers

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

// Run queries
Dataset<Row> df = session.sql("SELECT 1 as id, 'hello' as message");
```

## REST API Reference

### POST /api/v1/jobs - Submit Job

```bash
curl -X POST http://dcp-spark:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-job",
    "team": "data-infra",
    "user": "user@doordash.com",
    "config": {
      "spark_version": "3.5",
      "job_type": "BATCH",
      "core_etl": {
        "version": "2.7.0",
        "job_spec": {...}
      },
      "resources": {
        "driver": {"cores": 2, "memory": "4g"},
        "executor": {"instances": 5, "cores": 4, "memory": "8g"}
      }
    }
  }'
```

### GET /api/v1/jobs/{jobID} - Get Job Status

```bash
curl http://dcp-spark:8080/api/v1/jobs/abc-123
```

### DELETE /api/v1/jobs/{jobID} - Cancel Job

```bash
curl -X DELETE http://dcp-spark:8080/api/v1/jobs/abc-123
```

### GET /api/v1/jobs/{jobID}/logs - Get Logs

```bash
curl http://dcp-spark:8080/api/v1/jobs/abc-123/logs
```

## Directory Structure

```
.
├── ARCHITECTURE.md              # Detailed architecture documentation
├── README.md                    # This file
├── client-examples/
│   ├── README.md
│   ├── proto/
│   │   ├── spark_runner.proto   # Spark Runner gRPC definitions
│   │   └── dcp_spark.proto      # DCP Spark API definitions
│   ├── dcp-rest-client/         # Go REST client
│   ├── dcp-rest-server/         # Go REST server
│   └── java-client/             # Java REST client
├── docker/
│   ├── Dockerfile
│   ├── build.sh
│   ├── entrypoint.sh
│   └── spark-defaults.conf
├── spark-runner/
│   ├── go.mod
│   ├── runner.go
│   └── runner_test.go
├── spark-gateway/
│   └── gateway.go
├── testcontainers/
│   ├── build.gradle.kts
│   └── src/
└── dcp-example/
    └── manifest.yaml
```

## Integration with Pedregal

This framework mirrors the production Pedregal Spark Platform:

1. **DCP REST API** → Primary entry point for external services
2. **Spark Runner** → Core Submit/Check/Cancel primitives
3. **Spark Gateway** → Routes to compute backend (SK8 or EMR)
4. **SK8/Docker** → Executes Spark applications
5. **Spark Connect** → Enables interactive sessions (port 15002)

For testing, Docker replaces SK8 while maintaining the same interfaces.

## When to Use Each Entry Point

| Entry Point | Use Case |
|-------------|----------|
| DCP REST API | External services, CI/CD pipelines, programmatic job submission |
| DCP Manifest | GitOps workflows, declarative job definitions |
| Spark Runner gRPC | Internal Pedregal graphs, custom orchestrators |
| TestContainers | Unit/integration testing, local development |
