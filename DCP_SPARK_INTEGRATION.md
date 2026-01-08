# DCP Playground to Spark on K8s Integration

This document provides a complete end-to-end example of how DCP Playgrounds integrate with Spark on OSS (SK8), including request flows, isolation strategies, and code examples across all environments.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                     DCP PLAYGROUND → SPARK ON K8S FLOW                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                         DCP PLAYGROUND                                         │  │
│  │                                                                               │  │
│  │   User writes manifest.yaml                                                   │  │
│  │          │                                                                    │  │
│  │          ▼                                                                    │  │
│  │   ┌─────────────────┐                                                         │  │
│  │   │   DCP Plugin    │  Validates manifest, creates primitives                 │  │
│  │   │   (CoreETL)     │                                                         │  │
│  │   └────────┬────────┘                                                         │  │
│  │            │                                                                  │  │
│  │            ▼                                                                  │  │
│  │   ┌─────────────────┐                                                         │  │
│  │   │   Playground    │  "Run" button triggers sandbox execution                │  │
│  │   │   Sandbox       │                                                         │  │
│  │   └────────┬────────┘                                                         │  │
│  │            │                                                                  │  │
│  └────────────┼──────────────────────────────────────────────────────────────────┘  │
│               │                                                                     │
│               ▼                                                                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                         CONTROL PLANE                                          │  │
│  │                                                                               │  │
│  │   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │  │
│  │   │  Spark Runner   │───▶│  Spark Gateway  │───▶│  Cluster Proxy  │          │  │
│  │   │                 │    │                 │    │                 │          │  │
│  │   │ • Submit()      │    │ • Auth (Okta)   │    │ • Route to K8s  │          │  │
│  │   │ • Check()       │    │ • Route by env  │    │ • Load balance  │          │  │
│  │   │ • Cancel()      │    │ • Hot clusters  │    │ • HA failover   │          │  │
│  │   └─────────────────┘    └─────────────────┘    └────────┬────────┘          │  │
│  │                                                          │                    │  │
│  └──────────────────────────────────────────────────────────┼────────────────────┘  │
│                                                             │                       │
│                                                             ▼                       │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                         EXECUTION PLANE (K8s)                                  │  │
│  │                                                                               │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐ │  │
│  │   │                    Environment Namespace                                 │ │  │
│  │   │                                                                         │ │  │
│  │   │   local:   sjns-playground-local     (ephemeral, single-user)          │ │  │
│  │   │   test:    sjns-playground-test      (shared, CI/CD)                   │ │  │
│  │   │   staging: sjns-<team>-staging       (pre-prod validation)             │ │  │
│  │   │   prod:    sjns-<team>-prod          (production workloads)            │ │  │
│  │   │                                                                         │ │  │
│  │   │   ┌─────────────────────────────────────────────────────────────────┐   │ │  │
│  │   │   │                      SPARK APPLICATION                          │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │   │ │  │
│  │   │   │   │   Driver    │    │  Executor 1 │    │  Executor N │        │   │ │  │
│  │   │   │   │   Pod       │    │    Pod      │    │    Pod      │        │   │ │  │
│  │   │   │   │             │    │             │    │             │        │   │ │  │
│  │   │   │   │ spark:3.5   │    │ spark:3.5   │    │ spark:3.5   │        │   │ │  │
│  │   │   │   │ coreetl:2.7 │    │             │    │             │        │   │ │  │
│  │   │   │   └─────────────┘    └─────────────┘    └─────────────┘        │   │ │  │
│  │   │   │                                                                 │   │ │  │
│  │   │   └─────────────────────────────────────────────────────────────────┘   │ │  │
│  │   └─────────────────────────────────────────────────────────────────────────┘ │  │
│  │                                                                               │  │
│  └───────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

## Docker Image Strategy

### Image Registry

```
doordash-docker.jfrog.io/spark-platform/<spark-version>-<variant>:<tag>

Examples:
├── spark-platform/3.5-oss:latest           # OSS Spark 3.5
├── spark-platform/3.5-oss:20240115         # Dated build
├── spark-platform/3.5-oss-coreetl:2.7.0    # With CoreETL
├── spark-platform/4.0-oss:latest           # OSS Spark 4.0
└── spark-platform/3.5-dbr:14.3             # DBR compatible (fallback)
```

### Image Layers

```dockerfile
# Layer 1: Base
FROM eclipse-temurin:17-jre-jammy

# Layer 2: Spark OSS Runtime
ENV SPARK_VERSION=3.5.0
COPY --from=spark-builder /opt/spark /opt/spark
COPY --from=spark-builder /opt/spark/jars/spark-connect*.jar /opt/spark/jars/

# Layer 3: Connectors
COPY connectors/iceberg-spark-runtime-3.5_2.12-1.4.2.jar /opt/spark/jars/
COPY connectors/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY connectors/unity-catalog-spark-0.1.0.jar /opt/spark/jars/

# Layer 4: CoreETL (optional, for batch jobs)
ARG COREETL_VERSION=2.7.0
COPY coreetl/core-etl-driver-bundle-${COREETL_VERSION}.jar /opt/spark/jars/

# Layer 5: Observability
COPY otel/opentelemetry-javaagent.jar /opt/spark/jars/
COPY fluent-bit/fluent-bit.conf /etc/fluent-bit/

# Entrypoint
COPY entrypoint.sh /opt/spark/
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
```

### Environment-Specific Image Selection

| Environment | Image | Reason |
|-------------|-------|--------|
| local | `spark-platform/3.5-oss:latest` | Fast iteration, latest features |
| test | `spark-platform/3.5-oss:20240115` | Reproducible builds |
| staging | `spark-platform/3.5-oss-coreetl:2.7.0` | Match prod config |
| prod | `spark-platform/3.5-oss-coreetl:2.7.0` | Stable, tested |

---

## Environment Isolation

### Namespace Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         ENVIRONMENT ISOLATION                                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  LOCAL (Developer Sandbox)                                                          │
│  ─────────────────────────                                                          │
│  Namespace: sjns-playground-local-{user-id}                                         │
│  Cluster:   dev-spark-cluster (us-west-2)                                           │
│  Catalog:   pedregal-dev                                                            │
│  Data:      Sample/synthetic data only                                              │
│  TTL:       4 hours (auto-cleanup)                                                  │
│  Resources: driver=2c/4g, executor=2c/4g, max=2 executors                           │
│                                                                                     │
│  TEST (CI/CD)                                                                       │
│  ────────────                                                                       │
│  Namespace: sjns-playground-test                                                    │
│  Cluster:   test-spark-cluster (us-west-2)                                          │
│  Catalog:   pedregal-test                                                           │
│  Data:      Test fixtures, anonymized samples                                       │
│  TTL:       1 hour (per job)                                                        │
│  Resources: driver=2c/4g, executor=4c/8g, max=5 executors                           │
│                                                                                     │
│  STAGING (Pre-prod)                                                                 │
│  ──────────────────                                                                 │
│  Namespace: sjns-{team}-staging                                                     │
│  Cluster:   staging-spark-cluster (us-west-2)                                       │
│  Catalog:   pedregal-staging                                                        │
│  Data:      Staging tables (subset of prod)                                         │
│  TTL:       24 hours                                                                │
│  Resources: driver=4c/8g, executor=4c/8g, max=10 executors                          │
│                                                                                     │
│  PROD (Production)                                                                  │
│  ─────────────────                                                                  │
│  Namespace: sjns-{team}-prod                                                        │
│  Cluster:   prod-spark-cluster-{1,2,3} (us-west-2, us-east-1, eu-west-1)           │
│  Catalog:   pedregal-prod                                                           │
│  Data:      Production tables                                                       │
│  TTL:       Job completion + 1 hour                                                 │
│  Resources: driver=4c/16g, executor=8c/32g, max=100 executors                       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Catalog Configuration by Environment

```yaml
# Environment-specific Unity Catalog endpoints
catalogs:
  local:
    endpoint: https://unity-catalog-dev.doordash.team
    catalog_name: pedregal_dev
    warehouses:
      - s3://doordash-lakehouse-dev/warehouse

  test:
    endpoint: https://unity-catalog-test.doordash.team
    catalog_name: pedregal_test
    warehouses:
      - s3://doordash-lakehouse-test/warehouse

  staging:
    endpoint: https://unity-catalog-staging.doordash.team
    catalog_name: pedregal_staging
    warehouses:
      - s3://doordash-lakehouse-staging/warehouse

  prod:
    endpoint: https://unity-catalog.doordash.team
    catalog_name: pedregal
    warehouses:
      - s3://doordash-lakehouse/warehouse
```

---

## Complete Code Examples

### 1. DCP Manifest (manifest.yaml)

```yaml
# manifest.yaml - DCP Spark Job Definition
apiVersion: dcp.pedregal.doordash.com/v1
kind: SparkJob
metadata:
  name: user-features-daily
  namespace: data-platform
  team: feature-engineering
  owner: feature-eng@doordash.com
  labels:
    cost-center: ml-platform
    criticality: high

spec:
  # Spark version and image
  engine:
    type: spark-oss           # spark-oss | spark-dbr
    version: "3.5"
    image: spark-platform/3.5-oss-coreetl:2.7.0

  # Job type
  jobType: batch              # batch | streaming

  # Resource configuration
  resources:
    driver:
      cores: 4
      memory: "8g"
    executor:
      cores: 4
      memory: "8g"
      instances: 5
      minInstances: 1         # DRA min
      maxInstances: 20        # DRA max

  # CoreETL job specification
  coreEtl:
    version: "2.7.0"
    mainClass: com.doordash.coreetl.spark.Main
    jobSpec:
      sources:
        - name: raw_events
          type: iceberg
          catalog: pedregal
          database: raw
          table: events
          filter: "ds = '{{ ds }}'"

      transformations:
        - name: aggregate_user_features
          type: sql
          query: |
            SELECT
              user_id,
              COUNT(*) as total_events,
              COUNT(DISTINCT DATE(event_timestamp)) as active_days,
              SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
              SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
              SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
            FROM raw_events
            WHERE user_id IS NOT NULL
            GROUP BY user_id

      sinks:
        - name: user_features
          type: iceberg
          catalog: pedregal
          database: feature_store
          table: user_features_daily
          mode: overwrite
          partitionBy: ["ds"]

  # Orchestration (for scheduled jobs)
  orchestration:
    schedule: "0 6 * * *"     # Daily at 6 AM UTC
    startDate: "2024-01-01"
    retryPolicy:
      maxRetries: 3
      retryInterval: 300      # 5 minutes
    timeout: 3600             # 1 hour

  # Environment overrides
  environments:
    local:
      resources:
        executor:
          instances: 2
          maxInstances: 2
      coreEtl:
        jobSpec:
          sources:
            - name: raw_events
              filter: "ds = '2024-01-01' AND user_id < 1000"  # Sample data

    test:
      resources:
        executor:
          instances: 2
          maxInstances: 5

    staging:
      resources:
        executor:
          instances: 5
          maxInstances: 10

    prod:
      resources:
        executor:
          instances: 5
          maxInstances: 20
```

### 2. DCP Plugin Processing

```go
// dcp-plugin/spark_plugin.go
package spark

import (
    "context"
    "fmt"

    dcpv1 "github.com/doordash/dcp/api/v1"
    sparkv1 "github.com/doordash/spark-runner/api/v1"
)

// SparkPlugin processes SparkJob manifests
type SparkPlugin struct {
    sparkRunner sparkv1.SparkRunnerClient
    config      *PluginConfig
}

// ProcessManifest converts DCP manifest to Spark primitives
func (p *SparkPlugin) ProcessManifest(ctx context.Context, manifest *dcpv1.SparkJob) (*ProcessResult, error) {
    env := p.config.Environment

    // Resolve environment-specific overrides
    resolvedSpec := p.resolveEnvironment(manifest.Spec, env)

    // Build JobSpec for Spark Runner
    jobSpec := &sparkv1.JobSpec{
        Name:        manifest.Metadata.Name,
        Team:        manifest.Metadata.Team,
        Owner:       manifest.Metadata.Owner,
        Environment: env,

        Engine: &sparkv1.EngineSpec{
            Type:    resolvedSpec.Engine.Type,
            Version: resolvedSpec.Engine.Version,
            Image:   p.resolveImage(resolvedSpec.Engine, env),
        },

        Resources: &sparkv1.ResourceSpec{
            Driver: &sparkv1.PodResources{
                Cores:  resolvedSpec.Resources.Driver.Cores,
                Memory: resolvedSpec.Resources.Driver.Memory,
            },
            Executor: &sparkv1.PodResources{
                Cores:        resolvedSpec.Resources.Executor.Cores,
                Memory:       resolvedSpec.Resources.Executor.Memory,
                Instances:    resolvedSpec.Resources.Executor.Instances,
                MinInstances: resolvedSpec.Resources.Executor.MinInstances,
                MaxInstances: resolvedSpec.Resources.Executor.MaxInstances,
            },
        },

        SparkConf: p.buildSparkConf(resolvedSpec, env),

        CoreEtl: &sparkv1.CoreEtlSpec{
            Version:   resolvedSpec.CoreEtl.Version,
            MainClass: resolvedSpec.CoreEtl.MainClass,
            JobSpec:   resolvedSpec.CoreEtl.JobSpec,
        },
    }

    // Create primitives based on job type
    var primitives []dcpv1.Primitive

    if manifest.Spec.JobType == "batch" && manifest.Spec.Orchestration != nil {
        // Create Orchestrator primitive (Airflow DAG)
        primitives = append(primitives, &dcpv1.OrchestratorPrimitive{
            Schedule:    manifest.Spec.Orchestration.Schedule,
            RetryPolicy: manifest.Spec.Orchestration.RetryPolicy,
            JobSpec:     jobSpec,
        })
    } else if manifest.Spec.JobType == "streaming" {
        // Create ContinuousJob primitive
        primitives = append(primitives, &dcpv1.ContinuousJobPrimitive{
            JobSpec: jobSpec,
        })
    }

    return &ProcessResult{
        JobSpec:    jobSpec,
        Primitives: primitives,
    }, nil
}

// resolveImage determines the Docker image based on environment
func (p *SparkPlugin) resolveImage(engine *dcpv1.EngineSpec, env string) string {
    registry := "doordash-docker.jfrog.io"

    if engine.Image != "" {
        return fmt.Sprintf("%s/%s", registry, engine.Image)
    }

    // Default image selection
    base := fmt.Sprintf("spark-platform/%s-oss", engine.Version)

    switch env {
    case "local":
        return fmt.Sprintf("%s/%s:latest", registry, base)
    case "test":
        return fmt.Sprintf("%s/%s:%s", registry, base, p.config.ImageTag)
    case "staging", "prod":
        return fmt.Sprintf("%s/%s-coreetl:2.7.0", registry, base)
    default:
        return fmt.Sprintf("%s/%s:latest", registry, base)
    }
}

// buildSparkConf generates environment-specific Spark configuration
func (p *SparkPlugin) buildSparkConf(spec *dcpv1.SparkJobSpec, env string) map[string]string {
    conf := map[string]string{
        // Spark Connect
        "spark.connect.grpc.binding.port": "15002",

        // Dynamic Resource Allocation
        "spark.dynamicAllocation.enabled":           "true",
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",

        // Event logging for Spark History Server
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir":     fmt.Sprintf("s3://spark-event-logs-%s/", env),

        // Kubernetes
        "spark.kubernetes.container.image.pullPolicy": "Always",
        "spark.kubernetes.executor.deleteOnTermination": "true",
    }

    // Unity Catalog configuration by environment
    catalogConfig := p.config.Catalogs[env]
    conf["spark.sql.catalog.pedregal"] = "org.apache.iceberg.spark.SparkCatalog"
    conf["spark.sql.catalog.pedregal.type"] = "rest"
    conf["spark.sql.catalog.pedregal.uri"] = catalogConfig.Endpoint
    conf["spark.sql.catalog.pedregal.warehouse"] = catalogConfig.Warehouses[0]
    conf["spark.sql.defaultCatalog"] = "pedregal"

    // Environment-specific settings
    switch env {
    case "local":
        conf["spark.sql.shuffle.partitions"] = "4"
        conf["spark.executor.instances"] = "2"
    case "test":
        conf["spark.sql.shuffle.partitions"] = "10"
    case "staging":
        conf["spark.sql.shuffle.partitions"] = "100"
    case "prod":
        conf["spark.sql.shuffle.partitions"] = "200"
        conf["spark.sql.adaptive.enabled"] = "true"
    }

    return conf
}
```

### 3. DCP Playground Sandbox Execution

```python
# dcp-playground/sandbox_executor.py
"""
DCP Playground Sandbox - Executes Spark jobs in isolated environments
"""

import os
import uuid
from dataclasses import dataclass
from typing import Optional
import grpc

from spark_runner_pb2 import SubmitRequest, CheckRequest, CancelRequest
from spark_runner_pb2_grpc import SparkRunnerStub


@dataclass
class SandboxConfig:
    """Configuration for sandbox execution"""
    environment: str  # local, test, staging, prod
    user_id: str
    team: str
    timeout_seconds: int = 3600

    @property
    def namespace(self) -> str:
        if self.environment == "local":
            return f"sjns-playground-local-{self.user_id}"
        elif self.environment == "test":
            return "sjns-playground-test"
        else:
            return f"sjns-{self.team}-{self.environment}"


class PlaygroundSandbox:
    """
    Executes DCP manifests in sandbox environment.
    Used by DCP Playground UI for "Run" button.
    """

    def __init__(self, spark_runner_endpoint: str):
        self.channel = grpc.insecure_channel(spark_runner_endpoint)
        self.spark_runner = SparkRunnerStub(self.channel)

    def execute(self, manifest: dict, config: SandboxConfig) -> "ExecutionResult":
        """
        Execute a DCP manifest in the sandbox.

        Args:
            manifest: Parsed DCP manifest (from YAML)
            config: Sandbox configuration

        Returns:
            ExecutionResult with status and logs
        """
        # Generate idempotency key for this execution
        idempotency_key = f"playground-{config.user_id}-{uuid.uuid4()}"

        # Build job spec with environment overrides
        job_spec = self._build_job_spec(manifest, config)

        # Submit to Spark Runner
        submit_response = self.spark_runner.Submit(SubmitRequest(
            job_spec=job_spec,
            idempotency_key=idempotency_key,
            namespace=config.namespace,
            environment=config.environment,
        ))

        execution_id = submit_response.execution_id

        # Poll for completion
        return self._wait_for_completion(
            execution_id,
            timeout=config.timeout_seconds
        )

    def _build_job_spec(self, manifest: dict, config: SandboxConfig) -> dict:
        """Build job spec with environment-specific overrides"""
        spec = manifest.get("spec", {})

        # Apply environment overrides
        env_overrides = spec.get("environments", {}).get(config.environment, {})

        # Merge resources
        resources = spec.get("resources", {})
        if "resources" in env_overrides:
            resources = {**resources, **env_overrides["resources"]}

        # Select image based on environment
        image = self._select_image(spec.get("engine", {}), config.environment)

        # Build Spark configuration
        spark_conf = self._build_spark_conf(config)

        return {
            "name": manifest["metadata"]["name"],
            "team": manifest["metadata"]["team"],
            "owner": manifest["metadata"]["owner"],
            "environment": config.environment,
            "engine": {
                "type": spec["engine"]["type"],
                "version": spec["engine"]["version"],
                "image": image,
            },
            "resources": resources,
            "sparkConf": spark_conf,
            "coreEtl": spec.get("coreEtl"),
        }

    def _select_image(self, engine: dict, environment: str) -> str:
        """Select Docker image based on environment"""
        registry = "doordash-docker.jfrog.io"
        version = engine.get("version", "3.5")

        image_map = {
            "local": f"{registry}/spark-platform/{version}-oss:latest",
            "test": f"{registry}/spark-platform/{version}-oss:latest",
            "staging": f"{registry}/spark-platform/{version}-oss-coreetl:2.7.0",
            "prod": f"{registry}/spark-platform/{version}-oss-coreetl:2.7.0",
        }

        return engine.get("image") or image_map.get(environment, image_map["local"])

    def _build_spark_conf(self, config: SandboxConfig) -> dict:
        """Build environment-specific Spark configuration"""
        catalog_endpoints = {
            "local": "https://unity-catalog-dev.doordash.team",
            "test": "https://unity-catalog-test.doordash.team",
            "staging": "https://unity-catalog-staging.doordash.team",
            "prod": "https://unity-catalog.doordash.team",
        }

        warehouses = {
            "local": "s3://doordash-lakehouse-dev/warehouse",
            "test": "s3://doordash-lakehouse-test/warehouse",
            "staging": "s3://doordash-lakehouse-staging/warehouse",
            "prod": "s3://doordash-lakehouse/warehouse",
        }

        return {
            # Unity Catalog
            "spark.sql.catalog.pedregal": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.pedregal.type": "rest",
            "spark.sql.catalog.pedregal.uri": catalog_endpoints[config.environment],
            "spark.sql.catalog.pedregal.warehouse": warehouses[config.environment],
            "spark.sql.defaultCatalog": "pedregal",

            # Spark Connect
            "spark.connect.grpc.binding.port": "15002",

            # Environment-specific tuning
            "spark.sql.shuffle.partitions": "4" if config.environment == "local" else "100",
            "spark.dynamicAllocation.enabled": "true",

            # Observability
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": f"s3://spark-event-logs-{config.environment}/",
        }

    def _wait_for_completion(self, execution_id: str, timeout: int) -> "ExecutionResult":
        """Poll Spark Runner for job completion"""
        import time

        start_time = time.time()

        while time.time() - start_time < timeout:
            response = self.spark_runner.Check(CheckRequest(
                execution_id=execution_id
            ))

            if response.state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return ExecutionResult(
                    execution_id=execution_id,
                    state=response.state,
                    spark_ui_url=response.spark_ui_url,
                    logs_url=response.logs_url,
                    duration_seconds=time.time() - start_time,
                )

            time.sleep(5)  # Poll every 5 seconds

        # Timeout - cancel the job
        self.spark_runner.Cancel(CancelRequest(execution_id=execution_id))

        return ExecutionResult(
            execution_id=execution_id,
            state="TIMEOUT",
            duration_seconds=timeout,
        )


@dataclass
class ExecutionResult:
    execution_id: str
    state: str
    spark_ui_url: Optional[str] = None
    logs_url: Optional[str] = None
    duration_seconds: float = 0
    error_message: Optional[str] = None
```

### 4. Spark Runner Service

```go
// spark-runner/runner.go
package runner

import (
    "context"
    "fmt"
    "time"

    "github.com/google/uuid"
    sparkv1 "github.com/doordash/spark-runner/api/v1"
    "github.com/doordash/spark-runner/gateway"
    "github.com/doordash/spark-runner/taulu"
)

// SparkRunner implements the Spark Runner graph
type SparkRunner struct {
    gateway     gateway.SparkGatewayClient
    taulu       taulu.Client
    aps         APSClient        // Auto Parameter Selection
    ar          ARClient         // Auto Retry/Remediation
}

// Submit creates a new Spark execution
func (r *SparkRunner) Submit(ctx context.Context, req *sparkv1.SubmitRequest) (*sparkv1.SubmitResponse, error) {
    // Generate execution ID
    executionID := uuid.New().String()

    // Check idempotency
    if existing, err := r.taulu.GetExecutionByIdempotencyKey(ctx, req.IdempotencyKey); err == nil {
        return &sparkv1.SubmitResponse{
            ExecutionId:  existing.ExecutionID,
            VendorRunId:  existing.VendorRunID,
            Status:       existing.State,
        }, nil
    }

    // Get APS recommendations (if enabled)
    if req.JobSpec.EnableAPS {
        recommendations, err := r.aps.GetRecommendations(ctx, req.JobSpec)
        if err == nil {
            req.JobSpec = r.applyRecommendations(req.JobSpec, recommendations)
        }
    }

    // Build SparkApplication CRD
    sparkApp := r.buildSparkApplication(req, executionID)

    // Submit to Spark Gateway
    gatewayResp, err := r.gateway.SubmitJob(ctx, &gateway.SubmitRequest{
        SparkApplication: sparkApp,
        Namespace:        req.Namespace,
        Environment:      req.Environment,
    })
    if err != nil {
        return nil, fmt.Errorf("gateway submit failed: %w", err)
    }

    // Write ExecutionSnapshot to Taulu
    execution := &taulu.Execution{
        ExecutionID:    executionID,
        IdempotencyKey: req.IdempotencyKey,
        JobPath:        req.JobSpec.Name,
        Team:           req.JobSpec.Team,
        Owner:          req.JobSpec.Owner,
        Environment:    req.Environment,
        State:          "SUBMITTED",
        Vendor:         "SK8",
        VendorRunID:    gatewayResp.VendorRunID,
        StartTime:      time.Now(),
    }

    if err := r.taulu.WriteExecution(ctx, execution); err != nil {
        // Log but don't fail - job is already submitted
        log.Warn("Failed to write execution to Taulu", "error", err)
    }

    // Emit SUBMIT event
    r.taulu.EmitEvent(ctx, &taulu.ExecutionEvent{
        ExecutionID: executionID,
        EventType:   "SUBMIT",
        Timestamp:   time.Now(),
        Payload:     req.JobSpec,
    })

    return &sparkv1.SubmitResponse{
        ExecutionId:  executionID,
        VendorRunId:  gatewayResp.VendorRunID,
        Status:       "SUBMITTED",
        SparkUIUrl:   gatewayResp.SparkUIUrl,
    }, nil
}

// Check retrieves execution state
func (r *SparkRunner) Check(ctx context.Context, req *sparkv1.CheckRequest) (*sparkv1.CheckResponse, error) {
    // Get execution from Taulu
    execution, err := r.taulu.GetExecution(ctx, req.ExecutionId)
    if err != nil {
        return nil, fmt.Errorf("execution not found: %w", err)
    }

    // If terminal state, return cached result
    if isTerminalState(execution.State) {
        return &sparkv1.CheckResponse{
            ExecutionId: execution.ExecutionID,
            State:       execution.State,
            SparkUIUrl:  execution.SparkUIUrl,
            LogsUrl:     execution.LogsUrl,
            StartTime:   execution.StartTime,
            EndTime:     execution.EndTime,
        }, nil
    }

    // Query Spark Gateway for current state
    gatewayResp, err := r.gateway.GetJobStatus(ctx, &gateway.StatusRequest{
        VendorRunID: execution.VendorRunID,
        Environment: execution.Environment,
    })
    if err != nil {
        return nil, fmt.Errorf("gateway status failed: %w", err)
    }

    // Update execution if state changed
    if gatewayResp.State != execution.State {
        execution.State = gatewayResp.State
        execution.SparkUIUrl = gatewayResp.SparkUIUrl
        execution.LogsUrl = gatewayResp.LogsUrl

        if isTerminalState(gatewayResp.State) {
            execution.EndTime = time.Now()
        }

        r.taulu.WriteExecution(ctx, execution)

        // Emit state change event
        r.taulu.EmitEvent(ctx, &taulu.ExecutionEvent{
            ExecutionID: execution.ExecutionID,
            EventType:   gatewayResp.State,
            Timestamp:   time.Now(),
        })
    }

    return &sparkv1.CheckResponse{
        ExecutionId: execution.ExecutionID,
        State:       execution.State,
        SparkUIUrl:  execution.SparkUIUrl,
        LogsUrl:     execution.LogsUrl,
        StartTime:   execution.StartTime,
        EndTime:     execution.EndTime,
    }, nil
}

// Cancel stops a running execution
func (r *SparkRunner) Cancel(ctx context.Context, req *sparkv1.CancelRequest) (*sparkv1.CancelResponse, error) {
    execution, err := r.taulu.GetExecution(ctx, req.ExecutionId)
    if err != nil {
        return nil, fmt.Errorf("execution not found: %w", err)
    }

    // Cancel via Spark Gateway
    _, err = r.gateway.CancelJob(ctx, &gateway.CancelRequest{
        VendorRunID: execution.VendorRunID,
        Environment: execution.Environment,
    })
    if err != nil {
        return nil, fmt.Errorf("gateway cancel failed: %w", err)
    }

    // Update execution state
    execution.State = "CANCELLED"
    execution.EndTime = time.Now()
    r.taulu.WriteExecution(ctx, execution)

    // Emit CANCEL event
    r.taulu.EmitEvent(ctx, &taulu.ExecutionEvent{
        ExecutionID: execution.ExecutionID,
        EventType:   "CANCELLED",
        Timestamp:   time.Now(),
    })

    return &sparkv1.CancelResponse{
        Success: true,
    }, nil
}

// buildSparkApplication creates K8s SparkApplication CRD
func (r *SparkRunner) buildSparkApplication(req *sparkv1.SubmitRequest, executionID string) *SparkApplication {
    spec := req.JobSpec

    return &SparkApplication{
        APIVersion: "sparkoperator.k8s.io/v1beta2",
        Kind:       "SparkApplication",
        Metadata: Metadata{
            Name:      fmt.Sprintf("%s-%s", spec.Name, executionID[:8]),
            Namespace: req.Namespace,
            Labels: map[string]string{
                "spark-runner/execution-id": executionID,
                "spark-runner/job-name":     spec.Name,
                "spark-runner/team":         spec.Team,
                "spark-runner/environment":  req.Environment,
            },
        },
        Spec: SparkApplicationSpec{
            Type:             "Scala",
            Mode:             "cluster",
            Image:            spec.Engine.Image,
            ImagePullPolicy:  "Always",
            MainClass:        spec.CoreEtl.MainClass,
            MainApplicationFile: "local:///opt/spark/jars/core-etl-driver-bundle.jar",
            SparkVersion:     spec.Engine.Version,

            Driver: DriverSpec{
                Cores:          spec.Resources.Driver.Cores,
                Memory:         spec.Resources.Driver.Memory,
                ServiceAccount: "spark-driver",
                Labels: map[string]string{
                    "spark-role": "driver",
                },
            },

            Executor: ExecutorSpec{
                Cores:     spec.Resources.Executor.Cores,
                Memory:    spec.Resources.Executor.Memory,
                Instances: spec.Resources.Executor.Instances,
                Labels: map[string]string{
                    "spark-role": "executor",
                },
            },

            DynamicAllocation: &DynamicAllocationSpec{
                Enabled:          true,
                InitialExecutors: spec.Resources.Executor.Instances,
                MinExecutors:     spec.Resources.Executor.MinInstances,
                MaxExecutors:     spec.Resources.Executor.MaxInstances,
            },

            SparkConf: spec.SparkConf,

            RestartPolicy: RestartPolicy{
                Type:                       "OnFailure",
                OnFailureRetries:           3,
                OnFailureRetryInterval:     10,
                OnSubmissionFailureRetries: 3,
            },
        },
    }
}
```

### 5. Spark Gateway

```go
// spark-gateway/gateway.go
package gateway

import (
    "context"
    "fmt"

    "k8s.io/client-go/kubernetes"
    sparkoperator "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

// SparkGateway routes Spark jobs to appropriate clusters
type SparkGateway struct {
    clusterProxy  *ClusterProxy
    authenticator *OktaAuthenticator
    hotClusters   *HotClusterManager
}

// SubmitJob submits a SparkApplication to the appropriate cluster
func (g *SparkGateway) SubmitJob(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error) {
    // Authenticate request
    if err := g.authenticator.Authenticate(ctx); err != nil {
        return nil, fmt.Errorf("authentication failed: %w", err)
    }

    // Select target cluster based on environment
    cluster, err := g.clusterProxy.SelectCluster(ctx, req.Environment)
    if err != nil {
        return nil, fmt.Errorf("cluster selection failed: %w", err)
    }

    // Create SparkApplication in target cluster
    client := cluster.GetKubernetesClient()

    sparkApp := req.SparkApplication
    sparkApp.Metadata.Namespace = req.Namespace

    created, err := client.SparkApplications(req.Namespace).Create(ctx, sparkApp)
    if err != nil {
        return nil, fmt.Errorf("spark application creation failed: %w", err)
    }

    return &SubmitResponse{
        VendorRunID: string(created.UID),
        SparkUIUrl:  g.buildSparkUIUrl(cluster, req.Namespace, created.Name),
        LogsUrl:     g.buildLogsUrl(cluster, req.Namespace, created.Name),
    }, nil
}

// GetJobStatus retrieves current job status from cluster
func (g *SparkGateway) GetJobStatus(ctx context.Context, req *StatusRequest) (*StatusResponse, error) {
    cluster, err := g.clusterProxy.SelectCluster(ctx, req.Environment)
    if err != nil {
        return nil, err
    }

    client := cluster.GetKubernetesClient()

    sparkApp, err := client.SparkApplications(req.Namespace).Get(ctx, req.AppName)
    if err != nil {
        return nil, err
    }

    state := mapSparkAppState(sparkApp.Status.AppState.State)

    return &StatusResponse{
        State:      state,
        SparkUIUrl: g.buildSparkUIUrl(cluster, req.Namespace, sparkApp.Name),
        LogsUrl:    g.buildLogsUrl(cluster, req.Namespace, sparkApp.Name),
    }, nil
}

// ClusterProxy manages multi-cluster routing
type ClusterProxy struct {
    clusters map[string][]*Cluster  // environment -> clusters
}

// SelectCluster picks the best cluster for the environment
func (p *ClusterProxy) SelectCluster(ctx context.Context, environment string) (*Cluster, error) {
    clusters, ok := p.clusters[environment]
    if !ok || len(clusters) == 0 {
        return nil, fmt.Errorf("no clusters available for environment: %s", environment)
    }

    // Simple round-robin with health check
    for _, cluster := range clusters {
        if cluster.IsHealthy() {
            return cluster, nil
        }
    }

    return nil, fmt.Errorf("no healthy clusters for environment: %s", environment)
}

// Environment to cluster mapping
func NewClusterProxy(config *Config) *ClusterProxy {
    return &ClusterProxy{
        clusters: map[string][]*Cluster{
            "local": {
                {Name: "dev-spark-usw2", Region: "us-west-2", Endpoint: "..."},
            },
            "test": {
                {Name: "test-spark-usw2", Region: "us-west-2", Endpoint: "..."},
            },
            "staging": {
                {Name: "staging-spark-usw2", Region: "us-west-2", Endpoint: "..."},
            },
            "prod": {
                {Name: "prod-spark-usw2-1", Region: "us-west-2", Endpoint: "..."},
                {Name: "prod-spark-usw2-2", Region: "us-west-2", Endpoint: "..."},
                {Name: "prod-spark-use1-1", Region: "us-east-1", Endpoint: "..."},
                {Name: "prod-spark-euw1-1", Region: "eu-west-1", Endpoint: "..."},
            },
        },
    }
}
```

### 6. Interactive Spark Connect Session (DCP Sandbox)

```python
# dcp-playground/interactive_session.py
"""
Interactive Spark Connect session for DCP Playground.
Provides notebook-like experience through Spark Gateway.
"""

from pyspark.sql import SparkSession
from typing import Optional
import os


class PlaygroundSession:
    """
    Manages interactive Spark Connect sessions for DCP Playground.
    """

    def __init__(
        self,
        environment: str = "local",
        team: str = "playground",
        user_id: Optional[str] = None,
    ):
        self.environment = environment
        self.team = team
        self.user_id = user_id or os.getenv("USER", "anonymous")
        self._spark: Optional[SparkSession] = None

    @property
    def gateway_endpoint(self) -> str:
        """Get Spark Gateway endpoint for environment"""
        endpoints = {
            "local": "spark-gateway-dev.doordash.team",
            "test": "spark-gateway-test.doordash.team",
            "staging": "spark-gateway-staging.doordash.team",
            "prod": "spark-gateway.doordash.team",
        }
        return endpoints.get(self.environment, endpoints["local"])

    def get_spark(self) -> SparkSession:
        """
        Get or create a Spark session connected through Spark Gateway.
        """
        if self._spark is not None:
            return self._spark

        # Build connection URL
        # Spark Gateway handles routing to appropriate cluster
        connection_url = f"sc://{self.gateway_endpoint}:15002"

        # Add headers for routing
        headers = {
            "x-doordash-team": self.team,
            "x-doordash-environment": self.environment,
            "x-doordash-user": self.user_id,
        }

        # Create Spark session via Spark Connect
        builder = SparkSession.builder \
            .remote(connection_url) \
            .appName(f"playground-{self.user_id}-{self.environment}")

        # Add routing headers
        for key, value in headers.items():
            builder = builder.config(f"spark.connect.grpc.header.{key}", value)

        self._spark = builder.getOrCreate()

        return self._spark

    def close(self):
        """Close the Spark session"""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None


# Example usage in DCP Playground notebook
def run_interactive_query():
    """Example: Running queries in DCP Playground"""

    # Create session for local environment
    session = PlaygroundSession(
        environment="local",
        team="feature-engineering",
    )

    spark = session.get_spark()

    try:
        # Unity Catalog is pre-configured
        # Query tables directly
        spark.sql("SHOW DATABASES IN pedregal").show()

        # Run a sample query
        df = spark.sql("""
            SELECT
                user_id,
                COUNT(*) as event_count
            FROM pedregal.raw.events
            WHERE ds = '2024-01-01'
            GROUP BY user_id
            ORDER BY event_count DESC
            LIMIT 10
        """)

        df.show()

        # Convert to Pandas for visualization
        pdf = df.toPandas()
        return pdf

    finally:
        session.close()


# Example: Running in different environments
def run_in_staging():
    """Example: Validating in staging before prod"""

    session = PlaygroundSession(
        environment="staging",
        team="feature-engineering",
    )

    spark = session.get_spark()

    try:
        # Staging uses staging Unity Catalog
        # Same queries, different data
        result = spark.sql("""
            SELECT COUNT(*) as total_events
            FROM pedregal.raw.events
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        print(f"Total events in staging (last 7 days): {result[0]['total_events']}")

    finally:
        session.close()
```

### 7. CI/CD Integration (GitHub Actions)

```yaml
# .github/workflows/spark-job-test.yaml
name: Spark Job CI/CD

on:
  push:
    branches: [main]
    paths:
      - 'jobs/**'
      - 'manifests/**'
  pull_request:
    branches: [main]
    paths:
      - 'jobs/**'
      - 'manifests/**'

env:
  SPARK_CONNECT_ENDPOINT: spark-gateway-test.doordash.team:15002

jobs:
  validate-manifest:
    name: Validate DCP Manifest
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Validate manifest schema
        run: |
          pip install pyyaml jsonschema
          python scripts/validate_manifest.py manifests/*.yaml

  unit-tests:
    name: Unit Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pytest pyspark[connect]

      - name: Run unit tests (mocked Spark)
        run: |
          pytest tests/unit/ -v

  integration-tests:
    name: Integration Tests (Test Environment)
    runs-on: ubuntu-latest
    needs: [validate-manifest, unit-tests]
    environment: test

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pytest pyspark[connect] pandas

      - name: Run integration tests
        env:
          SPARK_CONNECT_TEAM: ${{ github.repository_owner }}
          SPARK_CONNECT_ENV: test
          SPARK_CONNECT_ENDPOINT: ${{ env.SPARK_CONNECT_ENDPOINT }}
        run: |
          pytest tests/integration/ -v --tb=short

      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results
          path: test-results.xml

  deploy-staging:
    name: Deploy to Staging
    runs-on: ubuntu-latest
    needs: integration-tests
    if: github.ref == 'refs/heads/main'
    environment: staging

    steps:
      - uses: actions/checkout@v4

      - name: Deploy manifest to staging
        env:
          DCP_API_TOKEN: ${{ secrets.DCP_API_TOKEN }}
          DCP_ENVIRONMENT: staging
        run: |
          # Deploy to DCP staging
          curl -X POST https://dcp-api-staging.doordash.team/api/v1/manifests \
            -H "Authorization: Bearer $DCP_API_TOKEN" \
            -H "Content-Type: application/yaml" \
            -d @manifests/user-features-daily.yaml

      - name: Run staging validation
        env:
          SPARK_CONNECT_TEAM: feature-engineering
          SPARK_CONNECT_ENV: staging
        run: |
          pytest tests/staging_validation.py -v

  deploy-prod:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: deploy-staging
    environment: production

    steps:
      - uses: actions/checkout@v4

      - name: Deploy manifest to production
        env:
          DCP_API_TOKEN: ${{ secrets.DCP_API_TOKEN_PROD }}
          DCP_ENVIRONMENT: prod
        run: |
          curl -X POST https://dcp-api.doordash.team/api/v1/manifests \
            -H "Authorization: Bearer $DCP_API_TOKEN" \
            -H "Content-Type: application/yaml" \
            -d @manifests/user-features-daily.yaml
```

### 8. TestContainers for Local Development

```java
// testcontainers/src/test/java/SparkOSSContainerTest.java
package com.doordash.spark.testing;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class SparkOSSContainerTest {

    private static final Network network = Network.newNetwork();

    // MinIO for S3-compatible storage
    @Container
    private static final MinIOContainer minio = new MinIOContainer("minio/minio:latest")
        .withNetwork(network)
        .withNetworkAliases("minio")
        .withUserName("minioadmin")
        .withPassword("minioadmin");

    // Spark OSS container with Spark Connect
    @Container
    private static final GenericContainer<?> spark = new GenericContainer<>(
        "doordash-docker.jfrog.io/spark-platform/3.5-oss:latest"
    )
        .withNetwork(network)
        .withNetworkAliases("spark")
        .withExposedPorts(15002, 4040)
        .withEnv("SPARK_MODE", "master")
        .withEnv("SPARK_CONNECT_ENABLED", "true")
        .withEnv("AWS_ACCESS_KEY_ID", "minioadmin")
        .withEnv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .withEnv("AWS_ENDPOINT_URL", "http://minio:9000")
        .withCommand("/opt/spark/sbin/start-connect-server.sh",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
            "--conf", "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog",
            "--conf", "spark.sql.catalog.local.type=hadoop",
            "--conf", "spark.sql.catalog.local.warehouse=s3a://warehouse/",
            "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true"
        );

    private SparkSession sparkSession;

    @BeforeEach
    void setUp() {
        String connectUrl = String.format("sc://localhost:%d",
            spark.getMappedPort(15002));

        sparkSession = SparkSession.builder()
            .remote(connectUrl)
            .appName("test")
            .getOrCreate();
    }

    @AfterEach
    void tearDown() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
    }

    @Test
    void testSparkConnectConnection() {
        // Verify connection works
        String version = sparkSession.version();
        assertThat(version).startsWith("3.5");
    }

    @Test
    void testCreateAndQueryTable() {
        // Create a simple table
        sparkSession.sql("""
            CREATE TABLE IF NOT EXISTS local.test.users (
                id LONG,
                name STRING,
                email STRING
            ) USING iceberg
        """);

        // Insert data
        sparkSession.sql("""
            INSERT INTO local.test.users VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com')
        """);

        // Query and verify
        Dataset<Row> result = sparkSession.sql("""
            SELECT * FROM local.test.users ORDER BY id
        """);

        assertThat(result.count()).isEqualTo(3);
        assertThat(result.first().getString(1)).isEqualTo("Alice");
    }

    @Test
    void testCoreETLTransformation() {
        // Simulate CoreETL transformation
        sparkSession.sql("""
            CREATE TABLE IF NOT EXISTS local.raw.events (
                user_id LONG,
                event_type STRING,
                event_timestamp TIMESTAMP
            ) USING iceberg
        """);

        // Insert test events
        sparkSession.sql("""
            INSERT INTO local.raw.events VALUES
            (1, 'view', TIMESTAMP '2024-01-01 10:00:00'),
            (1, 'click', TIMESTAMP '2024-01-01 10:01:00'),
            (1, 'purchase', TIMESTAMP '2024-01-01 10:02:00'),
            (2, 'view', TIMESTAMP '2024-01-01 11:00:00'),
            (2, 'view', TIMESTAMP '2024-01-01 11:05:00')
        """);

        // Run aggregation (like CoreETL would)
        Dataset<Row> userFeatures = sparkSession.sql("""
            SELECT
                user_id,
                COUNT(*) as total_events,
                SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
                SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
            FROM local.raw.events
            GROUP BY user_id
            ORDER BY user_id
        """);

        // Verify results
        assertThat(userFeatures.count()).isEqualTo(2);

        Row user1 = userFeatures.filter("user_id = 1").first();
        assertThat(user1.getLong(1)).isEqualTo(3);  // total_events
        assertThat(user1.getLong(2)).isEqualTo(1);  // views
        assertThat(user1.getLong(3)).isEqualTo(1);  // clicks
        assertThat(user1.getLong(4)).isEqualTo(1);  // purchases

        Row user2 = userFeatures.filter("user_id = 2").first();
        assertThat(user2.getLong(1)).isEqualTo(2);  // total_events
        assertThat(user2.getLong(2)).isEqualTo(2);  // views
    }
}
```

---

## Summary: Request Flow by Environment

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    REQUEST FLOW BY ENVIRONMENT                                       │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│  LOCAL (Developer)                                                                  │
│  ─────────────────                                                                  │
│  manifest.yaml                                                                      │
│       │                                                                             │
│       ▼                                                                             │
│  DCP Playground UI → "Run in Local"                                                 │
│       │                                                                             │
│       ▼                                                                             │
│  Spark Runner → Spark Gateway (dev)                                                 │
│       │                                                                             │
│       ▼                                                                             │
│  dev-spark-cluster / sjns-playground-local-{user}                                   │
│       │                                                                             │
│       ▼                                                                             │
│  Image: spark-platform/3.5-oss:latest                                               │
│  Catalog: pedregal-dev                                                              │
│  Data: Sample data (filtered)                                                       │
│  TTL: 4 hours                                                                       │
│                                                                                     │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                     │
│  TEST (CI/CD)                                                                       │
│  ────────────                                                                       │
│  GitHub PR → Integration Tests                                                      │
│       │                                                                             │
│       ▼                                                                             │
│  pyspark[connect] → spark-gateway-test:15002                                        │
│       │                                                                             │
│       ▼                                                                             │
│  test-spark-cluster / sjns-playground-test                                          │
│       │                                                                             │
│       ▼                                                                             │
│  Image: spark-platform/3.5-oss:{commit-sha}                                         │
│  Catalog: pedregal-test                                                             │
│  Data: Test fixtures                                                                │
│  TTL: 1 hour                                                                        │
│                                                                                     │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                     │
│  STAGING (Pre-prod)                                                                 │
│  ──────────────────                                                                 │
│  DCP Deploy → Staging                                                               │
│       │                                                                             │
│       ▼                                                                             │
│  Spark Runner → Spark Gateway (staging)                                             │
│       │                                                                             │
│       ▼                                                                             │
│  staging-spark-cluster / sjns-{team}-staging                                        │
│       │                                                                             │
│       ▼                                                                             │
│  Image: spark-platform/3.5-oss-coreetl:2.7.0                                        │
│  Catalog: pedregal-staging                                                          │
│  Data: Staging tables (subset)                                                      │
│  TTL: 24 hours                                                                      │
│                                                                                     │
│  ─────────────────────────────────────────────────────────────────────────────────  │
│                                                                                     │
│  PROD (Production)                                                                  │
│  ─────────────────                                                                  │
│  DCP Deploy → Prod + Airflow Schedule                                               │
│       │                                                                             │
│       ▼                                                                             │
│  Spark Runner → Spark Gateway (prod)                                                │
│       │                                                                             │
│       ▼                                                                             │
│  prod-spark-cluster-{1,2,3} / sjns-{team}-prod                                      │
│       │                                                                             │
│       ▼                                                                             │
│  Image: spark-platform/3.5-oss-coreetl:2.7.0                                        │
│  Catalog: pedregal-prod                                                             │
│  Data: Production tables                                                            │
│  TTL: Job completion + 1 hour                                                       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Differences: OSS vs DBR/EMR

| Aspect | Spark OSS (SK8) | DBR/EMR |
|--------|-----------------|---------|
| **Image** | `spark-platform/3.5-oss` | `spark-platform/3.5-dbr` |
| **Operator** | Apache SKO | Vendor-managed |
| **Startup** | ~30 seconds | 6-8 minutes |
| **Cost** | EC2 spot + K8s | Vendor markup |
| **Control** | Full | Limited |
| **Spark Connect** | Built-in | DBR only (14.x+) |
| **Unity Catalog** | REST API | Native (DBR) |

---

*Last updated: January 2025*
