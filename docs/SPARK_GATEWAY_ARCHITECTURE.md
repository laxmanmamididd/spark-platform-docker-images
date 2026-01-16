# Spark Gateway Architecture

This document explains the architecture behind Spark Gateway, including how clusters are managed, the interaction between components, and the job submission flow.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    CLIENT LAYER                                          │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│  grpcurl / SDK Client                                                                    │
│       │                                                                                  │
│       ▼ gRPC (port 50051)                                                               │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                  SPARK GATEWAY SERVICE                                   │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │  spark-gateway-grpc-server (namespace: spark-gateway)                             │   │
│  │  ├── SubmitSparkJob()   → Creates SparkApplication CRD                           │   │
│  │  ├── GetSparkJobStatus() → Reads SparkApplication status                         │   │
│  │  └── DeleteSparkJob()   → Deletes SparkApplication CRD                           │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│       │                                                                                  │
│       ▼ Kubernetes API (via Cluster Proxy)                                              │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                AUTHENTICATION LAYER                                      │
│  ┌─────────────────────┐      ┌─────────────────────┐                                   │
│  │  HashiCorp Vault    │ ──── │  Cluster Proxy      │                                   │
│  │  (Token Storage)    │      │  (K8s API Gateway)  │                                   │
│  │                     │      │                     │                                   │
│  │  AppRole Auth ──────│──────│  Bearer Token Auth  │                                   │
│  └─────────────────────┘      └─────────────────────┘                                   │
│       │                              │                                                   │
│       └──────────────────────────────┘                                                   │
│                     │                                                                    │
│                     ▼                                                                    │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                              KUBERNETES CLUSTER                                          │
│                        (dash-data-usw2-001-data-01)                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │  SPARK KUBERNETES OPERATOR (namespace: spark-kubernetes-operator)                 │   │
│  │  Image: apache/spark-kubernetes-operator:0.5.0                                    │   │
│  │                                                                                    │   │
│  │  WATCHES: SparkApplication CRDs across all namespaces                             │   │
│  │  CREATES: Driver pods, Executor pods                                              │   │
│  │  UPDATES: SparkApplication.status with pod info                                   │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
│       │                                                                                  │
│       │ Reconciliation Loop                                                             │
│       ▼                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐   │
│  │  TARGET NAMESPACE (e.g., sjns-data-control)                                       │   │
│  │  ┌────────────────────────┐    ┌────────────────────────┐                        │   │
│  │  │  SparkApplication CRD  │───▶│  Driver Pod            │                        │   │
│  │  │  (pi-test-e2e)         │    │  (pi-test-e2e-driver)  │                        │   │
│  │  └────────────────────────┘    └────────────────────────┘                        │   │
│  │                                         │                                         │   │
│  │                                         │ Spawns                                  │   │
│  │                                         ▼                                         │   │
│  │                                ┌────────────────────────┐                        │   │
│  │                                │  Executor Pods         │                        │   │
│  │                                │  (pi-test-e2e-exec-1)  │                        │   │
│  │                                │  (pi-test-e2e-exec-2)  │                        │   │
│  │                                └────────────────────────┘                        │   │
│  └──────────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Spark Gateway Service

**Location:** `spark-gateway` namespace
**Pods:** `spark-gateway-grpc-server-*`
**Port:** 50051 (gRPC)

**What it does:**
- Receives gRPC requests for job submission/status/deletion
- Authenticates to Kubernetes via Cluster Proxy + Vault tokens
- Creates/reads/deletes `SparkApplication` CRDs in the target namespace
- Does NOT run Spark itself - only creates Kubernetes resources

**Key operations:**
```go
// Creates SparkApplication CRD in Kubernetes
sparkApp := &unstructured.Unstructured{
    Object: map[string]interface{}{
        "apiVersion": "spark.apache.org/v1",
        "kind":       "SparkApplication",
        "metadata":   {...},
        "spec":       spec,  // driver, executor, image, sparkConf
    },
}
result, err := h.k8sClient.Resource(sparkApplicationGVR).
    Namespace(req.GetNamespace()).
    Create(ctx, sparkApp, metav1.CreateOptions{})
```

### 2. Cluster Proxy Service

**URL:** `https://dash-data-usw2-001-data-01.proxy.cluster.doordash.com`

**What it does:**
- Acts as a secure gateway to Kubernetes API
- Accepts Bearer token authentication (tokens stored in Vault)
- Routes requests to the target Kubernetes cluster
- Eliminates need for direct cluster access from spark-gateway

**Authentication Flow:**
```
spark-gateway
    │
    ├── 1. Load AppRole credentials from file
    │
    ├── 2. Authenticate to Vault via AppRole
    │
    ├── 3. Fetch K8s Bearer token from Vault path:
    │      "spark-gateway/cluster-proxy-service/token"
    │
    └── 4. Use token with Cluster Proxy URL
           Host: "https://...proxy.cluster.doordash.com"
           Authorization: Bearer <token>
```

### 3. Spark Kubernetes Operator

**Namespace:** `spark-kubernetes-operator`
**Image:** `apache/spark-kubernetes-operator:0.5.0`
**Replicas:** 1

**What it does:**
- Runs as a Kubernetes controller (reconciliation loop)
- **Watches** all `SparkApplication` CRDs across the cluster
- **Creates** Driver and Executor pods based on SparkApplication spec
- **Updates** SparkApplication status with pod info and state transitions
- **Manages** lifecycle (restart, cleanup based on TTL)

**State Machine:**
```
Submitted → DriverRequested → DriverStarted → DriverReady → RunningHealthy → Succeeded/Failed
                                                                                    │
                                                         ResourceReleased ◀─────────┘
                                                         (after TTL expires)
```

### 4. Spark Driver Pod

**Created by:** Spark Kubernetes Operator
**Naming:** `<app-name>-<attempt>-driver` (e.g., `pi-test-e2e-0-driver`)

**What it does:**
- Runs the Spark driver process (SparkContext)
- Coordinates job execution across executors
- Requests executor pods from Kubernetes via Spark's native K8s scheduler
- Reports status back (visible in SparkApplication.status)

### 5. Spark Executor Pods

**Created by:** Spark Driver (via Kubernetes scheduler)
**Naming:** `<app-name>-<hash>-exec-<id>`

**What it does:**
- Run Spark tasks assigned by the driver
- Process data in parallel
- Scale up/down based on `spark.dynamicAllocation.*` settings

## FAQ: Does Submitting a Job Spin Up a Cluster Automatically?

**NO - The Kubernetes cluster must already exist.**

When you submit a job:

| What Happens | What Does NOT Happen |
|--------------|---------------------|
| SparkApplication CRD is created | No new K8s cluster is provisioned |
| Operator detects CRD and creates Driver pod | No EC2/cloud instances are spun up |
| Driver requests Executor pods | No infrastructure automation runs |
| Pods run on existing K8s nodes | No cluster autoscaling by spark-gateway |

**However**, Kubernetes itself may scale:
- If the cluster has **Cluster Autoscaler** enabled, it may provision new nodes if there's not enough capacity for the driver/executor pods
- This is handled by Kubernetes, not Spark Gateway

## Prerequisites for a Job to Run

| Component | Must Be Running | Namespace |
|-----------|-----------------|-----------|
| Spark Gateway | Yes | `spark-gateway` |
| Spark Kubernetes Operator | Yes | `spark-kubernetes-operator` |
| SparkApplication CRD | Yes (Registered) | cluster-wide |
| Target namespace | Yes (Exists) | e.g., `sjns-data-control` |
| Service account (`spark`) | Yes (With RBAC) | target namespace |
| Container image | Yes (Accessible) | DockerHub/registry |

## Kubernetes Namespaces for Spark

```
spark-gateway              - Spark Gateway gRPC service
spark-kubernetes-operator  - Spark Operator controller
spark-jobs                 - One possible namespace for jobs
sjns-data-control          - Another namespace for jobs (Data Control team)
```

## Job Submission Flow

```
1. Client sends SubmitSparkJob gRPC request
       ↓
2. Spark Gateway authenticates via Vault → Cluster Proxy
       ↓
3. Spark Gateway creates SparkApplication CRD in target namespace
       ↓
4. Spark Operator (already running) detects new SparkApplication
       ↓
5. Operator creates Driver Pod in target namespace
       ↓
6. Driver Pod starts and requests Executor Pods
       ↓
7. Executors run tasks, Driver coordinates
       ↓
8. On completion, Operator updates SparkApplication.status
       ↓
9. After TTL, Operator cleans up pods (ResourceReleased)
```

## Testing Spark Gateway

### Connect to the Cluster

```bash
# Login to Teleport
tsh --proxy=teleport.usw2.dash-management.doordash.red login --auth=okta

# Connect to dash-data cluster
tsh kube login dash-data-usw2-001-data-01

# Port-forward to spark-gateway
tsh kubectl port-forward -n spark-gateway svc/spark-gateway-grpc-server 50051:50051 &
```

### Submit a Job

```bash
grpcurl -plaintext -d '{
  "application_name": "pi-test",
  "namespace": "sjns-data-control",
  "py_files": ["local:///opt/spark/examples/src/main/python/pi.py"],
  "image": "apache/spark:4.1.0-python3",
  "image_pull_policy": "IfNotPresent",
  "spark_application_type": "Python",
  "spark_conf": {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "3",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
    "spark.kubernetes.container.image": "apache/spark:4.1.0-python3"
  },
  "driver": {"cores": "1", "memory": "512m", "service_account": "spark"},
  "executor": {"instances": 2, "cores": "1", "memory": "512m"},
  "runtime_versions": {"spark_version": "4.1.0"},
  "application_tolerations": {"resource_retain_policy": "OnFailure", "ttl_after_stop_millis": 300000}
}' localhost:50051 spark_gateway.v1.SparkGateway/SubmitSparkJob
```

### Check Job Status

```bash
# Via gRPC
grpcurl -plaintext -d '{
  "application_name": "pi-test",
  "namespace": "sjns-data-control"
}' localhost:50051 spark_gateway.v1.SparkGateway/GetSparkJobStatus

# Via kubectl
tsh kubectl get sparkapplication pi-test -n sjns-data-control
tsh kubectl get pods -n sjns-data-control | grep pi-test
tsh kubectl logs <driver-pod-name> -n sjns-data-control
```

### Delete a Job

```bash
grpcurl -plaintext -d '{
  "application_name": "pi-test",
  "namespace": "sjns-data-control"
}' localhost:50051 spark_gateway.v1.SparkGateway/DeleteSparkJob
```

## Related Documentation

- [Spark Gateway README](https://github.com/doordash/pedregal/blob/main/services/spark-gateway/README.md)
- [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator)
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
