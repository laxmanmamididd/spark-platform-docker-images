# CLAUDE.md - Spark Platform Context for Claude Agents

This file provides context for Claude Code agents working on the Pedregal Spark Platform.

## Project Overview

This repository contains documentation and examples for DoorDash's **Pedregal Spark Platform** - a unified system for running Spark workloads on Kubernetes (SK8).

### Key Goals

1. Replace legacy SJEM (Spark Job Execution Manager) with Pedregal-native Spark Runner
2. Run Spark on Kubernetes (SK8) instead of EMR/Databricks
3. Provide Spark Connect for interactive sessions
4. Enable self-service cluster provisioning for teams

## Repository Structure

```
.
├── CLAUDE.md                          # This file - agent context
├── README.md                          # Project overview
├── ARCHITECTURE.md                    # Architecture diagrams
├── PEDREGAL_SPARK_PLATFORM.md         # Complete platform reference
├── INTERNAL_SPARK_RUNTIME.md          # SK8 runtime architecture
├── JIRA_TICKETS.md                    # All planned Jira tickets
│
├── spark-connect-client/              # Spark Connect client libraries
│   ├── python/
│   │   └── spark_connect_client.py    # Python helper
│   └── java/
│       └── SparkConnectClient.java    # Java helper
│
├── spark-gateway/                     # Spark Gateway service
│   ├── gateway.go
│   └── spark_connect_proxy.go         # gRPC proxy
│
├── spark-runner/                      # Spark Runner graph
│   ├── runner.go
│   └── runner_test.go
│
├── cluster-provisioning/              # Self-service provisioning
│   ├── proto/
│   │   └── cluster_provisioning.proto
│   └── api/
│       └── server.go
│
├── examples/                          # Usage examples
│   ├── metaflow/
│   │   └── feature_pipeline.py
│   ├── jupyter/
│   │   └── spark_connect_notebook.ipynb
│   └── ci-cd/
│       ├── feature_tests.py
│       └── github-workflow.yaml
│
├── testcontainers/                    # Java TestContainers
│   └── src/
│
├── docker/                            # Spark Docker image
│   ├── Dockerfile
│   └── entrypoint.sh
│
└── dcp-example/                       # DCP manifest example
    └── manifest.yaml
```

## Key Concepts

### Architecture Layers

1. **Control Plane**
   - **Spark Runner**: Stateless Pedregal Graph (Submit/Check/Cancel)
   - **Spark Gateway**: Routes to compute, proxies Spark Connect
   - **Cluster Proxy**: Multi-cluster routing, HA

2. **Execution Plane**
   - **Spark Kubernetes Operator (SKO)**: Manages SparkApplication CRDs
   - **Domain Namespaces**: `sjns-<team>-<env>` for isolation
   - **Driver/Executor Pods**: Actual Spark execution

3. **Observability**
   - **Chronosphere**: Metrics via OTel
   - **ODIN**: Logs via Fluent Bit
   - **SHS**: Spark History Server for Spark UI
   - **Taulu**: Execution history

### Two Access Patterns

1. **Batch Jobs**: DCP → Spark Runner → Spark Gateway → SK8
2. **Interactive**: Spark Connect Client → Spark Gateway → Spark Connect Server (on Driver)

### Key Services

| Service | Type | Purpose |
|---------|------|---------|
| Spark Runner | Pedregal Graph | Job lifecycle (Submit/Check/Cancel) |
| Spark Gateway | Pedregal Service | Routing, auth, Spark Connect proxy |
| Cluster Proxy | Service | Multi-cluster routing, HA |

### Domain Namespaces

Teams are isolated via Kubernetes namespaces:
```
sjns-<team>-<environment>
Example: sjns-feature-eng-prod
```

Each namespace has: ResourceQuota, LimitRange, NetworkPolicy, RBAC, Secrets

## Current Work Context

### Epic 1: Internal Spark Runtime

Building SK8 infrastructure:
- Deploy Spark Kubernetes Operator
- Create domain namespaces
- Build Spark Gateway with Spark Connect proxy
- Build Spark Runner graph
- Docker images with Unity Catalog
- Observability (OTel, ODIN, SHS)

See `JIRA_TICKETS.md` for detailed tickets.

### Epic 2: Unravel Recommendations (SJEM)

Integrating Unravel for optimization:
- Current state: Observability only (init script + ACL)
- Goal: Pre-submit recommendations, post-run feedback
- Location: `spark-job-execution-service` in asgard repo

Key files in SJEM:
- `DBRProvider.java`: Unravel init script injection (lines 2332-2340)
- `SparkJob.java`: `OPT_IN_UNRAVEL` user control (line 123)
- `DBRConstants.java`: Unravel constants (lines 134-137)

## Related Repositories

| Repo | Purpose |
|------|---------|
| `asgard/services/spark-job-execution-service` | SJEM - legacy job execution |
| `data-control-scripts` | Init scripts, OTEL collectors |
| `pedregal` | Pedregal platform |

## Useful Commands

```bash
# Build Spark Runner
cd spark-runner && go build ./...

# Run tests
cd spark-runner && go test ./...

# Build Docker image
cd docker && ./build.sh

# Run TestContainers tests
cd testcontainers && ./gradlew test
```

## Key Documentation Files

| File | Content |
|------|---------|
| `PEDREGAL_SPARK_PLATFORM.md` | Complete platform reference (1000+ lines) |
| `ARCHITECTURE.md` | Architecture diagrams and flows |
| `INTERNAL_SPARK_RUNTIME.md` | SK8 runtime details |
| `JIRA_TICKETS.md` | All planned work items |

## When Working on This Project

1. **Read the platform docs first**: Start with `PEDREGAL_SPARK_PLATFORM.md`
2. **Understand the two patterns**: Batch (Spark Runner) vs Interactive (Spark Connect)
3. **Know the layers**: Control Plane → Execution Plane → Observability
4. **Check JIRA_TICKETS.md**: For planned work and acceptance criteria
5. **Spark Gateway is key**: Single entry point for all Spark access

## Common Questions

**Q: Where does Spark Connect server run?**
A: Inside the Driver Pod (port 15002), NOT as a separate service. Clients access it through Spark Gateway.

**Q: What's the difference between SK8 and SJEM?**
A: SK8 is Spark on K8s (new). SJEM is the legacy service that submits to EMR/Databricks.

**Q: How do teams get isolated?**
A: Domain namespaces (`sjns-<team>`) with ResourceQuota, NetworkPolicy, RBAC.

**Q: What's APS?**
A: Auto Parameter Selection - recommends cluster sizing based on historical data.

**Q: What's AR?**
A: Auto-Retry/Remediation - classifies failures and recommends retries.

## Glossary

| Term | Definition |
|------|------------|
| APS | Auto Parameter Selection |
| AR | Auto-Retry/Remediation |
| CRD | Custom Resource Definition (K8s) |
| DCP | Data Control Plane |
| DRA | Dynamic Resource Allocation |
| SKO | Spark Kubernetes Operator |
| SK8 | Spark on Kubernetes |
| SJEM | Spark Job Execution Manager (legacy) |
| SHS | Spark History Server |
| SR | Spark Runner |

---

*Last updated: January 2025*
