# Spark Testing Design for DCP Playgrounds

Based on analysis of:
- `Pedregal Data Control Plane.txt` - DCP architecture and playground design
- `Improving Spark Testing.txt` - Spark testing requirements and solutions

## Executive Summary

DCP Playgrounds use a **batch job submission model** for Spark testing. **Spark Connect is NOT needed** for this use case. The existing architecture with Spark Runner and hot clusters is the correct approach.

However, there are two distinct testing patterns that need different solutions:

| Command | Purpose | Latency Target | Spark Connect? |
|---------|---------|----------------|----------------|
| `pretzel test` | Quick validation (schema, SQL syntax, execution plan) | < 60 seconds | **Optional** (could benefit) |
| `pretzel run` | Full sandbox execution (write to playground tables) | < 3 minutes | **NO** (batch job) |

---

## Current DCP Playground Architecture

From the DCP document:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        DCP PLAYGROUND FLOW (Current Design)                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   USER                                                                              │
│   │                                                                                 │
│   │  pretzel playground create                                                      │
│   │  pretzel derived-dataset apply B.yaml                                           │
│   │  pretzel derived-dataset run B.yaml                                             │
│   │                                                                                 │
│   ▼                                                                                 │
│   PRETZEL CLI                                                                       │
│   │                                                                                 │
│   │  • Parses manifest                                                              │
│   │  • Sets playground context                                                      │
│   │  • Calls DCP Graph Runner                                                       │
│   │                                                                                 │
│   ▼                                                                                 │
│   DCP GRAPH RUNNER (Pedregal)                                                       │
│   │                                                                                 │
│   │  • Playground Service: Create/Delete/Update playgrounds                         │
│   │  • Plugin Framework: Routes to derived-dataset plugin                           │
│   │  • Derived Dataset Plugin: Handles apply/run commands                           │
│   │                                                                                 │
│   ▼                                                                                 │
│   DERIVED DATASET PLUGIN                                                            │
│   │                                                                                 │
│   │  apply: Creates playground Iceberg table, registers CoreETL job                 │
│   │  run: Triggers CoreETL job execution                                            │
│   │                                                                                 │
│   ▼                                                                                 │
│   SJEM / SPARK RUNNER                                                               │
│   │                                                                                 │
│   │  • Submits Spark job to compute cluster                                         │
│   │  • Uses user's identity (PAT for Databricks)                                    │
│   │  • Routes to playground namespace                                               │
│   │                                                                                 │
│   ▼                                                                                 │
│   COMPUTE CLUSTER (EMR/DBR/SK8)                                                     │
│   │                                                                                 │
│   │  • Executes CoreETL Spark job                                                   │
│   │  • Reads from production tables (read-only)                                     │
│   │  • Writes to playground tables (isolated)                                       │
│   │                                                                                 │
│   ▼                                                                                 │
│   PLAYGROUND ICEBERG TABLE                                                          │
│       │                                                                             │
│       │  Results available for inspection                                           │
│       │  TTL-controlled cleanup                                                     │
│       ▼                                                                             │
│   USER INSPECTS RESULTS (Databricks/Snowflake/etc.)                                 │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Two Testing Commands: `test` vs `run`

From the DCP document comments:
> "DCP plugins will have both `sandbox` commands as well as `test` commands.
> `sandbox` would be to test the E2E functionality of a plan/apply
> `test` would be to quickly output the result of a query to the user.
> For the test command we would need to be much faster than 3 minutes IMO for anything meaningful"

### Command: `pretzel derived-dataset test`

**Purpose:** Quick validation without full job execution
- Schema validation
- SQL syntax checking
- Execution plan generation (EXPLAIN)
- Policy checks
- Lineage extraction

**Latency Target:** < 60 seconds (ideally < 30 seconds)

**Does NOT write data** - just validates the manifest is correct.

### Command: `pretzel derived-dataset run`

**Purpose:** Full sandbox execution
- Executes the actual Spark job
- Writes results to playground Iceberg table
- User can inspect results afterward

**Latency Target:** < 3 minutes startup (from "Improving Spark Testing" doc)

**Writes data** to playground tables.

---

## Where Spark Connect Could Help

**Spark Connect is beneficial for the `test` command**, not the `run` command.

### Why `test` could use Spark Connect:

1. **Quick SQL validation** - Parse and analyze SparkSQL without full job submission
2. **Execution plan generation** - Get EXPLAIN output in seconds
3. **Schema inference** - Validate column types and names
4. **Interactive feedback** - Fast iteration on SQL changes

### Why `run` should NOT use Spark Connect:

1. **Complete manifest execution** - Not ad-hoc queries
2. **Fire-and-forget** - Job runs to completion, user polls for status
3. **Results in S3/Iceberg** - Not streamed to client
4. **Job lifecycle management** - Submit/Check/Cancel via Spark Runner

---

## Recommended Architecture

### Option A: Hot Clusters Only (No Spark Connect)

Keep the current batch model. Optimize startup time with hot clusters.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    OPTION A: HOT CLUSTERS (Current + Optimized)                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   pretzel test / pretzel run                                                        │
│        │                                                                            │
│        ▼                                                                            │
│   DCP Graph Runner                                                                  │
│        │                                                                            │
│        ▼                                                                            │
│   Spark Runner                                                                      │
│        │                                                                            │
│        │  Both test and run submit batch jobs                                       │
│        │  test: Runs with LIMIT 0 or EXPLAIN mode                                   │
│        │  run: Full execution                                                       │
│        │                                                                            │
│        ▼                                                                            │
│   Spark Gateway                                                                     │
│        │                                                                            │
│        │  Hot cluster assignment (< 30s if available)                               │
│        │  Cold cluster creation (6-8 min worst case)                                │
│        │                                                                            │
│        ▼                                                                            │
│   SK8 Hot Cluster Pool                                                              │
│        │                                                                            │
│        │  Pre-warmed clusters per namespace                                         │
│        │  Karpenter node scaling                                                    │
│        │                                                                            │
│        ▼                                                                            │
│   SparkApplication CRD → Driver → Executors                                         │
│                                                                                     │
│   Latency:                                                                          │
│   • test (hot cluster): ~60-90 seconds                                              │
│   • test (cold cluster): ~6-8 minutes                                               │
│   • run (hot cluster): ~60 seconds + job time                                       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Pros:**
- Simpler architecture (single execution model)
- No new infrastructure needed
- Works with existing SJEM/Spark Runner

**Cons:**
- `test` command still requires cluster (even if just for EXPLAIN)
- Cold cluster case is slow (6-8 minutes)
- Over-provisioning hot clusters is expensive

---

### Option B: Spark Connect for `test` Command (Recommended)

Use Spark Connect for the `test` command (fast, interactive validation).
Keep batch submission for `run` command (full execution).

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    OPTION B: HYBRID (Spark Connect for test)                         │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   ┌─────────────────────────────────┐    ┌─────────────────────────────────┐        │
│   │        pretzel test             │    │         pretzel run             │        │
│   │   (Quick validation < 60s)      │    │   (Full execution < 3min)       │        │
│   └───────────────┬─────────────────┘    └───────────────┬─────────────────┘        │
│                   │                                      │                          │
│                   ▼                                      ▼                          │
│   ┌─────────────────────────────────┐    ┌─────────────────────────────────┐        │
│   │       DCP Graph Runner          │    │       DCP Graph Runner          │        │
│   │                                 │    │                                 │        │
│   │   TestNode: Uses Spark Connect  │    │   RunNode: Uses Spark Runner    │        │
│   └───────────────┬─────────────────┘    └───────────────┬─────────────────┘        │
│                   │                                      │                          │
│                   │ gRPC                                 │ gRPC                     │
│                   │                                      │                          │
│                   ▼                                      ▼                          │
│   ┌─────────────────────────────────┐    ┌─────────────────────────────────┐        │
│   │       Spark Gateway             │    │        Spark Runner             │        │
│   │                                 │    │                                 │        │
│   │   Spark Connect Proxy           │    │   Submit/Check/Cancel           │        │
│   │   Routes to shared test cluster │    │   Creates SparkApplication      │        │
│   └───────────────┬─────────────────┘    └───────────────┬─────────────────┘        │
│                   │                                      │                          │
│                   ▼                                      ▼                          │
│   ┌─────────────────────────────────┐    ┌─────────────────────────────────┐        │
│   │    Shared Test Cluster          │    │    Hot Cluster Pool             │        │
│   │    (Long-running, multi-tenant) │    │    (Per-job, isolated)          │        │
│   │                                 │    │                                 │        │
│   │    • Always-on driver           │    │    • Pre-warmed clusters        │        │
│   │    • Spark Connect Server       │    │    • Dedicated per job          │        │
│   │    • Read-only access           │    │    • Write to playground        │        │
│   │    • EXPLAIN/validation only    │    │    • Full execution             │        │
│   └─────────────────────────────────┘    └─────────────────────────────────┘        │
│                                                                                     │
│   Latency:                                                                          │
│   • test: ~5-15 seconds (Spark Connect to shared cluster)                           │
│   • run: ~60 seconds (hot cluster) to ~6-8 min (cold cluster)                       │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Pros:**
- `test` command is very fast (5-15 seconds)
- Shared cluster for test is cost-effective
- Spark Connect perfect for validation workloads
- `run` command uses appropriate batch model

**Cons:**
- Two execution paths to maintain
- Shared test cluster needs multi-tenancy
- New infrastructure (Spark Connect proxy in Gateway)

---

## Detailed Design: Option B (Recommended)

### Test Command Flow

```
pretzel derived-dataset test my-manifest.yaml
```

1. **CLI** parses manifest, extracts SparkSQL
2. **DCP Graph Runner** receives test request
3. **TestNode** (new GR node) calls Spark Gateway with:
   - SQL query
   - Playground context (team, user, environment)
   - Test mode flags (EXPLAIN, LIMIT 0, schema-only)
4. **Spark Gateway** routes to shared test cluster via Spark Connect
5. **Shared Test Cluster** (always-on):
   - Receives query via Spark Connect Server
   - Runs EXPLAIN or LIMIT 0 execution
   - Returns execution plan, schema, validation results
6. **Response** returned to CLI in 5-15 seconds

**Shared Test Cluster Configuration:**
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: dcp-test-cluster
  namespace: pedregal-system
spec:
  type: Scala
  mode: cluster
  image: "doordash-docker.jfrog.io/spark-platform/3.5-oss:latest"
  mainClass: org.apache.spark.sql.connect.service.SparkConnectServer

  sparkConf:
    # Spark Connect enabled
    spark.plugins: "org.apache.spark.sql.connect.SparkConnectPlugin"
    spark.connect.grpc.binding.port: "15002"

    # Read-only mode
    spark.sql.readOnly: "true"

    # Multi-tenant session isolation
    spark.connect.session.isolation: "true"

    # Unity Catalog (read-only)
    spark.sql.catalog.pedregal: "io.unitycatalog.spark.UCSingleCatalog"

  driver:
    cores: 4
    memory: "8g"

  executor:
    cores: 2
    memory: "4g"
    instances: 4

  # Keep running (no TTL)
  restartPolicy:
    type: Always
```

### Run Command Flow

```
pretzel derived-dataset run my-manifest.yaml
```

1. **CLI** parses manifest
2. **DCP Graph Runner** receives run request
3. **RunNode** calls **Spark Runner** with:
   - Full manifest
   - Playground context
4. **Spark Runner** creates SparkApplication CRD
5. **Spark Gateway** assigns hot cluster or creates new
6. **SparkApplication** runs to completion
7. **Results** written to playground Iceberg table
8. **User** polls for status, inspects results when done

*(No change from current design)*

---

## Architecture Changes Required

### For Option B (Spark Connect for test):

1. **Spark Gateway Enhancement:**
   - Add Spark Connect gRPC proxy (already designed)
   - Route test requests to shared test cluster
   - Route run requests to Spark Runner

2. **New GR Node: TestNode**
   ```go
   // TestNode handles quick validation via Spark Connect
   type TestNode struct {
       gatewayClient SparkGatewayClient
   }

   func (n *TestNode) Execute(ctx context.Context, req *TestRequest) (*TestResponse, error) {
       // Connect to Spark Gateway's Spark Connect endpoint
       spark, err := n.gatewayClient.SparkConnect(ctx, &SparkConnectRequest{
           Team:        req.Team,
           Environment: "playground",
           User:        req.User,
       })

       // Run validation query
       result, err := spark.SQL(fmt.Sprintf("EXPLAIN EXTENDED %s", req.SQL))

       return &TestResponse{
           ExecutionPlan: result.Plan,
           Schema:        result.Schema,
           Warnings:      result.Warnings,
       }, nil
   }
   ```

3. **Shared Test Cluster:**
   - Long-running SparkApplication with Spark Connect enabled
   - Multi-tenant session isolation
   - Read-only access to production data
   - Deployed in `pedregal-system` namespace

4. **Pretzel CLI Update:**
   - `pretzel test` routes to TestNode (Spark Connect)
   - `pretzel run` routes to RunNode (Spark Runner/batch)

---

## Decision Matrix

| Factor | Option A (Hot Clusters Only) | Option B (Hybrid) |
|--------|------------------------------|-------------------|
| `test` latency | 60-90s (hot) / 6-8min (cold) | **5-15 seconds** |
| `run` latency | 60s (hot) | 60s (hot) |
| Infrastructure complexity | Lower | Higher |
| Cost (test workloads) | Higher (dedicated clusters) | **Lower (shared cluster)** |
| Multi-tenancy | Not needed | Required for test cluster |
| Spark Connect required | No | Yes (test only) |

---

## Recommendation

**Implement Option B (Hybrid)** with phased rollout:

### Phase 1: Hot Clusters for Both (Current State)
- Deploy SK8 hot cluster pool
- Use batch submission for both `test` and `run`
- Optimize Karpenter for fast node scaling
- Target: `test` < 90 seconds, `run` < 3 minutes

### Phase 2: Spark Connect for Test
- Deploy shared test cluster with Spark Connect
- Add Spark Connect proxy to Spark Gateway
- Implement TestNode in DCP Graph Runner
- Target: `test` < 15 seconds

### Phase 3: Enhanced Testing
- Add support for:
  - Data preview (LIMIT 100)
  - Schema comparison (diff vs production)
  - Cost estimation (query complexity)
  - Lineage extraction at test time

---

## Summary

| Question | Answer |
|----------|--------|
| Does DCP Playground need Spark Connect? | **NO** for `run` command (batch jobs) |
| Could Spark Connect help DCP? | **YES** for `test` command (quick validation) |
| Should we change the architecture? | **Enhance** with Spark Connect for test, keep batch for run |
| What's the main latency issue? | Cluster acquisition (6-8 min) for cold starts |
| How do we solve it? | Hot clusters for `run`, shared Spark Connect cluster for `test` |
