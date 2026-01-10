# DCP Architecture Flow - CoreETL, Controller, Plugins & Primitives

This document details the internal architecture of DCP (Data Control Plane), showing how CoreETL, the DCP Controller, Plugins, and Primitives work together.

## Core Concepts

### Terminology

| Term | Definition |
|------|------------|
| **DCP** | Data Control Plane - the unified system for managing data platform resources |
| **Pretzel CLI** | Command-line tool for users to interact with DCP |
| **Manifest** | YAML configuration file describing user's intent (e.g., derived_dataset.yaml) |
| **Plugin** | Pluggable extension that handles specific manifest types (e.g., derived-dataset plugin) |
| **Primitive** | Reusable component that manages specific platform resources (e.g., Iceberg table, CoreETL job) |
| **Logical Plan** | Compiled internal representation of a manifest |
| **DCP Controller** | Service that reconciles desired state with actual state |
| **Graph Runner (GR)** | Pedregal's execution framework for running DCP logic |
| **CoreETL** | Spark job execution framework |

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DCP ARCHITECTURE LAYERS                                           │
├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                     │
│   USER LAYER                                                                                        │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │  Pedregal Monorepo                                                                          │  │
│   │  └── manifests/                                                                             │  │
│   │      ├── derived_dataset/foo.yaml     (Derived Dataset manifest)                            │  │
│   │      ├── derived_dataset/foo.sql      (SparkSQL)                                            │  │
│   │      ├── ingestion/bar.yaml           (Ingestion manifest)                                  │  │
│   │      └── ...                                                                                │  │
│   └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                              │                                                      │
│                                              │ pretzel CLI commands                                 │
│                                              │ (lint, test, apply, run, plan)                       │
│                                              ▼                                                      │
│   CLI LAYER                                                                                         │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │  Pretzel CLI (Go)                                                                           │  │
│   │  ├── Parse manifest YAML                                                                    │  │
│   │  ├── Convert to Logical Plan protobuf                                                       │  │
│   │  ├── Route to appropriate Plugin                                                            │  │
│   │  └── Call DCP Graph Runner endpoints                                                        │  │
│   └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                              │                                                      │
│                                              │ gRPC to DCP GR                                       │
│                                              ▼                                                      │
│   DCP GRAPH RUNNER LAYER                                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │  DCP Graph Runner (Pedregal GR Graphs)                                                      │  │
│   │                                                                                             │  │
│   │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │  │
│   │  │  DCP Controller │  │  Playground     │  │  Ops Service    │  │  Metadata       │        │  │
│   │  │  (Reconciler)   │  │  Service        │  │  (Manual Ops)   │  │  Service        │        │  │
│   │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘        │  │
│   │           │                    │                    │                    │                  │  │
│   │           └────────────────────┴────────────────────┴────────────────────┘                  │  │
│   │                                        │                                                    │  │
│   │                                        ▼                                                    │  │
│   │                              ┌─────────────────────┐                                        │  │
│   │                              │   Plugin Framework  │                                        │  │
│   │                              │                     │                                        │  │
│   │                              │   Routes to plugins │                                        │  │
│   │                              │   based on manifest │                                        │  │
│   │                              │   type              │                                        │  │
│   │                              └──────────┬──────────┘                                        │  │
│   │                                         │                                                   │  │
│   └─────────────────────────────────────────┼───────────────────────────────────────────────────┘  │
│                                             │                                                      │
│                                             ▼                                                      │
│   PLUGIN LAYER                                                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │                                                                                             │  │
│   │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐                 │  │
│   │  │  Derived Dataset    │  │  Ingestion Plugin   │  │  Feature Platform   │                 │  │
│   │  │  Plugin             │  │                     │  │  Plugin             │                 │  │
│   │  │                     │  │  Handles:           │  │                     │                 │  │
│   │  │  Handles:           │  │  • gr_ingestion     │  │  Handles:           │                 │  │
│   │  │  • derived_dataset  │  │  • taulu_ingestion  │  │  • feature_platform │                 │  │
│   │  │    manifests        │  │    manifests        │  │    manifests        │                 │  │
│   │  │                     │  │                     │  │                     │                 │  │
│   │  │  Commands:          │  │  Commands:          │  │  Commands:          │                 │  │
│   │  │  • lint             │  │  • lint             │  │  • lint             │                 │  │
│   │  │  • test             │  │  • test             │  │  • test             │                 │  │
│   │  │  • plan             │  │  • plan             │  │  • plan             │                 │  │
│   │  │  • apply            │  │  • apply            │  │  • apply            │                 │  │
│   │  │  • run              │  │  • run              │  │  • run              │                 │  │
│   │  └──────────┬──────────┘  └──────────┬──────────┘  └──────────┬──────────┘                 │  │
│   │             │                        │                        │                            │  │
│   └─────────────┼────────────────────────┼────────────────────────┼────────────────────────────┘  │
│                 │                        │                        │                               │
│                 ▼                        ▼                        ▼                               │
│   PRIMITIVE LAYER                                                                                   │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │                                                                                             │  │
│   │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                │  │
│   │  │ Iceberg Table │  │ CoreETL Job   │  │ Airflow DAG   │  │ Kafka Topic   │                │  │
│   │  │ Primitive     │  │ Primitive     │  │ Primitive     │  │ Primitive     │                │  │
│   │  │               │  │               │  │               │  │               │                │  │
│   │  │ • Create      │  │ • Create      │  │ • Create      │  │ • Create      │                │  │
│   │  │ • Update      │  │ • Start       │  │ • Update      │  │ • Update      │                │  │
│   │  │ • Delete      │  │ • Stop        │  │ • Delete      │  │ • Delete      │                │  │
│   │  │ • GetState    │  │ • GetState    │  │ • GetState    │  │ • GetState    │                │  │
│   │  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘  └───────┬───────┘                │  │
│   │          │                  │                  │                  │                         │  │
│   └──────────┼──────────────────┼──────────────────┼──────────────────┼─────────────────────────┘  │
│              │                  │                  │                  │                            │
│              ▼                  ▼                  ▼                  ▼                            │
│   PLATFORM SERVICE LAYER                                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │                                                                                             │  │
│   │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                │  │
│   │  │ Unity Catalog │  │ CoreETL       │  │ Airflow       │  │ Kafka         │                │  │
│   │  │ / Iceberg     │  │ Driver        │  │               │  │               │                │  │
│   │  │               │  │               │  │               │  │               │                │  │
│   │  │ Tables, ACLs  │  │ Submit jobs   │  │ DAG mgmt      │  │ Topic mgmt    │                │  │
│   │  │ Schema        │  │ to compute    │  │               │  │               │                │  │
│   │  └───────────────┘  └───────┬───────┘  └───────────────┘  └───────────────┘                │  │
│   │                             │                                                               │  │
│   └─────────────────────────────┼───────────────────────────────────────────────────────────────┘  │
│                                 │                                                                  │
│                                 ▼                                                                  │
│   COMPUTE LAYER                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│   │                                                                                             │  │
│   │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                                   │  │
│   │  │ EMR           │  │ Databricks    │  │ SK8 (Spark    │                                   │  │
│   │  │               │  │               │  │ on K8s)       │                                   │  │
│   │  │ Spark jobs    │  │ Spark jobs    │  │               │                                   │  │
│   │  │               │  │               │  │ Spark jobs    │                                   │  │
│   │  └───────────────┘  └───────────────┘  └───────────────┘                                   │  │
│   │                                                                                             │  │
│   └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Flow: `pretzel derived-dataset apply` + `run`

### Step 1: User Authors Manifest

```yaml
# manifests/derived_dataset/daily_orders.yaml
metadata:
  technical_owner: logistics-de
  tier: 1

dataset:
  name: daily_orders
  fields:
    - name: order_id
      type: string
    - name: total_amount
      type: decimal

workflow:
  schedule: "0 * * * *"  # hourly
  sql: daily_orders.sql
```

```sql
-- manifests/derived_dataset/daily_orders.sql
SELECT
  order_id,
  SUM(amount) as total_amount
FROM datalake.raw_orders
GROUP BY order_id
```

---

### Step 2: CLI Parses and Creates Logical Plan

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PRETZEL CLI: PARSE MANIFEST                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   pretzel derived-dataset apply daily_orders.yaml                                   │
│                                                                                     │
│   1. Load YAML file                                                                 │
│   2. Validate against JSON Schema                                                   │
│   3. Route to Derived Dataset Plugin                                                │
│   4. Plugin converts manifest → Logical Plan (protobuf)                             │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  LogicalPlan (protobuf)                                                     │   │
│   │                                                                             │   │
│   │  {                                                                          │   │
│   │    plugin_type: DERIVED_DATASET,                                            │   │
│   │    id: "derived_dataset/daily_orders",                                      │   │
│   │    environment: "playground_user123",  // or "production"                   │   │
│   │    plugin_spec: {                                                           │   │
│   │      // Derived Dataset specific config                                     │   │
│   │      table_name: "daily_orders",                                            │   │
│   │      sql: "SELECT order_id...",                                             │   │
│   │      schedule: "0 * * * *",                                                 │   │
│   │      owner_team: "logistics-de"                                             │   │
│   │    }                                                                        │   │
│   │  }                                                                          │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 3: CLI Calls DCP Graph Runner

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PRETZEL CLI → DCP GRAPH RUNNER                                │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   CLI sends gRPC request to DCP GR endpoint:                                        │
│                                                                                     │
│   POST https://data-control-plane.dashapi.com/apply                                 │
│   Headers:                                                                          │
│     - x-tenant-id: playground_user123                                               │
│     - authorization: Bearer <okta_token>                                            │
│   Body:                                                                             │
│     - logical_plan: <protobuf>                                                      │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                     DCP GRAPH RUNNER                                        │   │
│   │                                                                             │   │
│   │   ┌─────────────────────────────────────────────────────────────────────┐   │   │
│   │   │  Apply Graph (GR Graph)                                             │   │   │
│   │   │                                                                     │   │   │
│   │   │  1. AuthN/AuthZ Node: Validate token, check permissions             │   │   │
│   │   │  2. Plugin Router Node: Route to derived-dataset plugin             │   │   │
│   │   │  3. Plan Node: Compute reconciliation actions                       │   │   │
│   │   │  4. Apply Node: Execute actions (long-running)                      │   │   │
│   │   │  5. Audit Node: Log to Taulu audit table                            │   │   │
│   │   │                                                                     │   │   │
│   │   └─────────────────────────────────────────────────────────────────────┘   │   │
│   │                                                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 4: Plugin Decomposes to Primitives

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    DERIVED DATASET PLUGIN: DECOMPOSE TO PRIMITIVES                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   The plugin breaks down the logical plan into primitives:                          │
│                                                                                     │
│   LogicalPlan (derived_dataset/daily_orders)                                        │
│        │                                                                            │
│        ├──────────────────────────────────────────────────────────────────────────┐ │
│        │                                                                          │ │
│        ▼                                                                          │ │
│   ┌────────────────────┐                                                          │ │
│   │  Iceberg Table     │  Primitive 1: Storage                                    │ │
│   │  Primitive         │                                                          │ │
│   │                    │  • catalog: pedregal-playground-user123                  │ │
│   │                    │  • database: derived_datasets                            │ │
│   │                    │  • table: daily_orders                                   │ │
│   │                    │  • schema: {order_id: string, total_amount: decimal}     │ │
│   └────────────────────┘                                                          │ │
│        │                                                                          │ │
│        ▼                                                                          │ │
│   ┌────────────────────┐                                                          │ │
│   │  CoreETL Job       │  Primitive 2: Compute                                    │ │
│   │  Primitive         │                                                          │ │
│   │                    │  • job_name: daily_orders_job                            │ │
│   │                    │  • sql: "SELECT order_id..."                             │ │
│   │                    │  • sink: iceberg://daily_orders                          │ │
│   │                    │  • owner_team: logistics-de                              │ │
│   └────────────────────┘                                                          │ │
│        │                                                                          │ │
│        ▼                                                                          │ │
│   ┌────────────────────┐                                                          │ │
│   │  Airflow DAG       │  Primitive 3: Orchestration                              │ │
│   │  Primitive         │                                                          │ │
│   │                    │  • dag_id: daily_orders_dag                              │ │
│   │                    │  • schedule: "0 * * * *"                                 │ │
│   │                    │  • trigger: CoreETL job                                  │ │
│   └────────────────────┘                                                          │ │
│                                                                                   │ │
└───────────────────────────────────────────────────────────────────────────────────┘ │
                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 5: Plan - Compare Desired vs Actual State

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PLUGIN: PLAN (RECONCILIATION)                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   For each primitive, plugin compares desired state vs actual state:                │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  Iceberg Table Primitive                                                    │   │
│   │                                                                             │   │
│   │  Desired State:                     Actual State:                           │   │
│   │  ┌─────────────────────────┐        ┌─────────────────────────┐             │   │
│   │  │ table: daily_orders    │        │ (does not exist)        │             │   │
│   │  │ schema: {order_id,     │        │                         │             │   │
│   │  │          total_amount} │        │                         │             │   │
│   │  └─────────────────────────┘        └─────────────────────────┘             │   │
│   │                                                                             │   │
│   │  Action: CREATE_TABLE                                                       │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  CoreETL Job Primitive                                                      │   │
│   │                                                                             │   │
│   │  Desired State:                     Actual State:                           │   │
│   │  ┌─────────────────────────┐        ┌─────────────────────────┐             │   │
│   │  │ job: daily_orders_job  │        │ (does not exist)        │             │   │
│   │  │ sql: "SELECT..."       │        │                         │             │   │
│   │  └─────────────────────────┘        └─────────────────────────┘             │   │
│   │                                                                             │   │
│   │  Action: CREATE_JOB                                                         │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  Airflow DAG Primitive                                                      │   │
│   │                                                                             │   │
│   │  Desired State:                     Actual State:                           │   │
│   │  ┌─────────────────────────┐        ┌─────────────────────────┐             │   │
│   │  │ dag: daily_orders_dag  │        │ (does not exist)        │             │   │
│   │  │ schedule: "0 * * * *"  │        │                         │             │   │
│   │  └─────────────────────────┘        └─────────────────────────┘             │   │
│   │                                                                             │   │
│   │  Action: CREATE_DAG                                                         │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   Computed Actions:                                                                 │
│   1. CREATE_TABLE (Iceberg)                                                         │
│   2. CREATE_JOB (CoreETL)                                                           │
│   3. CREATE_DAG (Airflow)                                                           │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 6: Apply - Execute Actions

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PLUGIN: APPLY (EXECUTE ACTIONS)                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   Plugin Framework executes:                                                        │
│                                                                                     │
│   1. PRE-APPLY HOOK                                                                 │
│      └── Stop any existing CoreETL job (for safe migration)                         │
│                                                                                     │
│   2. APPLY                                                                          │
│      ┌─────────────────────────────────────────────────────────────────────────┐    │
│      │                                                                         │    │
│      │  Action 1: CREATE_TABLE                                                 │    │
│      │  ┌───────────────────┐                                                  │    │
│      │  │ Iceberg Primitive │ ──────────► Unity Catalog API                    │    │
│      │  │                   │             CREATE TABLE daily_orders (...)      │    │
│      │  └───────────────────┘                                                  │    │
│      │                                                                         │    │
│      │  Action 2: CREATE_JOB                                                   │    │
│      │  ┌───────────────────┐                                                  │    │
│      │  │ CoreETL Primitive │ ──────────► CoreETL Driver API                   │    │
│      │  │                   │             POST /jobs { name, sql, sink }       │    │
│      │  └───────────────────┘                                                  │    │
│      │                                                                         │    │
│      │  Action 3: CREATE_DAG                                                   │    │
│      │  ┌───────────────────┐                                                  │    │
│      │  │ Airflow Primitive │ ──────────► Airflow API                          │    │
│      │  │                   │             Create DAG with schedule             │    │
│      │  └───────────────────┘                                                  │    │
│      │                                                                         │    │
│      └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│   3. POST-APPLY HOOK                                                                │
│      └── Resume CoreETL job (if was stopped)                                        │
│                                                                                     │
│   4. AUDIT LOG                                                                      │
│      └── Write to Taulu audit table: { plan_id, actions, user, timestamp }          │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 7: Run - Trigger Job Execution

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                        PRETZEL RUN: TRIGGER COREETL JOB                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   pretzel derived-dataset run daily_orders.yaml                                     │
│                                                                                     │
│   1. CLI sends run request to DCP GR                                                │
│                                                                                     │
│   2. DCP GR routes to Derived Dataset Plugin                                        │
│                                                                                     │
│   3. Plugin invokes CoreETL Primitive.Run()                                         │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  CoreETL Primitive                                                          │   │
│   │                                                                             │   │
│   │  func (p *CoreETLPrimitive) Run(ctx, jobID, environment) {                  │   │
│   │      // Call CoreETL Driver API                                             │   │
│   │      resp := coreETLClient.RunJob(ctx, &RunJobRequest{                      │   │
│   │          JobID:       jobID,                                                │   │
│   │          Environment: environment,  // "playground_user123"                 │   │
│   │          UserToken:   ctx.UserPAT,  // Databricks PAT                       │   │
│   │      })                                                                     │   │
│   │      return resp.ExecutionID                                                │   │
│   │  }                                                                          │   │
│   │                                                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   4. CoreETL Driver submits Spark job to compute                                    │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  CoreETL Driver (core-etl-driver service)                                   │   │
│   │                                                                             │   │
│   │  POST /v1/jobs/{job_id}/run                                                 │   │
│   │                                                                             │   │
│   │  1. Load job configuration from job registry                                │   │
│   │  2. Resolve environment (playground vs production)                          │   │
│   │  3. Build SparkSubmit parameters                                            │   │
│   │  4. Submit to SJEM (or directly to SK8)                                     │   │
│   │                                                                             │   │
│   │  ┌─────────────────────────────────────────────────────────────────────┐    │   │
│   │  │  SJEM / Spark Runner                                                │    │   │
│   │  │                                                                     │    │   │
│   │  │  • Route to appropriate compute (EMR/DBR/SK8)                       │    │   │
│   │  │  • Use user's PAT for Databricks                                    │    │   │
│   │  │  • Write results to playground Iceberg table                        │    │   │
│   │  │                                                                     │    │   │
│   │  └─────────────────────────────────────────────────────────────────────┘    │   │
│   │                                                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   5. Spark job executes                                                             │
│                                                                                     │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │  Compute Cluster (EMR / Databricks / SK8)                                   │   │
│   │                                                                             │   │
│   │  ┌─────────────────────────────────────────────────────────────────────┐    │   │
│   │  │  Spark Job                                                          │    │   │
│   │  │                                                                     │    │   │
│   │  │  // Execute user's SQL                                              │    │   │
│   │  │  SELECT order_id, SUM(amount) as total_amount                       │    │   │
│   │  │  FROM datalake.raw_orders  -- READ from production                  │    │   │
│   │  │  GROUP BY order_id                                                  │    │   │
│   │  │                                                                     │    │   │
│   │  │  // Write to playground table                                       │    │   │
│   │  │  → pedregal-playground-user123.derived_datasets.daily_orders        │    │   │
│   │  │                                                                     │    │   │
│   │  └─────────────────────────────────────────────────────────────────────┘    │   │
│   │                                                                             │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                     │
│   6. User inspects results in Databricks/Snowflake                                  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Complete End-to-End Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              COMPLETE DCP FLOW: PLAYGROUND APPLY + RUN                               │
├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                     │
│   USER                                                                                              │
│   │                                                                                                 │
│   │  1. pretzel playground create                                                                   │
│   │  2. pretzel derived-dataset apply daily_orders.yaml                                             │
│   │  3. pretzel derived-dataset run daily_orders.yaml                                               │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   PRETZEL CLI                                                                                       │
│   │                                                                                                 │
│   │  • Parse YAML manifest                                                                          │
│   │  • Convert to LogicalPlan (protobuf)                                                            │
│   │  • Set playground context                                                                       │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   DCP GRAPH RUNNER (dash-compute)                                                                   │
│   │                                                                                                 │
│   ├──► Playground Service: Create/manage playground                                                 │
│   │                                                                                                 │
│   ├──► Plugin Framework: Route to derived-dataset plugin                                            │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   DERIVED DATASET PLUGIN                                                                            │
│   │                                                                                                 │
│   │  APPLY:                                                                                         │
│   │  ├── Decompose LogicalPlan → Primitives                                                         │
│   │  ├── Plan: Compare desired vs actual state                                                      │
│   │  ├── Compute reconciliation actions                                                             │
│   │  └── Apply: Execute actions                                                                     │
│   │                                                                                                 │
│   │  RUN:                                                                                           │
│   │  └── Trigger CoreETL job execution                                                              │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   PRIMITIVES                                                                                        │
│   │                                                                                                 │
│   ├──► Iceberg Primitive ──────────────────────────► Unity Catalog API                              │
│   │    └── Create playground table                    └── CREATE TABLE ...                          │
│   │                                                                                                 │
│   ├──► CoreETL Primitive ──────────────────────────► CoreETL Driver API                             │
│   │    └── Register/Run job                           └── POST /jobs, POST /jobs/{id}/run           │
│   │                                                                                                 │
│   └──► Airflow Primitive ──────────────────────────► Airflow API                                    │
│        └── Create DAG                                 └── Create scheduled DAG                      │
│                                                                                                     │
│   ▼                                                                                                 │
│   COREETL DRIVER                                                                                    │
│   │                                                                                                 │
│   │  • Load job config                                                                              │
│   │  • Resolve playground environment                                                               │
│   │  • Submit to compute                                                                            │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   SJEM / SPARK RUNNER                                                                               │
│   │                                                                                                 │
│   │  • Route to compute cluster                                                                     │
│   │  • Use user's credentials                                                                       │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   COMPUTE (EMR / Databricks / SK8)                                                                  │
│   │                                                                                                 │
│   │  • Execute SparkSQL                                                                             │
│   │  • Read from production tables (read-only)                                                      │
│   │  • Write to playground Iceberg table                                                            │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   PLAYGROUND ICEBERG TABLE                                                                          │
│   │                                                                                                 │
│   │  pedregal-playground-user123.derived_datasets.daily_orders                                      │
│   │                                                                                                 │
│   ▼                                                                                                 │
│   USER INSPECTS RESULTS                                                                             │
│   └── Databricks / Snowflake / other tools                                                          │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Interfaces

### Plugin Interface

Every plugin must implement:

```go
type Plugin interface {
    // Manifest handling
    GetManifestType() string           // e.g., "derived_dataset"
    ParseManifest(yaml []byte) (*LogicalPlan, error)

    // Commands
    Lint(plan *LogicalPlan) ([]Warning, error)
    Test(plan *LogicalPlan) (*TestResult, error)
    Plan(plan *LogicalPlan) ([]Action, error)
    Apply(plan *LogicalPlan, actions []Action) error
    Run(plan *LogicalPlan) (*ExecutionResult, error)

    // Sandbox
    CreateSandbox(user string) (*Sandbox, error)
    DestroySandbox(sandboxID string) error
}
```

### Primitive Interface

Every primitive must implement:

```go
type Primitive interface {
    // State management
    GetDesiredState(plan *LogicalPlan) (*State, error)
    GetActualState(ctx context.Context) (*State, error)

    // Actions
    Create(ctx context.Context, state *State) error
    Update(ctx context.Context, desired, actual *State) error
    Delete(ctx context.Context) error

    // For job primitives
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    GetStatus(ctx context.Context) (*Status, error)
}
```

### CoreETL Driver API

```protobuf
service CoreETLDriver {
    // Job management
    rpc CreateJob(CreateJobRequest) returns (Job);
    rpc UpdateJob(UpdateJobRequest) returns (Job);
    rpc DeleteJob(DeleteJobRequest) returns (Empty);
    rpc GetJob(GetJobRequest) returns (Job);

    // Execution
    rpc RunJob(RunJobRequest) returns (Execution);
    rpc StopJob(StopJobRequest) returns (Empty);
    rpc GetExecution(GetExecutionRequest) returns (Execution);
}

message RunJobRequest {
    string job_id = 1;
    string environment = 2;      // "production" or "playground_xxx"
    string user_token = 3;       // Databricks PAT for playground
    map<string, string> params = 4;
}
```

---

## Summary: Where Spark Connect Fits

Based on this architecture:

1. **Current flow has NO Spark Connect** - it's all batch job submission through CoreETL Driver → SJEM → Compute

2. **Spark Connect could enhance the `test` command** (not `run`):
   - Instead of submitting a full job to test SQL syntax
   - Connect to shared cluster via Spark Connect
   - Run EXPLAIN / LIMIT 0 for fast validation

3. **For SK8 migration**:
   - Replace SJEM with Spark Runner
   - CoreETL Primitive calls Spark Runner instead of SJEM
   - Spark Runner creates SparkApplication CRDs
   - No architectural change needed - just swap the compute backend