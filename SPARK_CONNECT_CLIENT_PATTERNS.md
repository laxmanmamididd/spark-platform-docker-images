# Spark Connect Client Connection Patterns

This document describes how clients connect to remote Spark Connect servers through various entry points: APIs, DCP Playground, Notebooks, and CI/CD pipelines.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                        SPARK CONNECT CONNECTION ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              CLIENT LAYER                                          │  │
│  │                                                                                   │  │
│  │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │  │
│  │   │   Direct    │  │    DCP      │  │  Jupyter    │  │   CI/CD     │             │  │
│  │   │  API Call   │  │ Playground  │  │  Notebook   │  │  Pipeline   │             │  │
│  │   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │  │
│  │          │                │                │                │                     │  │
│  │          │                │                │                │                     │  │
│  │          ▼                ▼                ▼                ▼                     │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐    │  │
│  │   │                    SPARK CONNECT CLIENT                                  │    │  │
│  │   │                                                                         │    │  │
│  │   │   Python: pyspark[connect]     Java: spark-connect-client-jvm          │    │  │
│  │   │   Scala: spark-connect-client  Go: spark-connect-go (community)        │    │  │
│  │   │                                                                         │    │  │
│  │   │   Connection URL: sc://<host>:<port>                                    │    │  │
│  │   │   Protocol: gRPC (HTTP/2)                                               │    │  │
│  │   │   Default Port: 15002                                                   │    │  │
│  │   └─────────────────────────────────────────────────────────────────────────┘    │  │
│  │                                         │                                         │  │
│  └─────────────────────────────────────────┼─────────────────────────────────────────┘  │
│                                            │                                            │
│                                            ▼                                            │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                           SPARK GATEWAY                                            │  │
│  │                                                                                   │  │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐    │  │
│  │   │                        gRPC Proxy (port 15002)                           │    │  │
│  │   │                                                                         │    │  │
│  │   │   1. Receive gRPC connection from client                                │    │  │
│  │   │   2. Extract routing headers (team, environment, user)                  │    │  │
│  │   │   3. Authenticate via Okta                                              │    │  │
│  │   │   4. Discover target cluster/driver pod                                 │    │  │
│  │   │   5. Proxy gRPC stream to Spark Connect Server                          │    │  │
│  │   └─────────────────────────────────────────────────────────────────────────┘    │  │
│  │                                                                                   │  │
│  │   Features:                                                                       │  │
│  │   • Cluster discovery by team/environment                                        │  │
│  │   • Hot cluster pool for fast startup                                            │  │
│  │   • Load balancing across driver pods                                            │  │
│  │   • Session affinity (sticky sessions)                                           │  │
│  │   • Automatic failover                                                           │  │
│  │                                                                                   │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                            │                                            │
│           ┌────────────────────────────────┼────────────────────────────────┐           │
│           │                                │                                │           │
│           ▼                                ▼                                ▼           │
│  ┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐  │
│  │  Hot Cluster 1  │              │  Hot Cluster 2  │              │  On-Demand      │  │
│  │  (Pre-warmed)   │              │  (Pre-warmed)   │              │  Cluster        │  │
│  │                 │              │                 │              │                 │  │
│  │  ┌───────────┐  │              │  ┌───────────┐  │              │  ┌───────────┐  │  │
│  │  │  Driver   │  │              │  │  Driver   │  │              │  │  Driver   │  │  │
│  │  │  Pod      │  │              │  │  Pod      │  │              │  │  Pod      │  │  │
│  │  │           │  │              │  │           │  │              │  │           │  │  │
│  │  │ SC Server │  │              │  │ SC Server │  │              │  │ SC Server │  │  │
│  │  │ (:15002)  │  │              │  │ (:15002)  │  │              │  │ (:15002)  │  │  │
│  │  └───────────┘  │              └───────────┘  │              │  └───────────┘  │  │
│  │                 │              │                 │              │                 │  │
│  │  Executors...   │              │  Executors...   │              │  Executors...   │  │
│  └─────────────────┘              └─────────────────┘              └─────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Connection Patterns

### Pattern 1: Direct API Connection

For services that need programmatic Spark access.

```
┌─────────────────────────────────────────────────────────────────┐
│                    DIRECT API CONNECTION                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Service/Application                                           │
│          │                                                      │
│          │  1. Create SparkSession.builder().remote(url)        │
│          │                                                      │
│          ▼                                                      │
│   ┌─────────────────┐                                           │
│   │ Spark Connect   │                                           │
│   │ Client Library  │                                           │
│   │                 │                                           │
│   │ • Serialize ops │                                           │
│   │ • gRPC channel  │                                           │
│   └────────┬────────┘                                           │
│            │                                                    │
│            │  2. gRPC connect to sc://gateway:15002             │
│            │     Headers: x-team, x-environment, x-user         │
│            │                                                    │
│            ▼                                                    │
│   ┌─────────────────┐                                           │
│   │  Spark Gateway  │                                           │
│   │                 │                                           │
│   │  3. Auth + Route│                                           │
│   └────────┬────────┘                                           │
│            │                                                    │
│            │  4. Proxy to driver pod                            │
│            │                                                    │
│            ▼                                                    │
│   ┌─────────────────┐                                           │
│   │ Spark Connect   │                                           │
│   │ Server (Driver) │                                           │
│   │                 │                                           │
│   │  5. Execute ops │                                           │
│   └─────────────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Python Example:**

```python
from pyspark.sql import SparkSession

# Direct connection via Spark Gateway
spark = SparkSession.builder \
    .remote("sc://spark-gateway.doordash.team:15002") \
    .config("spark.connect.grpc.header.x-doordash-team", "feature-engineering") \
    .config("spark.connect.grpc.header.x-doordash-environment", "prod") \
    .config("spark.connect.grpc.header.x-doordash-user", "service-account") \
    .appName("my-api-service") \
    .getOrCreate()

# Use Spark as normal
df = spark.sql("SELECT * FROM pedregal.feature_store.user_features LIMIT 10")
df.show()

spark.stop()
```

**Java Example:**

```java
import org.apache.spark.sql.SparkSession;

public class SparkConnectApiClient {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .remote("sc://spark-gateway.doordash.team:15002")
            .config("spark.connect.grpc.header.x-doordash-team", "feature-engineering")
            .config("spark.connect.grpc.header.x-doordash-environment", "prod")
            .appName("java-api-service")
            .getOrCreate();

        spark.sql("SELECT * FROM pedregal.raw.events LIMIT 10").show();

        spark.stop();
    }
}
```

---

### Pattern 2: DCP Playground (Interactive Sandbox)

For developers testing manifests before deployment.

```
┌─────────────────────────────────────────────────────────────────┐
│                    DCP PLAYGROUND CONNECTION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  DCP Playground UI                       │   │
│   │                                                         │   │
│   │   ┌───────────────────────────────────────────────┐     │   │
│   │   │  manifest.yaml editor                          │     │   │
│   │   │                                               │     │   │
│   │   │  apiVersion: dcp.pedregal.doordash.com/v1    │     │   │
│   │   │  kind: SparkJob                               │     │   │
│   │   │  metadata:                                    │     │   │
│   │   │    name: my-feature-job                       │     │   │
│   │   │  spec:                                        │     │   │
│   │   │    engine:                                    │     │   │
│   │   │      version: "3.5"                           │     │   │
│   │   │    ...                                        │     │   │
│   │   └───────────────────────────────────────────────┘     │   │
│   │                                                         │   │
│   │   [ Environment: local ▼ ]  [ Run ▶ ]  [ Stop ⏹ ]       │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 1. User clicks "Run"              │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                Playground Backend                        │   │
│   │                                                         │   │
│   │   def run_sandbox(manifest, environment, user):         │   │
│   │       # Parse manifest                                  │   │
│   │       job_spec = parse_manifest(manifest)               │   │
│   │                                                         │   │
│   │       # Create Spark Connect session                    │   │
│   │       spark = create_session(                           │   │
│   │           team=manifest.metadata.team,                  │   │
│   │           environment=environment,                      │   │
│   │           user=user                                     │   │
│   │       )                                                 │   │
│   │                                                         │   │
│   │       # Execute job                                     │   │
│   │       result = execute_coreetl(spark, job_spec)         │   │
│   │                                                         │   │
│   │       return result                                     │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 2. Connect via Spark Gateway      │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Spark Gateway                           │   │
│   │                                                         │   │
│   │   Route by headers:                                     │   │
│   │   • x-doordash-team: feature-engineering                │   │
│   │   • x-doordash-environment: local                       │   │
│   │   • x-doordash-user: user@doordash.com                  │   │
│   │                                                         │   │
│   │   → Route to: sjns-playground-local-{user-hash}         │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 3. Proxy to hot cluster           │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │           Hot Cluster (Pre-warmed)                       │   │
│   │                                                         │   │
│   │   Namespace: sjns-playground-local-abc123               │   │
│   │   Catalog: pedregal-dev                                 │   │
│   │   Data: Sample/filtered                                 │   │
│   │                                                         │   │
│   │   ┌─────────────────────────────────────────────────┐   │   │
│   │   │  Driver Pod                                      │   │   │
│   │   │  • Spark Connect Server (:15002)                │   │   │
│   │   │  • CoreETL execution                            │   │   │
│   │   │  • Results streamed back                        │   │   │
│   │   └─────────────────────────────────────────────────┘   │   │
│   │                                                         │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Playground Backend Code:**

```python
# dcp_playground/sandbox.py
from pyspark.sql import SparkSession
from typing import Optional
import yaml


class PlaygroundSandbox:
    """
    Executes DCP manifests in sandbox environment.
    """

    GATEWAY_ENDPOINTS = {
        "local": "spark-gateway-dev.doordash.team:15002",
        "test": "spark-gateway-test.doordash.team:15002",
        "staging": "spark-gateway-staging.doordash.team:15002",
        "prod": "spark-gateway.doordash.team:15002",
    }

    def __init__(self, user_id: str, team: str):
        self.user_id = user_id
        self.team = team
        self._spark: Optional[SparkSession] = None

    def run(self, manifest_yaml: str, environment: str = "local") -> dict:
        """
        Execute a manifest in the specified environment.

        Args:
            manifest_yaml: Raw YAML string of DCP manifest
            environment: local, test, staging, or prod

        Returns:
            Execution result with status, logs, and output
        """
        # Parse manifest
        manifest = yaml.safe_load(manifest_yaml)

        # Create Spark session
        spark = self._create_session(environment)

        try:
            # Execute based on job type
            if "coreEtl" in manifest.get("spec", {}):
                result = self._execute_coreetl(spark, manifest)
            else:
                result = self._execute_sql(spark, manifest)

            return {
                "status": "SUCCESS",
                "result": result,
                "spark_ui": self._get_spark_ui_url(),
            }

        except Exception as e:
            return {
                "status": "FAILED",
                "error": str(e),
                "spark_ui": self._get_spark_ui_url(),
            }

        finally:
            spark.stop()

    def _create_session(self, environment: str) -> SparkSession:
        """Create Spark Connect session through Spark Gateway."""
        gateway = self.GATEWAY_ENDPOINTS[environment]

        self._spark = SparkSession.builder \
            .remote(f"sc://{gateway}") \
            .config("spark.connect.grpc.header.x-doordash-team", self.team) \
            .config("spark.connect.grpc.header.x-doordash-environment", environment) \
            .config("spark.connect.grpc.header.x-doordash-user", self.user_id) \
            .appName(f"playground-{self.user_id}") \
            .getOrCreate()

        return self._spark

    def _execute_coreetl(self, spark: SparkSession, manifest: dict) -> dict:
        """Execute CoreETL job spec."""
        job_spec = manifest["spec"]["coreEtl"]["jobSpec"]

        # Process sources
        sources = {}
        for source in job_spec.get("sources", []):
            table = f"{source['catalog']}.{source['database']}.{source['table']}"
            df = spark.table(table)

            if "filter" in source:
                df = df.filter(source["filter"])

            sources[source["name"]] = df
            df.createOrReplaceTempView(source["name"])

        # Execute transformations
        for transform in job_spec.get("transformations", []):
            if transform["type"] == "sql":
                result = spark.sql(transform["query"])
                result.createOrReplaceTempView(transform["name"])

        # Preview sink output (don't write in playground)
        sink = job_spec.get("sinks", [{}])[0]
        if sink:
            final_df = spark.table(sink.get("name", transform["name"]))
            return {
                "preview": final_df.limit(100).toPandas().to_dict(),
                "count": final_df.count(),
                "schema": final_df.schema.json(),
            }

        return {}

    def _execute_sql(self, spark: SparkSession, manifest: dict) -> dict:
        """Execute raw SQL query."""
        query = manifest.get("spec", {}).get("sql", "SELECT 1")
        result = spark.sql(query)

        return {
            "preview": result.limit(100).toPandas().to_dict(),
            "count": result.count(),
        }

    def _get_spark_ui_url(self) -> Optional[str]:
        """Get Spark UI URL for debugging."""
        if self._spark:
            return self._spark.sparkContext.uiWebUrl
        return None


# Usage in DCP Playground API
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/api/v1/playground/run", methods=["POST"])
def run_playground():
    """API endpoint for running manifests in playground."""
    data = request.json

    sandbox = PlaygroundSandbox(
        user_id=data["user_id"],
        team=data["team"],
    )

    result = sandbox.run(
        manifest_yaml=data["manifest"],
        environment=data.get("environment", "local"),
    )

    return jsonify(result)
```

---

### Pattern 3: Jupyter Notebook (Interactive Analysis)

For data scientists doing ad-hoc analysis.

```
┌─────────────────────────────────────────────────────────────────┐
│                    JUPYTER NOTEBOOK CONNECTION                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  JupyterHub / JupyterLab                 │   │
│   │                                                         │   │
│   │   In [1]: from spark_connect_helper import get_spark    │   │
│   │                                                         │   │
│   │   In [2]: spark = get_spark(                            │   │
│   │               team="data-science",                      │   │
│   │               environment="staging"                     │   │
│   │           )                                             │   │
│   │           Connected to Spark 3.5.0                      │   │
│   │                                                         │   │
│   │   In [3]: df = spark.sql("""                            │   │
│   │               SELECT user_id, COUNT(*) as orders        │   │
│   │               FROM pedregal.raw.orders                  │   │
│   │               WHERE ds >= '2024-01-01'                  │   │
│   │               GROUP BY user_id                          │   │
│   │           """)                                          │   │
│   │                                                         │   │
│   │   In [4]: df.show()                                     │   │
│   │           +--------+--------+                           │   │
│   │           | user_id|  orders|                           │   │
│   │           +--------+--------+                           │   │
│   │           |     123|      15|                           │   │
│   │           |     456|      23|                           │   │
│   │           ...                                           │   │
│   │                                                         │   │
│   │   In [5]: pdf = df.toPandas()  # Pull to local          │   │
│   │                                                         │   │
│   │   In [6]: pdf.plot(kind='bar')  # Visualize locally     │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ gRPC via Spark Gateway            │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Spark Gateway                           │   │
│   │                                                         │   │
│   │   → Authenticate notebook user via Okta                 │   │
│   │   → Route to team's staging cluster                     │   │
│   │   → Session affinity for notebook session               │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │           Team Cluster (Staging)                         │   │
│   │                                                         │   │
│   │   Namespace: sjns-data-science-staging                  │   │
│   │   Catalog: pedregal-staging                             │   │
│   │                                                         │   │
│   │   Driver Pod:                                           │   │
│   │   • Long-running (for interactive session)              │   │
│   │   • Spark Connect Server (:15002)                       │   │
│   │   • Session state maintained                            │   │
│   │                                                         │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Jupyter Helper Library:**

```python
# spark_connect_helper.py
"""
Helper library for connecting to Spark from Jupyter notebooks.
Install: pip install pyspark[connect]
"""

from pyspark.sql import SparkSession
from typing import Optional
import os


def get_spark(
    team: str,
    environment: str = "staging",
    app_name: Optional[str] = None,
) -> SparkSession:
    """
    Get a Spark session connected through Spark Gateway.

    Args:
        team: Your team name (e.g., "data-science", "feature-engineering")
        environment: Target environment (local, staging, prod)
        app_name: Optional application name

    Returns:
        SparkSession connected to remote cluster

    Example:
        >>> spark = get_spark("data-science", "staging")
        >>> spark.sql("SELECT 1").show()
    """
    # Get user from environment
    user = os.getenv("JUPYTERHUB_USER", os.getenv("USER", "unknown"))

    # Gateway endpoints
    gateways = {
        "local": "spark-gateway-dev.doordash.team",
        "staging": "spark-gateway-staging.doordash.team",
        "prod": "spark-gateway.doordash.team",
    }

    gateway = gateways.get(environment, gateways["staging"])
    app = app_name or f"jupyter-{user}-{environment}"

    # Build session
    spark = SparkSession.builder \
        .remote(f"sc://{gateway}:15002") \
        .config("spark.connect.grpc.header.x-doordash-team", team) \
        .config("spark.connect.grpc.header.x-doordash-environment", environment) \
        .config("spark.connect.grpc.header.x-doordash-user", user) \
        .appName(app) \
        .getOrCreate()

    print(f"Connected to Spark {spark.version}")
    print(f"Team: {team}, Environment: {environment}")

    return spark


def show_catalogs(spark: SparkSession):
    """Show available catalogs."""
    spark.sql("SHOW CATALOGS").show()


def show_databases(spark: SparkSession, catalog: str = "pedregal"):
    """Show databases in a catalog."""
    spark.sql(f"SHOW DATABASES IN {catalog}").show()


def show_tables(spark: SparkSession, catalog: str, database: str):
    """Show tables in a database."""
    spark.sql(f"SHOW TABLES IN {catalog}.{database}").show()


def describe_table(spark: SparkSession, table: str):
    """Describe a table schema."""
    spark.sql(f"DESCRIBE TABLE {table}").show(truncate=False)
```

**Notebook Example:**

```python
# Cell 1: Setup
from spark_connect_helper import get_spark, show_databases, show_tables

# Connect to staging cluster
spark = get_spark(team="data-science", environment="staging")

# Cell 2: Explore data
show_databases(spark, "pedregal")

# Cell 3: Query data
df = spark.sql("""
    SELECT
        DATE(event_timestamp) as date,
        COUNT(*) as events,
        COUNT(DISTINCT user_id) as users
    FROM pedregal.raw.events
    WHERE ds >= date_sub(current_date(), 7)
    GROUP BY DATE(event_timestamp)
    ORDER BY date
""")

df.show()

# Cell 4: Pull to Pandas for visualization
import pandas as pd
import matplotlib.pyplot as plt

pdf = df.toPandas()
pdf.plot(x='date', y=['events', 'users'], kind='line', figsize=(12, 6))
plt.title('Daily Events and Users')
plt.show()

# Cell 5: Cleanup
spark.stop()
```

---

### Pattern 4: CI/CD Pipeline (Automated Testing)

For automated testing in GitHub Actions / BuildKite.

```
┌─────────────────────────────────────────────────────────────────┐
│                    CI/CD PIPELINE CONNECTION                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  GitHub Actions / BuildKite              │   │
│   │                                                         │   │
│   │   jobs:                                                 │   │
│   │     integration-test:                                   │   │
│   │       runs-on: ubuntu-latest                            │   │
│   │       env:                                              │   │
│   │         SPARK_CONNECT_TEAM: feature-engineering         │   │
│   │         SPARK_CONNECT_ENV: test                         │   │
│   │       steps:                                            │   │
│   │         - run: pytest tests/integration/                │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 1. Test starts                    │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Test Code (pytest)                      │   │
│   │                                                         │   │
│   │   @pytest.fixture(scope="module")                       │   │
│   │   def spark():                                          │   │
│   │       session = SparkSession.builder \                  │   │
│   │           .remote(f"sc://{GATEWAY}:15002") \            │   │
│   │           .config("x-team", TEAM) \                     │   │
│   │           .config("x-env", "test") \                    │   │
│   │           .getOrCreate()                                │   │
│   │       yield session                                     │   │
│   │       session.stop()                                    │   │
│   │                                                         │   │
│   │   def test_user_features_schema(spark):                 │   │
│   │       df = spark.table("pedregal.feature_store.users")  │   │
│   │       assert "user_id" in df.columns                    │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 2. Connect to test gateway        │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │             Spark Gateway (Test Environment)             │   │
│   │                                                         │   │
│   │   • Dedicated test clusters                             │   │
│   │   • Isolated from staging/prod                          │   │
│   │   • Auto-cleanup after TTL                              │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ 3. Execute tests                  │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              Test Cluster                                │   │
│   │                                                         │   │
│   │   Namespace: sjns-playground-test                       │   │
│   │   Catalog: pedregal-test                                │   │
│   │   Data: Test fixtures / anonymized samples              │   │
│   │   TTL: 1 hour per job                                   │   │
│   │                                                         │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**GitHub Actions Workflow:**

```yaml
# .github/workflows/spark-tests.yaml
name: Spark Integration Tests

on:
  pull_request:
    paths:
      - 'jobs/**'
      - 'tests/**'

env:
  SPARK_CONNECT_GATEWAY: spark-gateway-test.doordash.team:15002
  SPARK_CONNECT_TEAM: ${{ github.repository_owner }}
  SPARK_CONNECT_ENV: test

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pyspark[connect] pytest pandas

      - name: Run integration tests
        run: |
          pytest tests/integration/ -v --tb=short
        env:
          SPARK_CONNECT_USER: github-actions-${{ github.run_id }}

      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results
          path: test-results.xml
```

**Test Code:**

```python
# tests/integration/test_features.py
import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """
    Create Spark session for test module.
    Connects through Spark Gateway to test cluster.
    """
    gateway = os.environ.get("SPARK_CONNECT_GATEWAY", "spark-gateway-test.doordash.team:15002")
    team = os.environ.get("SPARK_CONNECT_TEAM", "test")
    env = os.environ.get("SPARK_CONNECT_ENV", "test")
    user = os.environ.get("SPARK_CONNECT_USER", "pytest")

    session = SparkSession.builder \
        .remote(f"sc://{gateway}") \
        .config("spark.connect.grpc.header.x-doordash-team", team) \
        .config("spark.connect.grpc.header.x-doordash-environment", env) \
        .config("spark.connect.grpc.header.x-doordash-user", user) \
        .appName(f"integration-test-{user}") \
        .getOrCreate()

    yield session
    session.stop()


class TestDataAvailability:
    """Test that required tables exist and are accessible."""

    def test_events_table_exists(self, spark):
        """Verify events table is accessible."""
        result = spark.sql("DESCRIBE TABLE pedregal.raw.events").collect()
        assert len(result) > 0

    def test_can_query_events(self, spark):
        """Verify we can query events."""
        result = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM pedregal.raw.events
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['cnt'] >= 0


class TestFeatureSchema:
    """Test feature table schemas."""

    def test_user_features_schema(self, spark):
        """Verify user features has expected columns."""
        result = spark.sql("DESCRIBE TABLE pedregal.feature_store.user_features").collect()
        columns = [row['col_name'] for row in result]

        expected = ['user_id', 'total_events', 'ds']
        for col in expected:
            assert col in columns, f"Missing column: {col}"

    def test_user_features_not_empty(self, spark):
        """Verify user features has recent data."""
        result = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM pedregal.feature_store.user_features
            WHERE ds >= date_sub(current_date(), 7)
        """).collect()

        assert result[0]['cnt'] > 0, "No recent user features found"
```

---

### Pattern 5: Metaflow Pipeline (ML Workflows)

For ML pipelines running on Kubernetes.

```
┌─────────────────────────────────────────────────────────────────┐
│                    METAFLOW PIPELINE CONNECTION                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Metaflow Flow                           │   │
│   │                                                         │   │
│   │   class FeatureFlow(FlowSpec):                          │   │
│   │                                                         │   │
│   │       @kubernetes(cpu=2, memory=4096)                   │   │
│   │       @step                                             │   │
│   │       def extract_features(self):                       │   │
│   │           spark = get_spark_session()                   │   │
│   │           self.features = spark.table(...).toPandas()   │   │
│   │           self.next(self.train)                         │   │
│   │                                                         │   │
│   │       @kubernetes(cpu=4, memory=8192, gpu=1)            │   │
│   │       @step                                             │   │
│   │       def train(self):                                  │   │
│   │           model = train_model(self.features)            │   │
│   │           self.next(self.end)                           │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ Metaflow step runs in K8s pod     │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │           Metaflow K8s Pod (ML Cluster)                  │   │
│   │                                                         │   │
│   │   def get_spark_session():                              │   │
│   │       return SparkSession.builder \                     │   │
│   │           .remote("sc://spark-gateway:15002") \         │   │
│   │           .config("x-team", "ml-platform") \            │   │
│   │           .config("x-env", "prod") \                    │   │
│   │           .getOrCreate()                                │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              │ gRPC (cross-cluster network)      │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                  Spark Gateway                           │   │
│   │                                                         │   │
│   │   Network Policy: Allow ingress from ML cluster         │   │
│   │   Auth: Service account token                           │   │
│   │                                                         │   │
│   └──────────────────────────┬──────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │           Spark Cluster (Data Cluster)                   │   │
│   │                                                         │   │
│   │   • Read features from Unity Catalog                    │   │
│   │   • Process data using Spark                            │   │
│   │   • Return results via Spark Connect                    │   │
│   │                                                         │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Metaflow Code:**

```python
# feature_flow.py
from metaflow import FlowSpec, step, kubernetes, Parameter
from pyspark.sql import SparkSession


def get_spark_session(team: str, environment: str) -> SparkSession:
    """Get Spark session from Metaflow pod."""
    gateways = {
        "staging": "spark-gateway-staging.doordash.team",
        "prod": "spark-gateway.doordash.team",
    }

    return SparkSession.builder \
        .remote(f"sc://{gateways[environment]}:15002") \
        .config("spark.connect.grpc.header.x-doordash-team", team) \
        .config("spark.connect.grpc.header.x-doordash-environment", environment) \
        .config("spark.connect.grpc.header.x-doordash-user", "metaflow-pipeline") \
        .appName("metaflow-feature-pipeline") \
        .getOrCreate()


class FeatureEngineeringFlow(FlowSpec):
    """
    ML pipeline that extracts features via Spark Connect.
    """

    environment = Parameter(
        "environment",
        help="Target environment",
        default="staging"
    )

    date = Parameter(
        "date",
        help="Date partition to process",
        default="2024-01-01"
    )

    @step
    def start(self):
        """Initialize pipeline."""
        print(f"Starting feature pipeline for {self.date}")
        self.next(self.extract_features)

    @kubernetes(cpu=2, memory=4096)
    @step
    def extract_features(self):
        """Extract features from Spark via Spark Connect."""
        spark = get_spark_session("ml-platform", self.environment)

        try:
            # Query features via Spark Connect
            self.user_features = spark.sql(f"""
                SELECT
                    user_id,
                    total_events,
                    views,
                    clicks,
                    purchases,
                    click_through_rate
                FROM pedregal.feature_store.user_features
                WHERE ds = '{self.date}'
            """).toPandas()

            print(f"Extracted {len(self.user_features)} user features")

        finally:
            spark.stop()

        self.next(self.train_model)

    @kubernetes(cpu=4, memory=8192, gpu=1)
    @step
    def train_model(self):
        """Train ML model on extracted features."""
        import sklearn.ensemble as ensemble

        X = self.user_features.drop(columns=['user_id'])
        # ... training logic

        self.next(self.end)

    @step
    def end(self):
        """Pipeline complete."""
        print("Feature engineering pipeline complete!")


if __name__ == '__main__':
    FeatureEngineeringFlow()
```

---

---

## Server-Side Implementation

This section covers the server-side components that enable Spark Connect:
1. **Spark Gateway** - gRPC proxy that routes client connections
2. **Spark Connect Server** - Embedded in Driver Pod (port 15002)
3. **Spark Kubernetes Operator** - Creates SparkApplication CRDs
4. **Hot Cluster Manager** - Pre-warms clusters for low latency

### Server Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              SERVER-SIDE ARCHITECTURE                                                │
├─────────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                     │
│   CLIENT REQUEST                                                                                    │
│        │                                                                                            │
│        │  gRPC (sc://gateway:15002)                                                                 │
│        │  Headers: x-team, x-env, x-user                                                            │
│        │                                                                                            │
│        ▼                                                                                            │
│   ┌───────────────────────────────────────────────────────────────────────────────────────────┐    │
│   │                            SPARK GATEWAY (Go Service)                                      │    │
│   │                                                                                           │    │
│   │   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐  │    │
│   │   │  gRPC Listener  │   │  Auth Handler   │   │  Router/LB      │   │  gRPC Proxy     │  │    │
│   │   │  (:15002)       │──▶│  (Okta OIDC)    │──▶│  (Cluster Disc) │──▶│  (Bidirectional)│  │    │
│   │   └─────────────────┘   └─────────────────┘   └─────────────────┘   └─────────────────┘  │    │
│   │                                                                                           │    │
│   │   Features:                                                                               │    │
│   │   • TLS termination                    • Team-based routing                               │    │
│   │   • Auth via Okta                      • Hot cluster assignment                           │    │
│   │   • Session affinity                   • Automatic failover                               │    │
│   │   • Metrics (Prometheus)               • Audit logging                                    │    │
│   │                                                                                           │    │
│   └────────────────────────────────────────────────┬──────────────────────────────────────────┘    │
│                                                    │                                                │
│                                                    │  Internal gRPC                                 │
│                                                    │  (mTLS, K8s Service mesh)                      │
│                                                    │                                                │
│                                                    ▼                                                │
│   ┌───────────────────────────────────────────────────────────────────────────────────────────┐    │
│   │                            KUBERNETES CLUSTER                                              │    │
│   │                                                                                           │    │
│   │   Namespace: sjns-{team}-{environment}                                                    │    │
│   │                                                                                           │    │
│   │   ┌───────────────────────────────────────────────────────────────────────────────────┐  │    │
│   │   │                         DRIVER POD                                                 │  │    │
│   │   │                                                                                   │  │    │
│   │   │   ┌─────────────────────────────────────────────────────────────────────────┐    │  │    │
│   │   │   │                  SPARK CONNECT SERVER                                    │    │  │    │
│   │   │   │                                                                         │    │  │    │
│   │   │   │   Port: 15002 (gRPC)                                                    │    │  │    │
│   │   │   │                                                                         │    │  │    │
│   │   │   │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │    │  │    │
│   │   │   │   │ ExecutePlan │  │ AnalyzePlan │  │ Config      │  │ Interrupt   │   │    │  │    │
│   │   │   │   │ Handler     │  │ Handler     │  │ Handler     │  │ Handler     │   │    │  │    │
│   │   │   │   └──────┬──────┘  └──────┬──────┘  └─────────────┘  └─────────────┘   │    │  │    │
│   │   │   │          │                │                                             │    │  │    │
│   │   │   │          ▼                ▼                                             │    │  │    │
│   │   │   │   ┌───────────────────────────────────────────────────────────────┐    │    │  │    │
│   │   │   │   │                    SPARK SESSION                               │    │    │  │    │
│   │   │   │   │                                                               │    │    │  │    │
│   │   │   │   │   • SQL Parser        • Catalyst Optimizer                    │    │    │  │    │
│   │   │   │   │   • DataFrame API     • Physical Planning                     │    │    │  │    │
│   │   │   │   │   • Unity Catalog     • Task Scheduler                        │    │    │  │    │
│   │   │   │   └───────────────────────────────────────────────────────────────┘    │    │  │    │
│   │   │   │                                                                         │    │  │    │
│   │   │   └─────────────────────────────────────────────────────────────────────────┘    │  │    │
│   │   │                                                                                   │  │    │
│   │   │   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                              │  │    │
│   │   │   │ Spark UI    │  │ OTel Sidecar│  │ Fluent Bit  │                              │  │    │
│   │   │   │ (:4040)     │  │ (Metrics)   │  │ (Logs)      │                              │  │    │
│   │   │   └─────────────┘  └─────────────┘  └─────────────┘                              │  │    │
│   │   │                                                                                   │  │    │
│   │   └───────────────────────────────────────────────────────────────────────────────────┘  │    │
│   │                                                                                           │    │
│   │   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                                │    │
│   │   │ Executor Pod 1│  │ Executor Pod 2│  │ Executor Pod N│                                │    │
│   │   │               │  │               │  │               │                                │    │
│   │   │ Tasks...      │  │ Tasks...      │  │ Tasks...      │                                │    │
│   │   └───────────────┘  └───────────────┘  └───────────────┘                                │    │
│   │                                                                                           │    │
│   └───────────────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Component 1: Spark Gateway (Go Service)

The Spark Gateway is the single entry point for all Spark Connect clients.

**spark_gateway.go:**

```go
// spark-gateway/gateway.go
package main

import (
    "context"
    "crypto/tls"
    "net"
    "sync"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials"
    "google.golang.org/grpc/metadata"
    "google.golang.org/grpc/status"

    pb "github.com/apache/spark-connect-go/v35/spark/connect"
    "github.com/doordash/pedregal/pkg/auth"
    "github.com/doordash/pedregal/pkg/discovery"
    "github.com/doordash/pedregal/pkg/metrics"
    "go.uber.org/zap"
)

// SparkGateway handles Spark Connect gRPC proxy
type SparkGateway struct {
    pb.UnimplementedSparkConnectServiceServer

    logger        *zap.Logger
    authenticator *auth.OktaAuthenticator
    discovery     *discovery.ClusterDiscovery
    hotPoolMgr    *HotClusterPoolManager
    sessionMgr    *SessionManager

    // Connection pool to driver pods
    driverConns   sync.Map // map[string]*grpc.ClientConn
}

// Config for Spark Gateway
type GatewayConfig struct {
    ListenAddr       string        `yaml:"listen_addr"`       // :15002
    TLSCertFile      string        `yaml:"tls_cert_file"`
    TLSKeyFile       string        `yaml:"tls_key_file"`
    OktaIssuer       string        `yaml:"okta_issuer"`
    OktaClientID     string        `yaml:"okta_client_id"`
    SessionTTL       time.Duration `yaml:"session_ttl"`       // 30m
    HotPoolSize      int           `yaml:"hot_pool_size"`     // 10
    MaxConnsPerHost  int           `yaml:"max_conns_per_host"` // 100
}

func NewSparkGateway(cfg *GatewayConfig, logger *zap.Logger) (*SparkGateway, error) {
    authenticator, err := auth.NewOktaAuthenticator(cfg.OktaIssuer, cfg.OktaClientID)
    if err != nil {
        return nil, fmt.Errorf("failed to create authenticator: %w", err)
    }

    discovery, err := discovery.NewClusterDiscovery()
    if err != nil {
        return nil, fmt.Errorf("failed to create discovery: %w", err)
    }

    return &SparkGateway{
        logger:        logger,
        authenticator: authenticator,
        discovery:     discovery,
        hotPoolMgr:    NewHotClusterPoolManager(cfg.HotPoolSize),
        sessionMgr:    NewSessionManager(cfg.SessionTTL),
    }, nil
}

// Start the gRPC server
func (g *SparkGateway) Start(cfg *GatewayConfig) error {
    // Load TLS credentials
    creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
    if err != nil {
        return fmt.Errorf("failed to load TLS credentials: %w", err)
    }

    // Create gRPC server with interceptors
    server := grpc.NewServer(
        grpc.Creds(creds),
        grpc.ChainUnaryInterceptor(
            g.metricsInterceptor,
            g.authInterceptor,
            g.routingInterceptor,
        ),
        grpc.ChainStreamInterceptor(
            g.streamMetricsInterceptor,
            g.streamAuthInterceptor,
            g.streamRoutingInterceptor,
        ),
        grpc.MaxRecvMsgSize(256 * 1024 * 1024), // 256MB for large plans
        grpc.MaxSendMsgSize(256 * 1024 * 1024),
    )

    pb.RegisterSparkConnectServiceServer(server, g)

    listener, err := net.Listen("tcp", cfg.ListenAddr)
    if err != nil {
        return fmt.Errorf("failed to listen: %w", err)
    }

    g.logger.Info("Spark Gateway starting", zap.String("addr", cfg.ListenAddr))
    return server.Serve(listener)
}

// ExecutePlan proxies ExecutePlan requests to the target driver
func (g *SparkGateway) ExecutePlan(req *pb.ExecutePlanRequest, stream pb.SparkConnectService_ExecutePlanServer) error {
    ctx := stream.Context()

    // Extract routing info from metadata
    routing, err := g.extractRoutingInfo(ctx)
    if err != nil {
        return status.Errorf(codes.InvalidArgument, "missing routing headers: %v", err)
    }

    // Get or assign cluster for this session
    target, err := g.getTargetDriver(ctx, routing, req.SessionId)
    if err != nil {
        return status.Errorf(codes.Unavailable, "no available cluster: %v", err)
    }

    g.logger.Debug("Proxying ExecutePlan",
        zap.String("session_id", req.SessionId),
        zap.String("team", routing.Team),
        zap.String("environment", routing.Environment),
        zap.String("target", target.Address),
    )

    // Get connection to driver pod
    conn, err := g.getDriverConnection(target.Address)
    if err != nil {
        return status.Errorf(codes.Unavailable, "failed to connect to driver: %v", err)
    }

    // Create client and proxy request
    client := pb.NewSparkConnectServiceClient(conn)

    proxyStream, err := client.ExecutePlan(ctx, req)
    if err != nil {
        return err
    }

    // Stream responses back to client
    for {
        resp, err := proxyStream.Recv()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }

        if err := stream.Send(resp); err != nil {
            return err
        }
    }
}

// AnalyzePlan proxies AnalyzePlan requests
func (g *SparkGateway) AnalyzePlan(ctx context.Context, req *pb.AnalyzePlanRequest) (*pb.AnalyzePlanResponse, error) {
    routing, err := g.extractRoutingInfo(ctx)
    if err != nil {
        return nil, status.Errorf(codes.InvalidArgument, "missing routing headers: %v", err)
    }

    target, err := g.getTargetDriver(ctx, routing, req.SessionId)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "no available cluster: %v", err)
    }

    conn, err := g.getDriverConnection(target.Address)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "failed to connect to driver: %v", err)
    }

    client := pb.NewSparkConnectServiceClient(conn)
    return client.AnalyzePlan(ctx, req)
}

// Config proxies Config requests
func (g *SparkGateway) Config(ctx context.Context, req *pb.ConfigRequest) (*pb.ConfigResponse, error) {
    routing, err := g.extractRoutingInfo(ctx)
    if err != nil {
        return nil, status.Errorf(codes.InvalidArgument, "missing routing headers: %v", err)
    }

    target, err := g.getTargetDriver(ctx, routing, req.SessionId)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "no available cluster: %v", err)
    }

    conn, err := g.getDriverConnection(target.Address)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "failed to connect to driver: %v", err)
    }

    client := pb.NewSparkConnectServiceClient(conn)
    return client.Config(ctx, req)
}

// Interrupt proxies Interrupt requests
func (g *SparkGateway) Interrupt(ctx context.Context, req *pb.InterruptRequest) (*pb.InterruptResponse, error) {
    routing, err := g.extractRoutingInfo(ctx)
    if err != nil {
        return nil, status.Errorf(codes.InvalidArgument, "missing routing headers: %v", err)
    }

    target, err := g.getTargetDriver(ctx, routing, req.SessionId)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "no available cluster: %v", err)
    }

    conn, err := g.getDriverConnection(target.Address)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "failed to connect to driver: %v", err)
    }

    client := pb.NewSparkConnectServiceClient(conn)
    return client.Interrupt(ctx, req)
}

// RoutingInfo extracted from gRPC metadata
type RoutingInfo struct {
    Team        string
    Environment string
    User        string
}

// extractRoutingInfo gets routing headers from gRPC metadata
func (g *SparkGateway) extractRoutingInfo(ctx context.Context) (*RoutingInfo, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, fmt.Errorf("no metadata in context")
    }

    team := getMetadataValue(md, "x-doordash-team")
    env := getMetadataValue(md, "x-doordash-environment")
    user := getMetadataValue(md, "x-doordash-user")

    if team == "" || env == "" {
        return nil, fmt.Errorf("missing required headers: x-doordash-team, x-doordash-environment")
    }

    return &RoutingInfo{
        Team:        team,
        Environment: env,
        User:        user,
    }, nil
}

func getMetadataValue(md metadata.MD, key string) string {
    values := md.Get(key)
    if len(values) > 0 {
        return values[0]
    }
    return ""
}

// DriverTarget represents a driver pod to proxy to
type DriverTarget struct {
    Address   string // driver-pod-ip:15002
    Namespace string
    SessionID string
}

// getTargetDriver finds or assigns a driver for the session
func (g *SparkGateway) getTargetDriver(ctx context.Context, routing *RoutingInfo, sessionID string) (*DriverTarget, error) {
    // Check if session already has an assigned driver (session affinity)
    if target := g.sessionMgr.GetTarget(sessionID); target != nil {
        return target, nil
    }

    // Build namespace from team/environment
    namespace := fmt.Sprintf("sjns-%s-%s", routing.Team, routing.Environment)

    // Try to get a hot cluster first
    if hotCluster := g.hotPoolMgr.GetAvailable(namespace); hotCluster != nil {
        target := &DriverTarget{
            Address:   fmt.Sprintf("%s:15002", hotCluster.DriverIP),
            Namespace: namespace,
            SessionID: sessionID,
        }
        g.sessionMgr.SetTarget(sessionID, target)
        return target, nil
    }

    // Fall back to discovering existing driver pods
    drivers, err := g.discovery.FindDriverPods(ctx, namespace)
    if err != nil {
        return nil, fmt.Errorf("failed to discover drivers: %w", err)
    }

    if len(drivers) == 0 {
        // No drivers available - request new cluster
        return g.requestNewCluster(ctx, namespace, sessionID)
    }

    // Load balance across available drivers
    driver := g.selectDriver(drivers)
    target := &DriverTarget{
        Address:   fmt.Sprintf("%s:15002", driver.IP),
        Namespace: namespace,
        SessionID: sessionID,
    }
    g.sessionMgr.SetTarget(sessionID, target)

    return target, nil
}

// getDriverConnection gets or creates connection to driver pod
func (g *SparkGateway) getDriverConnection(address string) (*grpc.ClientConn, error) {
    // Check cache
    if conn, ok := g.driverConns.Load(address); ok {
        return conn.(*grpc.ClientConn), nil
    }

    // Create new connection
    conn, err := grpc.Dial(
        address,
        grpc.WithTransportCredentials(insecure.NewCredentials()), // Internal traffic
        grpc.WithDefaultCallOptions(
            grpc.MaxCallRecvMsgSize(256 * 1024 * 1024),
            grpc.MaxCallSendMsgSize(256 * 1024 * 1024),
        ),
    )
    if err != nil {
        return nil, err
    }

    // Cache connection
    g.driverConns.Store(address, conn)
    return conn, nil
}

// authInterceptor validates authentication
func (g *SparkGateway) authInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, status.Error(codes.Unauthenticated, "missing metadata")
    }

    // Get auth token
    tokens := md.Get("authorization")
    if len(tokens) == 0 {
        // Allow service-to-service without token (rely on network policy)
        return handler(ctx, req)
    }

    // Validate token with Okta
    claims, err := g.authenticator.ValidateToken(ctx, tokens[0])
    if err != nil {
        return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
    }

    // Add claims to context
    ctx = context.WithValue(ctx, "user_claims", claims)

    return handler(ctx, req)
}

// metricsInterceptor records metrics
func (g *SparkGateway) metricsInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
    start := time.Now()

    routing, _ := g.extractRoutingInfo(ctx)
    labels := map[string]string{
        "method": info.FullMethod,
    }
    if routing != nil {
        labels["team"] = routing.Team
        labels["environment"] = routing.Environment
    }

    resp, err := handler(ctx, req)

    duration := time.Since(start)
    metrics.RecordLatency("spark_gateway_request_duration", duration, labels)

    if err != nil {
        metrics.IncrCounter("spark_gateway_request_errors", labels)
    } else {
        metrics.IncrCounter("spark_gateway_request_success", labels)
    }

    return resp, err
}

func main() {
    logger, _ := zap.NewProduction()

    cfg := &GatewayConfig{
        ListenAddr:      ":15002",
        TLSCertFile:     "/etc/certs/tls.crt",
        TLSKeyFile:      "/etc/certs/tls.key",
        OktaIssuer:      "https://doordash.okta.com",
        OktaClientID:    "spark-gateway",
        SessionTTL:      30 * time.Minute,
        HotPoolSize:     10,
        MaxConnsPerHost: 100,
    }

    gateway, err := NewSparkGateway(cfg, logger)
    if err != nil {
        logger.Fatal("Failed to create gateway", zap.Error(err))
    }

    if err := gateway.Start(cfg); err != nil {
        logger.Fatal("Gateway failed", zap.Error(err))
    }
}
```

---

### Component 2: Cluster Discovery Service

**discovery.go:**

```go
// spark-gateway/discovery/discovery.go
package discovery

import (
    "context"
    "fmt"

    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
)

// ClusterDiscovery finds Spark driver pods in K8s
type ClusterDiscovery struct {
    clientset *kubernetes.Clientset
}

// DriverPod represents a discovered driver
type DriverPod struct {
    Name      string
    Namespace string
    IP        string
    Ready     bool
    Labels    map[string]string
}

func NewClusterDiscovery() (*ClusterDiscovery, error) {
    config, err := rest.InClusterConfig()
    if err != nil {
        return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
    }

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        return nil, fmt.Errorf("failed to create clientset: %w", err)
    }

    return &ClusterDiscovery{clientset: clientset}, nil
}

// FindDriverPods discovers driver pods with Spark Connect enabled
func (d *ClusterDiscovery) FindDriverPods(ctx context.Context, namespace string) ([]*DriverPod, error) {
    // Find pods with spark-role=driver and spark-connect=enabled labels
    labelSelector := "spark-role=driver,spark-connect=enabled"

    pods, err := d.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
        LabelSelector: labelSelector,
    })
    if err != nil {
        return nil, fmt.Errorf("failed to list pods: %w", err)
    }

    var drivers []*DriverPod
    for _, pod := range pods.Items {
        // Only include running pods with IP assigned
        if pod.Status.Phase != "Running" || pod.Status.PodIP == "" {
            continue
        }

        // Check if pod is ready
        ready := false
        for _, cond := range pod.Status.Conditions {
            if cond.Type == "Ready" && cond.Status == "True" {
                ready = true
                break
            }
        }

        drivers = append(drivers, &DriverPod{
            Name:      pod.Name,
            Namespace: pod.Namespace,
            IP:        pod.Status.PodIP,
            Ready:     ready,
            Labels:    pod.Labels,
        })
    }

    return drivers, nil
}

// WatchDriverPods watches for driver pod changes
func (d *ClusterDiscovery) WatchDriverPods(ctx context.Context, namespace string, handler func(*DriverPod, string)) error {
    labelSelector := "spark-role=driver,spark-connect=enabled"

    watcher, err := d.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
        LabelSelector: labelSelector,
    })
    if err != nil {
        return err
    }

    go func() {
        for event := range watcher.ResultChan() {
            pod, ok := event.Object.(*corev1.Pod)
            if !ok {
                continue
            }

            driver := &DriverPod{
                Name:      pod.Name,
                Namespace: pod.Namespace,
                IP:        pod.Status.PodIP,
                Labels:    pod.Labels,
            }

            handler(driver, string(event.Type))
        }
    }()

    return nil
}
```

---

### Component 3: Hot Cluster Pool Manager

**hot_pool.go:**

```go
// spark-gateway/hotpool/manager.go
package hotpool

import (
    "context"
    "sync"
    "time"

    "go.uber.org/zap"
)

// HotCluster represents a pre-warmed Spark cluster
type HotCluster struct {
    ID            string
    Namespace     string
    DriverIP      string
    CreatedAt     time.Time
    LastUsed      time.Time
    Status        HotClusterStatus
    SparkVersion  string
}

type HotClusterStatus string

const (
    StatusAvailable HotClusterStatus = "available"
    StatusInUse     HotClusterStatus = "in_use"
    StatusWarming   HotClusterStatus = "warming"
    StatusExpired   HotClusterStatus = "expired"
)

// HotClusterPoolManager manages pre-warmed clusters
type HotClusterPoolManager struct {
    mu       sync.RWMutex
    pools    map[string][]*HotCluster // namespace -> clusters
    poolSize int
    ttl      time.Duration
    logger   *zap.Logger
}

func NewHotClusterPoolManager(poolSize int, ttl time.Duration, logger *zap.Logger) *HotClusterPoolManager {
    mgr := &HotClusterPoolManager{
        pools:    make(map[string][]*HotCluster),
        poolSize: poolSize,
        ttl:      ttl,
        logger:   logger,
    }

    // Start background maintenance
    go mgr.maintainPools()

    return mgr
}

// GetAvailable returns an available hot cluster for the namespace
func (m *HotClusterPoolManager) GetAvailable(namespace string) *HotCluster {
    m.mu.Lock()
    defer m.mu.Unlock()

    clusters, ok := m.pools[namespace]
    if !ok {
        return nil
    }

    for _, cluster := range clusters {
        if cluster.Status == StatusAvailable {
            cluster.Status = StatusInUse
            cluster.LastUsed = time.Now()
            m.logger.Info("Assigned hot cluster",
                zap.String("cluster_id", cluster.ID),
                zap.String("namespace", namespace),
            )
            return cluster
        }
    }

    return nil
}

// Release returns a cluster to the pool
func (m *HotClusterPoolManager) Release(clusterID string) {
    m.mu.Lock()
    defer m.mu.Unlock()

    for _, clusters := range m.pools {
        for _, cluster := range clusters {
            if cluster.ID == clusterID {
                cluster.Status = StatusAvailable
                cluster.LastUsed = time.Now()
                m.logger.Info("Released hot cluster", zap.String("cluster_id", clusterID))
                return
            }
        }
    }
}

// WarmPool ensures namespace has enough hot clusters
func (m *HotClusterPoolManager) WarmPool(ctx context.Context, namespace string, sparkOperator SparkOperatorClient) error {
    m.mu.Lock()
    defer m.mu.Unlock()

    clusters := m.pools[namespace]
    availableCount := 0

    for _, c := range clusters {
        if c.Status == StatusAvailable {
            availableCount++
        }
    }

    // Create new clusters if below target
    for availableCount < m.poolSize {
        cluster, err := m.createHotCluster(ctx, namespace, sparkOperator)
        if err != nil {
            return err
        }

        m.pools[namespace] = append(m.pools[namespace], cluster)
        availableCount++
    }

    return nil
}

// createHotCluster creates a new pre-warmed cluster via Spark Operator
func (m *HotClusterPoolManager) createHotCluster(ctx context.Context, namespace string, sparkOperator SparkOperatorClient) (*HotCluster, error) {
    clusterID := fmt.Sprintf("hot-%s-%d", namespace, time.Now().UnixNano())

    // Create SparkApplication with Spark Connect enabled
    spec := &SparkApplicationSpec{
        Type:         "Scala",
        Mode:         "cluster",
        SparkVersion: "3.5.0",
        MainClass:    "org.apache.spark.sql.connect.service.SparkConnectServer",
        Image:        "doordash-docker.jfrog.io/spark-platform/3.5-oss:latest",
        Driver: DriverSpec{
            Cores:     2,
            Memory:    "4g",
            Labels:    map[string]string{
                "spark-connect": "enabled",
                "hot-cluster":   "true",
            },
        },
        Executor: ExecutorSpec{
            Cores:     2,
            Memory:    "4g",
            Instances: 2,
        },
        SparkConf: map[string]string{
            "spark.connect.grpc.binding.port": "15002",
        },
    }

    app, err := sparkOperator.CreateSparkApplication(ctx, namespace, clusterID, spec)
    if err != nil {
        return nil, err
    }

    // Wait for driver to be ready
    driverIP, err := sparkOperator.WaitForDriver(ctx, namespace, clusterID, 5*time.Minute)
    if err != nil {
        return nil, err
    }

    return &HotCluster{
        ID:           clusterID,
        Namespace:    namespace,
        DriverIP:     driverIP,
        CreatedAt:    time.Now(),
        Status:       StatusAvailable,
        SparkVersion: "3.5.0",
    }, nil
}

// maintainPools runs periodic maintenance
func (m *HotClusterPoolManager) maintainPools() {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        m.expireOldClusters()
    }
}

// expireOldClusters removes clusters past TTL
func (m *HotClusterPoolManager) expireOldClusters() {
    m.mu.Lock()
    defer m.mu.Unlock()

    now := time.Now()

    for namespace, clusters := range m.pools {
        var active []*HotCluster

        for _, cluster := range clusters {
            if cluster.Status == StatusAvailable && now.Sub(cluster.LastUsed) > m.ttl {
                cluster.Status = StatusExpired
                m.logger.Info("Expired hot cluster",
                    zap.String("cluster_id", cluster.ID),
                    zap.Duration("age", now.Sub(cluster.CreatedAt)),
                )
                // TODO: Delete SparkApplication CRD
            } else if cluster.Status != StatusExpired {
                active = append(active, cluster)
            }
        }

        m.pools[namespace] = active
    }
}
```

---

### Component 4: Session Manager (Sticky Sessions)

**session_manager.go:**

```go
// spark-gateway/session/manager.go
package session

import (
    "sync"
    "time"
)

// Session tracks a client session
type Session struct {
    ID        string
    Target    *DriverTarget
    CreatedAt time.Time
    LastUsed  time.Time
    User      string
    Team      string
}

// SessionManager provides session affinity
type SessionManager struct {
    mu       sync.RWMutex
    sessions map[string]*Session // sessionID -> session
    ttl      time.Duration
}

func NewSessionManager(ttl time.Duration) *SessionManager {
    mgr := &SessionManager{
        sessions: make(map[string]*Session),
        ttl:      ttl,
    }

    go mgr.cleanupLoop()

    return mgr
}

// GetTarget returns the assigned driver for a session
func (m *SessionManager) GetTarget(sessionID string) *DriverTarget {
    m.mu.RLock()
    defer m.mu.RUnlock()

    if session, ok := m.sessions[sessionID]; ok {
        session.LastUsed = time.Now()
        return session.Target
    }

    return nil
}

// SetTarget assigns a driver to a session
func (m *SessionManager) SetTarget(sessionID string, target *DriverTarget) {
    m.mu.Lock()
    defer m.mu.Unlock()

    m.sessions[sessionID] = &Session{
        ID:        sessionID,
        Target:    target,
        CreatedAt: time.Now(),
        LastUsed:  time.Now(),
    }
}

// Remove deletes a session
func (m *SessionManager) Remove(sessionID string) {
    m.mu.Lock()
    defer m.mu.Unlock()

    delete(m.sessions, sessionID)
}

// cleanupLoop removes expired sessions
func (m *SessionManager) cleanupLoop() {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        m.cleanup()
    }
}

func (m *SessionManager) cleanup() {
    m.mu.Lock()
    defer m.mu.Unlock()

    now := time.Now()
    for id, session := range m.sessions {
        if now.Sub(session.LastUsed) > m.ttl {
            delete(m.sessions, id)
        }
    }
}
```

---

### Component 5: SparkApplication CRD (for Spark Operator)

**spark_application.yaml:**

```yaml
# Example SparkApplication CRD with Spark Connect enabled
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-connect-session-abc123
  namespace: sjns-feature-engineering-prod
  labels:
    spark-connect: "enabled"
    team: feature-engineering
    environment: prod
spec:
  type: Scala
  mode: cluster
  image: "doordash-docker.jfrog.io/spark-platform/3.5-oss:latest"
  imagePullPolicy: Always
  imagePullSecrets:
    - name: jfrog-docker

  mainClass: org.apache.spark.sql.connect.service.SparkConnectServer

  sparkVersion: "3.5.0"

  # Spark Connect configuration
  sparkConf:
    # Enable Spark Connect server
    spark.plugins: "org.apache.spark.sql.connect.SparkConnectPlugin"
    spark.connect.grpc.binding.port: "15002"
    spark.connect.grpc.arrow.maxBatchSize: "10485760"
    spark.connect.grpc.maxInboundMessageSize: "268435456"

    # Unity Catalog integration
    spark.sql.catalog.pedregal: "io.unitycatalog.spark.UCSingleCatalog"
    spark.sql.catalog.pedregal.uri: "https://unity-catalog.doordash.team"
    spark.sql.catalog.pedregal.token: "${UNITY_CATALOG_TOKEN}"

    # DRA for executor autoscaling
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.minExecutors: "1"
    spark.dynamicAllocation.maxExecutors: "50"
    spark.dynamicAllocation.executorIdleTimeout: "60s"
    spark.dynamicAllocation.shuffleTracking.enabled: "true"

  driver:
    cores: 2
    memory: "4g"
    labels:
      spark-role: driver
      spark-connect: "enabled"
    serviceAccount: spark-driver

    # Expose Spark Connect port
    ports:
      - name: spark-connect
        containerPort: 15002
        protocol: TCP
      - name: spark-ui
        containerPort: 4040
        protocol: TCP

    # Sidecars for observability
    sidecars:
      - name: otel-collector
        image: otel/opentelemetry-collector:latest
        ports:
          - containerPort: 4317
        volumeMounts:
          - name: otel-config
            mountPath: /etc/otel

      - name: fluent-bit
        image: fluent/fluent-bit:latest
        volumeMounts:
          - name: fluentbit-config
            mountPath: /fluent-bit/etc
          - name: spark-logs
            mountPath: /var/log/spark

    volumeMounts:
      - name: spark-logs
        mountPath: /var/log/spark

  executor:
    cores: 2
    memory: "4g"
    instances: 2
    labels:
      spark-role: executor

    volumeMounts:
      - name: spark-local
        mountPath: /tmp/spark

  volumes:
    - name: otel-config
      configMap:
        name: otel-collector-config
    - name: fluentbit-config
      configMap:
        name: fluent-bit-config
    - name: spark-logs
      emptyDir: {}
    - name: spark-local
      emptyDir:
        sizeLimit: 100Gi

  # Service for Spark Connect access
  driver:
    serviceType: ClusterIP
    serviceAnnotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "4040"

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10

  timeToLiveSeconds: 3600  # 1 hour TTL
```

---

### Component 6: Spark Connect Server Configuration

The Spark Connect server runs inside the Driver Pod. Here's how it's configured:

**spark-defaults.conf:**

```properties
# /opt/spark/conf/spark-defaults.conf

# =============================================================================
# SPARK CONNECT SERVER CONFIGURATION
# =============================================================================

# Enable Spark Connect plugin
spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin

# gRPC server settings
spark.connect.grpc.binding.port=15002
spark.connect.grpc.binding.address=0.0.0.0

# Message size limits (256MB)
spark.connect.grpc.maxInboundMessageSize=268435456

# Arrow batch size for data transfer (10MB)
spark.connect.grpc.arrow.maxBatchSize=10485760

# Session timeout
spark.connect.session.timeout=3600s

# Enable session cleanup
spark.connect.session.cleanup.enabled=true

# =============================================================================
# UNITY CATALOG CONFIGURATION
# =============================================================================

spark.sql.catalog.pedregal=io.unitycatalog.spark.UCSingleCatalog
spark.sql.catalog.pedregal.uri=https://unity-catalog.doordash.team
spark.sql.catalog.pedregal.token=${UNITY_CATALOG_TOKEN}

# Default catalog
spark.sql.defaultCatalog=pedregal

# =============================================================================
# DYNAMIC RESOURCE ALLOCATION
# =============================================================================

spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=1
spark.dynamicAllocation.maxExecutors=50
spark.dynamicAllocation.initialExecutors=2
spark.dynamicAllocation.executorIdleTimeout=60s
spark.dynamicAllocation.schedulerBacklogTimeout=1s
spark.dynamicAllocation.shuffleTracking.enabled=true

# =============================================================================
# PERFORMANCE TUNING
# =============================================================================

spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.parquet.compression.codec=zstd

# =============================================================================
# OBSERVABILITY
# =============================================================================

# Metrics
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusSink
spark.metrics.namespace=spark
spark.metrics.appStatusSource.enabled=true

# Event logging for Spark History Server
spark.eventLog.enabled=true
spark.eventLog.dir=s3a://doordash-spark-logs/event-logs/
```

**entrypoint.sh (Driver startup script):**

```bash
#!/bin/bash
# /opt/spark/entrypoint.sh

set -e

echo "Starting Spark Connect Server..."

# Set environment
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export SPARK_CONF_DIR=${SPARK_CONF_DIR:-$SPARK_HOME/conf}

# Configure Unity Catalog token from secret
if [ -f /etc/secrets/unity-catalog-token ]; then
    export UNITY_CATALOG_TOKEN=$(cat /etc/secrets/unity-catalog-token)
fi

# Configure AWS credentials for S3 access
if [ -f /etc/secrets/aws-credentials ]; then
    source /etc/secrets/aws-credentials
fi

# Start Spark Connect server
exec $SPARK_HOME/sbin/start-connect-server.sh \
    --master k8s://https://kubernetes.default.svc:443 \
    --deploy-mode client \
    --name "spark-connect-${HOSTNAME}" \
    --conf spark.kubernetes.namespace=${SPARK_NAMESPACE:-default} \
    --conf spark.kubernetes.driver.pod.name=${HOSTNAME} \
    --conf spark.kubernetes.executor.podNamePrefix=${HOSTNAME}-exec \
    --conf spark.driver.host=${POD_IP} \
    --conf spark.driver.port=7078 \
    --conf spark.blockManager.port=7079 \
    "$@"
```

---

### Component 7: Kubernetes Services

**spark-gateway-service.yaml:**

```yaml
# Service for Spark Gateway
apiVersion: v1
kind: Service
metadata:
  name: spark-gateway
  namespace: pedregal-system
  labels:
    app: spark-gateway
spec:
  type: ClusterIP
  ports:
    - name: grpc
      port: 15002
      targetPort: 15002
      protocol: TCP
    - name: metrics
      port: 9090
      targetPort: 9090
      protocol: TCP
  selector:
    app: spark-gateway

---

# External ingress for clients outside cluster
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: spark-gateway-ingress
  namespace: pedregal-system
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
    - hosts:
        - spark-gateway.doordash.team
      secretName: spark-gateway-tls
  rules:
    - host: spark-gateway.doordash.team
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: spark-gateway
                port:
                  number: 15002

---

# Headless service for driver pod discovery
apiVersion: v1
kind: Service
metadata:
  name: spark-driver-headless
  namespace: sjns-feature-engineering-prod
spec:
  clusterIP: None
  selector:
    spark-role: driver
    spark-connect: "enabled"
  ports:
    - name: spark-connect
      port: 15002
      targetPort: 15002
```

---

### Summary: Server-Side Request Flow

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      SERVER-SIDE REQUEST FLOW                                 │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. CLIENT                                                                   │
│     │                                                                        │
│     │  SparkSession.builder().remote("sc://gateway:15002")                   │
│     │  + Headers: x-team=feature-eng, x-env=prod, x-user=alice               │
│     │                                                                        │
│     ▼                                                                        │
│  2. SPARK GATEWAY                                                            │
│     │                                                                        │
│     ├─▶ TLS Termination                                                      │
│     ├─▶ Auth: Validate Okta token (if present)                               │
│     ├─▶ Extract routing headers                                              │
│     ├─▶ Session lookup: Check if session has assigned driver                 │
│     │                                                                        │
│     │   IF new session:                                                      │
│     │   ├─▶ Try hot pool: GetAvailable(sjns-feature-eng-prod)                │
│     │   ├─▶ Or discover: FindDriverPods(namespace)                           │
│     │   └─▶ Or create: SparkOperator.CreateSparkApplication()                │
│     │                                                                        │
│     ├─▶ Assign driver to session (sticky)                                    │
│     ├─▶ Get/create gRPC connection to driver                                 │
│     │                                                                        │
│     ▼                                                                        │
│  3. DRIVER POD (sjns-feature-eng-prod)                                       │
│     │                                                                        │
│     │  Spark Connect Server (:15002)                                         │
│     │                                                                        │
│     ├─▶ Receive ExecutePlanRequest                                           │
│     ├─▶ Parse Spark Connect protocol                                         │
│     ├─▶ Build Catalyst plan                                                  │
│     ├─▶ Optimize with AQE                                                    │
│     ├─▶ Execute on executors                                                 │
│     │                                                                        │
│     ▼                                                                        │
│  4. EXECUTORS                                                                │
│     │                                                                        │
│     ├─▶ Read from Unity Catalog / S3                                         │
│     ├─▶ Process partitions                                                   │
│     └─▶ Return results to driver                                             │
│                                                                              │
│     ▼                                                                        │
│  5. RESPONSE FLOW                                                            │
│                                                                              │
│     Driver ──▶ Gateway ──▶ Client                                            │
│     (Arrow batches streamed via gRPC)                                        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Connection URL Reference

### URL Format

```
sc://<host>:<port>[/<session_id>][?<params>]

Examples:
  sc://spark-gateway.doordash.team:15002
  sc://localhost:15002
  sc://spark-gateway:15002/session-123?token=abc
```

### Headers for Routing

| Header | Purpose | Example |
|--------|---------|---------|
| `x-doordash-team` | Team for namespace routing | `feature-engineering` |
| `x-doordash-environment` | Environment (local/test/staging/prod) | `prod` |
| `x-doordash-user` | User identity for audit | `user@doordash.com` |

### Gateway Endpoints by Environment

| Environment | Gateway Endpoint | K8s Cluster |
|-------------|------------------|-------------|
| local | `spark-gateway-dev.doordash.team:15002` | dev-spark-usw2 |
| test | `spark-gateway-test.doordash.team:15002` | test-spark-usw2 |
| staging | `spark-gateway-staging.doordash.team:15002` | staging-spark-usw2 |
| prod | `spark-gateway.doordash.team:15002` | prod-spark-{usw2,use1,euw1} |

---

## Summary: When to Use Each Pattern

| Pattern | Use Case | Latency | Session Duration |
|---------|----------|---------|------------------|
| **Direct API** | Service-to-service | Low (hot cluster) | Short (per request) |
| **DCP Playground** | Manifest testing | Medium | Medium (sandbox TTL) |
| **Jupyter Notebook** | Interactive analysis | Low (hot cluster) | Long (hours) |
| **CI/CD Pipeline** | Automated testing | Medium | Short (test duration) |
| **Metaflow Pipeline** | ML workflows | Medium | Medium (step duration) |

---

*Last updated: January 2025*
