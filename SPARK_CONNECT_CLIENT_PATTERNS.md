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
