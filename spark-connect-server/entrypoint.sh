#!/bin/bash
# ============================================================================
# DoorDash Spark Connect Server - Entrypoint Script
# ============================================================================
# This script handles different modes:
#   - connect:  Start Spark Connect Server (for interactive sessions)
#   - driver:   Start as Spark Driver (for batch jobs via SparkApplication)
#   - executor: Start as Spark Executor
# ============================================================================

set -e

# ============================================================================
# ENVIRONMENT VARIABLES (with defaults)
# ============================================================================
SPARK_MODE=${SPARK_MODE:-connect}
SPARK_CONNECT_PORT=${SPARK_CONNECT_GRPC_PORT:-15002}
SPARK_MASTER=${SPARK_MASTER:-local[*]}

# Unity Catalog
UNITY_CATALOG_URI=${UNITY_CATALOG_URI:-http://unity-catalog.doordash.team:8080}
UNITY_CATALOG_TOKEN=${UNITY_CATALOG_TOKEN:-}

# AWS
AWS_REGION=${AWS_REGION:-us-west-2}

echo "=============================================="
echo "DoorDash Spark Connect Server"
echo "=============================================="
echo "Mode:              $SPARK_MODE"
echo "Spark Connect Port: $SPARK_CONNECT_PORT"
echo "Spark Master:      $SPARK_MASTER"
echo "Unity Catalog URI: $UNITY_CATALOG_URI"
echo "=============================================="

# ============================================================================
# WAIT FOR DEPENDENCIES (optional)
# ============================================================================
wait_for_service() {
    local host=$1
    local port=$2
    local timeout=${3:-30}

    echo "Waiting for $host:$port..."
    for i in $(seq 1 $timeout); do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo "$host:$port is available"
            return 0
        fi
        sleep 1
    done
    echo "WARNING: $host:$port not available after ${timeout}s"
    return 1
}

# Wait for Unity Catalog if configured
if [ -n "$UNITY_CATALOG_URI" ] && [ "$WAIT_FOR_UC" = "true" ]; then
    UC_HOST=$(echo "$UNITY_CATALOG_URI" | sed -E 's|https?://([^:/]+).*|\1|')
    UC_PORT=$(echo "$UNITY_CATALOG_URI" | sed -E 's|.*:([0-9]+).*|\1|')
    UC_PORT=${UC_PORT:-8080}
    wait_for_service "$UC_HOST" "$UC_PORT" 60 || true
fi

# ============================================================================
# START BASED ON MODE
# ============================================================================
case "$SPARK_MODE" in

    # ========================================================================
    # SPARK CONNECT SERVER MODE
    # ========================================================================
    # This is the main mode for interactive Spark sessions via Spark Gateway
    # Clients connect via gRPC to port 15002
    # ========================================================================
    connect)
        echo "Starting Spark Connect Server on port $SPARK_CONNECT_PORT..."

        # Build the command with all configurations
        exec /opt/spark/sbin/start-connect-server.sh \
            --master "$SPARK_MASTER" \
            --conf "spark.connect.grpc.binding.port=$SPARK_CONNECT_PORT" \
            --conf "spark.driver.host=$(hostname -i)" \
            --conf "spark.sql.catalog.datalake.uri=$UNITY_CATALOG_URI" \
            --conf "spark.sql.catalog.datalake.token=$UNITY_CATALOG_TOKEN" \
            --conf "spark.hadoop.fs.s3a.endpoint.region=$AWS_REGION" \
            "$@"
        ;;

    # ========================================================================
    # DRIVER MODE
    # ========================================================================
    # For batch jobs submitted via Spark Runner / SparkApplication CRD
    # This is NOT Spark Connect - it's traditional spark-submit
    # ========================================================================
    driver)
        echo "Starting Spark Driver (batch mode)..."

        # The SparkApplication CRD will pass the mainClass or mainApplicationFile
        exec /opt/spark/bin/spark-submit \
            --master "$SPARK_MASTER" \
            --deploy-mode client \
            --conf "spark.driver.host=$(hostname -i)" \
            --conf "spark.sql.catalog.datalake.uri=$UNITY_CATALOG_URI" \
            --conf "spark.sql.catalog.datalake.token=$UNITY_CATALOG_TOKEN" \
            "$@"
        ;;

    # ========================================================================
    # DRIVER WITH SPARK CONNECT ENABLED
    # ========================================================================
    # Batch job that ALSO exposes Spark Connect for monitoring/debugging
    # ========================================================================
    driver-connect)
        echo "Starting Spark Driver with Spark Connect enabled..."

        exec /opt/spark/bin/spark-submit \
            --master "$SPARK_MASTER" \
            --deploy-mode client \
            --conf "spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin" \
            --conf "spark.connect.grpc.binding.port=$SPARK_CONNECT_PORT" \
            --conf "spark.driver.host=$(hostname -i)" \
            --conf "spark.sql.catalog.datalake.uri=$UNITY_CATALOG_URI" \
            --conf "spark.sql.catalog.datalake.token=$UNITY_CATALOG_TOKEN" \
            "$@"
        ;;

    # ========================================================================
    # EXECUTOR MODE
    # ========================================================================
    # Started by Kubernetes when driver requests executors
    # ========================================================================
    executor)
        echo "Starting Spark Executor..."

        exec /opt/spark/bin/spark-class org.apache.spark.executor.CoarseGrainedExecutorBackend "$@"
        ;;

    # ========================================================================
    # SHELL MODE (for debugging)
    # ========================================================================
    shell)
        echo "Starting interactive shell..."
        exec /bin/bash
        ;;

    *)
        echo "ERROR: Unknown SPARK_MODE: $SPARK_MODE"
        echo "Valid modes: connect, driver, driver-connect, executor, shell"
        exit 1
        ;;
esac
