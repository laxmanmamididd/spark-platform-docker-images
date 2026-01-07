#!/bin/bash
# Spark Container Entrypoint Script
# Handles different roles: driver, executor, connect-server

set -e

# Default values
SPARK_ROLE=${SPARK_ROLE:-driver}
SPARK_CONNECT_ENABLED=${SPARK_CONNECT_ENABLED:-true}
SPARK_CONNECT_PORT=${SPARK_CONNECT_PORT:-15002}
SPARK_MASTER=${SPARK_MASTER:-local[*]}

echo "Starting Spark container with role: $SPARK_ROLE"
echo "Spark Connect enabled: $SPARK_CONNECT_ENABLED"

# Function to start Spark Connect server
start_spark_connect() {
    echo "Starting Spark Connect server on port $SPARK_CONNECT_PORT..."

    # Start Spark Connect server
    $SPARK_HOME/sbin/start-connect-server.sh \
        --packages org.apache.spark:spark-connect_2.12:$SPARK_VERSION \
        --conf spark.connect.grpc.binding.port=$SPARK_CONNECT_PORT \
        --conf spark.connect.grpc.arrow.maxBatchSize=10485760 \
        --master $SPARK_MASTER

    # Wait for server to be ready
    echo "Waiting for Spark Connect to be ready..."
    for i in {1..30}; do
        if nc -z localhost $SPARK_CONNECT_PORT 2>/dev/null; then
            echo "Spark Connect server is ready on port $SPARK_CONNECT_PORT"
            break
        fi
        sleep 1
    done
}

# Function to run as driver (batch job)
run_as_driver() {
    echo "Running as Spark driver..."

    if [ "$SPARK_CONNECT_ENABLED" = "true" ]; then
        start_spark_connect
    fi

    # If command arguments provided, run them
    if [ $# -gt 0 ]; then
        exec "$@"
    else
        # Keep container running
        echo "No command provided. Container will stay running."
        tail -f /dev/null
    fi
}

# Function to run as executor
run_as_executor() {
    echo "Running as Spark executor..."

    # Executor-specific configuration
    exec $SPARK_HOME/bin/spark-class org.apache.spark.executor.CoarseGrainedExecutorBackend \
        --driver-url "$SPARK_DRIVER_URL" \
        --executor-id "$SPARK_EXECUTOR_ID" \
        --hostname "$HOSTNAME" \
        --cores "$SPARK_EXECUTOR_CORES" \
        --app-id "$SPARK_APPLICATION_ID"
}

# Function to run Spark Connect server only
run_connect_server() {
    echo "Running as dedicated Spark Connect server..."
    start_spark_connect

    # Keep the container running
    tail -f /dev/null
}

# Route to appropriate function based on role
case $SPARK_ROLE in
    driver)
        run_as_driver "$@"
        ;;
    executor)
        run_as_executor
        ;;
    connect-server)
        run_connect_server
        ;;
    *)
        echo "Unknown role: $SPARK_ROLE"
        echo "Valid roles: driver, executor, connect-server"
        exit 1
        ;;
esac
