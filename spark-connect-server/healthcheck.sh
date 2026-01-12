#!/bin/bash
# ============================================================================
# Health Check for Spark Connect Server
# ============================================================================
# Checks if Spark Connect gRPC server is responding on port 15002
# Used by Kubernetes liveness/readiness probes
# ============================================================================

SPARK_CONNECT_PORT=${SPARK_CONNECT_GRPC_PORT:-15002}

# Check if the gRPC port is listening
if nc -z localhost "$SPARK_CONNECT_PORT" 2>/dev/null; then
    echo "Spark Connect Server is healthy on port $SPARK_CONNECT_PORT"
    exit 0
else
    echo "Spark Connect Server is NOT responding on port $SPARK_CONNECT_PORT"
    exit 1
fi
