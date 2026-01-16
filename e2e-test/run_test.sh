#!/bin/bash
# Spark Connect E2E Test Runner
# Run this script to start the server and test connectivity

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "SPARK CONNECT E2E TEST RUNNER"
echo "=============================================="

# Check Docker
if ! docker info &> /dev/null; then
    echo "ERROR: Docker is not running"
    exit 1
fi

# Start server if not running
if ! docker ps | grep -q spark-connect-server; then
    echo "Starting Spark Connect server..."
    docker-compose up -d spark-connect-server
    echo "Waiting for server to be ready (60 seconds)..."
    sleep 60
else
    echo "Spark Connect server is already running"
fi

# Check server status
if docker ps | grep -q spark-connect-server; then
    echo "Server is running on port 15002"
else
    echo "ERROR: Server failed to start"
    docker logs spark-connect-server 2>&1 | tail -20
    exit 1
fi

# Install Python dependencies if needed
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "Installing Python dependencies..."
    pip3 install -q 'pyspark[connect]==3.5.3' grpcio grpcio-status pyarrow
fi

# Run tests
echo ""
echo "Running Spark Connect client tests..."
python3 "$SCRIPT_DIR/spark_connect_client.py" --host localhost --port 15002 --pi

echo ""
echo "=============================================="
echo "TESTS COMPLETED"
echo "=============================================="
echo ""
echo "Useful commands:"
echo "  docker-compose logs -f spark-connect-server  # View logs"
echo "  docker-compose down                          # Stop server"
echo "  open http://localhost:4040                   # Spark UI"
