#!/bin/bash
# Build script for Spark Docker image

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
IMAGE_NAME="${IMAGE_NAME:-spark-testing}"
SPARK_VERSION="${SPARK_VERSION:-3.5.0}"

echo "Building Spark Docker image..."
echo "  Image name: $IMAGE_NAME"
echo "  Spark version: $SPARK_VERSION"

# Build the image
docker build \
    --build-arg SPARK_VERSION="$SPARK_VERSION" \
    -t "${IMAGE_NAME}:latest" \
    -t "${IMAGE_NAME}:${SPARK_VERSION}" \
    -f Dockerfile \
    .

echo ""
echo "Build complete!"
echo "  Image: ${IMAGE_NAME}:latest"
echo "  Image: ${IMAGE_NAME}:${SPARK_VERSION}"
echo ""
echo "To run the container with Spark Connect:"
echo "  docker run -d -p 15002:15002 -p 4040:4040 ${IMAGE_NAME}:latest"
echo ""
echo "To connect via Spark Connect:"
echo "  spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()"
