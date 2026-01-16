# Spark Connect End-to-End Test

This directory contains everything needed to test Spark Connect client-server communication locally.

## Quick Start

### 1. Start Spark Connect Server

```bash
# Start the Spark Connect server
docker-compose up -d spark-connect-server

# Wait for it to be ready (check logs)
docker-compose logs -f spark-connect-server
```

### 2. Install Python Dependencies

```bash
# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Client Test

```bash
# Run basic connectivity tests
python spark_connect_client.py

# Run with Pi calculation benchmark
python spark_connect_client.py --pi --samples 1000000
```

## Testing Against Remote Spark Gateway

If you have access to a Spark Gateway via port-forward:

```bash
# 1. Port forward to the gateway (in another terminal)
kubectl port-forward -n spark-gateway svc/spark-gateway 15002:15002

# 2. Run the client
python spark_connect_client.py --host localhost --port 15002
```

## Architecture

```
┌─────────────────┐     gRPC (port 15002)     ┌─────────────────────────┐
│                 │ ──────────────────────────> │                         │
│  Python Client  │                             │  Spark Connect Server   │
│  (PySpark)      │ <────────────────────────── │  (Docker container)     │
│                 │     Arrow/Protobuf          │                         │
└─────────────────┘                             └─────────────────────────┘
```

## Files

- `docker-compose.yaml` - Docker Compose configuration for Spark Connect server
- `spark_connect_client.py` - Python test client
- `requirements.txt` - Python dependencies

## Spark Connect Protocol

Spark Connect uses gRPC for client-server communication:

- **Port**: 15002 (default)
- **Protocol**: `sc://<host>:<port>`
- **Transport**: gRPC over HTTP/2
- **Data Format**: Apache Arrow for efficient data transfer

## Troubleshooting

### Connection Refused
```bash
# Check if server is running
docker-compose ps

# Check server logs
docker-compose logs spark-connect-server
```

### Server Not Ready
```bash
# Wait for healthcheck
docker-compose exec spark-connect-server bash -c 'echo > /dev/tcp/localhost/15002'
```

### gRPC Issues
```bash
# Test with grpcurl
grpcurl -plaintext localhost:15002 list
```

## Spark Gateway vs Spark Connect

| Feature | Spark Gateway | Spark Connect |
|---------|---------------|---------------|
| Protocol | gRPC | gRPC |
| Port | 50051 | 15002 |
| Purpose | Job submission | Interactive sessions |
| Use Case | Batch jobs via SparkApplication CRD | DataFrame operations |

## Additional Resources

- [Spark Connect Documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [PySpark Connect API](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_connect.html)
