# Spark Docker Testing Framework

A comprehensive testing framework for Spark jobs using Docker containers, designed to integrate with DoorDash's Pedregal Spark Platform architecture.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         TEST ENVIRONMENT                                  │
│                                                                          │
│  ┌─────────────┐     ┌─────────────────────────────────────────────────┐ │
│  │ Test Client │────▶│          Spark Docker Container                  │ │
│  │ (JUnit/Go)  │     │  ┌─────────────────────────────────────────────┐ │ │
│  │             │     │  │  Driver + Spark Connect Server (port 15002)  │ │ │
│  │             │◀────│  │                                              │ │ │
│  │             │     │  └─────────────────────────────────────────────┘ │ │
│  └─────────────┘     │  ┌─────────────────────────────────────────────┐ │ │
│                      │  │  Executors (local mode or separate pods)     │ │ │
│                      │  └─────────────────────────────────────────────┘ │ │
│                      └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Spark Docker Image (`docker/`)
- Multi-layer Docker image with Spark + Spark Connect
- Configurable for different Spark versions
- Includes Unity Catalog and S3 connectors

### 2. Spark Runner Mock (`spark-runner/`)
- Go implementation of the Spark Runner interface
- Submit/Check/Cancel primitives
- Integration with Spark Gateway

### 3. Spark Gateway Mock (`spark-gateway/`)
- Routes jobs to containerized Spark
- Manages SparkApplication lifecycle
- Supports local Docker and K8s backends

### 4. TestContainers Integration (`testcontainers/`)
- Java/Go TestContainers modules
- Automatic container lifecycle management
- Port mapping and health checks

### 5. DCP Integration Example (`dcp-example/`)
- Sample DCP manifest
- CoreETL job specification
- End-to-end test workflow

## Quick Start

```bash
# Build the Spark Docker image
cd docker && ./build.sh

# Run integration tests
cd testcontainers && ./gradlew test

# Or with Go
cd spark-runner && go test ./...
```

## Directory Structure

```
.
├── README.md
├── docker/
│   ├── Dockerfile
│   ├── entrypoint.sh
│   └── spark-defaults.conf
├── spark-runner/
│   ├── go.mod
│   ├── runner.go
│   ├── runner_test.go
│   └── client/
├── spark-gateway/
│   ├── go.mod
│   ├── gateway.go
│   └── k8s/
├── testcontainers/
│   ├── build.gradle.kts
│   └── src/
└── dcp-example/
    ├── manifest.yaml
    └── job_spec.proto
```

## Integration with Pedregal

This framework mirrors the production Pedregal Spark Platform:

1. **DCP** → defines job manifests
2. **Spark Runner** → submits/monitors jobs
3. **Spark Gateway** → routes to compute backend
4. **SK8/Docker** → executes Spark applications
5. **Spark Connect** → enables interactive sessions

For testing, Docker replaces SK8 while maintaining the same interfaces.
