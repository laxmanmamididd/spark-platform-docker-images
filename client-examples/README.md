# Client Examples for Spark Platform

This directory contains example code for submitting Spark jobs via different entry points.

## Entry Points

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CLIENT ENTRY POINTS                                  │
│                                                                             │
│  Option 1: DCP REST API (Recommended)                                       │
│  ─────────────────────────────────────                                      │
│  Client → DCP REST API → DCP Plugin → Spark Runner → Spark Gateway → SK8   │
│                                                                             │
│  Option 2: Spark Runner gRPC (Direct)                                       │
│  ─────────────────────────────────────                                      │
│  Client → Spark Runner gRPC → Spark Gateway → SK8                           │
│                                                                             │
│  Option 3: CoreETL Library (In-Process)                                     │
│  ─────────────────────────────────────                                      │
│  Graph Runner Node → CoreETL Component → SJEM/Spark Runner → SK8            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## When to Use Each

| Entry Point | Use Case |
|-------------|----------|
| DCP REST API | External services, CI/CD pipelines, programmatic job submission |
| Spark Runner gRPC | Internal Pedregal graphs, custom orchestrators |
| CoreETL Library | Graph Runner nodes, DCP plugins |

## Files

- `dcp-rest-client/` - REST client for DCP API
- `spark-runner-grpc-client/` - gRPC client for Spark Runner
- `dcp-rest-server/` - Example DCP REST server implementation
