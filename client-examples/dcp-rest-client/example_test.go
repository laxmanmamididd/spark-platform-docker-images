package dcpclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	dcpclient "github.com/doordash/spark-platform-docker-images/client-examples/dcp-rest-client"
)

// Example_submitCoreETLJob demonstrates submitting a CoreETL-based Spark job
func Example_submitCoreETLJob() {
	// Create client
	client := dcpclient.NewClient(
		"http://dcp-spark.service.prod.ddsd:8080",
		dcpclient.WithTimeout(60*time.Second),
	)

	// Define CoreETL job spec
	coreETLJobSpec := map[string]interface{}{
		"sources": []map[string]interface{}{
			{
				"type":     "iceberg",
				"catalog":  "pedregal",
				"database": "raw",
				"table":    "events",
				"filter":   "event_date >= current_date - interval 7 days",
			},
		},
		"transformations": []map[string]interface{}{
			{
				"type": "sql",
				"query": `
					SELECT
						event_id,
						user_id,
						event_type,
						event_timestamp,
						payload
					FROM source
					WHERE event_type IN ('order_created', 'order_completed')
				`,
			},
		},
		"sinks": []map[string]interface{}{
			{
				"type":         "iceberg",
				"catalog":      "pedregal",
				"database":     "derived",
				"table":        "processed_events",
				"mode":         "append",
				"partition_by": []string{"event_date"},
			},
		},
	}

	jobSpecJSON, _ := json.Marshal(coreETLJobSpec)

	// Submit job
	ctx := context.Background()
	resp, err := client.SubmitJob(ctx, &dcpclient.SubmitJobRequest{
		Name: "example-etl-job",
		Team: "data-infra",
		User: "user@doordash.com",
		Config: dcpclient.SparkJobConfig{
			SparkVersion: "3.5",
			JobType:      "BATCH",
			CoreETL: &dcpclient.CoreETLConfig{
				Version: "2.7.0",
				JobSpec: jobSpecJSON,
			},
			Resources: dcpclient.ResourceConfig{
				Driver: dcpclient.DriverConfig{
					Cores:          2,
					Memory:         "4g",
					MemoryOverhead: "1g",
				},
				Executor: dcpclient.ExecutorConfig{
					Instances:      5,
					Cores:          4,
					Memory:         "8g",
					MemoryOverhead: "2g",
				},
			},
			SparkConfig: map[string]string{
				"spark.sql.shuffle.partitions":                   "200",
				"spark.sql.adaptive.enabled":                     "true",
				"spark.sql.adaptive.coalescePartitions.enabled":  "true",
			},
		},
	})

	if err != nil {
		fmt.Printf("Failed to submit job: %v\n", err)
		return
	}

	fmt.Printf("Job submitted: %s (execution: %s)\n", resp.JobID, resp.ExecutionID)
	fmt.Printf("Status: %s\n", resp.Status)
}

// Example_submitCustomJarJob demonstrates submitting a custom JAR Spark job
func Example_submitCustomJarJob() {
	client := dcpclient.NewClient("http://dcp-spark.service.prod.ddsd:8080")

	ctx := context.Background()
	resp, err := client.SubmitJob(ctx, &dcpclient.SubmitJobRequest{
		Name: "custom-spark-job",
		Team: "ml-platform",
		User: "ml-engineer@doordash.com",
		Config: dcpclient.SparkJobConfig{
			SparkVersion: "3.5",
			JobType:      "BATCH",
			CustomJar: &dcpclient.CustomJarConfig{
				JarPath:   "s3://ml-artifacts/jobs/feature-engineering.jar",
				MainClass: "com.doordash.ml.FeatureEngineering",
				Args: []string{
					"--date", "2024-01-15",
					"--output", "s3://ml-features/output/",
				},
			},
			Resources: dcpclient.ResourceConfig{
				Driver: dcpclient.DriverConfig{
					Cores:  4,
					Memory: "8g",
				},
				Executor: dcpclient.ExecutorConfig{
					Instances: 10,
					Cores:     8,
					Memory:    "16g",
				},
			},
		},
	})

	if err != nil {
		fmt.Printf("Failed to submit job: %v\n", err)
		return
	}

	fmt.Printf("Custom JAR job submitted: %s\n", resp.JobID)
}

// Example_waitForJobCompletion demonstrates waiting for a job to complete
func Example_waitForJobCompletion() {
	client := dcpclient.NewClient("http://dcp-spark.service.prod.ddsd:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	// Submit job first
	submitResp, err := client.SubmitJob(ctx, &dcpclient.SubmitJobRequest{
		Name: "batch-job",
		Team: "data-infra",
		User: "user@doordash.com",
		Config: dcpclient.SparkJobConfig{
			SparkVersion: "3.5",
			JobType:      "BATCH",
			CoreETL: &dcpclient.CoreETLConfig{
				Version: "2.7.0",
				JobSpec: []byte(`{"sources":[],"transformations":[],"sinks":[]}`),
			},
			Resources: dcpclient.ResourceConfig{
				Driver:   dcpclient.DriverConfig{Cores: 2, Memory: "4g"},
				Executor: dcpclient.ExecutorConfig{Instances: 5, Cores: 4, Memory: "8g"},
			},
		},
	})
	if err != nil {
		fmt.Printf("Failed to submit: %v\n", err)
		return
	}

	fmt.Printf("Job submitted: %s, waiting for completion...\n", submitResp.JobID)

	// Wait for completion with 30-second poll interval
	status, err := client.WaitForCompletion(ctx, submitResp.JobID, 30*time.Second)
	if err != nil {
		fmt.Printf("Error waiting for job: %v\n", err)
		return
	}

	fmt.Printf("Job completed with status: %s\n", status.Status)
	if status.Status == "FAILED" {
		fmt.Printf("Error: %s\n", status.ErrorMessage)
	}
}

// Example_streamingJob demonstrates submitting a streaming Spark job
func Example_streamingJob() {
	client := dcpclient.NewClient("http://dcp-spark.service.prod.ddsd:8080")

	streamingJobSpec := map[string]interface{}{
		"sources": []map[string]interface{}{
			{
				"type":           "kafka",
				"bootstrap":      "kafka.service.prod.ddsd:9092",
				"topic":          "events.orders",
				"starting_offset": "latest",
			},
		},
		"transformations": []map[string]interface{}{
			{
				"type": "sql",
				"query": `
					SELECT
						from_json(value, 'struct<order_id:string,amount:double>') as order,
						timestamp
					FROM source
				`,
			},
		},
		"sinks": []map[string]interface{}{
			{
				"type":                "iceberg",
				"catalog":             "pedregal",
				"database":            "streaming",
				"table":               "order_events",
				"mode":                "append",
				"checkpoint_location": "s3://checkpoints/order_events/",
				"trigger_interval":    "1 minute",
			},
		},
	}

	jobSpecJSON, _ := json.Marshal(streamingJobSpec)

	ctx := context.Background()
	resp, err := client.SubmitJob(ctx, &dcpclient.SubmitJobRequest{
		Name: "order-events-streaming",
		Team: "data-streaming",
		User: "streaming@doordash.com",
		Config: dcpclient.SparkJobConfig{
			SparkVersion: "3.5",
			JobType:      "STREAMING",
			CoreETL: &dcpclient.CoreETLConfig{
				Version: "2.7.0",
				JobSpec: jobSpecJSON,
			},
			Resources: dcpclient.ResourceConfig{
				Driver: dcpclient.DriverConfig{
					Cores:  2,
					Memory: "4g",
				},
				Executor: dcpclient.ExecutorConfig{
					Instances:         3,
					Cores:             4,
					Memory:            "8g",
					DynamicAllocation: true,
					MinExecutors:      2,
					MaxExecutors:      10,
				},
			},
		},
	})

	if err != nil {
		fmt.Printf("Failed to submit streaming job: %v\n", err)
		return
	}

	fmt.Printf("Streaming job submitted: %s\n", resp.JobID)
}
