// Package dcpclient provides a REST client for the DCP Spark API.
// This is the recommended way for external services to submit Spark jobs.
//
// Example usage:
//
//	client := dcpclient.NewClient("http://dcp-spark.service.prod.ddsd:8080")
//	jobID, err := client.SubmitJob(ctx, &dcpclient.SubmitJobRequest{
//	    Name: "my-etl-job",
//	    Team: "data-infra",
//	    User: "user@doordash.com",
//	    Config: dcpclient.SparkJobConfig{
//	        SparkVersion: "3.5",
//	        JobType: "BATCH",
//	        CoreETL: &dcpclient.CoreETLConfig{
//	            Version: "2.7.0",
//	            JobSpec: myJobSpec,
//	        },
//	    },
//	})
package dcpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is a REST client for the DCP Spark API
type Client struct {
	baseURL    string
	httpClient *http.Client
	authToken  string
}

// ClientOption configures the client
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = client
	}
}

// WithAuthToken sets the authentication token
func WithAuthToken(token string) ClientOption {
	return func(c *Client) {
		c.authToken = token
	}
}

// WithTimeout sets the HTTP client timeout
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// NewClient creates a new DCP Spark API client
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// SubmitJobRequest represents a job submission request
type SubmitJobRequest struct {
	Name           string         `json:"name"`
	Team           string         `json:"team"`
	User           string         `json:"user"`
	Config         SparkJobConfig `json:"config"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	CallbackURL    string         `json:"callback_url,omitempty"`
}

// SparkJobConfig contains Spark job configuration
type SparkJobConfig struct {
	SparkVersion string            `json:"spark_version"`
	JobType      string            `json:"job_type"`
	CoreETL      *CoreETLConfig    `json:"core_etl,omitempty"`
	CustomJar    *CustomJarConfig  `json:"custom_jar,omitempty"`
	Resources    ResourceConfig    `json:"resources"`
	SparkConfig  map[string]string `json:"spark_config,omitempty"`
	EnvVars      map[string]string `json:"env_vars,omitempty"`
}

// CoreETLConfig for CoreETL-based jobs
type CoreETLConfig struct {
	Version string          `json:"version"`
	JobSpec json.RawMessage `json:"job_spec"`
}

// CustomJarConfig for custom JAR jobs
type CustomJarConfig struct {
	JarPath   string   `json:"jar_path"`
	MainClass string   `json:"main_class"`
	Args      []string `json:"args,omitempty"`
}

// ResourceConfig defines resource allocation
type ResourceConfig struct {
	Driver   DriverConfig   `json:"driver"`
	Executor ExecutorConfig `json:"executor"`
}

// DriverConfig for Spark driver
type DriverConfig struct {
	Cores          int    `json:"cores"`
	Memory         string `json:"memory"`
	MemoryOverhead string `json:"memory_overhead,omitempty"`
}

// ExecutorConfig for Spark executors
type ExecutorConfig struct {
	Instances         int    `json:"instances"`
	Cores             int    `json:"cores"`
	Memory            string `json:"memory"`
	MemoryOverhead    string `json:"memory_overhead,omitempty"`
	DynamicAllocation bool   `json:"dynamic_allocation,omitempty"`
	MinExecutors      int    `json:"min_executors,omitempty"`
	MaxExecutors      int    `json:"max_executors,omitempty"`
}

// SubmitJobResponse is the response from job submission
type SubmitJobResponse struct {
	JobID       string `json:"job_id"`
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
	SparkUIURL  string `json:"spark_ui_url,omitempty"`
	Message     string `json:"message,omitempty"`
}

// JobStatus represents job status response
type JobStatus struct {
	JobID        string     `json:"job_id"`
	ExecutionID  string     `json:"execution_id"`
	Status       string     `json:"status"`
	StartTime    *time.Time `json:"start_time,omitempty"`
	EndTime      *time.Time `json:"end_time,omitempty"`
	SparkUIURL   string     `json:"spark_ui_url,omitempty"`
	LogsURL      string     `json:"logs_url,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// JobLogs represents job logs response
type JobLogs struct {
	Logs    string `json:"logs"`
	LogsURL string `json:"logs_url"`
}

// SubmitJob submits a new Spark job
func (c *Client) SubmitJob(ctx context.Context, req *SubmitJobRequest) (*SubmitJobResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/api/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result SubmitJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetJobStatus retrieves the status of a job
func (c *Client) GetJobStatus(ctx context.Context, jobID string) (*JobStatus, error) {
	httpReq, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/api/v1/jobs/"+jobID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result JobStatus
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// CancelJob cancels a running job
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, "DELETE", c.baseURL+"/api/v1/jobs/"+jobID, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetJobLogs retrieves logs for a job
func (c *Client) GetJobLogs(ctx context.Context, jobID string) (*JobLogs, error) {
	httpReq, err := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/api/v1/jobs/"+jobID+"/logs", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.authToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result JobLogs
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// WaitForCompletion waits for a job to complete (blocking)
func (c *Client) WaitForCompletion(ctx context.Context, jobID string, pollInterval time.Duration) (*JobStatus, error) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			status, err := c.GetJobStatus(ctx, jobID)
			if err != nil {
				return nil, err
			}

			switch status.Status {
			case "SUCCEEDED", "FAILED", "CANCELLED":
				return status, nil
			}
		}
	}
}
