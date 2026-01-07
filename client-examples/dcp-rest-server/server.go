// Package dcpserver implements a DCP REST/gRPC server for Spark job submission.
// This is the recommended entry point for external clients to submit Spark jobs.
//
// Architecture:
//
//	Client (REST/gRPC) → DCP Server → Spark Runner → Spark Gateway → SK8
package dcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the status of a Spark job
type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusSubmitted JobStatus = "SUBMITTED"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusSucceeded JobStatus = "SUCCEEDED"
	JobStatusFailed    JobStatus = "FAILED"
	JobStatusCancelled JobStatus = "CANCELLED"
)

// SubmitJobRequest is the REST API request for submitting a job
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
	JobType      string            `json:"job_type"` // BATCH or STREAMING
	CoreETL      *CoreETLConfig    `json:"core_etl,omitempty"`
	CustomJar    *CustomJarConfig  `json:"custom_jar,omitempty"`
	Resources    ResourceConfig    `json:"resources"`
	SparkConfig  map[string]string `json:"spark_config,omitempty"`
	EnvVars      map[string]string `json:"env_vars,omitempty"`
}

// CoreETLConfig for CoreETL-based jobs
type CoreETLConfig struct {
	Version string          `json:"version"`
	JobSpec json.RawMessage `json:"job_spec"` // CoreETL job specification
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

// SubmitJobResponse is the REST API response for job submission
type SubmitJobResponse struct {
	JobID       string    `json:"job_id"`
	ExecutionID string    `json:"execution_id"`
	Status      JobStatus `json:"status"`
	SparkUIURL  string    `json:"spark_ui_url,omitempty"`
	Message     string    `json:"message,omitempty"`
}

// GetJobStatusResponse is the REST API response for job status
type GetJobStatusResponse struct {
	JobID        string     `json:"job_id"`
	ExecutionID  string     `json:"execution_id"`
	Status       JobStatus  `json:"status"`
	StartTime    *time.Time `json:"start_time,omitempty"`
	EndTime      *time.Time `json:"end_time,omitempty"`
	SparkUIURL   string     `json:"spark_ui_url,omitempty"`
	LogsURL      string     `json:"logs_url,omitempty"`
	Metrics      *JobMetrics `json:"metrics,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// JobMetrics contains job execution metrics
type JobMetrics struct {
	RecordsRead     int64   `json:"records_read"`
	RecordsWritten  int64   `json:"records_written"`
	BytesRead       int64   `json:"bytes_read"`
	BytesWritten    int64   `json:"bytes_written"`
	StagesCompleted int     `json:"stages_completed"`
	TasksCompleted  int     `json:"tasks_completed"`
	ExecutorCPUTime float64 `json:"executor_cpu_time_ms"`
}

// Job represents a submitted job
type Job struct {
	ID           string
	Name         string
	Team         string
	User         string
	ExecutionID  string
	Status       JobStatus
	StartTime    time.Time
	EndTime      *time.Time
	SparkUIURL   string
	LogsURL      string
	Config       SparkJobConfig
	ErrorMessage string
}

// SparkRunnerClient is the interface for calling Spark Runner
type SparkRunnerClient interface {
	Submit(ctx context.Context, spec *JobSpec) (executionID string, err error)
	Check(ctx context.Context, executionID string) (status string, err error)
	Cancel(ctx context.Context, executionID string) error
}

// JobSpec is passed to Spark Runner
type JobSpec struct {
	Name        string
	Team        string
	User        string
	MainClass   string
	JarPath     string
	Args        []string
	SparkConfig map[string]string
	JobType     string
	Driver      DriverConfig
	Executor    ExecutorConfig
}

// Server implements the DCP REST API for Spark jobs
type Server struct {
	sparkRunner SparkRunnerClient
	jobs        map[string]*Job
	mu          sync.RWMutex
	port        int
}

// NewServer creates a new DCP server
func NewServer(sparkRunner SparkRunnerClient, port int) *Server {
	return &Server{
		sparkRunner: sparkRunner,
		jobs:        make(map[string]*Job),
		port:        port,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// REST API endpoints
	mux.HandleFunc("POST /api/v1/jobs", s.handleSubmitJob)
	mux.HandleFunc("GET /api/v1/jobs/{jobID}", s.handleGetJobStatus)
	mux.HandleFunc("DELETE /api/v1/jobs/{jobID}", s.handleCancelJob)
	mux.HandleFunc("GET /api/v1/jobs", s.handleListJobs)
	mux.HandleFunc("GET /api/v1/jobs/{jobID}/logs", s.handleGetJobLogs)

	// Health check
	mux.HandleFunc("GET /health", s.handleHealthCheck)

	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("Starting DCP Spark server on %s", addr)
	return http.ListenAndServe(addr, mux)
}

// handleSubmitJob handles POST /api/v1/jobs
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request
	if err := s.validateSubmitRequest(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Generate job ID
	jobID := uuid.New().String()

	// Convert to Spark Runner JobSpec
	jobSpec := s.convertToJobSpec(&req)

	// Submit to Spark Runner
	ctx := r.Context()
	executionID, err := s.sparkRunner.Submit(ctx, jobSpec)
	if err != nil {
		log.Printf("Failed to submit job: %v", err)
		http.Error(w, fmt.Sprintf("failed to submit job: %v", err), http.StatusInternalServerError)
		return
	}

	// Store job
	job := &Job{
		ID:          jobID,
		Name:        req.Name,
		Team:        req.Team,
		User:        req.User,
		ExecutionID: executionID,
		Status:      JobStatusSubmitted,
		StartTime:   time.Now(),
		Config:      req.Config,
	}

	s.mu.Lock()
	s.jobs[jobID] = job
	s.mu.Unlock()

	// Return response
	resp := SubmitJobResponse{
		JobID:       jobID,
		ExecutionID: executionID,
		Status:      JobStatusSubmitted,
		Message:     "Job submitted successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(resp)
}

// handleGetJobStatus handles GET /api/v1/jobs/{jobID}
func (s *Server) handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	s.mu.RLock()
	job, exists := s.jobs[jobID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// Check current status from Spark Runner if job is still active
	if job.Status == JobStatusSubmitted || job.Status == JobStatusRunning {
		ctx := r.Context()
		status, err := s.sparkRunner.Check(ctx, job.ExecutionID)
		if err == nil {
			s.mu.Lock()
			job.Status = s.mapStatus(status)
			if job.Status == JobStatusSucceeded || job.Status == JobStatusFailed {
				now := time.Now()
				job.EndTime = &now
			}
			s.mu.Unlock()
		}
	}

	resp := GetJobStatusResponse{
		JobID:        job.ID,
		ExecutionID:  job.ExecutionID,
		Status:       job.Status,
		StartTime:    &job.StartTime,
		EndTime:      job.EndTime,
		SparkUIURL:   job.SparkUIURL,
		LogsURL:      job.LogsURL,
		ErrorMessage: job.ErrorMessage,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleCancelJob handles DELETE /api/v1/jobs/{jobID}
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	s.mu.RLock()
	job, exists := s.jobs[jobID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// Cancel via Spark Runner
	ctx := r.Context()
	if err := s.sparkRunner.Cancel(ctx, job.ExecutionID); err != nil {
		http.Error(w, fmt.Sprintf("failed to cancel job: %v", err), http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	job.Status = JobStatusCancelled
	now := time.Now()
	job.EndTime = &now
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Job cancelled successfully",
	})
}

// handleListJobs handles GET /api/v1/jobs
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	team := r.URL.Query().Get("team")
	statusFilter := r.URL.Query().Get("status")

	s.mu.RLock()
	var jobs []*Job
	for _, job := range s.jobs {
		if team != "" && job.Team != team {
			continue
		}
		if statusFilter != "" && string(job.Status) != statusFilter {
			continue
		}
		jobs = append(jobs, job)
	}
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jobs": jobs,
	})
}

// handleGetJobLogs handles GET /api/v1/jobs/{jobID}/logs
func (s *Server) handleGetJobLogs(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("jobID")

	s.mu.RLock()
	job, exists := s.jobs[jobID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	// In production, this would fetch logs from ODIN
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"logs":     "Logs would be fetched from ODIN...",
		"logs_url": fmt.Sprintf("https://odin.doordash.team/logs?job_id=%s", job.ExecutionID),
	})
}

// handleHealthCheck handles GET /health
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
	})
}

// validateSubmitRequest validates the submit request
func (s *Server) validateSubmitRequest(req *SubmitJobRequest) error {
	if req.Name == "" {
		return fmt.Errorf("name is required")
	}
	if req.Team == "" {
		return fmt.Errorf("team is required")
	}
	if req.User == "" {
		return fmt.Errorf("user is required")
	}
	if req.Config.CoreETL == nil && req.Config.CustomJar == nil {
		return fmt.Errorf("either core_etl or custom_jar config is required")
	}
	return nil
}

// convertToJobSpec converts the REST request to Spark Runner JobSpec
func (s *Server) convertToJobSpec(req *SubmitJobRequest) *JobSpec {
	spec := &JobSpec{
		Name:        req.Name,
		Team:        req.Team,
		User:        req.User,
		SparkConfig: req.Config.SparkConfig,
		JobType:     req.Config.JobType,
		Driver:      req.Config.Resources.Driver,
		Executor:    req.Config.Resources.Executor,
	}

	if req.Config.CoreETL != nil {
		// CoreETL job
		spec.MainClass = "com.doordash.coreetl.spark.Main"
		spec.JarPath = fmt.Sprintf("s3://core-etl-build-artifacts/jars/%s/core-etl-driver-bundle.jar",
			req.Config.CoreETL.Version)
		spec.Args = []string{
			"--job-name", req.Name,
			"--job-spec", string(req.Config.CoreETL.JobSpec),
		}
	} else if req.Config.CustomJar != nil {
		// Custom JAR job
		spec.MainClass = req.Config.CustomJar.MainClass
		spec.JarPath = req.Config.CustomJar.JarPath
		spec.Args = req.Config.CustomJar.Args
	}

	return spec
}

// mapStatus maps Spark Runner status to JobStatus
func (s *Server) mapStatus(status string) JobStatus {
	switch status {
	case "SUBMITTED":
		return JobStatusSubmitted
	case "RUNNING":
		return JobStatusRunning
	case "SUCCEEDED":
		return JobStatusSucceeded
	case "FAILED":
		return JobStatusFailed
	case "CANCELLED":
		return JobStatusCancelled
	default:
		return JobStatusPending
	}
}
