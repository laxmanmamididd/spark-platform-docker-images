// Package sparkrunner provides a Spark Runner implementation for testing
// that mirrors the Pedregal Spark Runner architecture.
//
// The Spark Runner provides three primitives:
//   - Submit: Create a new Spark execution
//   - Check: Retrieve execution state and metadata
//   - Cancel: Attempt to cancel a running execution
package sparkrunner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ExecutionState represents the state of a Spark job execution
type ExecutionState string

const (
	StateSubmitted ExecutionState = "SUBMITTED"
	StateRunning   ExecutionState = "RUNNING"
	StateSucceeded ExecutionState = "SUCCEEDED"
	StateFailed    ExecutionState = "FAILED"
	StateCancelled ExecutionState = "CANCELLED"
)

// JobSpec represents a Spark job specification (mirrors Pedregal JobSpec)
type JobSpec struct {
	Name        string            `json:"name"`
	Team        string            `json:"team"`
	User        string            `json:"user"`
	MainClass   string            `json:"main_class"`
	JarPath     string            `json:"jar_path"`
	Args        []string          `json:"args"`
	SparkConfig map[string]string `json:"spark_config"`
	JobType     JobType           `json:"job_type"`
}

// JobType represents the type of Spark job
type JobType string

const (
	JobTypeBatch     JobType = "BATCH"
	JobTypeStreaming JobType = "STREAMING"
)

// Execution represents a Spark job execution
type Execution struct {
	ID           string          `json:"execution_id"`
	JobPath      string          `json:"job_path"`
	Owner        string          `json:"owner"`
	Team         string          `json:"team"`
	StartTime    time.Time       `json:"start_time"`
	EndTime      *time.Time      `json:"end_time,omitempty"`
	State        ExecutionState  `json:"state"`
	Vendor       string          `json:"vendor"`
	VendorRunID  string          `json:"vendor_run_id,omitempty"`
	VendorWebUI  string          `json:"vendor_webui,omitempty"`
	VendorLogs   string          `json:"vendor_logs_link,omitempty"`
}

// SubmitRequest represents a request to submit a Spark job
type SubmitRequest struct {
	JobSpec         *JobSpec `json:"job_spec"`
	IdempotencyKey  string   `json:"idempotency_key"`
}

// SubmitResponse represents the response from a Submit call
type SubmitResponse struct {
	ExecutionID string `json:"execution_id"`
	VendorRunID string `json:"vendor_run_id"`
}

// CheckRequest represents a request to check job status
type CheckRequest struct {
	ExecutionID string `json:"execution_id"`
}

// CheckResponse represents the response from a Check call
type CheckResponse struct {
	Execution *Execution `json:"execution"`
}

// CancelRequest represents a request to cancel a job
type CancelRequest struct {
	ExecutionID string `json:"execution_id"`
	Team        string `json:"team"`
}

// CancelResponse represents the response from a Cancel call
type CancelResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// SparkGateway is the interface for the Spark Gateway service
type SparkGateway interface {
	// SubmitJob submits a job to the Spark compute backend
	SubmitJob(ctx context.Context, spec *JobSpec) (vendorRunID string, err error)

	// GetJobStatus retrieves the status of a job
	GetJobStatus(ctx context.Context, vendorRunID string) (ExecutionState, error)

	// CancelJob cancels a running job
	CancelJob(ctx context.Context, vendorRunID string) error
}

// Runner implements the Spark Runner interface
type Runner struct {
	gateway    SparkGateway
	executions map[string]*Execution
	mu         sync.RWMutex
}

// NewRunner creates a new Spark Runner instance
func NewRunner(gateway SparkGateway) *Runner {
	return &Runner{
		gateway:    gateway,
		executions: make(map[string]*Execution),
	}
}

// Submit creates a new Spark execution
func (r *Runner) Submit(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error) {
	if req.JobSpec == nil {
		return nil, fmt.Errorf("job spec is required")
	}

	// Generate execution ID (UUID)
	executionID := uuid.New().String()

	// Submit to gateway
	vendorRunID, err := r.gateway.SubmitJob(ctx, req.JobSpec)
	if err != nil {
		return nil, fmt.Errorf("failed to submit job to gateway: %w", err)
	}

	// Create execution record
	execution := &Execution{
		ID:          executionID,
		JobPath:     req.JobSpec.Name,
		Owner:       req.JobSpec.User,
		Team:        req.JobSpec.Team,
		StartTime:   time.Now(),
		State:       StateSubmitted,
		Vendor:      "SK8", // Default to SK8 (Spark on K8s)
		VendorRunID: vendorRunID,
	}

	// Store execution
	r.mu.Lock()
	r.executions[executionID] = execution
	r.mu.Unlock()

	return &SubmitResponse{
		ExecutionID: executionID,
		VendorRunID: vendorRunID,
	}, nil
}

// Check retrieves the current execution state
func (r *Runner) Check(ctx context.Context, req *CheckRequest) (*CheckResponse, error) {
	r.mu.RLock()
	execution, exists := r.executions[req.ExecutionID]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("execution not found: %s", req.ExecutionID)
	}

	// If still running, check with gateway for updated status
	if execution.State == StateSubmitted || execution.State == StateRunning {
		state, err := r.gateway.GetJobStatus(ctx, execution.VendorRunID)
		if err != nil {
			return nil, fmt.Errorf("failed to get job status: %w", err)
		}

		r.mu.Lock()
		execution.State = state
		if state == StateSucceeded || state == StateFailed || state == StateCancelled {
			now := time.Now()
			execution.EndTime = &now
		}
		r.mu.Unlock()
	}

	return &CheckResponse{
		Execution: execution,
	}, nil
}

// Cancel attempts to cancel a running execution
func (r *Runner) Cancel(ctx context.Context, req *CancelRequest) (*CancelResponse, error) {
	r.mu.RLock()
	execution, exists := r.executions[req.ExecutionID]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("execution not found: %s", req.ExecutionID)
	}

	// Can only cancel if not already terminal
	if execution.State == StateSucceeded || execution.State == StateFailed || execution.State == StateCancelled {
		return &CancelResponse{
			Success: false,
			Message: fmt.Sprintf("cannot cancel execution in state: %s", execution.State),
		}, nil
	}

	// Cancel via gateway
	if err := r.gateway.CancelJob(ctx, execution.VendorRunID); err != nil {
		return nil, fmt.Errorf("failed to cancel job: %w", err)
	}

	// Update execution state
	r.mu.Lock()
	execution.State = StateCancelled
	now := time.Now()
	execution.EndTime = &now
	r.mu.Unlock()

	return &CancelResponse{
		Success: true,
		Message: "execution cancelled successfully",
	}, nil
}

// ListExecutions returns all executions for a job
func (r *Runner) ListExecutions(ctx context.Context, jobName string, limit int) ([]*Execution, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []*Execution
	for _, exec := range r.executions {
		if exec.JobPath == jobName {
			results = append(results, exec)
		}
		if len(results) >= limit {
			break
		}
	}

	return results, nil
}
