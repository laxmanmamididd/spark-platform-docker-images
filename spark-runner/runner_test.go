package sparkrunner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockGateway is a mock implementation of SparkGateway
type MockGateway struct {
	mock.Mock
}

func (m *MockGateway) SubmitJob(ctx context.Context, spec *JobSpec) (string, error) {
	args := m.Called(ctx, spec)
	return args.String(0), args.Error(1)
}

func (m *MockGateway) GetJobStatus(ctx context.Context, vendorRunID string) (ExecutionState, error) {
	args := m.Called(ctx, vendorRunID)
	return args.Get(0).(ExecutionState), args.Error(1)
}

func (m *MockGateway) CancelJob(ctx context.Context, vendorRunID string) error {
	args := m.Called(ctx, vendorRunID)
	return args.Error(0)
}

func TestRunner_Submit(t *testing.T) {
	tests := []struct {
		name          string
		jobSpec       *JobSpec
		setupMock     func(*MockGateway)
		expectError   bool
		validateResp  func(*testing.T, *SubmitResponse)
	}{
		{
			name: "successful submission",
			jobSpec: &JobSpec{
				Name:      "test-job",
				Team:      "data-infra",
				User:      "test-user",
				MainClass: "com.example.Main",
				JarPath:   "s3://bucket/app.jar",
				JobType:   JobTypeBatch,
			},
			setupMock: func(m *MockGateway) {
				m.On("SubmitJob", mock.Anything, mock.Anything).Return("vendor-123", nil)
			},
			expectError: false,
			validateResp: func(t *testing.T, resp *SubmitResponse) {
				assert.NotEmpty(t, resp.ExecutionID)
				assert.Equal(t, "vendor-123", resp.VendorRunID)
			},
		},
		{
			name:    "nil job spec",
			jobSpec: nil,
			setupMock: func(m *MockGateway) {
				// No mock needed - should fail before gateway call
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockGateway := new(MockGateway)
			tt.setupMock(mockGateway)

			runner := NewRunner(mockGateway)
			ctx := context.Background()

			resp, err := runner.Submit(ctx, &SubmitRequest{
				JobSpec:        tt.jobSpec,
				IdempotencyKey: "test-key",
			})

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, resp)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, resp)
				if tt.validateResp != nil {
					tt.validateResp(t, resp)
				}
			}

			mockGateway.AssertExpectations(t)
		})
	}
}

func TestRunner_Check(t *testing.T) {
	tests := []struct {
		name         string
		setupRunner  func(*Runner)
		executionID  string
		gatewayState ExecutionState
		expectError  bool
		expectState  ExecutionState
	}{
		{
			name: "check running job",
			setupRunner: func(r *Runner) {
				r.executions["exec-1"] = &Execution{
					ID:          "exec-1",
					JobPath:     "test-job",
					State:       StateRunning,
					VendorRunID: "vendor-123",
				}
			},
			executionID:  "exec-1",
			gatewayState: StateRunning,
			expectError:  false,
			expectState:  StateRunning,
		},
		{
			name: "check completed job",
			setupRunner: func(r *Runner) {
				r.executions["exec-2"] = &Execution{
					ID:          "exec-2",
					JobPath:     "test-job",
					State:       StateRunning,
					VendorRunID: "vendor-456",
				}
			},
			executionID:  "exec-2",
			gatewayState: StateSucceeded,
			expectError:  false,
			expectState:  StateSucceeded,
		},
		{
			name:         "execution not found",
			setupRunner:  func(r *Runner) {},
			executionID:  "nonexistent",
			gatewayState: StateRunning,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockGateway := new(MockGateway)
			runner := NewRunner(mockGateway)
			tt.setupRunner(runner)

			if !tt.expectError {
				mockGateway.On("GetJobStatus", mock.Anything, mock.Anything).Return(tt.gatewayState, nil)
			}

			ctx := context.Background()
			resp, err := runner.Check(ctx, &CheckRequest{
				ExecutionID: tt.executionID,
			})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectState, resp.Execution.State)
			}
		})
	}
}

func TestRunner_Cancel(t *testing.T) {
	tests := []struct {
		name        string
		setupRunner func(*Runner)
		executionID string
		expectError bool
		expectState ExecutionState
	}{
		{
			name: "cancel running job",
			setupRunner: func(r *Runner) {
				r.executions["exec-1"] = &Execution{
					ID:          "exec-1",
					State:       StateRunning,
					VendorRunID: "vendor-123",
				}
			},
			executionID: "exec-1",
			expectError: false,
			expectState: StateCancelled,
		},
		{
			name: "cannot cancel completed job",
			setupRunner: func(r *Runner) {
				r.executions["exec-2"] = &Execution{
					ID:          "exec-2",
					State:       StateSucceeded,
					VendorRunID: "vendor-456",
				}
			},
			executionID: "exec-2",
			expectError: false, // Returns success=false, not error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockGateway := new(MockGateway)
			runner := NewRunner(mockGateway)
			tt.setupRunner(runner)

			if tt.expectState == StateCancelled {
				mockGateway.On("CancelJob", mock.Anything, mock.Anything).Return(nil)
			}

			ctx := context.Background()
			resp, err := runner.Cancel(ctx, &CancelRequest{
				ExecutionID: tt.executionID,
				Team:        "test-team",
			})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectState == StateCancelled {
					assert.True(t, resp.Success)

					// Verify state was updated
					checkResp, _ := runner.Check(ctx, &CheckRequest{
						ExecutionID: tt.executionID,
					})
					assert.Equal(t, StateCancelled, checkResp.Execution.State)
				}
			}
		})
	}
}

func TestRunner_ListExecutions(t *testing.T) {
	runner := NewRunner(new(MockGateway))

	// Add some executions
	runner.executions["exec-1"] = &Execution{
		ID:        "exec-1",
		JobPath:   "job-a",
		StartTime: time.Now().Add(-2 * time.Hour),
	}
	runner.executions["exec-2"] = &Execution{
		ID:        "exec-2",
		JobPath:   "job-a",
		StartTime: time.Now().Add(-1 * time.Hour),
	}
	runner.executions["exec-3"] = &Execution{
		ID:        "exec-3",
		JobPath:   "job-b",
		StartTime: time.Now(),
	}

	ctx := context.Background()

	// List executions for job-a
	executions, err := runner.ListExecutions(ctx, "job-a", 10)
	assert.NoError(t, err)
	assert.Len(t, executions, 2)

	// List with limit
	executions, err = runner.ListExecutions(ctx, "job-a", 1)
	assert.NoError(t, err)
	assert.Len(t, executions, 1)

	// List executions for job-b
	executions, err = runner.ListExecutions(ctx, "job-b", 10)
	assert.NoError(t, err)
	assert.Len(t, executions, 1)
}
