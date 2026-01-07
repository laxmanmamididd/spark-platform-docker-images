// Package sparkgateway provides a Spark Gateway implementation for testing
// that routes jobs to Docker containers or Kubernetes clusters.
//
// The Spark Gateway sits between SparkRunner and the compute backend:
//   - Receives SparkApplication CRD requests from Spark Runner
//   - Enforces domain-specific namespace model
//   - Routes to appropriate cluster via Cluster Proxy Service
//   - Manages job TTL and cleanup
package sparkgateway

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"

	sparkrunner "github.com/doordash/spark-testing-example/spark-runner"
)

// Backend represents the compute backend type
type Backend string

const (
	BackendDocker Backend = "DOCKER"
	BackendK8s    Backend = "K8S"
	BackendEMR    Backend = "EMR" // Fallback
)

// GatewayConfig holds configuration for the gateway
type GatewayConfig struct {
	Backend          Backend
	SparkImage       string
	SparkConnectPort int
	SparkUIPort      int
	DefaultNamespace string
}

// DefaultConfig returns a default gateway configuration for testing
func DefaultConfig() *GatewayConfig {
	return &GatewayConfig{
		Backend:          BackendDocker,
		SparkImage:       "spark-testing:latest",
		SparkConnectPort: 15002,
		SparkUIPort:      4040,
		DefaultNamespace: "spark-test",
	}
}

// Job represents a running Spark job
type Job struct {
	ID           string
	ContainerID  string
	State        sparkrunner.ExecutionState
	StartTime    time.Time
	EndTime      *time.Time
	SparkUIURL   string
	SparkConnect string
}

// DockerGateway implements SparkGateway using Docker containers
type DockerGateway struct {
	config       *GatewayConfig
	dockerClient *client.Client
	jobs         map[string]*Job
	mu           sync.RWMutex
}

// NewDockerGateway creates a new Docker-based Spark Gateway
func NewDockerGateway(config *GatewayConfig) (*DockerGateway, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &DockerGateway{
		config:       config,
		dockerClient: cli,
		jobs:         make(map[string]*Job),
	}, nil
}

// SubmitJob submits a Spark job to the Docker backend
func (g *DockerGateway) SubmitJob(ctx context.Context, spec *sparkrunner.JobSpec) (string, error) {
	jobID := uuid.New().String()

	// Pull image if needed
	_, err := g.dockerClient.ImagePull(ctx, g.config.SparkImage, image.PullOptions{})
	if err != nil {
		// Image might already exist locally, continue
		fmt.Printf("Warning: could not pull image: %v\n", err)
	}

	// Build environment variables from spec
	env := []string{
		"SPARK_ROLE=driver",
		"SPARK_CONNECT_ENABLED=true",
		fmt.Sprintf("SPARK_CONNECT_PORT=%d", g.config.SparkConnectPort),
	}

	// Add spark config as environment
	for key, value := range spec.SparkConfig {
		env = append(env, fmt.Sprintf("SPARK_CONF_%s=%s", key, value))
	}

	// Port bindings
	portBindings := nat.PortMap{
		nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkConnectPort)): []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: "0"}, // Random port
		},
		nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkUIPort)): []nat.PortBinding{
			{HostIP: "0.0.0.0", HostPort: "0"}, // Random port
		},
	}

	// Build command
	var cmd []string
	if spec.MainClass != "" {
		cmd = append(cmd, "spark-submit",
			"--class", spec.MainClass,
			"--master", "local[*]",
		)

		// Add spark config
		for key, value := range spec.SparkConfig {
			cmd = append(cmd, "--conf", fmt.Sprintf("%s=%s", key, value))
		}

		cmd = append(cmd, spec.JarPath)
		cmd = append(cmd, spec.Args...)
	}

	// Create container
	resp, err := g.dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: g.config.SparkImage,
			Env:   env,
			Cmd:   cmd,
			ExposedPorts: nat.PortSet{
				nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkConnectPort)): struct{}{},
				nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkUIPort)):      struct{}{},
			},
			Labels: map[string]string{
				"spark.job.id":   jobID,
				"spark.job.name": spec.Name,
				"spark.job.team": spec.Team,
			},
		},
		&container.HostConfig{
			PortBindings: portBindings,
			AutoRemove:   false, // Keep for inspection
		},
		nil, nil, fmt.Sprintf("spark-job-%s", jobID[:8]),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	// Start container
	if err := g.dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("failed to start container: %w", err)
	}

	// Get container info for port mappings
	info, err := g.dockerClient.ContainerInspect(ctx, resp.ID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container: %w", err)
	}

	// Extract mapped ports
	sparkConnectHost := ""
	sparkUIHost := ""
	if ports := info.NetworkSettings.Ports; ports != nil {
		if binding := ports[nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkConnectPort))]; len(binding) > 0 {
			sparkConnectHost = fmt.Sprintf("localhost:%s", binding[0].HostPort)
		}
		if binding := ports[nat.Port(fmt.Sprintf("%d/tcp", g.config.SparkUIPort))]; len(binding) > 0 {
			sparkUIHost = fmt.Sprintf("http://localhost:%s", binding[0].HostPort)
		}
	}

	// Store job info
	job := &Job{
		ID:           jobID,
		ContainerID:  resp.ID,
		State:        sparkrunner.StateRunning,
		StartTime:    time.Now(),
		SparkUIURL:   sparkUIHost,
		SparkConnect: sparkConnectHost,
	}

	g.mu.Lock()
	g.jobs[jobID] = job
	g.mu.Unlock()

	return jobID, nil
}

// GetJobStatus retrieves the status of a job
func (g *DockerGateway) GetJobStatus(ctx context.Context, vendorRunID string) (sparkrunner.ExecutionState, error) {
	g.mu.RLock()
	job, exists := g.jobs[vendorRunID]
	g.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("job not found: %s", vendorRunID)
	}

	// Check container status
	info, err := g.dockerClient.ContainerInspect(ctx, job.ContainerID)
	if err != nil {
		return sparkrunner.StateFailed, nil
	}

	switch {
	case info.State.Running:
		return sparkrunner.StateRunning, nil
	case info.State.ExitCode == 0:
		return sparkrunner.StateSucceeded, nil
	case info.State.ExitCode != 0:
		return sparkrunner.StateFailed, nil
	default:
		return sparkrunner.StateSubmitted, nil
	}
}

// CancelJob cancels a running job
func (g *DockerGateway) CancelJob(ctx context.Context, vendorRunID string) error {
	g.mu.RLock()
	job, exists := g.jobs[vendorRunID]
	g.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job not found: %s", vendorRunID)
	}

	// Stop container
	timeout := 30 // seconds
	if err := g.dockerClient.ContainerStop(ctx, job.ContainerID, container.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	// Update job state
	g.mu.Lock()
	job.State = sparkrunner.StateCancelled
	now := time.Now()
	job.EndTime = &now
	g.mu.Unlock()

	return nil
}

// GetSparkConnectAddress returns the Spark Connect address for a job
func (g *DockerGateway) GetSparkConnectAddress(jobID string) (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	job, exists := g.jobs[jobID]
	if !exists {
		return "", fmt.Errorf("job not found: %s", jobID)
	}

	if job.SparkConnect == "" {
		return "", fmt.Errorf("spark connect not available for job: %s", jobID)
	}

	return fmt.Sprintf("sc://%s", job.SparkConnect), nil
}

// GetJobLogs retrieves logs for a job
func (g *DockerGateway) GetJobLogs(ctx context.Context, jobID string) (string, error) {
	g.mu.RLock()
	job, exists := g.jobs[jobID]
	g.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("job not found: %s", jobID)
	}

	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Timestamps: true,
	}

	logs, err := g.dockerClient.ContainerLogs(ctx, job.ContainerID, options)
	if err != nil {
		return "", fmt.Errorf("failed to get logs: %w", err)
	}
	defer logs.Close()

	logBytes, err := io.ReadAll(logs)
	if err != nil {
		return "", fmt.Errorf("failed to read logs: %w", err)
	}

	return string(logBytes), nil
}

// Cleanup removes all containers created by the gateway
func (g *DockerGateway) Cleanup(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, job := range g.jobs {
		// Force remove container
		if err := g.dockerClient.ContainerRemove(ctx, job.ContainerID, container.RemoveOptions{
			Force: true,
		}); err != nil {
			fmt.Printf("Warning: failed to remove container %s: %v\n", job.ContainerID, err)
		}
	}

	g.jobs = make(map[string]*Job)
	return nil
}
