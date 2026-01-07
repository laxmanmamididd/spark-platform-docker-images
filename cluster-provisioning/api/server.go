// Package api provides the Cluster Provisioning API server implementation.
//
// This service allows teams to request, manage, and monitor Spark Connect clusters
// through a self-service API similar to Fabricator.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Environment represents the deployment environment.
type Environment int32

const (
	EnvironmentUnspecified Environment = 0
	EnvironmentDev         Environment = 1
	EnvironmentStaging     Environment = 2
	EnvironmentCI          Environment = 3
	EnvironmentProd        Environment = 4
)

// Region represents the AWS region.
type Region int32

const (
	RegionUnspecified Region = 0
	RegionUSWest2     Region = 1
	RegionUSEast1     Region = 2
	RegionEUWest1     Region = 3
)

// ClusterSize represents predefined cluster sizes.
type ClusterSize int32

const (
	ClusterSizeUnspecified ClusterSize = 0
	ClusterSizeSmall       ClusterSize = 1
	ClusterSizeMedium      ClusterSize = 2
	ClusterSizeLarge       ClusterSize = 3
	ClusterSizeXLarge      ClusterSize = 4
)

// ClusterStatus represents the cluster state.
type ClusterStatus int32

const (
	ClusterStatusUnspecified  ClusterStatus = 0
	ClusterStatusPending      ClusterStatus = 1
	ClusterStatusProvisioning ClusterStatus = 2
	ClusterStatusRunning      ClusterStatus = 3
	ClusterStatusScaling      ClusterStatus = 4
	ClusterStatusTerminating  ClusterStatus = 5
	ClusterStatusTerminated   ClusterStatus = 6
	ClusterStatusFailed       ClusterStatus = 7
)

// Backend represents the compute backend.
type Backend int32

const (
	BackendUnspecified Backend = 0
	BackendSK8         Backend = 1
	BackendEMR         Backend = 2
	BackendDBR         Backend = 3
)

// ResourceConfig specifies cluster resources.
type ResourceConfig struct {
	DriverCores       int32
	DriverMemory      string
	ExecutorInstances int32
	ExecutorCores     int32
	ExecutorMemory    string
}

// CatalogConfig specifies Unity Catalog configuration.
type CatalogConfig struct {
	Name      string
	IsDefault bool
}

// Cluster represents a Spark Connect cluster.
type Cluster struct {
	ClusterID       string
	Team            string
	Name            string
	Environment     Environment
	Region          Region
	Status          ClusterStatus
	Endpoint        string
	Backend         Backend
	Size            ClusterSize
	CustomResources *ResourceConfig
	Catalogs        []CatalogConfig
	CreatedAt       time.Time
	ExpiresAt       *time.Time
	LastActivityAt  time.Time
	Namespace       string
	SparkVersion    string
}

// RequestClusterRequest is the request to create a cluster.
type RequestClusterRequest struct {
	Team             string
	Name             string
	Environment      Environment
	Region           Region
	Size             ClusterSize
	CustomResources  *ResourceConfig
	Catalogs         []string
	TTL              string
	PreferredBackend Backend
	SparkVersion     string
	RequesterEmail   string
	IdempotencyKey   string
}

// RequestClusterResponse is the response after creating a cluster.
type RequestClusterResponse struct {
	Cluster               *Cluster
	EstimatedReadySeconds int32
}

// ClusterProvisioningServer implements the cluster provisioning service.
type ClusterProvisioningServer struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster

	// Backend clients
	sparkGatewayAddr string
	sparkRunnerAddr  string

	// Default configurations
	defaultSparkVersion string
	defaultCatalogs     []string
}

// NewClusterProvisioningServer creates a new server instance.
func NewClusterProvisioningServer(sparkGatewayAddr, sparkRunnerAddr string) *ClusterProvisioningServer {
	return &ClusterProvisioningServer{
		clusters:            make(map[string]*Cluster),
		sparkGatewayAddr:    sparkGatewayAddr,
		sparkRunnerAddr:     sparkRunnerAddr,
		defaultSparkVersion: "3.5.0",
		defaultCatalogs:     []string{"pedregal"},
	}
}

// RequestCluster creates a new Spark Connect cluster.
func (s *ClusterProvisioningServer) RequestCluster(ctx context.Context, req *RequestClusterRequest) (*RequestClusterResponse, error) {
	// Validate request
	if req.Team == "" {
		return nil, status.Error(codes.InvalidArgument, "team is required")
	}
	if req.Environment == EnvironmentUnspecified {
		return nil, status.Error(codes.InvalidArgument, "environment is required")
	}
	if req.Region == RegionUnspecified {
		return nil, status.Error(codes.InvalidArgument, "region is required")
	}

	// Generate cluster ID
	clusterID := generateClusterID(req.Team, req.Environment, req.Region)

	// Determine cluster name
	name := req.Name
	if name == "" {
		name = fmt.Sprintf("%s-%s-cluster", req.Team, environmentToString(req.Environment))
	}

	// Determine catalogs
	catalogs := req.Catalogs
	if len(catalogs) == 0 {
		catalogs = s.defaultCatalogs
	}

	// Determine Spark version
	sparkVersion := req.SparkVersion
	if sparkVersion == "" {
		sparkVersion = s.defaultSparkVersion
	}

	// Build namespace
	namespace := buildNamespace(req.Team, req.Environment, req.Region)

	// Build endpoint
	endpoint := buildEndpoint(req.Team, req.Environment, req.Region)

	// Determine backend
	backend := req.PreferredBackend
	if backend == BackendUnspecified {
		backend = selectBackend(req.Region)
	}

	// Parse TTL
	var expiresAt *time.Time
	if req.TTL != "" && req.TTL != "persistent" {
		duration, err := time.ParseDuration(req.TTL)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid TTL: %v", err)
		}
		t := time.Now().Add(duration)
		expiresAt = &t
	}

	// Build catalog configs
	catalogConfigs := make([]CatalogConfig, len(catalogs))
	for i, c := range catalogs {
		catalogConfigs[i] = CatalogConfig{
			Name:      c,
			IsDefault: i == 0,
		}
	}

	// Create cluster record
	now := time.Now()
	cluster := &Cluster{
		ClusterID:       clusterID,
		Team:            req.Team,
		Name:            name,
		Environment:     req.Environment,
		Region:          req.Region,
		Status:          ClusterStatusProvisioning,
		Endpoint:        endpoint,
		Backend:         backend,
		Size:            req.Size,
		CustomResources: req.CustomResources,
		Catalogs:        catalogConfigs,
		CreatedAt:       now,
		ExpiresAt:       expiresAt,
		LastActivityAt:  now,
		Namespace:       namespace,
		SparkVersion:    sparkVersion,
	}

	// Store cluster
	s.mu.Lock()
	s.clusters[clusterID] = cluster
	s.mu.Unlock()

	// Start async provisioning (in production, this would call Spark Gateway/Runner)
	go s.provisionCluster(ctx, cluster)

	// Estimate ready time based on backend
	estimatedSeconds := int32(120) // 2 minutes for SK8
	if backend == BackendEMR {
		estimatedSeconds = 360 // 6 minutes for EMR
	}

	return &RequestClusterResponse{
		Cluster:               cluster,
		EstimatedReadySeconds: estimatedSeconds,
	}, nil
}

// GetCluster retrieves cluster details.
func (s *ClusterProvisioningServer) GetCluster(ctx context.Context, clusterID string) (*Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return nil, status.Error(codes.NotFound, "cluster not found")
	}

	return cluster, nil
}

// ListClusters lists clusters for a team.
func (s *ClusterProvisioningServer) ListClusters(ctx context.Context, team string, env Environment, region Region) ([]*Cluster, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*Cluster
	for _, c := range s.clusters {
		if c.Team != team {
			continue
		}
		if env != EnvironmentUnspecified && c.Environment != env {
			continue
		}
		if region != RegionUnspecified && c.Region != region {
			continue
		}
		result = append(result, c)
	}

	return result, nil
}

// DeleteCluster terminates a cluster.
func (s *ClusterProvisioningServer) DeleteCluster(ctx context.Context, clusterID string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return status.Error(codes.NotFound, "cluster not found")
	}

	// Update status
	cluster.Status = ClusterStatusTerminating

	// Start async termination
	go s.terminateCluster(ctx, cluster)

	return nil
}

// ScaleCluster adjusts cluster resources.
func (s *ClusterProvisioningServer) ScaleCluster(ctx context.Context, clusterID string, newSize ClusterSize, newResources *ResourceConfig) (*Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return nil, status.Error(codes.NotFound, "cluster not found")
	}

	if cluster.Status != ClusterStatusRunning {
		return nil, status.Error(codes.FailedPrecondition, "cluster must be running to scale")
	}

	// Update status
	cluster.Status = ClusterStatusScaling
	cluster.Size = newSize
	cluster.CustomResources = newResources

	// Start async scaling
	go s.scaleCluster(ctx, cluster)

	return cluster, nil
}

// ExtendClusterTTL extends the cluster expiration.
func (s *ClusterProvisioningServer) ExtendClusterTTL(ctx context.Context, clusterID string, extension string) (*Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cluster, ok := s.clusters[clusterID]
	if !ok {
		return nil, status.Error(codes.NotFound, "cluster not found")
	}

	duration, err := time.ParseDuration(extension)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid extension duration: %v", err)
	}

	if cluster.ExpiresAt == nil {
		// Cluster is persistent, nothing to extend
		return cluster, nil
	}

	newExpiry := cluster.ExpiresAt.Add(duration)
	cluster.ExpiresAt = &newExpiry

	return cluster, nil
}

// provisionCluster handles async cluster provisioning.
func (s *ClusterProvisioningServer) provisionCluster(ctx context.Context, cluster *Cluster) {
	// Simulate provisioning time
	time.Sleep(5 * time.Second)

	// In production, this would:
	// 1. Call Spark Gateway to create namespace
	// 2. Call Spark Runner to submit SparkApplication
	// 3. Wait for cluster to be ready
	// 4. Register DNS endpoint

	s.mu.Lock()
	cluster.Status = ClusterStatusRunning
	s.mu.Unlock()
}

// terminateCluster handles async cluster termination.
func (s *ClusterProvisioningServer) terminateCluster(ctx context.Context, cluster *Cluster) {
	time.Sleep(2 * time.Second)

	s.mu.Lock()
	cluster.Status = ClusterStatusTerminated
	s.mu.Unlock()
}

// scaleCluster handles async cluster scaling.
func (s *ClusterProvisioningServer) scaleCluster(ctx context.Context, cluster *Cluster) {
	time.Sleep(3 * time.Second)

	s.mu.Lock()
	cluster.Status = ClusterStatusRunning
	s.mu.Unlock()
}

// Helper functions

func generateClusterID(team string, env Environment, region Region) string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return fmt.Sprintf("sc-%s-%s-%s-%s",
		team[:min(6, len(team))],
		environmentToString(env)[:3],
		regionToString(region)[:4],
		hex.EncodeToString(bytes)[:8],
	)
}

func buildNamespace(team string, env Environment, region Region) string {
	return fmt.Sprintf("sjns-%s-%s-%s", team, environmentToString(env), regionToString(region))
}

func buildEndpoint(team string, env Environment, region Region) string {
	teamShort := team
	if len(team) > 10 {
		teamShort = team[:10]
	}
	return fmt.Sprintf("sc://%s-%s-%s.doordash.team:15002",
		teamShort,
		environmentToString(env),
		regionToString(region),
	)
}

func selectBackend(region Region) Backend {
	// EU region defaults to EMR (no SK8 clusters there yet)
	if region == RegionEUWest1 {
		return BackendEMR
	}
	return BackendSK8
}

func environmentToString(env Environment) string {
	switch env {
	case EnvironmentDev:
		return "dev"
	case EnvironmentStaging:
		return "staging"
	case EnvironmentCI:
		return "ci"
	case EnvironmentProd:
		return "prod"
	default:
		return "unknown"
	}
}

func regionToString(region Region) string {
	switch region {
	case RegionUSWest2:
		return "us-west-2"
	case RegionUSEast1:
		return "us-east-1"
	case RegionEUWest1:
		return "eu-west-1"
	default:
		return "unknown"
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// StartServer starts the gRPC server.
func StartServer(addr string, sparkGatewayAddr, sparkRunnerAddr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	server := grpc.NewServer()
	// Register ClusterProvisioningServer here

	return server.Serve(lis)
}
