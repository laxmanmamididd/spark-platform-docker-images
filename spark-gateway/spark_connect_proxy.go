// Package sparkgateway provides Spark Connect gRPC proxy functionality.
//
// The Spark Gateway acts as a proxy for Spark Connect traffic, providing:
//   - Team-based authentication (Okta integration)
//   - Cluster discovery (mapping team/env to cluster endpoint)
//   - gRPC routing to the correct Driver Pod
//   - Hot cluster management
//   - Load balancing across cluster pools
package sparkgateway

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ClusterInfo represents a Spark Connect cluster endpoint.
type ClusterInfo struct {
	ClusterID       string
	Team            string
	Environment     string
	Region          string
	Endpoint        string // e.g., "10.0.1.5:15002" (driver pod IP:port)
	Status          string
	LastHealthCheck time.Time
	ActiveSessions  int32
}

// SparkConnectProxy handles gRPC proxying for Spark Connect traffic.
type SparkConnectProxy struct {
	mu       sync.RWMutex
	clusters map[string]*ClusterInfo // key: team-env-region

	// Authentication
	authEnabled bool
	authService AuthService

	// Health checking
	healthCheckInterval time.Duration
	stopHealthCheck     chan struct{}

	// Hot clusters
	hotClusters map[string][]*ClusterInfo // key: team, value: pre-warmed clusters

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener
}

// AuthService interface for authentication.
type AuthService interface {
	// ValidateToken validates an auth token and returns the team it belongs to.
	ValidateToken(ctx context.Context, token string) (team string, err error)

	// GetTeamForUser returns the team for a given user.
	GetTeamForUser(ctx context.Context, user string) (team string, err error)
}

// ProxyConfig holds configuration for the proxy.
type ProxyConfig struct {
	ListenAddr          string
	AuthEnabled         bool
	HealthCheckInterval time.Duration
}

// DefaultProxyConfig returns default proxy configuration.
func DefaultProxyConfig() *ProxyConfig {
	return &ProxyConfig{
		ListenAddr:          ":15002",
		AuthEnabled:         true,
		HealthCheckInterval: 30 * time.Second,
	}
}

// NewSparkConnectProxy creates a new Spark Connect proxy.
func NewSparkConnectProxy(config *ProxyConfig) *SparkConnectProxy {
	return &SparkConnectProxy{
		clusters:            make(map[string]*ClusterInfo),
		hotClusters:         make(map[string][]*ClusterInfo),
		authEnabled:         config.AuthEnabled,
		healthCheckInterval: config.HealthCheckInterval,
		stopHealthCheck:     make(chan struct{}),
	}
}

// RegisterCluster registers a new cluster endpoint.
func (p *SparkConnectProxy) RegisterCluster(cluster *ClusterInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := p.clusterKey(cluster.Team, cluster.Environment, cluster.Region)
	p.clusters[key] = cluster
}

// UnregisterCluster removes a cluster endpoint.
func (p *SparkConnectProxy) UnregisterCluster(team, environment, region string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := p.clusterKey(team, environment, region)
	delete(p.clusters, key)
}

// GetCluster returns the cluster info for a team/env/region.
func (p *SparkConnectProxy) GetCluster(team, environment, region string) (*ClusterInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := p.clusterKey(team, environment, region)
	cluster, ok := p.clusters[key]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no cluster found for %s/%s/%s", team, environment, region)
	}

	return cluster, nil
}

// Start starts the gRPC proxy server.
func (p *SparkConnectProxy) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	p.listener = lis

	// Create gRPC server with unknown service handler for proxying
	p.grpcServer = grpc.NewServer(
		grpc.UnknownServiceHandler(p.proxyHandler),
	)

	// Start health checking
	go p.healthCheckLoop()

	// Start serving
	return p.grpcServer.Serve(lis)
}

// Stop stops the proxy server.
func (p *SparkConnectProxy) Stop() {
	close(p.stopHealthCheck)
	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}
}

// proxyHandler handles unknown gRPC methods by proxying to the appropriate cluster.
func (p *SparkConnectProxy) proxyHandler(srv interface{}, serverStream grpc.ServerStream) error {
	ctx := serverStream.Context()

	// Extract metadata for routing
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.InvalidArgument, "missing metadata")
	}

	// Authenticate request
	team, err := p.authenticate(ctx, md)
	if err != nil {
		return err
	}

	// Extract routing info from metadata
	environment := getMetadataValue(md, "x-spark-environment", "dev")
	region := getMetadataValue(md, "x-spark-region", "us-west-2")

	// Get cluster endpoint
	cluster, err := p.GetCluster(team, environment, region)
	if err != nil {
		return err
	}

	// Create connection to backend cluster
	backendConn, err := grpc.Dial(
		cluster.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return status.Errorf(codes.Unavailable, "failed to connect to cluster: %v", err)
	}
	defer backendConn.Close()

	// Get the full method name
	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Error(codes.Internal, "failed to get method name")
	}

	// Create client stream to backend
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	clientStream, err := backendConn.NewStream(clientCtx, &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}, fullMethodName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create client stream: %v", err)
	}

	// Forward metadata to backend
	outMd := metadata.New(map[string]string{
		"x-spark-team":        team,
		"x-spark-environment": environment,
		"x-spark-region":      region,
	})
	clientStream.SetHeader(outMd)

	// Bidirectional streaming proxy
	errChan := make(chan error, 2)

	// Server -> Backend
	go func() {
		for {
			msg := new([]byte)
			if err := serverStream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					clientStream.CloseSend()
					errChan <- nil
					return
				}
				errChan <- err
				return
			}
			if err := clientStream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Backend -> Server
	go func() {
		for {
			msg := new([]byte)
			if err := clientStream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					errChan <- nil
					return
				}
				errChan <- err
				return
			}
			if err := serverStream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Wait for both directions to complete
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

// authenticate validates the request and returns the team.
func (p *SparkConnectProxy) authenticate(ctx context.Context, md metadata.MD) (string, error) {
	if !p.authEnabled {
		// Without auth, require team in metadata
		teams := md.Get("x-spark-team")
		if len(teams) == 0 {
			return "", status.Error(codes.Unauthenticated, "x-spark-team header required")
		}
		return teams[0], nil
	}

	// With auth, validate token
	tokens := md.Get("authorization")
	if len(tokens) == 0 {
		return "", status.Error(codes.Unauthenticated, "authorization header required")
	}

	if p.authService == nil {
		return "", status.Error(codes.Internal, "auth service not configured")
	}

	team, err := p.authService.ValidateToken(ctx, tokens[0])
	if err != nil {
		return "", status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	return team, nil
}

// healthCheckLoop periodically checks cluster health.
func (p *SparkConnectProxy) healthCheckLoop() {
	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.checkAllClusters()
		case <-p.stopHealthCheck:
			return
		}
	}
}

// checkAllClusters health checks all registered clusters.
func (p *SparkConnectProxy) checkAllClusters() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for key, cluster := range p.clusters {
		healthy := p.checkClusterHealth(cluster)
		if healthy {
			cluster.Status = "HEALTHY"
		} else {
			cluster.Status = "UNHEALTHY"
		}
		cluster.LastHealthCheck = time.Now()
		p.clusters[key] = cluster
	}
}

// checkClusterHealth checks if a cluster is healthy.
func (p *SparkConnectProxy) checkClusterHealth(cluster *ClusterInfo) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, cluster.Endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	return true
}

// clusterKey generates a key for the cluster map.
func (p *SparkConnectProxy) clusterKey(team, environment, region string) string {
	return fmt.Sprintf("%s-%s-%s", team, environment, region)
}

// getMetadataValue extracts a value from metadata with a default.
func getMetadataValue(md metadata.MD, key, defaultValue string) string {
	values := md.Get(key)
	if len(values) > 0 {
		return values[0]
	}
	return defaultValue
}

// AddHotCluster adds a pre-warmed cluster for a team.
func (p *SparkConnectProxy) AddHotCluster(team string, cluster *ClusterInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.hotClusters[team] = append(p.hotClusters[team], cluster)
}

// GetHotCluster returns and removes an available hot cluster for a team.
func (p *SparkConnectProxy) GetHotCluster(team string) (*ClusterInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	clusters, ok := p.hotClusters[team]
	if !ok || len(clusters) == 0 {
		return nil, false
	}

	// Pop first cluster
	cluster := clusters[0]
	p.hotClusters[team] = clusters[1:]

	return cluster, true
}

// GetHotClusterCount returns the number of hot clusters for a team.
func (p *SparkConnectProxy) GetHotClusterCount(team string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return len(p.hotClusters[team])
}
