// ============================================================================
// Cluster Registry - Discovers and manages Spark Connect clusters
// ============================================================================
// This component:
//   1. Discovers Spark Connect clusters via Kubernetes API
//   2. Tracks cluster health status
//   3. Manages hot cluster pool for fast assignment
//   4. Routes teams to their designated clusters
// ============================================================================

package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ============================================================================
// KUBERNETES CLUSTER REGISTRY
// ============================================================================

// K8sClusterRegistry discovers clusters from Kubernetes
type K8sClusterRegistry struct {
	k8sClient *kubernetes.Clientset

	// Cache of clusters
	clusters     map[string]*ClusterInfo // key: "team-env"
	clustersLock sync.RWMutex

	// Hot cluster pool (pre-warmed clusters)
	hotPool     []*ClusterInfo
	hotPoolLock sync.RWMutex

	// Team to cluster mapping (from ConfigMap or CRD)
	teamMapping     map[string]string // team -> namespace
	teamMappingLock sync.RWMutex
}

// NewK8sClusterRegistry creates a new Kubernetes-based cluster registry
func NewK8sClusterRegistry() (*K8sClusterRegistry, error) {
	// Create in-cluster Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	registry := &K8sClusterRegistry{
		k8sClient:   clientset,
		clusters:    make(map[string]*ClusterInfo),
		teamMapping: make(map[string]string),
	}

	// Load initial team mapping
	if err := registry.loadTeamMapping(context.Background()); err != nil {
		return nil, err
	}

	// Start background refresh
	go registry.refreshLoop()

	return registry, nil
}

// ============================================================================
// CLUSTER DISCOVERY
// ============================================================================

// GetClusterForTeam returns the Spark Connect cluster for a team/environment
func (r *K8sClusterRegistry) GetClusterForTeam(
	ctx context.Context,
	team, env string,
) (*ClusterInfo, error) {
	cacheKey := fmt.Sprintf("%s-%s", team, env)

	// Check cache first
	r.clustersLock.RLock()
	cluster, exists := r.clusters[cacheKey]
	r.clustersLock.RUnlock()

	if exists && cluster.Status == "READY" {
		return cluster, nil
	}

	// Discover from Kubernetes
	namespace := r.getNamespaceForTeam(team, env)
	cluster, err := r.discoverClusterInNamespace(ctx, namespace)
	if err != nil {
		// Try hot pool as fallback
		return r.assignHotCluster(ctx, team, env)
	}

	// Cache the result
	r.clustersLock.Lock()
	r.clusters[cacheKey] = cluster
	r.clustersLock.Unlock()

	return cluster, nil
}

// GetPlaygroundCluster returns/creates a playground cluster for a user
func (r *K8sClusterRegistry) GetPlaygroundCluster(
	ctx context.Context,
	userID string,
) (*ClusterInfo, error) {
	// Playground namespace pattern: sjns-playground-<user-hash>
	namespace := fmt.Sprintf("sjns-playground-%s", hashUserID(userID))

	// Check if cluster exists
	cluster, err := r.discoverClusterInNamespace(ctx, namespace)
	if err == nil {
		return cluster, nil
	}

	// Create playground cluster on-demand
	return r.createPlaygroundCluster(ctx, userID, namespace)
}

// ListClusters returns all available clusters
func (r *K8sClusterRegistry) ListClusters(ctx context.Context) ([]*ClusterInfo, error) {
	r.clustersLock.RLock()
	defer r.clustersLock.RUnlock()

	clusters := make([]*ClusterInfo, 0, len(r.clusters))
	for _, c := range r.clusters {
		clusters = append(clusters, c)
	}
	return clusters, nil
}

// ============================================================================
// KUBERNETES DISCOVERY
// ============================================================================

// discoverClusterInNamespace finds Spark Connect server in a namespace
func (r *K8sClusterRegistry) discoverClusterInNamespace(
	ctx context.Context,
	namespace string,
) (*ClusterInfo, error) {
	// Look for Service named "spark-connect-server"
	svc, err := r.k8sClient.CoreV1().Services(namespace).Get(
		ctx,
		"spark-connect-server",
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("spark-connect-server not found in %s: %w", namespace, err)
	}

	// Build endpoint
	endpoint := fmt.Sprintf("%s.%s.svc.cluster.local:15002", svc.Name, namespace)

	// Check if the backing pods are ready
	status := "READY"
	endpoints, err := r.k8sClient.CoreV1().Endpoints(namespace).Get(
		ctx,
		"spark-connect-server",
		metav1.GetOptions{},
	)
	if err != nil || len(endpoints.Subsets) == 0 {
		status = "STARTING"
	}

	// Extract team from namespace (sjns-<team>-<env>)
	team := extractTeamFromNamespace(namespace)

	return &ClusterInfo{
		Endpoint:    endpoint,
		Team:        team,
		Environment: extractEnvFromNamespace(namespace),
		Status:      status,
		LastUsed:    time.Now(),
	}, nil
}

// ============================================================================
// HOT CLUSTER POOL
// ============================================================================

// assignHotCluster assigns a pre-warmed cluster from the hot pool
func (r *K8sClusterRegistry) assignHotCluster(
	ctx context.Context,
	team, env string,
) (*ClusterInfo, error) {
	r.hotPoolLock.Lock()
	defer r.hotPoolLock.Unlock()

	// Find an available hot cluster
	for i, cluster := range r.hotPool {
		if cluster.Status == "READY" {
			// Remove from pool
			r.hotPool = append(r.hotPool[:i], r.hotPool[i+1:]...)

			// Update cluster info
			cluster.Team = team
			cluster.Environment = env
			cluster.LastUsed = time.Now()

			// Cache it
			cacheKey := fmt.Sprintf("%s-%s", team, env)
			r.clustersLock.Lock()
			r.clusters[cacheKey] = cluster
			r.clustersLock.Unlock()

			// Trigger replenishment of hot pool
			go r.replenishHotPool(ctx)

			return cluster, nil
		}
	}

	return nil, fmt.Errorf("no hot clusters available")
}

// replenishHotPool creates new hot clusters to maintain pool size
func (r *K8sClusterRegistry) replenishHotPool(ctx context.Context) {
	// Target pool size
	targetSize := 3

	r.hotPoolLock.Lock()
	currentSize := len(r.hotPool)
	r.hotPoolLock.Unlock()

	if currentSize >= targetSize {
		return
	}

	// Create new clusters
	for i := currentSize; i < targetSize; i++ {
		cluster, err := r.createHotCluster(ctx, i)
		if err != nil {
			fmt.Printf("Failed to create hot cluster: %v\n", err)
			continue
		}

		r.hotPoolLock.Lock()
		r.hotPool = append(r.hotPool, cluster)
		r.hotPoolLock.Unlock()
	}
}

// createHotCluster creates a new pre-warmed Spark Connect cluster
func (r *K8sClusterRegistry) createHotCluster(ctx context.Context, index int) (*ClusterInfo, error) {
	namespace := fmt.Sprintf("sjns-hot-pool-%d", index)

	// Here you would create the SparkApplication CRD
	// This is simplified - real implementation would use Spark Operator client

	return &ClusterInfo{
		Endpoint:    fmt.Sprintf("spark-connect-server.%s.svc.cluster.local:15002", namespace),
		Team:        "",
		Environment: "",
		Status:      "READY",
		LastUsed:    time.Time{},
	}, nil
}

// ============================================================================
// PLAYGROUND CLUSTERS
// ============================================================================

// createPlaygroundCluster creates a new playground cluster for a user
func (r *K8sClusterRegistry) createPlaygroundCluster(
	ctx context.Context,
	userID, namespace string,
) (*ClusterInfo, error) {
	// 1. Create namespace if not exists
	// 2. Create SparkApplication CRD
	// 3. Wait for cluster to be ready

	// Simplified - real implementation would use Spark Operator client
	endpoint := fmt.Sprintf("spark-connect-server.%s.svc.cluster.local:15002", namespace)

	return &ClusterInfo{
		Endpoint:    endpoint,
		Team:        "playground",
		Environment: "playground",
		Status:      "STARTING",
		LastUsed:    time.Now(),
	}, nil
}

// ============================================================================
// TEAM MAPPING
// ============================================================================

// loadTeamMapping loads team to namespace mapping from ConfigMap
func (r *K8sClusterRegistry) loadTeamMapping(ctx context.Context) error {
	cm, err := r.k8sClient.CoreV1().ConfigMaps("spark-gateway").Get(
		ctx,
		"team-mapping",
		metav1.GetOptions{},
	)
	if err != nil {
		// Use default mapping pattern
		return nil
	}

	r.teamMappingLock.Lock()
	defer r.teamMappingLock.Unlock()

	for team, namespace := range cm.Data {
		r.teamMapping[team] = namespace
	}

	return nil
}

// getNamespaceForTeam returns the Kubernetes namespace for a team
func (r *K8sClusterRegistry) getNamespaceForTeam(team, env string) string {
	r.teamMappingLock.RLock()
	defer r.teamMappingLock.RUnlock()

	// Check explicit mapping
	if ns, exists := r.teamMapping[team]; exists {
		return ns
	}

	// Default pattern: sjns-<team>-<env>
	return fmt.Sprintf("sjns-%s-%s", team, env)
}

// ============================================================================
// BACKGROUND REFRESH
// ============================================================================

// refreshLoop periodically refreshes cluster status
func (r *K8sClusterRegistry) refreshLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ctx := context.Background()

		// Refresh team mapping
		r.loadTeamMapping(ctx)

		// Refresh cluster status
		r.clustersLock.RLock()
		clusters := make(map[string]*ClusterInfo)
		for k, v := range r.clusters {
			clusters[k] = v
		}
		r.clustersLock.RUnlock()

		for key, cluster := range clusters {
			// Re-discover to check health
			parts := extractTeamEnvFromKey(key)
			if len(parts) == 2 {
				namespace := r.getNamespaceForTeam(parts[0], parts[1])
				updated, err := r.discoverClusterInNamespace(ctx, namespace)
				if err != nil {
					cluster.Status = "UNHEALTHY"
				} else {
					cluster.Status = updated.Status
				}
			}
		}

		// Replenish hot pool
		r.replenishHotPool(ctx)
	}
}

// ============================================================================
// HELPERS
// ============================================================================

func hashUserID(userID string) string {
	// Simple hash for namespace name
	// Real implementation would use proper hashing
	return fmt.Sprintf("%x", userID)[:8]
}

func extractTeamFromNamespace(namespace string) string {
	// sjns-feature-eng-prod -> feature-eng
	// This is simplified
	return "unknown"
}

func extractEnvFromNamespace(namespace string) string {
	// sjns-feature-eng-prod -> prod
	return "prod"
}

func extractTeamEnvFromKey(key string) []string {
	// Split "team-env" into ["team", "env"]
	return []string{"team", "env"}
}
