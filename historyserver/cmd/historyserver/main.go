package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/historyserver"
)

func main() {
	storageBackend := ""
	storageRootDir := ""
	kubeconfigs := ""
	storageBackendConfigPath := ""
	dashboardDir := ""
	useKubernetesProxy := false
	useAuthTokenMode := false
	enableLiveClusters := false
	qps := historyserver.DefaultKubeAPIQPS
	burst := historyserver.DefaultKubeAPIBurst
	sessionProcessTimeout := historyserver.DefaultSessionProcessTimeout
	sessionCacheSize := historyserver.DefaultSessionCacheSize
	sessionCacheMaxMemory := historyserver.DefaultSessionCacheMaxMemory
	sessionCacheTTL := historyserver.DefaultSessionCacheTTL
	flag.StringVar(&storageBackend, "storage-backend", "", "Storage backend: s3 / gcs / azureblob / aliyunoss / localtest")
	flag.StringVar(&storageRootDir, "storage-root-dir", "", "The root dir inside the bucket")
	flag.StringVar(&kubeconfigs, "kubeconfigs", "", "Kubeconfig path; empty = in-cluster")
	flag.StringVar(&dashboardDir, "dashboard-dir", "/dashboard", "Path to Ray Dashboard static assets")
	flag.StringVar(&storageBackendConfigPath, "storage-backend-config-path", "", "Path to backend config JSON")
	flag.BoolVar(&useKubernetesProxy, "use-kubernetes-proxy", false, "Use local kubeconfig instead of in-cluster config")
	flag.BoolVar(&useAuthTokenMode, "use-auth-token-mode", false, "Enable Ray dashboard token authentication mode (requires x-ray-authorization header)")
	flag.BoolVar(&enableLiveClusters, "enable-live-clusters", false, "Enable access to live clusters")
	flag.Float64Var(&qps, "kube-api-qps", historyserver.DefaultKubeAPIQPS, "The QPS value for the client communicating with the Kubernetes API server.")
	flag.IntVar(&burst, "kube-api-burst", historyserver.DefaultKubeAPIBurst, "The maximum burst for throttling requests from this client to the Kubernetes API server.")
	flag.DurationVar(&sessionProcessTimeout, "session-process-timeout", historyserver.DefaultSessionProcessTimeout, "Timeout duration for processing and loading a single Ray cluster session.")
	flag.IntVar(&sessionCacheSize, "session-cache-size", historyserver.DefaultSessionCacheSize, "Max number of dead-session snapshots held in the LRU cache.")
	flag.StringVar(&sessionCacheMaxMemory, "session-cache-max-memory", historyserver.DefaultSessionCacheMaxMemory, `Soft cap on memory held by cached snapshots, as a Kubernetes quantity (e.g. "1Gi"); tune under pod memory. "0" disables the bound.`)
	flag.DurationVar(&sessionCacheTTL, "session-cache-ttl", historyserver.DefaultSessionCacheTTL, "How long a dead-session snapshot stays cached after last access. 0 disables TTL.")
	flag.Parse()

	if val := os.Getenv("STORAGE_BACKEND"); val != "" {
		storageBackend = val
	}
	if storageBackend == "" {
		logrus.Fatal("--storage-backend or STORAGE_BACKEND environment variable is required")
	}
	storageBackend = strings.ToLower(storageBackend)

	if val := os.Getenv("STORAGE_ROOT_DIR"); val != "" {
		storageRootDir = val
	}

	if qps <= 0 {
		logrus.Fatalf("--kube-api-qps must be > 0, got %v", qps)
	}
	if burst <= 0 {
		logrus.Fatalf("--kube-api-burst must be > 0, got %d", burst)
	}
	if sessionCacheSize <= 0 {
		logrus.Fatalf("--session-cache-size must be > 0, got %d", sessionCacheSize)
	}
	sessionCacheMaxBytes, err := historyserver.ParseSessionCacheMaxMemory(sessionCacheMaxMemory)
	if err != nil {
		logrus.Fatalf("--session-cache-max-memory %q: %v", sessionCacheMaxMemory, err)
	}
	if sessionCacheTTL < 0 {
		logrus.Fatalf("--session-cache-ttl must be >= 0, got %s", sessionCacheTTL)
	}
	// Auth-token mode only affects proxied live-cluster requests. Warn rather than failing so that
	// deployments upgrading across the default flip do not CrashLoopBackOff.
	if useAuthTokenMode && !enableLiveClusters {
		logrus.Warnf("--use-auth-token-mode has no effect while --enable-live-clusters=false")
	}

	cliMgr, err := historyserver.NewClientManager(historyserver.ClientManagerConfig{
		Kubeconfigs:        kubeconfigs,
		UseKubernetesProxy: useKubernetesProxy,
		QPS:                float32(qps),
		Burst:              burst,
	})
	if err != nil {
		logrus.Fatalf("Failed to create client manager: %v", err)
	}

	jsonData := make(map[string]any)
	if storageBackendConfigPath != "" {
		data, err := os.ReadFile(storageBackendConfigPath)
		if err != nil {
			logrus.Fatalf("Failed to read storage-backend-config-path from %s: %v", storageBackendConfigPath, err)
		}
		if err := json.Unmarshal(data, &jsonData); err != nil {
			logrus.Fatalf("Failed to parse storage-backend-config-path from %s: %v", storageBackendConfigPath, err)
		}
	}

	registry := collector.GetReaderRegistry()
	factory, ok := registry[storageBackend]
	if !ok {
		logrus.Fatalf("Unsupported storage-backend for reader: %s", storageBackend)
	}

	globalConfig := types.RayHistoryServerConfig{
		RootDir: storageRootDir,
	}

	reader, err := factory(&globalConfig, jsonData)
	if err != nil {
		logrus.Fatalf("Failed to create reader for storage backend %s: %v", storageBackend, err)
	}

	serverCtx, serverCancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer serverCancel()

	processor := historyserver.NewSessionProcessor(reader, cliMgr.Client())
	sessionLoader := historyserver.NewSessionLoader(serverCtx, processor, sessionProcessTimeout, sessionCacheSize, sessionCacheMaxBytes, sessionCacheTTL)

	// ServerHandler.Run consumes a stop chan; bridge serverCtx into it.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	go func() {
		<-serverCtx.Done()
		logrus.Info("Received shutdown signal, initiating graceful shutdown...")
		close(stop)
	}()

	handler, err := historyserver.NewServerHandler(&globalConfig, dashboardDir, reader, cliMgr, sessionLoader, useKubernetesProxy, useAuthTokenMode, enableLiveClusters)
	if err != nil {
		logrus.Fatalf("Failed to create server handler: %v", err)
	}

	wg.Go(func() {
		if err := handler.Run(stop); err != nil {
			logrus.Errorf("HTTP server exited with error: %v", err)
		}
		logrus.Info("HTTP server shutdown complete")
	})

	wg.Wait()
	logrus.Info("Graceful shutdown complete")
}
