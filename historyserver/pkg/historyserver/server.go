package historyserver

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/transport"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
)

type ServerHandler struct {
	maxClusters  int
	rootDir      string
	dashboardDir string

	reader        storage.StorageReader
	clientManager *ClientManager
	sessionLoader *SessionLoader
	httpClient    *http.Client

	useKubernetesProxy bool
	useAuthTokenMode   bool
	enableLiveClusters bool
}

func NewServerHandler(
	c *types.RayHistoryServerConfig,
	dashboardDir string,
	reader storage.StorageReader,
	clientManager *ClientManager,
	sessionLoader *SessionLoader,
	useKubernetesProxy bool,
	useAuthTokenMode bool,
	enableLiveClusters bool,
) (*ServerHandler, error) {
	handler := &ServerHandler{
		reader:        reader,
		clientManager: clientManager,
		sessionLoader: sessionLoader,

		rootDir:      c.RootDir,
		dashboardDir: dashboardDir,
		// TODO: make this configurable
		maxClusters: 100,

		useAuthTokenMode:   useAuthTokenMode,
		enableLiveClusters: enableLiveClusters,
	}

	if len(clientManager.configs) > 0 {
		k8sRestConfig := clientManager.configs[0]
		if useKubernetesProxy {
			transportConfig, err := k8sRestConfig.TransportConfig()
			if err != nil {
				return nil, fmt.Errorf("failed to get transport config: %w", err)
			}

			// Create a Kubernetes-aware round tripper that can handle authentication and transport security.
			rt, err := transport.New(transportConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create Kubernetes-aware round tripper: %w", err)
			}

			handler.httpClient = &http.Client{
				Timeout:   30 * time.Second,
				Transport: rt,
			}
			handler.useKubernetesProxy = true
		} else {
			// Create a simple HTTP client that doesn't use Kubernetes API server proxy.
			handler.httpClient = &http.Client{
				Timeout: 30 * time.Second,
			}
			handler.useKubernetesProxy = false
		}
	}
	return handler, nil
}

func (s *ServerHandler) Run(stop <-chan struct{}) error {
	s.RegisterRouter()
	port := ":8080"
	server := &http.Server{
		Addr:         port,             // Listen address
		ReadTimeout:  5 * time.Second,  // Read timeout
		WriteTimeout: 35 * time.Second, // Write response timeout (must be >= httpClient.Timeout for proxy requests)
		IdleTimeout:  60 * time.Second, // Idle timeout
	}
	go func() {
		logrus.Infof("Starting server on %s", port)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("Error starting server: %v", err)
		}
		logrus.Infof("Server stopped gracefully")
	}()

	<-stop
	logrus.Warnf("Receive stop single, so stop ray history server")
	// Create a context with 1 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Shutdown the server
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Ray HistoryServer forced to shutdown: %v", err)
	}
	return nil
}
