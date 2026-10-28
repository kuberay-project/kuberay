package runtime

import (
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/logcollector/runtime/logcollector"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func NewCollector(config *types.RayCollectorConfig, writer storage.StorageWriter) RayLogCollector {
	handler := logcollector.RayLogHandler{
		IsHead:   config.Role == "Head",
		LogFiles: make(chan string),

		RootDir:    config.RootDir,
		SessionDir: config.SessionDir,

		RayClusterName:      config.RayClusterName,
		RayClusterNamespace: config.RayClusterNamespace,
		RayNodeName:         config.RayNodeName,

		LogBatching:          config.LogBatching,
		PushInterval:         config.PushInterval,
		DashboardAddress:     config.DashboardAddress,
		AdditionalEndpoints:  config.AdditionalEndpoints,
		EndpointPollInterval: config.EndpointPollInterval,

		LogUploadInterval: config.LogUploadInterval,

		HttpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,              // Max idle connections
				MaxIdleConnsPerHost: 20,               // Max idle connections per host
				IdleConnTimeout:     90 * time.Second, // Idle connection timeout
			},
		},
		Writer:       writer,
		ShutdownChan: make(chan struct{}),
	}

	handler.OwnerKind = config.OwnerKind
	handler.OwnerName = config.OwnerName
	if handler.OwnerKind != "" && handler.OwnerName != "" {
		logrus.Infof("The associated owner resource is: %s/%s", handler.OwnerKind, handler.OwnerName)
	}

	logDir := strings.TrimSpace(filepath.Join(config.SessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME))
	handler.LogDir = logDir
	clusterRootDir := clusterlogs.Prefix(handler.RootDir, handler.OwnerKind, handler.OwnerName, handler.RayClusterNamespace, handler.RayClusterName)
	handler.ClusterDir = clusterRootDir

	return &handler
}
