package manager

import (
	"k8s.io/klog/v2"

	"github.com/ray-project/kuberay/apiserver/pkg/client"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
)

//go:generate mockgen -source=client_manager.go -destination=client_manager_mock.go -package=manager

type ClientManagerInterface interface {
	ClusterClient() client.ClusterClientInterface
	JobClient() client.JobClientInterface
	ServiceClient() client.ServiceClientInterface
	KubernetesClient() client.KubernetesClientInterface
	Time() util.TimeInterface
}

// Container for all service clients
type ClientManager struct {
	// Kubernetes clients
	clusterClient    client.ClusterClientInterface
	jobClient        client.JobClientInterface
	serviceClient    client.ServiceClientInterface
	kubernetesClient client.KubernetesClientInterface
	// auxiliary tools
	time util.TimeInterface
}

func (c *ClientManager) ClusterClient() client.ClusterClientInterface {
	return c.clusterClient
}

func (c *ClientManager) JobClient() client.JobClientInterface {
	return c.jobClient
}

func (c *ClientManager) ServiceClient() client.ServiceClientInterface {
	return c.serviceClient
}

func (c *ClientManager) KubernetesClient() client.KubernetesClientInterface {
	return c.kubernetesClient
}

func (c *ClientManager) Time() util.TimeInterface {
	return c.time
}

func (c *ClientManager) init() {
	// db, kubernetes initialization
	klog.Info("Initializing client manager")

	// configure configs
	defaultKubernetesClientConfig := util.ClientOptions{
		QPS:   5,
		Burst: 10,
	}

	// 1. utils initialization
	c.time = util.NewRealTime()

	// TODO: Potentially, we may need storage layer clients to help persist the data.
	// 2. kubernetes client initialization
	c.clusterClient = client.NewRayClusterClientOrFatal(defaultKubernetesClientConfig)
	c.jobClient = client.NewRayJobClientOrFatal(defaultKubernetesClientConfig)
	c.serviceClient = client.NewRayServiceClientOrFatal(defaultKubernetesClientConfig)
	c.kubernetesClient = client.CreateKubernetesCoreOrFatal(defaultKubernetesClientConfig)

	klog.Infof("Client manager initialized successfully")
}

func NewClientManager() ClientManager {
	clientManager := ClientManager{}
	clientManager.init()

	return clientManager
}
