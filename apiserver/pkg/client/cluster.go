package client

import (
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/ray-project/kuberay/apiserver/pkg/util"
	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	rayv1 "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
)

//go:generate mockgen -source=cluster.go -destination=cluster_mock.go -package=client

type ClusterClientInterface interface {
	RayClusterClient(namespace string) rayv1.RayClusterInterface
}

type RayClusterClient struct {
	client rayv1.RayV1Interface
}

func (cc RayClusterClient) RayClusterClient(namespace string) rayv1.RayClusterInterface {
	return cc.client.RayClusters(namespace)
}

func NewRayClusterClientOrFatal(options util.ClientOptions) ClusterClientInterface {
	cfg, err := config.GetConfig()
	if err != nil {
		klog.Fatalf("Failed to create RayCluster client. Error: %v", err)
	}
	cfg.QPS = options.QPS
	cfg.Burst = options.Burst

	rayClusterClient := rayclient.NewForConfigOrDie(cfg).RayV1()
	return &RayClusterClient{client: rayClusterClient}
}
