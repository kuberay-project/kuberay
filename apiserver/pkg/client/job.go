package client

import (
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/ray-project/kuberay/apiserver/pkg/util"
	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	rayv1 "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
)

type JobClientInterface interface {
	RayJobClient(namespace string) rayv1.RayJobInterface
}

type RayJobClient struct {
	client rayv1.RayV1Interface
}

func (cc RayJobClient) RayJobClient(namespace string) rayv1.RayJobInterface {
	return cc.client.RayJobs(namespace)
}

func NewRayJobClientOrFatal(options util.ClientOptions) JobClientInterface {
	cfg, err := config.GetConfig()
	if err != nil {
		klog.Fatalf("Failed to create RayCluster client. Error: %v", err)
	}
	cfg.QPS = options.QPS
	cfg.Burst = options.Burst

	rayJobClient := rayclient.NewForConfigOrDie(cfg).RayV1()
	return &RayJobClient{client: rayJobClient}
}
