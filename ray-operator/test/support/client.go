package support

import (
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.) to ensure that exec-entrypoint and run can make use of them
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"

	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
)

type Client interface {
	Core() kubernetes.Interface
	Ray() rayclient.Interface
	Dynamic() dynamic.Interface
	Config() rest.Config
	Gateway() gatewayclient.Interface
}

type testClient struct {
	core    kubernetes.Interface
	ray     rayclient.Interface
	dynamic dynamic.Interface
	config  rest.Config
	gateway gatewayclient.Interface
}

var _ Client = (*testClient)(nil)

func (t *testClient) Core() kubernetes.Interface {
	return t.core
}

func (t *testClient) Ray() rayclient.Interface {
	return t.ray
}

func (t *testClient) Dynamic() dynamic.Interface {
	return t.dynamic
}

func (t *testClient) Config() rest.Config {
	return t.config
}

func (t *testClient) Gateway() gatewayclient.Interface {
	return t.gateway
}

func newTestClient() (Client, error) {
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	rayClient, err := rayclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	gatewayClient, err := gatewayclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &testClient{
		core:    kubeClient,
		ray:     rayClient,
		dynamic: dynamicClient,
		config:  *cfg,
		gateway: gatewayClient,
	}, nil
}
