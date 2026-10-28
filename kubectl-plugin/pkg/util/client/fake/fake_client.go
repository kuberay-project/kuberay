package fake

import (
	"context"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
)

type FakeClient struct {
	err                 error
	kuberayImageVersion string
}

func NewFakeClient() *FakeClient {
	return &FakeClient{}
}

func (c *FakeClient) WithKubeRayImageVersion(version string) *FakeClient {
	c.kuberayImageVersion = version
	return c
}

func (c *FakeClient) WithKubeRayOperatorVersionError(err error) *FakeClient {
	c.err = err
	return c
}

func (c *FakeClient) GetKubeRayOperatorVersion(_ context.Context) (string, error) {
	return c.kuberayImageVersion, c.err
}

func (c *FakeClient) GetRayHeadSvcName(_ context.Context, _ string, _ util.ResourceType, _ string) (string, error) {
	return "", nil
}

func (c *FakeClient) WaitRayClusterProvisioned(_ context.Context, _ string, _ string, _ time.Duration) error {
	return nil
}

func (c *FakeClient) KubernetesClient() kubernetes.Interface {
	return nil
}

func (c *FakeClient) RayClient() rayclient.Interface {
	return nil
}
