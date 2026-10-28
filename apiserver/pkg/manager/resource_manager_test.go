package manager

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/ray-project/kuberay/apiserver/pkg/client"
	api "github.com/ray-project/kuberay/proto/go_client"
)

func TestPopulateComputeTemplate(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	headConfigMapName := "head"
	workerConfigMapName := "worker"

	headConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			"name":      headConfigMapName,
			"namespace": namespace,
			"cpu":       "2",
			"memory":    "4",
		},
	}
	workerConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerConfigMapName,
			Namespace: namespace,
		},
		Data: map[string]string{
			"name":      workerConfigMapName,
			"namespace": namespace,
			"cpu":       "2",
			"memory":    "4",
		},
	}

	clusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: headConfigMapName,
		},
		WorkerGroupSpec: []*api.WorkerGroupSpec{
			{
				ComputeTemplate: workerConfigMapName,
			},
		},
	}

	// mock controller
	ctrl := gomock.NewController(t)

	// mock client manager
	mockClientManager := NewMockClientManagerInterface(ctrl)
	mockKubeClient := client.NewMockKubernetesClientInterface(ctrl)
	mockClientManager.EXPECT().KubernetesClient().Return(mockKubeClient).Times(2)

	// mock config map client
	fakeClientset := kubernetesfake.NewClientset(headConfigMap, workerConfigMap)
	configMapClient := fakeClientset.CoreV1().ConfigMaps(namespace)
	mockKubeClient.EXPECT().ConfigMapClient(namespace).Return(configMapClient).Times(2)

	// Run
	resourceManager := NewResourceManager(mockClientManager)
	computeTemplates, err := resourceManager.PopulateComputeTemplate(ctx, clusterSpec, namespace)

	// Assert
	require.NoError(t, err)
	assert.Len(t, computeTemplates, 2)
	assert.Contains(t, computeTemplates, headConfigMapName)
	assert.Contains(t, computeTemplates, workerConfigMapName)
}
