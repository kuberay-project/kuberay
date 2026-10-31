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
			Labels: map[string]string{
				"ray.io/config-type": "compute-template",
			},
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
			Labels: map[string]string{
				"ray.io/config-type": "compute-template",
			},
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

func TestGetComputeTemplateRequiresComputeTemplateLabel(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	unlabelledConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unlabelled",
			Namespace: namespace,
		},
	}
	computeTemplateConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "compute-template",
			Namespace: namespace,
			Labels: map[string]string{
				"ray.io/config-type": "compute-template",
			},
		},
	}

	configMapClient := kubernetesfake.NewClientset(unlabelledConfigMap, computeTemplateConfigMap).CoreV1().ConfigMaps(namespace)
	ctrl := gomock.NewController(t)
	mockClientManager := NewMockClientManagerInterface(ctrl)
	mockKubeClient := client.NewMockKubernetesClientInterface(ctrl)
	mockClientManager.EXPECT().KubernetesClient().Return(mockKubeClient).Times(2)
	mockKubeClient.EXPECT().ConfigMapClient(namespace).Return(configMapClient).Times(2)

	resourceManager := NewResourceManager(mockClientManager)

	configMap, err := resourceManager.GetComputeTemplate(ctx, unlabelledConfigMap.Name, namespace)
	require.Error(t, err)
	assert.Nil(t, configMap)

	configMap, err = resourceManager.GetComputeTemplate(ctx, computeTemplateConfigMap.Name, namespace)
	require.NoError(t, err)
	assert.Equal(t, computeTemplateConfigMap.Name, configMap.Name)
}

func TestDeleteComputeTemplateDoesNotDeleteUnlabelledConfigMap(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unlabelled",
			Namespace: namespace,
		},
	}
	configMapClient := kubernetesfake.NewClientset(configMap).CoreV1().ConfigMaps(namespace)

	ctrl := gomock.NewController(t)
	mockClientManager := NewMockClientManagerInterface(ctrl)
	mockKubeClient := client.NewMockKubernetesClientInterface(ctrl)
	mockClientManager.EXPECT().KubernetesClient().Return(mockKubeClient)
	mockKubeClient.EXPECT().ConfigMapClient(namespace).Return(configMapClient)

	resourceManager := NewResourceManager(mockClientManager)
	err := resourceManager.DeleteComputeTemplate(ctx, configMap.Name, namespace)
	require.Error(t, err)

	_, err = configMapClient.Get(ctx, configMap.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestPopulateComputeTemplateRejectsUnlabelledConfigMap(t *testing.T) {
	ctx := context.Background()
	namespace := "test-namespace"
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unlabelled",
			Namespace: namespace,
		},
	}
	clusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: configMap.Name,
		},
	}
	configMapClient := kubernetesfake.NewClientset(configMap).CoreV1().ConfigMaps(namespace)

	ctrl := gomock.NewController(t)
	mockClientManager := NewMockClientManagerInterface(ctrl)
	mockKubeClient := client.NewMockKubernetesClientInterface(ctrl)
	mockClientManager.EXPECT().KubernetesClient().Return(mockKubeClient)
	mockKubeClient.EXPECT().ConfigMapClient(namespace).Return(configMapClient)

	resourceManager := NewResourceManager(mockClientManager)
	computeTemplates, err := resourceManager.PopulateComputeTemplate(ctx, clusterSpec, namespace)
	require.Error(t, err)
	assert.Nil(t, computeTemplates)
}
