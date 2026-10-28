package common

import (
	"context"
	"reflect"
	"testing"

	routev1 "github.com/openshift/api/route/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestRayServiceServeServiceNamespacedName(t *testing.T) {
	svc, err := BuildServeServiceForRayService(context.Background(), *serviceInstance, *instanceWithWrongSvc)
	require.NoError(t, err)
	namespaced := RayServiceServeServiceNamespacedName(serviceInstance)
	assert.Equal(t, svc.Name, namespaced.Name)
	assert.Equal(t, svc.Namespace, namespaced.Namespace)
}

func TestRayServiceServeServiceNamespacedNameForUserSpecifiedServeService(t *testing.T) {
	testRayServiceWithServeService := serviceInstance.DeepCopy()
	testRayServiceWithServeService.Spec.ServeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "user-custom-name",
		},
	}
	svc, err := BuildServeServiceForRayService(context.Background(), *testRayServiceWithServeService, *instanceWithWrongSvc)
	require.NoError(t, err)

	namespaced := RayServiceServeServiceNamespacedName(testRayServiceWithServeService)
	assert.Equal(t, svc.Namespace, namespaced.Namespace)
	assert.Equal(t, svc.Name, namespaced.Name)
	assert.Equal(t, "user-custom-name", namespaced.Name)
}

// TestRayClusterServeServiceNamespacedName tests the function for generating a NamespacedName for a RayCluster's serve service
func TestRayClusterServeServiceNamespacedName(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-example",
			Namespace: "default",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      utils.GenerateServeServiceName(instance.Name),
	}
	result := RayClusterServeServiceNamespacedName(instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayClusterAutoscalerRoleNamespacedName tests the function for generating a NamespacedName for a RayCluster's autoscaler role
func TestRayClusterAutoscalerRoleNamespacedName(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-autoscaler",
			Namespace: "default",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      instance.Name,
	}
	result := RayClusterAutoscalerRoleNamespacedName(instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayClusterAutoscalerRoleBindingNamespacedName tests the function for generating a NamespacedName for a RayCluster's autoscaler role binding
func TestRayClusterAutoscalerRoleBindingNamespacedName(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-autoscaler-binding",
			Namespace: "default",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      instance.Name,
	}
	result := RayClusterAutoscalerRoleBindingNamespacedName(instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayClusterAutoscalerServiceAccountNamespacedName tests the function for generating a NamespacedName for a RayCluster's autoscaler service account
func TestRayClusterAutoscalerServiceAccountNamespacedName(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-autoscaler-account",
			Namespace: "default",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      utils.GetHeadGroupServiceAccountName(instance),
	}
	result := RayClusterAutoscalerServiceAccountNamespacedName(instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRayClusterHeadlessServiceListOptions(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-headless",
			Namespace: "test-ns",
		},
	}
	headlessSvc := BuildHeadlessServiceForRayCluster(*instance)

	rayClusterName := ""
	for k, v := range headlessSvc.Labels {
		if k == utils.RayClusterHeadlessServiceLabelKey {
			rayClusterName = v
			break
		}
	}
	assert.Equal(t, rayClusterName, instance.Name)

	expected := []client.ListOption{
		client.InNamespace(headlessSvc.Namespace),
		client.MatchingLabels(map[string]string{utils.RayClusterHeadlessServiceLabelKey: rayClusterName}),
	}
	result := RayClusterHeadlessServiceListOptions(instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestRayClusterHeadServiceListOptions(t *testing.T) {
	instance := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster",
			Namespace: "test-ns",
		},
	}

	labels := HeadServiceLabels(instance)
	delete(labels, utils.KubernetesCreatedByLabelKey)
	delete(labels, utils.KubernetesApplicationNameLabelKey)

	expected := []client.ListOption{
		client.InNamespace(instance.Namespace),
		client.MatchingLabels(labels),
	}
	result := RayClusterHeadServiceListOptions(&instance)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayServiceActiveRayClusterNamespacedName tests the function for generating a NamespacedName for a RayService's active RayCluster
func TestRayServiceActiveRayClusterNamespacedName(t *testing.T) {
	rayService := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayservice-active",
			Namespace: "default",
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "active-ray-cluster",
			},
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      "active-ray-cluster",
	}
	result := RayServiceActiveRayClusterNamespacedName(rayService)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayServiceGatewayNamespacedName covers both the default per-RayService Gateway
// naming and the GatewayRef case, where the function must resolve to the
// referenced (shared) Gateway instead.
func TestRayServiceGatewayNamespacedName(t *testing.T) {
	t.Run("default per-RayService gateway", func(t *testing.T) {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "default"},
		}
		expected := types.NamespacedName{Namespace: "default", Name: "my-svc-gateway"}
		assert.Equal(t, expected, RayServiceGatewayNamespacedName(rayService))
	})

	t.Run("GatewayRef targets the shared gateway", func(t *testing.T) {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "default"},
			Spec: rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
						GatewayRef: &rayv1.GatewayReference{Name: "shared-gw", Namespace: "gateways"},
					},
				},
			},
		}
		expected := types.NamespacedName{Namespace: "gateways", Name: "shared-gw"}
		assert.Equal(t, expected, RayServiceGatewayNamespacedName(rayService))
	})

	t.Run("GatewayRef with empty namespace defaults to the RayService namespace", func(t *testing.T) {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{Name: "my-svc", Namespace: "default"},
			Spec: rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
						GatewayRef: &rayv1.GatewayReference{Name: "shared-gw"},
					},
				},
			},
		}
		expected := types.NamespacedName{Namespace: "default", Name: "shared-gw"}
		assert.Equal(t, expected, RayServiceGatewayNamespacedName(rayService))
	})
}

// TestRayServicePendingRayClusterNamespacedName tests the function for generating a NamespacedName for a RayService's pending RayCluster
func TestRayServicePendingRayClusterNamespacedName(t *testing.T) {
	rayService := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayservice-pending",
			Namespace: "default",
		},
		Status: rayv1.RayServiceStatuses{
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "pending-ray-cluster",
			},
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      "pending-ray-cluster",
	}
	result := RayServicePendingRayClusterNamespacedName(rayService)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayJobK8sJobNamespacedName tests the function for generating a NamespacedName for a RayJob's Kubernetes Job
func TestRayJobK8sJobNamespacedName(t *testing.T) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-k8s",
			Namespace: "default",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      "rayjob-k8s",
	}
	result := RayJobK8sJobNamespacedName(rayJob)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// TestRayJobRayClusterNamespacedName tests the function for generating a NamespacedName for a RayJob's RayCluster
func TestRayJobRayClusterNamespacedName(t *testing.T) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-cluster",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			RayClusterName: "associated-ray-cluster",
		},
	}
	expected := types.NamespacedName{
		Namespace: "default",
		Name:      "associated-ray-cluster",
	}
	result := RayJobRayClusterNamespacedName(rayJob)
	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestGetRayClusterHeadPod(t *testing.T) {
	// Create a new scheme with CRDs, Pod, Service schemes.
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Mock data
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	headPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "head-pod",
			Namespace: cluster.ObjectMeta.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey:  cluster.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
			},
		},
	}

	// Initialize a fake client with newScheme and runtimeObjects.
	runtimeObjects := []runtime.Object{headPod}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	ctx := context.TODO()

	ret, err := GetRayClusterHeadPod(ctx, fakeClient, &cluster)
	require.NoError(t, err)
	assert.Equal(t, ret, headPod)
}

func TestRayClusterRedisCleanupJobAssociationOptions(t *testing.T) {
	// Create a new scheme
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-example",
			Namespace: "default",
		},
	}

	_ = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cleanup",
			Namespace: instance.ObjectMeta.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey:  instance.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.RedisCleanupNode),
			},
		},
	}

	expected := []client.ListOption{
		client.InNamespace(instance.ObjectMeta.Namespace),
		client.MatchingLabels(map[string]string{
			utils.RayClusterLabelKey:  instance.Name,
			utils.RayNodeTypeLabelKey: string(rayv1.RedisCleanupNode),
		}),
	}
	result := RayClusterRedisCleanupJobAssociationOptions(instance).ToListOptions()

	assert.Equal(t, expected, result)
}

func TestRayClusterNetworkResourcesOptions(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = routev1.AddToScheme(newScheme)
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-example",
			Namespace: "default",
			Annotations: map[string]string{
				IngressClassAnnotationKey: "nginx",
			},
		},
	}
	_ = &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GenerateRouteName(instance.Name),
			Namespace: instance.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey: instance.Name,
			},
		},
	}
	expected := []client.ListOption{
		client.InNamespace(instance.ObjectMeta.Namespace),
		client.MatchingLabels(map[string]string{
			utils.RayClusterLabelKey: instance.Name,
		}),
	}

	result := RayClusterNetworkResourcesOptions(instance).ToListOptions()

	assert.Equal(t, expected, result)
}
