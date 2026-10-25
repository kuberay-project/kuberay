package schedulerplugins

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func createTestRayCluster(numOfHosts int32) rayv1.RayCluster {
	headSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-head",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	workerSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-worker",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
						"nvidia.com/gpu":      resource.MustParse("1"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	return rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: headSpec,
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					Template: corev1.PodTemplateSpec{
						Spec: workerSpec,
					},
					Replicas:    ptr.To[int32](2),
					NumOfHosts:  numOfHosts,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](4),
				},
			},
		},
	}
}

func TestCreatePodGroup(t *testing.T) {
	a := assert.New(t)

	cluster := createTestRayCluster(1)

	podGroup := createPodGroup(&cluster)

	// 256m * 3 (requests, not limits)
	a.Equal("768m", podGroup.Spec.MinResources.Cpu().String())

	// 256Mi * 3 (requests, not limits)
	a.Equal("768Mi", podGroup.Spec.MinResources.Memory().String())

	// 2 GPUs total
	a.Equal("2", podGroup.Spec.MinResources.Name("nvidia.com/gpu", resource.BinarySI).String())

	// 1 head and 2 workers
	a.Equal(int32(3), podGroup.Spec.MinMember)
}

func TestCreatePodGroupWithMultipleHosts(t *testing.T) {
	a := assert.New(t)

	cluster := createTestRayCluster(2) // 2 hosts

	podGroup := createPodGroup(&cluster)

	// 256m * 5 (requests, not limits)
	a.Equal("1280m", podGroup.Spec.MinResources.Cpu().String())

	// 256Mi * 5 (requests, not limits)
	a.Equal("1280Mi", podGroup.Spec.MinResources.Memory().String())

	// 4 GPUs total
	a.Equal("4", podGroup.Spec.MinResources.Name("nvidia.com/gpu", resource.BinarySI).String())

	// 1 head and 4 workers
	a.Equal(int32(5), podGroup.Spec.MinMember)
}

func TestDoBatchSchedulingOnSubmissionUpdatesExistingPodGroup(t *testing.T) {
	cluster := createTestRayCluster(1)
	cluster.Labels = map[string]string{utils.RayGangSchedulingEnabled: "true"}

	// Create the PodGroup for the original replica count, then scale the cluster
	// down before the next reconciliation.
	stalePodGroup := createPodGroup(&cluster)
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](1)

	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stalePodGroup).Build()
	scheduler := &KubeScheduler{cli: fakeClient}

	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(context.Background(), &cluster))

	updatedPodGroup := &v1alpha1.PodGroup{}
	require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKeyFromObject(stalePodGroup), updatedPodGroup))
	assert.Equal(t, int32(2), updatedPodGroup.Spec.MinMember)
	assert.Equal(t, "512m", updatedPodGroup.Spec.MinResources.Cpu().String())
	assert.Equal(t, "512Mi", updatedPodGroup.Spec.MinResources.Memory().String())
}

func TestAddMetadataToChildResource(t *testing.T) {
	tests := []struct {
		name         string
		enableGang   bool
		podHasLabels bool
	}{
		{"GangEnabled_WithLabels", true, true},
		{"GangDisabled_WithLabels", false, true},
		{"GangDisabled_WithoutLabels", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			cluster := createTestRayCluster(1)
			cluster.Labels = make(map[string]string)

			if tt.enableGang {
				cluster.Labels["ray.io/gang-scheduling-enabled"] = "true"
			}

			var pod *corev1.Pod
			if tt.podHasLabels {
				pod = &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{},
					},
				}
			} else {
				pod = &corev1.Pod{}
			}

			scheduler := &KubeScheduler{}
			scheduler.AddMetadataToChildResource(context.TODO(), &cluster, pod, "worker")

			if tt.enableGang {
				a.Equal(cluster.Name, pod.Labels[kubeSchedulerPodGroupLabelKey])
			} else {
				_, exists := pod.Labels[kubeSchedulerPodGroupLabelKey]
				a.False(exists)
			}

			a.Equal(scheduler.Name(), pod.Spec.SchedulerName)
			// The default scheduler plugins name is "scheduler-plugins-scheduler"
			// The batchScheduler name is "scheduler-plugins"
			// This is to ensure batchScheduler and default scheduler plugins name are not the same.
			a.NotEqual(scheduler.Name(), GetPluginName())
		})
	}
}
