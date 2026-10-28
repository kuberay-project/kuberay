package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	kubeFake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

func TestGetKubeRayOperatorVersion(t *testing.T) {
	helmKubeObjects := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kuberay-operator-helm-chart",
				Namespace: "default",
				Labels: map[string]string{
					"app.kubernetes.io/name": "kuberay-operator",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "kuberay/operator:v0.5.0@sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
							},
						},
					},
				},
			},
		},
	}
	kustomizeObjects := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kuberay-operator-kustomize",
				Namespace: "test",
				Labels: map[string]string{
					"app.kubernetes.io/name": "kuberay",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "kuberay/operator:v0.6.0@sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
							},
						},
					},
				},
			},
		},
	}
	kustomizeObjectsImageWithOnlyTag := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kuberay-operator-kustomize",
				Namespace: "test",
				Labels: map[string]string{
					"app.kubernetes.io/name": "kuberay",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "kuberay/operator:v0.6.0",
							},
						},
					},
				},
			},
		},
	}
	kustomizeObjectsImageWithOnlyDigest := []runtime.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "kuberay-operator-kustomize",
				Namespace: "test",
				Labels: map[string]string{
					"app.kubernetes.io/name": "kuberay",
				},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "kuberay/operator@sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
							},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		name            string
		expectedVersion string
		expectedError   string
		kubeObjects     []runtime.Object
	}{
		{
			name:            "KubeRay operator not found",
			expectedVersion: "",
			expectedError:   "no KubeRay operator deployments found in any namespace",
			kubeObjects:     nil,
		},
		{
			name:            "find KubeRay operator version for helm chart",
			expectedVersion: "v0.5.0@sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
			expectedError:   "",
			kubeObjects:     helmKubeObjects,
		},
		{
			name:            "find KubeRay operator version for Kustomize",
			expectedVersion: "v0.6.0@sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
			expectedError:   "",
			kubeObjects:     kustomizeObjects,
		},
		{
			name:            "find KubeRay operator version for Kustomize",
			expectedVersion: "v0.6.0",
			expectedError:   "",
			kubeObjects:     kustomizeObjectsImageWithOnlyTag,
		},
		{
			name:            "find KubeRay operator version for Kustomize",
			expectedVersion: "sha256:cc8ce713f3b4be3c72cca1f63ee78e3733bc7283472ecae367b47a128f7e4478",
			expectedError:   "",
			kubeObjects:     kustomizeObjectsImageWithOnlyDigest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeClientSet := kubeFake.NewClientset(tc.kubeObjects...)
			client := NewClientForTesting(kubeClientSet, nil)

			version, err := client.GetKubeRayOperatorVersion(context.Background())

			if tc.expectedVersion != "" {
				assert.Equal(t, tc.expectedVersion, version)
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectedError)
			}
		})
	}
}

func TestGetRayHeadSvcNameByRayCluster(t *testing.T) {
	kubeObjects := []runtime.Object{}

	rayObjects := []runtime.Object{
		&rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-default",
				Namespace: "default",
			},
			Status: rayv1.RayClusterStatus{
				Head: rayv1.HeadInfo{
					ServiceName: "raycluster-default-head-svc",
				},
			},
		},
		&rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-test",
				Namespace: "test",
			},
			Status: rayv1.RayClusterStatus{
				Head: rayv1.HeadInfo{
					ServiceName: "raycluster-test-head-svc",
				},
			},
		},
	}

	kubeClientSet := kubeFake.NewClientset(kubeObjects...)
	rayClient := rayClientFake.NewClientset(rayObjects...)
	client := NewClientForTesting(kubeClientSet, rayClient)

	tests := []struct {
		name         string
		namespace    string
		resourceName string
		serviceName  string
	}{
		{
			name:         "find service name in default namespace",
			namespace:    "default",
			resourceName: "raycluster-default",
			serviceName:  "raycluster-default-head-svc",
		},
		{
			name:         "find service name in test namespace",
			namespace:    "test",
			resourceName: "raycluster-test",
			serviceName:  "raycluster-test-head-svc",
		},
		{
			name:         "resource not found",
			namespace:    "default",
			resourceName: "raycluster-not-found",
			serviceName:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svcName, err := client.GetRayHeadSvcName(context.Background(), tc.namespace, util.RayCluster, tc.resourceName)
			if tc.serviceName == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.serviceName, svcName)
			}
		})
	}
}

func TestGetRayHeadSvcNameByRayJob(t *testing.T) {
	kubeObjects := []runtime.Object{}

	rayObjects := []runtime.Object{
		&rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayjob-default",
				Namespace: "default",
			},
			Status: rayv1.RayJobStatus{
				RayClusterStatus: rayv1.RayClusterStatus{
					Head: rayv1.HeadInfo{
						ServiceName: "rayjob-default-raycluster-xxxxx-head-svc",
					},
				},
			},
		},
		&rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayjob-test",
				Namespace: "test",
			},
			Status: rayv1.RayJobStatus{
				RayClusterStatus: rayv1.RayClusterStatus{
					Head: rayv1.HeadInfo{
						ServiceName: "rayjob-test-raycluster-xxxxx-head-svc",
					},
				},
			},
		},
	}

	kubeClientSet := kubeFake.NewClientset(kubeObjects...)
	rayClient := rayClientFake.NewClientset(rayObjects...)
	client := NewClientForTesting(kubeClientSet, rayClient)

	tests := []struct {
		name         string
		namespace    string
		resourceName string
		serviceName  string
	}{
		{
			name:         "find service name in default namespace",
			namespace:    "default",
			resourceName: "rayjob-default",
			serviceName:  "rayjob-default-raycluster-xxxxx-head-svc",
		},
		{
			name:         "find service name in test namespace",
			namespace:    "test",
			resourceName: "rayjob-test",
			serviceName:  "rayjob-test-raycluster-xxxxx-head-svc",
		},
		{
			name:         "resource not found",
			namespace:    "default",
			resourceName: "rayjob-not-found",
			serviceName:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svcName, err := client.GetRayHeadSvcName(context.Background(), tc.namespace, util.RayJob, tc.resourceName)
			if tc.serviceName == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.serviceName, svcName)
			}
		})
	}
}

func TestGetRayHeadSvcNameByRayService(t *testing.T) {
	kubeObjects := []runtime.Object{}

	rayObjects := []runtime.Object{
		&rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayservice-default",
				Namespace: "default",
			},
			Status: rayv1.RayServiceStatuses{
				ActiveServiceStatus: rayv1.RayServiceStatus{
					RayClusterStatus: rayv1.RayClusterStatus{
						Head: rayv1.HeadInfo{
							ServiceName: "rayservice-default-raycluster-xxxxx-head-svc",
						},
					},
				},
			},
		},
		&rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayservice-test",
				Namespace: "test",
			},
			Status: rayv1.RayServiceStatuses{
				ActiveServiceStatus: rayv1.RayServiceStatus{
					RayClusterStatus: rayv1.RayClusterStatus{
						Head: rayv1.HeadInfo{
							ServiceName: "rayservice-test-raycluster-xxxxx-head-svc",
						},
					},
				},
			},
		},
	}

	kubeClientSet := kubeFake.NewClientset(kubeObjects...)
	rayClient := rayClientFake.NewClientset(rayObjects...)
	client := NewClientForTesting(kubeClientSet, rayClient)

	tests := []struct {
		name         string
		namespace    string
		resourceName string
		serviceName  string
	}{
		{
			name:         "find service name in default namespace",
			namespace:    "default",
			resourceName: "rayservice-default",
			serviceName:  "rayservice-default-raycluster-xxxxx-head-svc",
		},
		{
			name:         "find service name in test namespace",
			namespace:    "test",
			resourceName: "rayservice-test",
			serviceName:  "rayservice-test-raycluster-xxxxx-head-svc",
		},
		{
			name:         "resource not found",
			namespace:    "default",
			resourceName: "rayservice-not-found",
			serviceName:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svcName, err := client.GetRayHeadSvcName(context.Background(), tc.namespace, util.RayService, tc.resourceName)
			if tc.serviceName == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.serviceName, svcName)
			}
		})
	}
}

func TestWaitRayClusterProvisioned(t *testing.T) {
	tests := []struct {
		name          string
		expectedError string
		objectsAdded  []runtime.Object
		timeout       time.Duration
	}{
		{
			name: "function shouldn't error if the RayCluster is provisioned in time",
			objectsAdded: []runtime.Object{
				&rayv1.RayCluster{
					Status: rayv1.RayClusterStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(rayv1.RayClusterProvisioned),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
			},
			timeout:       5 * time.Second,
			expectedError: "",
		},
		{
			name: "function shouldn't error if the RayCluster is ready in time",
			objectsAdded: []runtime.Object{
				&rayv1.RayCluster{
					Status: rayv1.RayClusterStatus{
						State: rayv1.Ready,
					},
				},
			},
			timeout:       5 * time.Second,
			expectedError: "",
		},
		{
			name:          "function should error if the RayCluster is not provisioned in time",
			objectsAdded:  []runtime.Object{},
			timeout:       1 * time.Second,
			expectedError: "timed out waiting for Ray cluster foo in namespace bar to be provisioned",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeClientSet := kubeFake.NewClientset()
			rayClient := rayClientFake.NewClientset()

			fakeWatcher := watch.NewFake()
			go func() {
				for _, obj := range tc.objectsAdded {
					fakeWatcher.Add(obj)
				}
			}()
			defer fakeWatcher.Stop()

			rayClient.PrependWatchReactor("rayclusters", kubetesting.DefaultWatchReactor(fakeWatcher, nil))

			client := NewClientForTesting(kubeClientSet, rayClient)

			err := client.WaitRayClusterProvisioned(context.Background(), "bar", "foo", tc.timeout)

			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError)
			}
		})
	}
}
