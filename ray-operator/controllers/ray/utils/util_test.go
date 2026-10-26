package utils

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestGetClusterDomainName(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want string
	}{{
		name: "all good from env",
		env:  "abc.com",
		want: "abc.com",
	}, {
		name: "No env set",
		env:  "",
		want: DefaultDomainName,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.env) > 0 {
				t.Setenv(ClusterDomainEnvKey, tt.env)
			}
			got := GetClusterDomainName()
			if got != tt.want {
				t.Errorf("Test %s failed expected: %s but got: %s", tt.name, tt.want, got)
			}
		})
	}
}

func TestStatus(t *testing.T) {
	pod := createSomePod()
	pod.Status.Phase = corev1.PodPending
	if !IsCreated(pod) {
		t.Fail()
	}
}

func TestCheckAllPodsRunning(t *testing.T) {
	tests := []struct {
		name     string
		pods     corev1.PodList
		expected bool
	}{
		{
			name: "should return true if all Pods are running",
			pods: corev1.PodList{
				Items: []corev1.Pod{
					*createSomePodWithPhase(corev1.PodRunning),
					*createSomePodWithPhase(corev1.PodRunning),
				},
			},
			expected: true,
		},
		{
			name: "should return false if there are no Pods",
			pods: corev1.PodList{
				Items: []corev1.Pod{},
			},
			expected: false,
		},
		{
			name: "should return false if any Pods don't have .status.phase Running",
			pods: corev1.PodList{
				Items: []corev1.Pod{
					*createSomePodWithPhase(corev1.PodPending),
					*createSomePodWithPhase(corev1.PodRunning),
				},
			},
			expected: false,
		},
		{
			name: "should return false if any Pods have a .status.condition of type: Ready that's not status: True",
			pods: corev1.PodList{
				Items: []corev1.Pod{
					*createSomePodWithPhase(corev1.PodRunning),
					*createSomePodWithCondition(corev1.PodReady, corev1.ConditionFalse),
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, CheckAllPodsRunning(context.Background(), tc.pods))
		})
	}
}

func TestWorkerPodName(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		expected string
	}{
		{
			name:     "short cluster name, worker pod",
			prefix:   "ray-cluster-group-name-01",
			expected: "ray-cluster-group-name-01-worker-",
		},
		{
			name:     "long cluster name, worker pod",
			prefix:   "ray-cluster-0000000000000000000000011111111122222233333333333333-group-name",
			expected: "ray-cluster-00000000000000000000000111111111222222-worker-",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			str := PodName(test.prefix, rayv1.WorkerNode, true)
			if str != test.expected {
				t.Logf("expected: %q", test.expected)
				t.Logf("actual: %q", str)
				t.Error("PodName returned an unexpected string")
			}

			// 63 (max pod name length) - 5 random hexadecimal characters from generateName
			if len(str) > 58 {
				t.Error("Generated pod name is too long")
			}
		})
	}
}

func TestHeadPodName(t *testing.T) {
	defer os.Unsetenv(ENABLE_DETERMINISTIC_HEAD_POD_NAME)

	tests := []struct {
		name                       string
		prefix                     string
		enableDeterministicHeadPod string
		expected                   string
	}{
		{
			name:                       "short cluster name, deterministic head pod name",
			prefix:                     "ray-cluster-01",
			enableDeterministicHeadPod: "true",
			expected:                   "ray-cluster-01-head",
		},
		{
			name:                       "short cluster name, non-deterministic head pod name",
			prefix:                     "ray-cluster-01",
			enableDeterministicHeadPod: "false",
			expected:                   "ray-cluster-01-head-",
		},
		{
			name:                       "short cluster name, feature flag not set",
			prefix:                     "ray-cluster-01",
			enableDeterministicHeadPod: "unset",
			expected:                   "ray-cluster-01-head-",
		},
		{
			name:                       "long cluster name, deterministic head pod name",
			prefix:                     "ray-cluster-0000000000000000000000011111111122222233333333333333",
			enableDeterministicHeadPod: "true",
			expected:                   "ray-cluster-00000000000000000000000111111111222222-head",
		},
		{
			name:                       "long cluster name, non-deterministic head pod name",
			prefix:                     "ray-cluster-0000000000000000000000011111111122222233333333333333",
			enableDeterministicHeadPod: "false",
			expected:                   "ray-cluster-00000000000000000000000111111111222222-head-",
		},
		{
			name:                       "long cluster name, feature flag not set",
			prefix:                     "ray-cluster-0000000000000000000000011111111122222233333333333333",
			enableDeterministicHeadPod: "unset",
			expected:                   "ray-cluster-00000000000000000000000111111111222222-head-",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.enableDeterministicHeadPod == "unset" {
				os.Unsetenv(ENABLE_DETERMINISTIC_HEAD_POD_NAME)
			} else {
				os.Setenv(ENABLE_DETERMINISTIC_HEAD_POD_NAME, test.enableDeterministicHeadPod)
			}

			str := PodName(test.prefix, rayv1.HeadNode, !IsDeterministicHeadPodNameEnabled())
			if str != test.expected {
				t.Logf("expected: %q", test.expected)
				t.Logf("actual: %q", str)
				t.Error("PodName returned an unexpected string")
			}

			// 63 (max pod name length) - 5 random hexadecimal characters from generateName
			if len(str) > 58 {
				t.Error("Generated pod name is too long")
			}
		})
	}
}

func TestCheckName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "shorten long string starting with numeric character",
			input:    "72fbcc7e-a661-4b18e-ca41-e903-fc3ae634b18e-lazer090scholar-director-s",
			expected: "rca41-e903-fc3ae634b18e-lazer090scholar-director-s",
		},
		{
			name:     "shorten long string starting with special character",
			input:    "--------566666--------444433-----------222222----------4444",
			expected: "r6666--------444433-----------222222----------4444",
		},
		{
			name:     "unchanged",
			input:    "acceptable-name-head-12345",
			expected: "acceptable-name-head-12345",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			str := CheckName(test.input)
			if str != test.expected {
				t.Logf("expected: %q", test.expected)
				t.Logf("actual: %q", str)
				t.Error("CheckName returned an unexpected string")
			}
		})
	}
}

func TestCheckRouteName(t *testing.T) {
	tests := []struct {
		name      string
		routeName string
		namespace string
		want      string
	}{{
		name:      "long route name truncated",
		routeName: "cv-traffic-training-202402090958",
		namespace: "development-namespace",
		want:      "cv-traffic-training-2024020909",
	}, {
		name:      "long route name w/number start truncated and number replaced",
		routeName: "2-step-cv-training-network-revisited",
		namespace: "development-namespace",
		want:      "r-step-cv-training-network-rev",
	}, {
		name:      "well-formatted and well-sized route name unaffected",
		routeName: "acceptable-name-head-12345",
		namespace: "development-namespace",
		want:      "acceptable-name-head-12345",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := CheckRouteName(context.Background(), tc.routeName, tc.namespace)
			assert.Equal(t, tc.want, name)
		})
	}
}

func createSomePod() (pod *corev1.Pod) {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-small-group-worker-0",
			Namespace: "default",
		},
	}
}

func createSomePodWithPhase(phase corev1.PodPhase) (pod *corev1.Pod) {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-small-group-worker-0",
			Namespace: "default",
		},
		Status: corev1.PodStatus{
			Phase: phase,
		},
	}
}

func createSomePodWithCondition(typ corev1.PodConditionType, status corev1.ConditionStatus) (pod *corev1.Pod) {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-small-group-worker-0",
			Namespace: "default",
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   typ,
					Status: status,
				},
			},
		},
	}
}

func createRayHeadPodWithPhaseAndCondition(phase corev1.PodPhase, status corev1.ConditionStatus) (pod *corev1.Pod) {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-head",
			Namespace: "default",
			Labels: map[string]string{
				"ray.io/node-type": string(rayv1.HeadNode),
			},
		},
		Status: corev1.PodStatus{
			Phase: phase,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: status,
					Reason: ContainersNotReady,
				},
			},
		},
	}
}

func TestGetHeadGroupServiceAccountName(t *testing.T) {
	tests := []struct {
		name  string
		input *rayv1.RayCluster
		want  string
	}{
		{
			name: "Ray cluster with head group service account",
			input: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "raycluster-sample",
					Namespace: "default",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								ServiceAccountName: "my-service-account",
							},
						},
					},
				},
			},
			want: "my-service-account",
		},
		{
			name: "Ray cluster without head group service account",
			input: &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "raycluster-sample",
					Namespace: "default",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{},
						},
					},
				},
			},
			want: "raycluster-sample",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, GetHeadGroupServiceAccountName(tc.input))
		})
	}
}

func TestCalculateAvailableReplicas(t *testing.T) {
	podList := corev1.PodList{
		Items: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod1",
					Labels: map[string]string{
						"ray.io/node-type": string(rayv1.HeadNode),
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod2",
					Labels: map[string]string{
						"ray.io/node-type": string(rayv1.WorkerNode),
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod2",
					Labels: map[string]string{
						"ray.io/node-type": string(rayv1.WorkerNode),
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					Conditions: []corev1.PodCondition{
						{
							Type:   corev1.PodReady,
							Status: corev1.ConditionFalse,
						},
					},
				},
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod2",
					Labels: map[string]string{
						"ray.io/node-type": string(rayv1.WorkerNode),
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
				},
			},
		},
	}

	availableCount := CalculateAvailableReplicas(podList)
	assert.Equal(t, int32(1), availableCount, "expect 1 available replica")

	readyCount := CalculateReadyReplicas(podList)
	assert.Equal(t, int32(1), readyCount, "expect 1 ready replica")
}

func TestFindContainerPort(t *testing.T) {
	container := corev1.Container{
		Name: "ray-head",
		Ports: []corev1.ContainerPort{
			{
				Name:          "port1",
				ContainerPort: 10001,
			},
			{
				Name:          "port2",
				ContainerPort: 10002,
			},
		},
	}
	port := FindContainerPort(&container, "port1", -1)
	assert.NotEqual(t, int32(-1), port, "expect port1 found")
	port = FindContainerPort(&container, "port2", -1)
	assert.NotEqual(t, int32(-1), port, "expect port2 found")
	port = FindContainerPort(&container, "port3", -1)
	assert.Equal(t, int32(-1), port, "expect port3 not found")
}

func TestGenerateHeadServiceName(t *testing.T) {
	// GenerateHeadServiceName generates a Ray head service name. Note that there are two types of head services:
	//
	// (1) For RayCluster: If `HeadService.Name` in the cluster spec is not empty, it will be used as the head service name.
	// Otherwise, the name is generated based on the RayCluster CR's name.
	// (2) For RayService: It's important to note that the RayService CR not only possesses a head service owned by its RayCluster CR
	// but also maintains a separate head service for itself to facilitate zero-downtime upgrades. The name of the head service owned
	// by the RayService CR is generated based on the RayService CR's name.

	// [RayCluster]
	// Test 1: `HeadService.Name` is empty.
	headSvcName, err := GenerateHeadServiceName(RayClusterCRD, rayv1.RayClusterSpec{}, "raycluster-sample")
	expectedGeneratedSvcName := "raycluster-sample-head-svc"
	require.NoError(t, err)
	assert.Equal(t, expectedGeneratedSvcName, headSvcName)

	// Test 2: `HeadService.Name` is not empty.
	clusterSpecWithHeadService := rayv1.RayClusterSpec{
		HeadGroupSpec: rayv1.HeadGroupSpec{
			HeadService: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-head-svc",
				},
			},
		},
	}

	headSvcName, err = GenerateHeadServiceName(RayClusterCRD, *clusterSpecWithHeadService.DeepCopy(), "raycluster-sample")
	require.NoError(t, err)
	assert.Equal(t, "my-head-svc", headSvcName)

	// [RayService]
	// Test 3: `HeadService.Name` is empty.
	headSvcName, err = GenerateHeadServiceName(RayServiceCRD, rayv1.RayClusterSpec{}, "rayservice-sample")
	expectedGeneratedSvcName = "rayservice-sample-head-svc"
	require.NoError(t, err)
	assert.Equal(t, expectedGeneratedSvcName, headSvcName)

	// Test 4: `HeadService.Name` is not empty.
	headSvcName, err = GenerateHeadServiceName(RayServiceCRD, *clusterSpecWithHeadService.DeepCopy(), "rayservice-sample")
	require.NoError(t, err)
	assert.Equal(t, expectedGeneratedSvcName, headSvcName)

	// [RayJob]
	// Test 5: `HeadService.Name` is empty.
	headSvcName, err = GenerateHeadServiceName(RayJobCRD, rayv1.RayClusterSpec{}, "rayjob-sample")
	expectedGeneratedSvcName = "rayjob-sample-head-svc"
	require.NoError(t, err)
	assert.Equal(t, expectedGeneratedSvcName, headSvcName)

	// Test 6: `HeadService.Name` is not empty.
	headSvcName, err = GenerateHeadServiceName(RayJobCRD, *clusterSpecWithHeadService.DeepCopy(), "rayjob-sample")
	require.NoError(t, err)
	assert.Equal(t, expectedGeneratedSvcName, headSvcName)
}

func TestGetWorkerGroupDesiredReplicas(t *testing.T) {
	// Test 1: `WorkerGroupSpec.Replicas` is nil.
	// `Replicas` is impossible to be nil in a real RayCluster CR as it has a default value assigned in the CRD.
	numOfHosts := int32(1)
	minReplicas := int32(1)
	maxReplicas := int32(5)

	workerGroupSpec := rayv1.WorkerGroupSpec{
		NumOfHosts:  numOfHosts,
		MinReplicas: &minReplicas,
		MaxReplicas: &maxReplicas,
	}
	assert.Equal(t, GetWorkerGroupDesiredReplicas(workerGroupSpec), minReplicas)

	// Test 2: `WorkerGroupSpec.Replicas` is not nil and is within the range.
	replicas := int32(3)
	workerGroupSpec.Replicas = &replicas
	assert.Equal(t, GetWorkerGroupDesiredReplicas(workerGroupSpec), replicas)

	// Test 3: `WorkerGroupSpec.Replicas` is not nil but is more than maxReplicas.
	replicas = int32(6)
	workerGroupSpec.Replicas = &replicas
	assert.Equal(t, GetWorkerGroupDesiredReplicas(workerGroupSpec), maxReplicas)

	// Test 4: `WorkerGroupSpec.Replicas` is not nil but is less than minReplicas.
	replicas = int32(0)
	workerGroupSpec.Replicas = &replicas
	assert.Equal(t, GetWorkerGroupDesiredReplicas(workerGroupSpec), minReplicas)

	// Test 5: `WorkerGroupSpec.Suspend` is true.
	suspend := true
	workerGroupSpec.MinReplicas = &maxReplicas
	workerGroupSpec.MaxReplicas = &minReplicas
	workerGroupSpec.Suspend = &suspend
	assert.Zero(t, GetWorkerGroupDesiredReplicas(workerGroupSpec))

	// Test 6: `WorkerGroupSpec.NumOfHosts` is 4.
	numOfHosts = int32(4)
	replicas = int32(5)
	suspend = false
	workerGroupSpec.NumOfHosts = numOfHosts
	workerGroupSpec.Replicas = &replicas
	workerGroupSpec.Suspend = &suspend
	workerGroupSpec.MinReplicas = &minReplicas
	workerGroupSpec.MaxReplicas = &maxReplicas
	assert.Equal(t, GetWorkerGroupDesiredReplicas(workerGroupSpec), replicas*numOfHosts)
}

func TestCalculateMinAndMaxReplicas(t *testing.T) {
	suspend := true

	tests := []struct {
		name     string
		specs    []rayv1.WorkerGroupSpec
		expected struct {
			minReplicas int32
			maxReplicas int32
		}
	}{
		{
			name: "Single group with one host",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](2),
					MaxReplicas: ptr.To[int32](3),
				},
			},
			expected: struct {
				minReplicas int32
				maxReplicas int32
			}{
				minReplicas: 2,
				maxReplicas: 3,
			},
		},
		{
			name: "Single group with four hosts",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  4,
					MinReplicas: ptr.To[int32](2),
					MaxReplicas: ptr.To[int32](3),
				},
			},
			expected: struct {
				minReplicas int32
				maxReplicas int32
			}{
				minReplicas: 8,
				maxReplicas: 12,
			},
		},
		{
			name: "Two worker groups: one with a single host, one with two hosts",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](4),
					MaxReplicas: ptr.To[int32](4),
				},
				{
					NumOfHosts:  2,
					MinReplicas: ptr.To[int32](3),
					MaxReplicas: ptr.To[int32](3),
				},
			},
			expected: struct {
				minReplicas int32
				maxReplicas int32
			}{
				minReplicas: 10,
				maxReplicas: 10,
			},
		},
		{
			name: "Two groups with suspended",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](3),
					MaxReplicas: ptr.To[int32](3),
					Suspend:     &suspend,
				},
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](1),
					Suspend:     &suspend,
				},
			},
			expected: struct {
				minReplicas int32
				maxReplicas int32
			}{
				minReplicas: 0,
				maxReplicas: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					WorkerGroupSpecs: tt.specs,
				},
			}

			// Check min replicas
			assert.Equal(t, tt.expected.minReplicas, CalculateMinReplicas(cluster))
			// Check max replicas
			assert.Equal(t, tt.expected.maxReplicas, CalculateMaxReplicas(cluster))
		})
	}
}

func TestCalculateDesiredReplicas(t *testing.T) {
	tests := []struct {
		group1Replicas    *int32
		group1MinReplicas *int32
		group1MaxReplicas *int32
		group2Replicas    *int32
		group2MinReplicas *int32
		group2MaxReplicas *int32
		name              string
		group1NumOfHosts  int32
		group2NumOfHosts  int32
		answer            int32
	}{
		{
			group1Replicas:    nil,
			group1NumOfHosts:  1,
			group1MinReplicas: ptr.To[int32](1),
			group1MaxReplicas: ptr.To[int32](5),
			group2Replicas:    nil,
			group2NumOfHosts:  1,
			group2MinReplicas: ptr.To[int32](2),
			group2MaxReplicas: ptr.To[int32](5),
			name:              "Both groups' Replicas are nil",
			answer:            3,
		},
		{
			group1Replicas:    ptr.To[int32](0),
			group1NumOfHosts:  1,
			group1MinReplicas: ptr.To[int32](2),
			group1MaxReplicas: ptr.To[int32](5),
			group2Replicas:    ptr.To[int32](6),
			group2NumOfHosts:  1,
			group2MinReplicas: ptr.To[int32](2),
			group2MaxReplicas: ptr.To[int32](5),
			name:              "Group1's Replicas is smaller than MinReplicas, and Group2's Replicas is more than MaxReplicas.",
			answer:            7,
		},
		{
			group1Replicas:    ptr.To[int32](6),
			group1NumOfHosts:  1,
			group1MinReplicas: ptr.To[int32](2),
			group1MaxReplicas: ptr.To[int32](5),
			group2Replicas:    ptr.To[int32](3),
			group2NumOfHosts:  1,
			group2MinReplicas: ptr.To[int32](2),
			group2MaxReplicas: ptr.To[int32](5),
			name:              "Group1's Replicas is more than MaxReplicas.",
			answer:            8,
		},
		{
			group1Replicas:    ptr.To[int32](3),
			group1NumOfHosts:  4,
			group1MinReplicas: ptr.To[int32](1),
			group1MaxReplicas: ptr.To[int32](6),
			group2Replicas:    ptr.To[int32](3),
			group2NumOfHosts:  1,
			group2MinReplicas: ptr.To[int32](2),
			group2MaxReplicas: ptr.To[int32](5),
			name:              "Group1's NumOfHosts is 4, and Group2's Replicas is 1.",
			answer:            15,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName:   "group1",
							Replicas:    tc.group1Replicas,
							NumOfHosts:  tc.group1NumOfHosts,
							MinReplicas: tc.group1MinReplicas,
							MaxReplicas: tc.group1MaxReplicas,
						},
						{
							GroupName:   "group2",
							Replicas:    tc.group2Replicas,
							NumOfHosts:  tc.group2NumOfHosts,
							MinReplicas: tc.group2MinReplicas,
							MaxReplicas: tc.group2MaxReplicas,
						},
					},
				},
			}
			assert.Equal(t, CalculateDesiredReplicas(&cluster), tc.answer)
		})
	}
}

func TestCalculateMaxReplicasOverflow(t *testing.T) {
	tests := []struct {
		name     string
		specs    []rayv1.WorkerGroupSpec
		expected int32
	}{
		{
			name: "Bug reproduction: issue report with replicas=1, minReplicas=3, numOfHosts=4",
			specs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "workergroup",
					Replicas:    ptr.To[int32](1),
					MinReplicas: ptr.To[int32](3),
					MaxReplicas: ptr.To[int32](2147483647), // Default max int32
					NumOfHosts:  4,
				},
			},
			expected: 2147483647, // Was -4 before fix, should be capped at max int32
		},
		{
			name: "Single group overflow with default maxReplicas and numOfHosts=4",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  4,
					MinReplicas: ptr.To[int32](3),
					MaxReplicas: ptr.To[int32](2147483647),
				},
			},
			expected: 2147483647, // Should be capped at max int32
		},
		{
			name: "Single group overflow with large values",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  1000,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](2147483647),
				},
			},
			expected: 2147483647, // Should be capped
		},
		{
			name: "Multiple groups causing overflow when summed",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  2,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](1500000000),
				},
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](1000000000),
				},
			},
			expected: 2147483647, // 3B + 1B > max int32, should be capped
		},
		{
			name: "No overflow with reasonable values",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  4,
					MinReplicas: ptr.To[int32](2),
					MaxReplicas: ptr.To[int32](100),
				},
			},
			expected: 400, // 100 * 4 = 400, no overflow
		},
		{
			name: "Edge case: exactly at max int32",
			specs: []rayv1.WorkerGroupSpec{
				{
					NumOfHosts:  1,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](2147483647),
				},
			},
			expected: 2147483647, // Exactly at limit
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					WorkerGroupSpecs: tc.specs,
				},
			}
			result := CalculateMaxReplicas(cluster)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestUnmarshalRuntimeEnv(t *testing.T) {
	tests := []struct {
		name           string
		runtimeEnvYAML string
		isErrorNil     bool
	}{
		{
			name:           "Empty runtimeEnvYAML",
			runtimeEnvYAML: "",
			isErrorNil:     true,
		},
		{
			name: "Valid runtimeEnvYAML",
			runtimeEnvYAML: `
env_vars:
  counter_name: test_counter
`,
			isErrorNil: true,
		},
		{
			name:           "Invalid runtimeEnvYAML",
			runtimeEnvYAML: `invalid_yaml_str`,
			isErrorNil:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dashboardclient.UnmarshalRuntimeEnvYAML(tc.runtimeEnvYAML)
			if tc.isErrorNil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestFindHeadPodReadyCondition(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected metav1.Condition
	}{
		{
			name: "condition true if Ray head pod is running and ready",
			pod:  createRayHeadPodWithPhaseAndCondition(corev1.PodRunning, corev1.ConditionTrue),
			expected: metav1.Condition{
				Type:   string(rayv1.HeadPodReady),
				Status: metav1.ConditionTrue,
			},
		},
		{
			name: "condition false if Ray head pod is not running",
			pod:  createRayHeadPodWithPhaseAndCondition(corev1.PodPending, corev1.ConditionFalse),
			expected: metav1.Condition{
				Type:   string(rayv1.HeadPodReady),
				Status: metav1.ConditionFalse,
			},
		},
		{
			name: "condition false if Ray head pod is not ready",
			pod:  createRayHeadPodWithPhaseAndCondition(corev1.PodRunning, corev1.ConditionFalse),
			expected: metav1.Condition{
				Type:   string(rayv1.HeadPodReady),
				Status: metav1.ConditionFalse,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			headPodReadyCondition := FindHeadPodReadyCondition(tc.pod)
			assert.Equal(t, tc.expected.Status, headPodReadyCondition.Status)
		})
	}
}

func TestFindHeadPodReadyMessage(t *testing.T) {
	tests := []struct {
		name        string
		message     string
		wantMessage string
		wantReason  string
		status      []corev1.ContainerStatus
	}{{
		name:       "no message no status want original reason",
		wantReason: ContainersNotReady,
	}, {
		name:        "no container status want original reason",
		message:     "TooEarlyInTheMorning",
		wantMessage: "TooEarlyInTheMorning",
		wantReason:  ContainersNotReady,
	}, {
		name:    "one reason one status",
		message: "containers not ready",
		status: []corev1.ContainerStatus{{
			Name: "ray",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "ImagePullBackOff",
					Message: `Back-off pulling image royproject/roy:latest: ErrImagePull: rpc error: code = NotFound`,
				},
			},
		}},
		wantReason:  "ImagePullBackOff",
		wantMessage: `containers not ready; ray: Back-off pulling image royproject/roy:latest: ErrImagePull: rpc error: code = NotFound`,
	}, {
		name:    "one reason two statuses only copy first",
		message: "aesthetic problems",
		status: []corev1.ContainerStatus{{
			Name: "indigo",
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "BadColor",
					Message: "too blue",
				},
			},
		}, {
			Name: "circle",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:  "BadGeometry",
					Message: "too round",
				},
			},
		}},
		wantReason:  "BadColor",
		wantMessage: "aesthetic problems; indigo: too blue",
	}, {
		name: "no reason one status",
		status: []corev1.ContainerStatus{{
			Name: "my-image",
			State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					Reason:  "Crashed",
					Message: "bash not found",
				},
			},
		}},
		wantReason:  "Crashed",
		wantMessage: "my-image: bash not found",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := createRayHeadPodWithPhaseAndCondition(corev1.PodPending, corev1.ConditionFalse)
			pod.Status.Conditions[0].Message = tc.message
			pod.Status.ContainerStatuses = tc.status
			cond := FindHeadPodReadyCondition(pod)
			if cond.Message != tc.wantMessage {
				t.Errorf("FindHeadPodReadyCondition(...) returned condition with message %q, but wanted %q", cond.Message, tc.wantMessage)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("FindHeadPodReadyCondition(...) returned condition with reason %q, but wanted %q", cond.Reason, tc.wantReason)
			}
		})
	}
}

func TestErrRayClusterReplicaFailureReason(t *testing.T) {
	assert.Equal(t, "FailedDeleteAllPods", RayClusterReplicaFailureReason(ErrFailedDeleteAllPods))
	assert.Equal(t, "FailedDeleteHeadPod", RayClusterReplicaFailureReason(ErrFailedDeleteHeadPod))
	assert.Equal(t, "FailedCreateHeadPod", RayClusterReplicaFailureReason(ErrFailedCreateHeadPod))
	assert.Equal(t, "FailedDeleteWorkerPod", RayClusterReplicaFailureReason(ErrFailedDeleteWorkerPod))
	assert.Equal(t, "FailedCreateWorkerPod", RayClusterReplicaFailureReason(ErrFailedCreateWorkerPod))
	assert.Equal(t, "FailedDeleteAllPods", RayClusterReplicaFailureReason(errors.Join(ErrFailedDeleteAllPods, errors.New("other error"))))
	assert.Equal(t, "FailedDeleteHeadPod", RayClusterReplicaFailureReason(errors.Join(ErrFailedDeleteHeadPod, errors.New("other error"))))
	assert.Equal(t, "FailedCreateHeadPod", RayClusterReplicaFailureReason(errors.Join(ErrFailedCreateHeadPod, errors.New("other error"))))
	assert.Equal(t, "FailedDeleteWorkerPod", RayClusterReplicaFailureReason(errors.Join(ErrFailedDeleteWorkerPod, errors.New("other error"))))
	assert.Equal(t, "FailedCreateWorkerPod", RayClusterReplicaFailureReason(errors.Join(ErrFailedCreateWorkerPod, errors.New("other error"))))
	assert.Empty(t, RayClusterReplicaFailureReason(errors.New("other error")))
}

func TestIsAutoscalingEnabled(t *testing.T) {
	tests := map[string]struct {
		spec     *rayv1.RayClusterSpec
		expected bool
	}{
		"should be false when spec is nil": {
			spec:     nil,
			expected: false,
		},
		"should be false when enableInTreeAutoscaling is nil": {
			spec: &rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: nil,
			},
			expected: false,
		},
		"should be false when enableInTreeAutoscaling is false": {
			spec: &rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(false),
			},
			expected: false,
		},
		"should be true when enableInTreeAutoscaling is true": {
			spec: &rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
			},
			expected: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsAutoscalingEnabled(tc.spec))
		})
	}
}

func TestIsAutoscalingV2Enabled(t *testing.T) {
	tests := map[string]struct {
		spec     *rayv1.RayClusterSpec
		expected bool
	}{
		"should be false when spec is nil": {
			spec:     nil,
			expected: false,
		},
		"should be false when autoscaler options is nil": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: nil,
			},
			expected: false,
		},
		"should be false when autoscaler options is not v2": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV1)},
			},
			expected: false,
		},
		"should be true when autoscaler options is v2": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV2)},
			},
			expected: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsAutoscalingV2Enabled(tc.spec))
		})
	}
}

func TestIsAutoscalingV1Enabled(t *testing.T) {
	tests := map[string]struct {
		spec     *rayv1.RayClusterSpec
		expected bool
	}{
		"should be false when spec is nil": {
			spec:     nil,
			expected: false,
		},
		"should be false when autoscaler options is nil": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: nil,
			},
			expected: false,
		},
		"should be false when autoscaler options is not v1": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV2)},
			},
			expected: false,
		},
		"should be true when autoscaler options is v1": {
			spec: &rayv1.RayClusterSpec{
				AutoscalerOptions: &rayv1.AutoscalerOptions{Version: ptr.To(rayv1.AutoscalerVersionV1)},
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsAutoscalingV1Enabled(tc.spec))
		})
	}
}

func TestIsGCSFaultToleranceEnabled(t *testing.T) {
	tests := []struct {
		name     string
		instance rayv1.RayCluster
		expected bool
	}{
		{
			name: "ray.io/ft-enabled is true",
			instance: rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						RayFTEnabledAnnotationKey: "true",
					},
				},
			},
			expected: true,
		},
		{
			name: "ray.io/ft-enabled is not set and GcsFaultToleranceOptions is set",
			instance: rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					GcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
				},
			},
			expected: true,
		},
		{
			name: "ray.io/ft-enabled is false",
			instance: rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						RayFTEnabledAnnotationKey: "false",
					},
				},
			},
			expected: false,
		},
		{
			name: "ray.io/ft-enabled is not set and GcsFaultToleranceOptions is not set",
			instance: rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			},
			expected: false,
		},
		{
			name: "ray.io/ft-enabled is using uppercase true",
			instance: rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						RayFTEnabledAnnotationKey: "TRUE",
					},
				},
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := IsGCSFaultToleranceEnabled(&test.instance.Spec, test.instance.Annotations)
			assert.Equal(t, test.expected, result)
		})
	}
}

func createPodSpec(cpu, memory string) corev1.PodSpec {
	return corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse(cpu),
						corev1.ResourceMemory: resource.MustParse(memory),
					},
				},
			},
		},
	}
}

func createRayClusterTemplate(
	head struct {
		cpu    string
		memory string
	},
	workers []struct {
		replicas    *int32
		minReplicas *int32
		suspend     *bool
		cpu         string
		memory      string
		numOfHosts  int32
	},
) *rayv1.RayCluster {
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: createPodSpec(head.cpu, head.memory),
				},
			},
		},
	}

	for _, w := range workers {
		cluster.Spec.WorkerGroupSpecs = append(cluster.Spec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{
			NumOfHosts:  w.numOfHosts,
			Replicas:    w.replicas,
			MinReplicas: w.minReplicas,
			Suspend:     w.suspend,
			Template: corev1.PodTemplateSpec{
				Spec: createPodSpec(w.cpu, w.memory),
			},
		})
	}

	return cluster
}

func TestCalculatePodResourceDoesNotMutateInput(t *testing.T) {
	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("200Mi"),
					},
				},
			},
		},
	}
	wantRequests := podSpec.Containers[0].Resources.Requests.DeepCopy()

	got := CalculatePodResource(podSpec)

	assert.Equal(t, "1", got.Cpu().String())
	assert.Equal(t, "200Mi", got.Memory().String())
	assert.Equal(t, wantRequests, podSpec.Containers[0].Resources.Requests)
}

func TestCalculateResources(t *testing.T) {
	headStruct := struct {
		cpu    string
		memory string
	}{
		cpu:    "1",
		memory: "100Mi",
	}

	tests := []struct {
		expected struct {
			desiredResources corev1.ResourceList
			minResources     corev1.ResourceList
		}
		cluster *rayv1.RayCluster
		name    string
	}{
		{
			name:    "Single head pod with no worker groups",
			cluster: createRayClusterTemplate(headStruct, nil),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		},
		{
			name: "Head pod with one worker group",
			cluster: createRayClusterTemplate(headStruct, []struct {
				replicas    *int32
				minReplicas *int32
				suspend     *bool
				cpu         string
				memory      string
				numOfHosts  int32
			}{
				{
					numOfHosts:  2,
					replicas:    ptr.To[int32](4),
					minReplicas: ptr.To[int32](3),
					cpu:         "1",
					memory:      "200Mi",
					suspend:     nil,
				},
			}),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("9"),
					corev1.ResourceMemory: resource.MustParse("1700Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("7"),
					corev1.ResourceMemory: resource.MustParse("1300Mi"),
				},
			},
		},
		{
			name: "Head pod with two worker group with different resources",
			cluster: createRayClusterTemplate(headStruct, []struct {
				replicas    *int32
				minReplicas *int32
				suspend     *bool
				cpu         string
				memory      string
				numOfHosts  int32
			}{
				{
					numOfHosts:  2,
					replicas:    ptr.To[int32](2),
					minReplicas: ptr.To[int32](1),
					cpu:         "2",
					memory:      "100Mi",
					suspend:     nil,
				},
				{
					numOfHosts:  1,
					replicas:    ptr.To[int32](3),
					minReplicas: ptr.To[int32](0),
					cpu:         "1",
					memory:      "200Mi",
					suspend:     nil,
				},
			}),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("12"),
					corev1.ResourceMemory: resource.MustParse("1100Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("5"),
					corev1.ResourceMemory: resource.MustParse("300Mi"),
				},
			},
		},
		{
			name: "Head pod with suspended worker group",
			cluster: createRayClusterTemplate(headStruct, []struct {
				replicas    *int32
				minReplicas *int32
				suspend     *bool
				cpu         string
				memory      string
				numOfHosts  int32
			}{
				{
					numOfHosts:  2,
					replicas:    ptr.To[int32](3),
					minReplicas: ptr.To[int32](0),
					cpu:         "2",
					memory:      "200Mi",
					suspend:     new(true),
				},
			}),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		},
		{
			name: "Head pod with suspended worker group with min replicas",
			cluster: createRayClusterTemplate(headStruct, []struct {
				replicas    *int32
				minReplicas *int32
				suspend     *bool
				cpu         string
				memory      string
				numOfHosts  int32
			}{
				{
					numOfHosts:  1,
					replicas:    ptr.To[int32](3),
					minReplicas: ptr.To[int32](2),
					cpu:         "4",
					memory:      "200Mi",
					suspend:     new(true),
				},
			}),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		},
		{
			name: "Head pod with worker group having max possible replicas",
			cluster: createRayClusterTemplate(headStruct, []struct {
				replicas    *int32
				minReplicas *int32
				suspend     *bool
				cpu         string
				memory      string
				numOfHosts  int32
			}{
				{
					numOfHosts:  1,
					replicas:    ptr.To[int32](2147483647),
					minReplicas: ptr.To[int32](2147483647),
					cpu:         "1",
					memory:      "0",
					suspend:     nil,
				},
			}),
			expected: struct {
				desiredResources corev1.ResourceList
				minResources     corev1.ResourceList
			}{
				desiredResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2147483648"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
				minResources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2147483648"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deepCopyCluster := tt.cluster.DeepCopy()

			desiredResource := CalculateDesiredResources(tt.cluster)
			assert.Equal(t, tt.expected.desiredResources.Cpu().String(), desiredResource.Cpu().String())
			assert.Equal(t, tt.expected.desiredResources.Memory().String(), desiredResource.Memory().String())
			assert.Equal(t, deepCopyCluster, tt.cluster)

			minResource := CalculateMinResources(tt.cluster)
			assert.Equal(t, tt.expected.minResources.Cpu().String(), minResource.Cpu().String())
			assert.Equal(t, tt.expected.minResources.Memory().String(), minResource.Memory().String())
			assert.Equal(t, deepCopyCluster, tt.cluster)
		})
	}
}

// helper function to return a Gateway object with GatewayStatus Conditions for testing.
func makeGatewayWithCondition(accepted bool, programmed bool) *gwv1.Gateway {
	var conditions []metav1.Condition

	if accepted {
		conditions = append(conditions, metav1.Condition{
			Type:   string(gwv1.GatewayConditionAccepted),
			Status: metav1.ConditionTrue,
		})
	}

	if programmed {
		conditions = append(conditions, metav1.Condition{
			Type:   string(gwv1.GatewayConditionProgrammed),
			Status: metav1.ConditionTrue,
		})
	}

	return &gwv1.Gateway{
		Status: gwv1.GatewayStatus{
			Conditions: conditions,
		},
	}
}

func TestIsGatewayReady(t *testing.T) {
	tests := []struct {
		gateway  *gwv1.Gateway
		name     string
		expected bool
	}{
		{
			name:     "missing Gateway instance",
			gateway:  nil,
			expected: false,
		},
		{
			name:     "Gateway created with Programmed condition only",
			gateway:  makeGatewayWithCondition(false, true),
			expected: false,
		},
		{
			name:     "Gateway created with Accepted condition only",
			gateway:  makeGatewayWithCondition(true, false),
			expected: false,
		},
		{
			name:     "Gateway created with both Accepted and Programmed conditions",
			gateway:  makeGatewayWithCondition(true, true),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsGatewayReady(tt.gateway))
		})
	}
}

// helper function to return a HTTPRoute with HTTPRouteStatus for testing
func makeHTTPRouteWithParentRef(
	parentRefName string,
	namespace string,
	accepted bool,
	resolvedRefs bool,
) *gwv1.HTTPRoute {
	var acceptedStatus, resolvedRefsStatus metav1.ConditionStatus
	if accepted {
		acceptedStatus = metav1.ConditionTrue
	} else {
		acceptedStatus = metav1.ConditionFalse
	}
	if resolvedRefs {
		resolvedRefsStatus = metav1.ConditionTrue
	} else {
		resolvedRefsStatus = metav1.ConditionFalse
	}

	return &gwv1.HTTPRoute{
		Status: gwv1.HTTPRouteStatus{
			RouteStatus: gwv1.RouteStatus{
				Parents: []gwv1.RouteParentStatus{
					{
						ParentRef: gwv1.ParentReference{
							Name:      gwv1.ObjectName(parentRefName),
							Namespace: new(gwv1.Namespace(namespace)),
						},
						Conditions: []metav1.Condition{
							{
								Type:   string(gwv1.RouteConditionAccepted),
								Status: acceptedStatus,
							},
							{
								Type:   string(gwv1.RouteConditionResolvedRefs),
								Status: resolvedRefsStatus,
							},
						},
					},
				},
			},
		},
	}
}

func TestIsHTTPRouteReady(t *testing.T) {
	gateway := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "test-gateway", Namespace: "test-ns"},
	}

	tests := []struct {
		httpRoute *gwv1.HTTPRoute
		name      string
		expected  bool
	}{
		{
			name:      "missing HTTPRoute",
			httpRoute: nil,
			expected:  false,
		},
		{
			name:      "ParentRef does not match",
			httpRoute: makeHTTPRouteWithParentRef("not-a-match", "other-test-ns", true, true),
			expected:  false,
		},
		{
			name:      "matching ParentRef with Accepted condition but without ResolvedRefs",
			httpRoute: makeHTTPRouteWithParentRef("test-gateway", "test-ns", true, false),
			expected:  false,
		},
		{
			name:      "matching ParentRef with ResolvedRefs but without Accepted",
			httpRoute: makeHTTPRouteWithParentRef("test-gateway", "test-ns", false, true),
			expected:  false,
		},
		{
			name:      "ready HTTPRoute with all required conditions",
			httpRoute: makeHTTPRouteWithParentRef("test-gateway", "test-ns", true, true),
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsHTTPRouteReady(gateway, tt.httpRoute))
		})
	}
}

func TestIsIncrementalUpgradeEnabled(t *testing.T) {
	tests := []struct {
		spec           *rayv1.RayServiceSpec
		name           string
		featureEnabled bool
		expected       bool
	}{
		{
			name:           "missing UpgradeStrategy Type",
			spec:           &rayv1.RayServiceSpec{},
			featureEnabled: true,
			expected:       false,
		},
		{
			name: "UpgradeStrategy Type is NewClusterWithIncrementalUpgrade but feature disabled",
			spec: &rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				},
			},
			featureEnabled: false,
			expected:       false,
		},
		{
			name: "UpgradeStrategy Type is NewClusterWithIncrementalUpgrade and feature enabled",
			spec: &rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				},
			},
			featureEnabled: true,
			expected:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, tc.featureEnabled)
			assert.Equal(t, tc.expected, IsIncrementalUpgradeEnabled(tc.spec))
		})
	}
}

func TestGetRayServiceClusterUpgradeOptions(t *testing.T) {
	upgradeOptions := &rayv1.ClusterUpgradeOptions{GatewayClassName: "gateway-class"}

	tests := []struct {
		rayServiceSpec  *rayv1.RayServiceSpec
		expectedOptions *rayv1.ClusterUpgradeOptions
		name            string
	}{
		{
			name:            "RayServiceSpec is nil, return nil ClusterUpgradeOptions",
			rayServiceSpec:  nil,
			expectedOptions: nil,
		},
		{
			name:            "UpgradeStrategy is nil, return nil ClusterUpgradeOptions",
			rayServiceSpec:  &rayv1.RayServiceSpec{},
			expectedOptions: nil,
		},
		{
			name: "Valid ClusterUpgradeOptions",
			rayServiceSpec: &rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					ClusterUpgradeOptions: upgradeOptions,
				},
			},
			expectedOptions: upgradeOptions,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualOptions := GetRayServiceClusterUpgradeOptions(tt.rayServiceSpec)
			assert.Equal(t, tt.expectedOptions, actualOptions)
		})
	}
}

func TestGetContainerCommand(t *testing.T) {
	tests := []struct {
		name              string
		additionalOptions []string
		expected          []string
		enableLoginShell  bool
	}{
		{
			name:              "enable login shell is false",
			enableLoginShell:  false,
			additionalOptions: []string{},
			expected:          []string{"/bin/bash", "-c", "--"},
		},
		{
			name:              "enable login shell is true",
			enableLoginShell:  true,
			additionalOptions: []string{},
			expected:          []string{"/bin/bash", "-cl", "--"},
		},
		{
			name:              "enable login shell is false and additional options is not empty",
			enableLoginShell:  false,
			additionalOptions: []string{"e"},
			expected:          []string{"/bin/bash", "-ce", "--"},
		},
		{
			name:              "enable login shell is true and additional options is not empty",
			enableLoginShell:  true,
			additionalOptions: []string{"e"},
			expected:          []string{"/bin/bash", "-cel", "--"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.enableLoginShell {
				os.Setenv("ENABLE_LOGIN_SHELL", "true")
				defer os.Unsetenv("ENABLE_LOGIN_SHELL")
			}
			assert.Equal(t, test.expected, GetContainerCommand(test.additionalOptions))
		})
	}
}

func TestGetWeightsFromHTTPRoute(t *testing.T) {
	activeClusterName := "rayservice-active"
	pendingClusterName := "rayservice-pending"

	// Helper to create a RayService with specified cluster names in its status.
	makeRayService := func(activeName, pendingName string) *rayv1.RayService {
		return &rayv1.RayService{
			Status: rayv1.RayServiceStatuses{
				ActiveServiceStatus:  rayv1.RayServiceStatus{RayClusterName: activeName},
				PendingServiceStatus: rayv1.RayServiceStatus{RayClusterName: pendingName},
			},
		}
	}

	// Helper to create an HTTPRoute with specified backend weights.
	makeHTTPRoute := func(activeWeight, pendingWeight *int32) *gwv1.HTTPRoute {
		backends := []gwv1.HTTPBackendRef{}
		if activeWeight != nil {
			backends = append(backends, gwv1.HTTPBackendRef{
				BackendRef: gwv1.BackendRef{
					BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(GenerateServeServiceName(activeClusterName))},
					Weight:                 activeWeight,
				},
			})
		}
		if pendingWeight != nil {
			backends = append(backends, gwv1.HTTPBackendRef{
				BackendRef: gwv1.BackendRef{
					BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(GenerateServeServiceName(pendingClusterName))},
					Weight:                 pendingWeight,
				},
			})
		}
		return &gwv1.HTTPRoute{
			Spec: gwv1.HTTPRouteSpec{
				Rules: []gwv1.HTTPRouteRule{{BackendRefs: backends}},
			},
		}
	}

	tests := []struct {
		httpRoute       *gwv1.HTTPRoute
		rayService      *rayv1.RayService
		name            string
		expectedActive  int32
		expectedPending int32
	}{
		{
			name:            "No HTTPRoute, return defaults for both weights",
			httpRoute:       nil,
			rayService:      makeRayService(activeClusterName, ""),
			expectedActive:  -1,
			expectedPending: -1,
		},
		{
			name:            "HTTPRoute with missing backends, return defaults for both weights",
			httpRoute:       &gwv1.HTTPRoute{Spec: gwv1.HTTPRouteSpec{Rules: []gwv1.HTTPRouteRule{{}}}},
			rayService:      makeRayService(activeClusterName, pendingClusterName),
			expectedActive:  -1,
			expectedPending: -1,
		},
		{
			name:            "Valid weights returned for both active and pending clusters",
			httpRoute:       makeHTTPRoute(new(int32(80)), new(int32(20))),
			rayService:      makeRayService(activeClusterName, pendingClusterName),
			expectedActive:  80,
			expectedPending: 20,
		},
		{
			name:            "Valid HTTPRoute with only active cluster backend",
			httpRoute:       makeHTTPRoute(new(int32(100)), nil),
			rayService:      makeRayService(activeClusterName, ""),
			expectedActive:  100,
			expectedPending: -1,
		},
		{
			name:            "Valid HTTPRoute with only pending cluster backend",
			httpRoute:       makeHTTPRoute(nil, new(int32(100))),
			rayService:      makeRayService("", pendingClusterName),
			expectedActive:  -1,
			expectedPending: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			active, pending := GetWeightsFromHTTPRoute(tt.httpRoute, tt.rayService)
			assert.Equal(t, tt.expectedActive, active, "Active weight mismatch")
			assert.Equal(t, tt.expectedPending, pending, "Pending weight mismatch")
		})
	}
}

func TestIsHTTPRouteEqual(t *testing.T) {
	tests := []struct {
		existing *gwv1.HTTPRoute
		desired  *gwv1.HTTPRoute
		name     string
		expected bool
	}{
		{
			name: "Exactly equal HTTPRoutes",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Port: ptr.To(gwv1.PortNumber(8000))}, Weight: new(int32(100))}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Port: ptr.To(gwv1.PortNumber(8000))}, Weight: new(int32(100))}},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Hostnames nil vs empty slice - equal",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					// Empty slice
					Hostnames: []gwv1.Hostname{},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					// Nil slice
					Hostnames: nil,
				},
			},
			expected: true,
		},
		{
			name: "Different number of rules",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}}}}},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}}}}},
						{BackendRefs: []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-b"}}}}},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different number of backends",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}}},
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-b"}}},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different backend weights",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}, Weight: new(int32(100))}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a"}, Weight: new(int32(75))}},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different backend names",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-old"}}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-new"}}},
							},
						},
					},
				},
			},
			expected: false,
		},

		{
			name: "Different ParentRefs",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gateway-old"},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gateway-new"},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different ParentRef SectionName",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", SectionName: ptr.To(gwv1.SectionName("http"))},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", SectionName: ptr.To(gwv1.SectionName("https"))},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different ParentRef Port",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", Port: ptr.To(gwv1.PortNumber(80))},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", Port: ptr.To(gwv1.PortNumber(443))},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Equal ParentRef SectionName and Port",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", SectionName: ptr.To(gwv1.SectionName("http")), Port: ptr.To(gwv1.PortNumber(80))},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					CommonRouteSpec: gwv1.CommonRouteSpec{
						ParentRefs: []gwv1.ParentReference{
							{Name: "gw", SectionName: ptr.To(gwv1.SectionName("http")), Port: ptr.To(gwv1.PortNumber(80))},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Different Hostnames",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Hostnames: []gwv1.Hostname{"example.com"},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Hostnames: []gwv1.Hostname{"different.com"},
				},
			},
			expected: false,
		},
		{
			name: "Different Matches",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							Matches: []gwv1.HTTPRouteMatch{
								{Path: &gwv1.HTTPPathMatch{Value: new("/api")}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							Matches: []gwv1.HTTPRouteMatch{
								{Path: &gwv1.HTTPPathMatch{Value: new("/v2")}},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different Filters",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							Filters: []gwv1.HTTPRouteFilter{
								{Type: gwv1.HTTPRouteFilterRequestHeaderModifier},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							Filters: []gwv1.HTTPRouteFilter{
								{Type: gwv1.HTTPRouteFilterRequestRedirect},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Filters nil vs empty slice, should be equal for our check.",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							// Nil slice
							Filters: nil,
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							Filters: []gwv1.HTTPRouteFilter{},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Different Backend Namespace",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Namespace: ptr.To(gwv1.Namespace("default"))}}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Namespace: ptr.To(gwv1.Namespace("other-ns"))}}},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different Backend Port",
			existing: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Port: ptr.To(gwv1.PortNumber(8000))}}},
							},
						},
					},
				},
			},
			desired: &gwv1.HTTPRoute{
				Spec: gwv1.HTTPRouteSpec{
					Rules: []gwv1.HTTPRouteRule{
						{
							BackendRefs: []gwv1.HTTPBackendRef{
								{BackendRef: gwv1.BackendRef{BackendObjectReference: gwv1.BackendObjectReference{Name: "svc-a", Port: ptr.To(gwv1.PortNumber(9000))}}},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHTTPRouteEqual(tt.existing, tt.desired)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsGatewayEqual(t *testing.T) {
	tests := []struct {
		existing *gwv1.Gateway
		desired  *gwv1.Gateway
		name     string
		expected bool
	}{
		{
			name:     "Both nil",
			existing: nil,
			desired:  nil,
			expected: true,
		},
		{
			name:     "Existing nil",
			desired:  &gwv1.Gateway{},
			expected: false,
		},
		{
			name:     "Desired nil",
			existing: &gwv1.Gateway{},
			expected: false,
		},
		{
			name: "Exactly equal Gateways",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class",
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class",
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			expected: true,
		},
		{
			name: "Different GatewayClassName",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class-old",
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class-new",
				},
			},
			expected: false,
		},
		{
			name: "Different number of Listeners",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
						{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different Listener Name",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http-old", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http-new", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different Listener Protocol",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPSProtocolType, Port: 80},
					},
				},
			},
			expected: false,
		},
		{
			name: "Different Listener Port",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 8080},
					},
				},
			},
			expected: false,
		},
		{
			name: "Equal Gateways ignoring defaulted AllowedRoutes",
			existing: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class",
					Listeners: []gwv1.Listener{
						{
							Name:     "http",
							Protocol: gwv1.HTTPProtocolType,
							Port:     80,
							AllowedRoutes: &gwv1.AllowedRoutes{
								Namespaces: &gwv1.RouteNamespaces{
									From: ptr.To(gwv1.NamespacesFromSame),
								},
							},
						},
					},
				},
			},
			desired: &gwv1.Gateway{
				Spec: gwv1.GatewaySpec{
					GatewayClassName: "test-class",
					Listeners: []gwv1.Listener{
						{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsGatewayEqual(tt.existing, tt.desired)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsJobFinished(t *testing.T) {
	makeJob := func(conditions ...batchv1.JobCondition) *batchv1.Job {
		return &batchv1.Job{Status: batchv1.JobStatus{Conditions: conditions}}
	}
	cond := func(t batchv1.JobConditionType, s corev1.ConditionStatus) batchv1.JobCondition {
		return batchv1.JobCondition{Type: t, Status: s}
	}

	tests := []struct {
		name         string
		job          *batchv1.Job
		wantType     batchv1.JobConditionType
		wantFinished bool
	}{
		{
			name:         "no conditions",
			job:          makeJob(),
			wantFinished: false,
		},
		{
			name:         "complete",
			job:          makeJob(cond(batchv1.JobComplete, corev1.ConditionTrue)),
			wantType:     batchv1.JobComplete,
			wantFinished: true,
		},
		{
			name:         "failed",
			job:          makeJob(cond(batchv1.JobFailed, corev1.ConditionTrue)),
			wantType:     batchv1.JobFailed,
			wantFinished: true,
		},
		{
			name:         "failure target only (activeDeadlineSeconds window before Failed appears)",
			job:          makeJob(cond(batchv1.JobFailureTarget, corev1.ConditionTrue)),
			wantType:     batchv1.JobFailed,
			wantFinished: true,
		},
		{
			name:         "failure target false status is not finished",
			job:          makeJob(cond(batchv1.JobFailureTarget, corev1.ConditionFalse)),
			wantFinished: false,
		},
		{
			name: "failure target then failed",
			job: makeJob(
				cond(batchv1.JobFailureTarget, corev1.ConditionTrue),
				cond(batchv1.JobFailed, corev1.ConditionTrue),
			),
			wantType:     batchv1.JobFailed,
			wantFinished: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotFinished := IsJobFinished(tt.job)
			assert.Equal(t, tt.wantFinished, gotFinished)
			if tt.wantFinished {
				assert.Equal(t, tt.wantType, gotType)
			}
		})
	}
}

func TestGetGcsFaultToleranceBackend(t *testing.T) {
	assert.Equal(t, rayv1.GcsFTBackendRedis, GetGcsFaultToleranceBackend(nil))
	assert.Equal(t, rayv1.GcsFTBackendRedis, GetGcsFaultToleranceBackend(&rayv1.GcsFaultToleranceOptions{}))
	assert.Equal(t, rayv1.GcsFTBackendRedis, GetGcsFaultToleranceBackend(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRedis}))
	assert.Equal(t, rayv1.GcsFTBackendRocksDB, GetGcsFaultToleranceBackend(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB}))
}

func TestIsGCSFaultToleranceEmbedded(t *testing.T) {
	assert.False(t, IsGCSFaultToleranceEmbedded(nil))
	assert.False(t, IsGCSFaultToleranceEmbedded(&rayv1.GcsFaultToleranceOptions{}))
	assert.False(t, IsGCSFaultToleranceEmbedded(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRedis}))
	assert.True(t, IsGCSFaultToleranceEmbedded(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB}))
}

func TestSupportsFlexibleRestartPolicy(t *testing.T) {
	// Empty / unspecified version → original behavior (restrict to Never).
	assert.False(t, SupportsFlexibleRestartPolicy(""))
	// Invalid version string → treated as unsupported.
	assert.False(t, SupportsFlexibleRestartPolicy("not-a-version"))
	// Below the minimum → restrict to Never.
	assert.False(t, SupportsFlexibleRestartPolicy("2.55.0"))
	assert.False(t, SupportsFlexibleRestartPolicy("2.55.9"))
	// Exactly the minimum → flexible policy allowed.
	assert.True(t, SupportsFlexibleRestartPolicy("2.56.0"))
	// Above the minimum → flexible policy allowed.
	assert.True(t, SupportsFlexibleRestartPolicy("2.57.0"))
	assert.True(t, SupportsFlexibleRestartPolicy("3.0.0"))
}

func TestGetGCSStoragePVCName(t *testing.T) {
	instance := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-cluster"},
		Spec: rayv1.RayClusterSpec{
			GcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB},
		},
	}
	assert.Equal(t, "my-cluster-gcs-pvc", GetGCSStoragePVCName(instance))

	instance.Spec.GcsFaultToleranceOptions.Storage = &rayv1.GcsEmbeddedStorage{ClaimName: "byo-pvc"}
	assert.Equal(t, "byo-pvc", GetGCSStoragePVCName(instance))
}

func TestNodeLabelKey(t *testing.T) {
	key, err := NodeLabelKey("metadata.labels['topology.kubernetes.io/zone']")
	require.NoError(t, err)
	assert.Equal(t, "topology.kubernetes.io/zone", key)
	for _, bad := range []string{"", "metadata.labels", "metadata.labels[\"zone\"]", "metadata.annotations['zone']", "spec.providerID"} {
		_, err := NodeLabelKey(bad)
		require.ErrorContains(t, err, "only metadata.labels['<key>'] is supported", bad)
	}
}
