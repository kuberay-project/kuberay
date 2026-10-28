package get

import (
	"bytes"
	"context"
	"errors"
	"math"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	fakecorev1 "k8s.io/client-go/kubernetes/typed/core/v1/fake"
	kubetesting "k8s.io/client-go/testing"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/utils/ptr"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestRayWorkerGroupGetComplete(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := []struct {
		opts                *GetWorkerGroupsOptions
		name                string
		namespace           string
		expectedNamespace   string
		expectedWorkerGroup string
		args                []string
	}{
		{
			name: "specifying all namespaces should set namespace to empty string",
			opts: &GetWorkerGroupsOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: true,
			},
			expectedNamespace: "",
		},
		{
			name: "not specifying a namespace should set namespace to 'default'",
			opts: &GetWorkerGroupsOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: false,
			},
			expectedNamespace: "default",
		},
		{
			name: "specifying a namespace should set that namespace",
			opts: &GetWorkerGroupsOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: false,
			},
			namespace:         "some-namespace",
			expectedNamespace: "some-namespace",
		},
		{
			name: "specifying all namespaces takes precedence over specifying a namespace",
			opts: &GetWorkerGroupsOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: true,
			},
			namespace:         "some-namespace",
			expectedNamespace: "",
		},
		{
			name: "first positional argument should be set as the worker group name",
			opts: &GetWorkerGroupsOptions{
				cmdFactory: cmdFactory,
			},
			args:                []string{"my-group", "other-arg"},
			expectedNamespace:   "default",
			expectedWorkerGroup: "my-group",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := genericclioptions.NewConfigFlags(true)
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			tc.opts.cmdFactory = cmdutil.NewFactory(configFlags)
			cmd := &cobra.Command{}
			flags := cmd.Flags()
			configFlags.AddFlags(flags)
			err := tc.opts.Complete(tc.args)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedNamespace, tc.opts.namespace)
			assert.Equal(t, tc.expectedWorkerGroup, tc.opts.workerGroup)
		})
	}
}

func TestRayWorkerGroupsGetRun(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("2"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
	}

	podTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: resources,
						Limits:   resources,
					},
				},
			},
		},
	}

	readyStatus := corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		},
	}

	rayClusters := []runtime.Object{
		&rayv1.RayCluster{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "cluster-1",
			},
			Spec: rayv1.RayClusterSpec{
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName:   "group-1",
						Replicas:    new(int32(1)),
						MinReplicas: new(int32(1)),
						MaxReplicas: new(int32(5)),
						Template:    podTemplate,
					},
				},
			},
		},
		&rayv1.RayCluster{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "cluster-2",
			},
			Spec: rayv1.RayClusterSpec{
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName:   "group-2",
						Replicas:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: ptr.To(int32(math.MaxInt32)),
						Template:    podTemplate,
					},
				},
			},
		},
		&rayv1.RayCluster{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-2",
				Name:      "cluster-1",
			},
			Spec: rayv1.RayClusterSpec{
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName:   "group-1",
						Replicas:    new(int32(2)),
						MinReplicas: new(int32(1)),
						MaxReplicas: new(int32(4)),
						Template:    podTemplate,
					},
					{
						GroupName:   "group-4",
						Replicas:    new(int32(0)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(3)),
						Template:    podTemplate,
					},
				},
			},
		},
	}

	pods := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-1",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeTypeLabelKey:  string(rayv1.HeadNode),
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-2",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-1",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Status: readyStatus,
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-3",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-2",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeTypeLabelKey:  string(rayv1.HeadNode),
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-4",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-2",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-2",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Status: readyStatus,
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-2",
				Name:      "pod-5",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-1",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Status: readyStatus,
		},
	}

	tests := []struct {
		name          string
		cluster       string
		namespace     string
		workerGroup   string
		expected      string
		expectedError string
		rayClusters   []runtime.Object
		pods          []runtime.Object
		allNamespaces bool
	}{
		{
			name:          "no cluster; no namespace; no worker group",
			allNamespaces: true,
			rayClusters:   rayClusters,
			pods:          pods,
			expected: `NAMESPACE     NAME      MIN   MAX          REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
namespace-1   group-1   1     5            1/1        2      1      1      1Gi      cluster-1
namespace-1   group-2   0     2147483647   1/1        2      1      1      1Gi      cluster-2
namespace-2   group-1   1     4            1/2        4      2      2      2Gi      cluster-1
namespace-2   group-4   0     3            0/0        0      0      0      0        cluster-1
`,
		},
		{
			name:          "cluster; no namespace; no worker group",
			cluster:       "cluster-1",
			allNamespaces: true,
			// We filter RayClusters by name with a field selector which fake clients don't support.
			// So we need to pass only the RayCluster we expect to get.
			// See https://github.com/kubernetes/client-go/issues/326
			rayClusters: []runtime.Object{rayClusters[0], rayClusters[2]},
			pods:        pods,
			expected: `NAMESPACE     NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
namespace-1   group-1   1     5     1/1        2      1      1      1Gi      cluster-1
namespace-2   group-1   1     4     1/2        4      2      2      2Gi      cluster-1
namespace-2   group-4   0     3     0/0        0      0      0      0        cluster-1
`,
		},
		{
			name:          "no cluster; namespace; no worker group",
			namespace:     "namespace-1",
			allNamespaces: false,
			rayClusters:   rayClusters,
			pods:          pods,
			expected: `NAME      MIN   MAX          REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
group-1   1     5            1/1        2      1      1      1Gi      cluster-1
group-2   0     2147483647   1/1        2      1      1      1Gi      cluster-2
`,
		},
		{
			name:          "cluster; namespace; no worker group",
			cluster:       "cluster-1",
			namespace:     "namespace-1",
			allNamespaces: false,
			// We filter RayClusters by name with a field selector which fake clients don't support.
			// So we need to pass only the RayCluster we expect to get.
			// See https://github.com/kubernetes/client-go/issues/326
			rayClusters: rayClusters[:1],
			pods:        pods,
			expected: `NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
group-1   1     5     1/1        2      1      1      1Gi      cluster-1
`,
		},
		{
			name:          "no cluster; no namespace; worker group",
			allNamespaces: true,
			workerGroup:   "group-1",
			rayClusters:   rayClusters,
			pods:          pods,
			expected: `NAMESPACE     NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
namespace-1   group-1   1     5     1/1        2      1      1      1Gi      cluster-1
namespace-2   group-1   1     4     1/2        4      2      2      2Gi      cluster-1
`,
		},
		{
			name:          "cluster; no namespace; worker group",
			cluster:       "cluster-1",
			allNamespaces: true,
			workerGroup:   "group-1",
			// We filter RayClusters by name with a field selector which fake clients don't support.
			// So we need to pass only the RayCluster we expect to get.
			// See https://github.com/kubernetes/client-go/issues/326
			rayClusters: []runtime.Object{rayClusters[0], rayClusters[2]},
			pods:        pods,
			expected: `NAMESPACE     NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
namespace-1   group-1   1     5     1/1        2      1      1      1Gi      cluster-1
namespace-2   group-1   1     4     1/2        4      2      2      2Gi      cluster-1
`,
		},
		{
			name:          "no cluster; namespace; worker group",
			namespace:     "namespace-1",
			allNamespaces: false,
			workerGroup:   "group-1",
			rayClusters:   rayClusters,
			pods:          pods,
			expected: `NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
group-1   1     5     1/1        2      1      1      1Gi      cluster-1
`,
		},
		{
			name:          "cluster; namespace; worker group",
			cluster:       "cluster-1",
			namespace:     "namespace-1",
			allNamespaces: false,
			workerGroup:   "group-1",
			// We filter RayClusters by name with a field selector which fake clients don't support.
			// So we need to pass only the RayCluster we expect to get.
			// See https://github.com/kubernetes/client-go/issues/326
			rayClusters: rayClusters[:1],
			pods:        pods,
			expected: `NAME      MIN   MAX   REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
group-1   1     5     1/1        2      1      1      1Gi      cluster-1
`,
		},
		{
			name:          "worker group set but no RayClusters returned",
			workerGroup:   "group-2",
			allNamespaces: true,
			rayClusters:   []runtime.Object{},
			pods:          []runtime.Object{},
			expectedError: "Ray worker group group-2 not found in any namespace in any Ray cluster",
		},
		{
			name:          "worker group set but no Pods returned",
			workerGroup:   "group-2",
			allNamespaces: true,
			pods:          []runtime.Object{},
			expectedError: "Ray worker group group-2 not found in any namespace in any Ray cluster",
		},
		{
			name:     "no worker group set and no Pods returned",
			pods:     []runtime.Object{},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, resBuf, _ := genericclioptions.NewTestIOStreams()

			fakeGetWorkerGroupsOptions := GetWorkerGroupsOptions{
				cmdFactory:    cmdFactory,
				ioStreams:     &testStreams,
				cluster:       tc.cluster,
				namespace:     tc.namespace,
				allNamespaces: tc.allNamespaces,
				workerGroup:   tc.workerGroup,
			}

			kubeClientSet := kubefake.NewClientset(tc.pods...)
			rayClient := clienttesting.NewRayClientset(tc.rayClusters...)
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			err := fakeGetWorkerGroupsOptions.Run(context.Background(), k8sClients)

			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
				return
			}

			assert.Equal(t, tc.expected, resBuf.String())
		})
	}
}

func TestErrorMessageForWorkerGroupNotFound(t *testing.T) {
	tests := []struct {
		name          string
		cluster       string
		namespace     string
		expected      string
		allNamespaces bool
	}{
		{
			name:          "neither cluster nor namespace are set",
			allNamespaces: true,
			expected:      "Ray worker group my-group not found in any namespace in any Ray cluster",
		},
		{
			name:          "cluster set, namespace not set",
			cluster:       "my-cluster",
			allNamespaces: true,
			expected:      "Ray worker group my-group not found in any namespace in Ray cluster my-cluster",
		},
		{
			name:          "cluster not set, namespace set",
			namespace:     "my-namespace",
			allNamespaces: false,
			expected:      "Ray worker group my-group not found in namespace my-namespace in any Ray cluster",
		},
		{
			name:          "both cluster and namespace are set",
			cluster:       "my-cluster",
			namespace:     "my-namespace",
			allNamespaces: false,
			expected:      "Ray worker group my-group not found in namespace my-namespace in Ray cluster my-cluster",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := errorMessageForWorkerGroupNotFound("my-group", tc.cluster, tc.namespace, tc.allNamespaces)
			assert.Equal(t, tc.expected, msg)
		})
	}
}

func TestGetWorkerGroupDetails(t *testing.T) {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("2"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
	}

	podTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Requests: resources,
						Limits:   resources,
					},
				},
			},
		},
	}

	readyStatus := corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		},
	}

	pods := []runtime.Object{
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-1",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeTypeLabelKey:  string(rayv1.HeadNode),
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-2",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-1",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Spec:   podTemplate.Spec,
			Status: readyStatus,
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-3",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-2",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeTypeLabelKey:  string(rayv1.HeadNode),
				},
			},
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-4",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-2",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-2",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Spec:   podTemplate.Spec,
			Status: readyStatus,
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-2",
				Name:      "pod-5",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-1",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
			Spec:   podTemplate.Spec,
			Status: readyStatus,
		},
	}

	tests := []struct {
		name                     string
		enrichedWorkerGroupSpecs []enrichedWorkerGroupSpec
		pods                     []runtime.Object
		listPodsError            string
		expectedError            string
		expectedWorkerGroups     []workerGroup
	}{
		{
			name: "should error if an input worker group spec is missing its group name",
			enrichedWorkerGroupSpecs: []enrichedWorkerGroupSpec{
				{
					namespace: "namespace-1",
					cluster:   "cluster-1",
					spec:      rayv1.WorkerGroupSpec{},
				},
			},
			expectedError: "could not create K8s label selectors for a worker group in cluster cluster-1, namespace namespace-1: group name cannot be empty",
		},
		{
			name: "should error if listing Pods fails",
			enrichedWorkerGroupSpecs: []enrichedWorkerGroupSpec{
				{
					namespace: "namespace-1",
					cluster:   "cluster-1",
					spec: rayv1.WorkerGroupSpec{
						GroupName: "group-1",
						Replicas:  new(int32(1)),
					},
				},
			},
			listPodsError: "DEADBEEF",
			expectedError: "DEADBEEF",
		},
		{
			name: "should return expected worker groups",
			enrichedWorkerGroupSpecs: []enrichedWorkerGroupSpec{
				{
					namespace: "namespace-1",
					cluster:   "cluster-1",
					spec: rayv1.WorkerGroupSpec{
						GroupName: "group-1",
						Replicas:  new(int32(1)),
						Template:  podTemplate,
					},
				},
				{
					namespace: "namespace-1",
					cluster:   "cluster-2",
					spec: rayv1.WorkerGroupSpec{
						GroupName: "group-2",
						Replicas:  new(int32(1)),
						Template:  podTemplate,
					},
				},
				{
					namespace: "namespace-2",
					cluster:   "cluster-1",
					spec: rayv1.WorkerGroupSpec{
						GroupName: "group-1",
						Replicas:  new(int32(1)),
						Template:  podTemplate,
					},
				},
			},
			pods: pods,
			expectedWorkerGroups: []workerGroup{
				{
					namespace:       "namespace-1",
					cluster:         "cluster-1",
					name:            "group-1",
					maxReplicas:     math.MaxInt32,
					readyReplicas:   1,
					desiredReplicas: 1,
					totalCPU:        *resources.Cpu(),
					totalGPU:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
					totalTPU:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
					totalMemory:     *resources.Memory(),
				},
				{
					namespace:       "namespace-1",
					cluster:         "cluster-2",
					name:            "group-2",
					maxReplicas:     math.MaxInt32,
					readyReplicas:   1,
					desiredReplicas: 1,
					totalCPU:        *resources.Cpu(),
					totalGPU:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
					totalTPU:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
					totalMemory:     *resources.Memory(),
				},
				{
					namespace:       "namespace-2",
					cluster:         "cluster-1",
					name:            "group-1",
					maxReplicas:     math.MaxInt32,
					readyReplicas:   1,
					desiredReplicas: 1,
					totalCPU:        *resources.Cpu(),
					totalGPU:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
					totalTPU:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
					totalMemory:     *resources.Memory(),
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeClientSet := kubefake.NewClientset(tc.pods...)

			if tc.listPodsError != "" {
				kubeClientSet.CoreV1().(*fakecorev1.FakeCoreV1).PrependReactor("list", "pods", func(_ kubetesting.Action) (handled bool, ret runtime.Object, err error) {
					return true, &corev1.PodList{}, errors.New(tc.listPodsError)
				})
			}

			rayClient := clienttesting.NewRayClientset()
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			workerGroups, err := getWorkerGroupDetails(context.Background(), tc.enrichedWorkerGroupSpecs, k8sClients)

			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
				return
			}

			assert.Equal(t, tc.expectedWorkerGroups, workerGroups)
		})
	}
}

func TestCreateRayWorkerGroupLabelSelectors(t *testing.T) {
	tests := []struct {
		expected      map[string]string
		name          string
		groupName     string
		cluster       string
		expectedError string
	}{
		{
			name:          "should error if group name isn't set",
			expectedError: "group name cannot be empty",
		},
		{
			name:      "should return the correct selectors if cluster isn't set",
			groupName: "my-group",
			cluster:   "",
			expected: map[string]string{
				util.RayNodeGroupLabelKey: "my-group",
				util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
			},
		},
		{
			name:      "should return label selector for node name",
			groupName: "my-group",
			cluster:   "my-cluster",
			expected: map[string]string{
				util.RayClusterLabelKey:   "my-cluster",
				util.RayNodeGroupLabelKey: "my-group",
				util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelSelector, err := createRayWorkerGroupLabelSelectors(tc.groupName, tc.cluster)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				assert.Equal(t, tc.expected, labelSelector)
			}
		})
	}
}

func TestPrintWorkerGroups(t *testing.T) {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("1"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
	}

	workerGroups := []workerGroup{
		{
			namespace:       "namespace-1",
			cluster:         "cluster-1",
			name:            "pod-1",
			readyReplicas:   1,
			desiredReplicas: 2,
			minReplicas:     0,
			maxReplicas:     math.MaxInt32,
			totalCPU:        *resources.Cpu(),
			totalGPU:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			totalTPU:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			totalMemory:     *resources.Memory(),
		},
		{
			namespace:       "namespace-2",
			cluster:         "cluster-2",
			name:            "pod-2",
			readyReplicas:   3,
			desiredReplicas: 3,
			minReplicas:     1,
			maxReplicas:     5,
			totalCPU:        *resources.Cpu(),
			totalGPU:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			totalTPU:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			totalMemory:     *resources.Memory(),
		},
	}

	tests := []struct {
		name          string
		expected      string
		allNamespaces bool
	}{
		{
			name:          "one namespace",
			allNamespaces: false,
			expected: `NAME    MIN   MAX          REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
pod-1   0     2147483647   1/2        1      1      1      1Gi      cluster-1
pod-2   1     5            3/3        1      1      1      1Gi      cluster-2
`,
		},
		{
			name:          "all namespaces",
			allNamespaces: true,
			expected: `NAMESPACE     NAME    MIN   MAX          REPLICAS   CPUS   GPUS   TPUS   MEMORY   CLUSTER
namespace-1   pod-1   0     2147483647   1/2        1      1      1      1Gi      cluster-1
namespace-2   pod-2   1     5            3/3        1      1      1      1Gi      cluster-2
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			err := printWorkerGroups(workerGroups, tc.allNamespaces, &output)
			require.NoError(t, err)

			assert.Equal(t, tc.expected, output.String())
		})
	}
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

	got := calculatePodResource(podSpec)

	assert.Equal(t, "1", got.Cpu().String())
	assert.Equal(t, "200Mi", got.Memory().String())
	assert.Equal(t, wantRequests, podSpec.Containers[0].Resources.Requests)
}
