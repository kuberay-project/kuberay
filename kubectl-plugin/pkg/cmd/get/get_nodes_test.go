package get

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestRayNodesGetComplete(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := []struct {
		opts              *GetNodesOptions
		name              string
		namespace         string
		expectedNamespace string
		expectedNode      string
		args              []string
	}{
		{
			name: "specifying all namespaces should set namespace to empty string",
			opts: &GetNodesOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: true,
			},
			expectedNamespace: "",
		},
		{
			name: "not specifying a namespace should set namespace to 'default'",
			opts: &GetNodesOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: false,
			},
			expectedNamespace: "default",
		},
		{
			name: "specifying a namespace should set that namespace",
			opts: &GetNodesOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: false,
			},
			namespace:         "some-namespace",
			expectedNamespace: "some-namespace",
		},
		{
			name: "specifying all namespaces takes precedence over specifying a namespace",
			opts: &GetNodesOptions{
				cmdFactory:    cmdFactory,
				allNamespaces: true,
			},
			expectedNamespace: "",
		},
		{
			name: "first positional argument should be set as the node name",
			opts: &GetNodesOptions{
				cmdFactory: cmdFactory,
			},
			args:              []string{"my-node", "other-arg"},
			expectedNamespace: "default",
			expectedNode:      "my-node",
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
			assert.Equal(t, tc.expectedNode, tc.opts.node)
		})
	}
}

func TestRayNodesGetRun(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("1"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
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
				CreationTimestamp: v1.NewTime(time.Now().Add(-1 * time.Hour)),
			},
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
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-2",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-2",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
				CreationTimestamp: v1.NewTime(time.Now().Add(-2 * time.Hour)),
			},
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
				CreationTimestamp: v1.NewTime(time.Now().Add(-3 * time.Hour)),
			},
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
		},
		&corev1.Pod{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-2",
				Name:      "pod-1",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayIsRayNodeLabelKey: "yes",
					util.RayNodeGroupLabelKey: "group-1",
					util.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
				CreationTimestamp: v1.NewTime(time.Now().Add(-4 * time.Hour)),
			},
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
		},
	}

	tests := []struct {
		name          string
		cluster       string
		namespace     string
		node          string
		expected      string
		expectedError string
		pods          []runtime.Object
		allNamespaces bool
	}{
		{
			name:          "no cluster; no namespace; no node",
			allNamespaces: true,
			pods:          pods,
			expected: `NAMESPACE     NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE     WORKER GROUP   AGE
namespace-1   pod-1   1      1      1      1Gi      cluster-1   head                    60m
namespace-1   pod-2   1      1      1      1Gi      cluster-1   worker   group-2        120m
namespace-1   pod-3   1      1      1      1Gi      cluster-2   head                    3h
namespace-2   pod-1   1      1      1      1Gi      cluster-1   worker   group-1        4h
`,
		},
		{
			name:          "cluster; no namespace; no node",
			cluster:       "cluster-2",
			allNamespaces: true,
			pods:          pods,
			expected: `NAMESPACE     NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE   WORKER GROUP   AGE
namespace-1   pod-3   1      1      1      1Gi      cluster-2   head                  3h
`,
		},
		{
			name:          "no cluster; namespace; no node",
			namespace:     "namespace-1",
			allNamespaces: false,
			pods:          pods,
			expected: `NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE     WORKER GROUP   AGE
pod-1   1      1      1      1Gi      cluster-1   head                    60m
pod-2   1      1      1      1Gi      cluster-1   worker   group-2        120m
pod-3   1      1      1      1Gi      cluster-2   head                    3h
`,
		},
		{
			name:          "cluster; namespace; no node",
			cluster:       "cluster-1",
			namespace:     "namespace-1",
			allNamespaces: false,
			pods:          pods,
			expected: `NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE     WORKER GROUP   AGE
pod-1   1      1      1      1Gi      cluster-1   head                    60m
pod-2   1      1      1      1Gi      cluster-1   worker   group-2        120m
`,
		},
		// We don't test for cases where the node is specified.
		// We filter Pods by name with a field selector which k8s.io/client-go/kubernetes/fake doesn't support.
		// See https://github.com/kubernetes/client-go/issues/326
		{
			name:          "node set but no Pods returned",
			node:          "pod-2",
			allNamespaces: true,
			pods:          []runtime.Object{},
			expectedError: "Ray node pod-2 not found in any namespace in any Ray cluster",
		},
		{
			name: "no node set and no Pods returned",
			pods: []runtime.Object{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, resBuf, _ := genericclioptions.NewTestIOStreams()

			fakeGetNodesOptions := GetNodesOptions{
				cmdFactory:    cmdFactory,
				ioStreams:     &testStreams,
				cluster:       tc.cluster,
				namespace:     tc.namespace,
				allNamespaces: tc.allNamespaces,
				node:          tc.node,
			}

			kubeClientSet := kubefake.NewClientset(tc.pods...)
			rayClient := clienttesting.NewRayClientset()
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			err := fakeGetNodesOptions.Run(context.Background(), k8sClients)

			if tc.expectedError != "" {
				assert.EqualError(t, err, tc.expectedError)
				return
			}

			assert.Equal(t, tc.expected, resBuf.String())
		})
	}
}

func TestCreateRayNodeLabelSelectors(t *testing.T) {
	tests := []struct {
		expected map[string]string
		name     string
		cluster  string
	}{
		{
			name:    "should return the correct selectors if cluster isn't set",
			cluster: "",
			expected: map[string]string{
				util.RayIsRayNodeLabelKey: "yes",
			},
		},
		{
			name:    "should return label selector for node name",
			cluster: "my-cluster",
			expected: map[string]string{
				util.RayIsRayNodeLabelKey: "yes",
				util.RayClusterLabelKey:   "my-cluster",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			labelSelector := createRayNodeLabelSelectors(tc.cluster)
			assert.Equal(t, tc.expected, labelSelector)
		})
	}
}

func TestErrorMessageForNodeNotFound(t *testing.T) {
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
			expected:      "Ray node my-node not found in any namespace in any Ray cluster",
		},
		{
			name:          "cluster set, namespace not set",
			cluster:       "my-cluster",
			allNamespaces: true,
			expected:      "Ray node my-node not found in any namespace in Ray cluster my-cluster",
		},
		{
			name:          "cluster not set, namespace set",
			namespace:     "my-namespace",
			allNamespaces: false,
			expected:      "Ray node my-node not found in namespace my-namespace in any Ray cluster",
		},
		{
			name:          "both cluster and namespace are set",
			cluster:       "my-cluster",
			namespace:     "my-namespace",
			allNamespaces: false,
			expected:      "Ray node my-node not found in namespace my-namespace in Ray cluster my-cluster",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := errorMessageForNodeNotFound("my-node", tc.cluster, tc.namespace, tc.allNamespaces)
			assert.Equal(t, tc.expected, msg)
		})
	}
}

func TestPodsToNodes(t *testing.T) {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("1"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
	}

	pods := []corev1.Pod{
		{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-1",
				Name:      "pod-1",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-1",
					util.RayNodeGroupLabelKey: "group-1",
				},
			},
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
		},
		{
			ObjectMeta: v1.ObjectMeta{
				Namespace: "namespace-2",
				Name:      "pod-2",
				Labels: map[string]string{
					util.RayClusterLabelKey:   "cluster-2",
					util.RayNodeGroupLabelKey: "group-2",
				},
			},
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
		},
	}

	expectedNodes := []node{
		{
			namespace:   "namespace-1",
			cluster:     "cluster-1",
			workerGroup: "group-1",
			name:        "pod-1",
			cpus:        *resources.Cpu(),
			gpus:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			tpus:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			memory:      *resources.Memory(),
		},
		{
			namespace:   "namespace-2",
			cluster:     "cluster-2",
			workerGroup: "group-2",
			name:        "pod-2",
			cpus:        *resources.Cpu(),
			gpus:        *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			tpus:        *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			memory:      *resources.Memory(),
		},
	}

	assert.Equal(t, expectedNodes, podsToNodes(pods))
}

func TestPrintNodes(t *testing.T) {
	resources := corev1.ResourceList{
		corev1.ResourceCPU:     resource.MustParse("1"),
		corev1.ResourceMemory:  resource.MustParse("1Gi"),
		util.ResourceNvidiaGPU: resource.MustParse("1"),
		util.ResourceGoogleTPU: resource.MustParse("1"),
	}

	nodes := []node{
		{
			namespace:         "namespace-1",
			cluster:           "cluster-1",
			_type:             string(rayv1.HeadNode),
			name:              "pod-1",
			cpus:              *resources.Cpu(),
			gpus:              *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			tpus:              *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			memory:            *resources.Memory(),
			creationTimestamp: v1.NewTime(time.Now().Add(-1 * time.Hour)),
		},
		{
			namespace:         "namespace-2",
			cluster:           "cluster-2",
			_type:             string(rayv1.WorkerNode),
			workerGroup:       "group-2",
			name:              "pod-2",
			cpus:              *resources.Cpu(),
			gpus:              *resources.Name(util.ResourceNvidiaGPU, resource.DecimalSI),
			tpus:              *resources.Name(util.ResourceGoogleTPU, resource.DecimalSI),
			memory:            *resources.Memory(),
			creationTimestamp: v1.NewTime(time.Now().Add(-12 * time.Hour)),
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
			expected: `NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE     WORKER GROUP   AGE
pod-1   1      1      1      1Gi      cluster-1   head                    60m
pod-2   1      1      1      1Gi      cluster-2   worker   group-2        12h
`,
		},
		{
			name:          "all namespaces",
			allNamespaces: true,
			expected: `NAMESPACE     NAME    CPUS   GPUS   TPUS   MEMORY   CLUSTER     TYPE     WORKER GROUP   AGE
namespace-1   pod-1   1      1      1      1Gi      cluster-1   head                    60m
namespace-2   pod-2   1      1      1      1Gi      cluster-2   worker   group-2        12h
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			err := printNodes(nodes, tc.allNamespaces, &output)
			require.NoError(t, err)

			assert.Equal(t, tc.expected, output.String())
		})
	}
}
