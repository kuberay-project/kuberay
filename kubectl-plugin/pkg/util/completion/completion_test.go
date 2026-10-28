package completion

import (
	"sort"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

func TestRayResourceTypeCompletionFunc(t *testing.T) {
	compFunc := RayResourceTypeCompletionFunc()
	comps, directive := compFunc(nil, []string{}, "")
	checkCompletion(t, comps, []string{"raycluster", "rayjob", "rayservice"}, directive, cobra.ShellCompDirectiveNoFileComp)
}

func checkCompletion(t *testing.T, comps, expectedComps []string, directive, expectedDirective cobra.ShellCompDirective) {
	if e, d := expectedDirective, directive; e != d {
		t.Errorf("expected directive\n%v\nbut got\n%v", e, d)
	}

	sort.Strings(comps)
	sort.Strings(expectedComps)

	require.ElementsMatch(t, expectedComps, comps)
}

func TestWorkerGroupCompletionFunc(t *testing.T) {
	tests := []struct {
		name              string
		namespace         string
		clusterName       string
		toComplete        string
		args              []string
		rayClusters       []runtime.Object
		expectedComps     []string
		expectedDirective cobra.ShellCompDirective
		allNamespaces     bool
	}{
		{
			name:          "should return workergroup names in default namespace when no flag is set",
			namespace:     "",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			toComplete:    "",
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
							{
								GroupName: "group-4",
							},
						},
					},
				},
			},
			expectedComps:     []string{"group-1", "group-4"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return empty when all-namespaces flag is set",
			namespace:     "",
			allNamespaces: true,
			clusterName:   "",
			args:          []string{},
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
						},
					},
				},
			},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return workergroups in specified namespace",
			namespace:     "my-namespace",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "my-namespace",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
							{
								GroupName: "group-2",
							},
						},
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-2",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-3",
							},
						},
					},
				},
			},
			expectedComps:     []string{"group-1", "group-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should filter by ray-cluster flag",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "cluster-1",
			args:          []string{},
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
							{
								GroupName: "group-2",
							},
						},
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-2",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-3",
							},
						},
					},
				},
			},
			expectedComps:     []string{"group-1", "group-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should handle prefix matching",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			toComplete:    "small",
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "small-workers",
							},
							{
								GroupName: "large-workers",
							},
							{
								GroupName: "small-gpu",
							},
						},
					},
				},
			},
			expectedComps:     []string{"small-workers", "small-gpu"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return empty after first positional argument",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{"group-1"}, // Already has 1 arg
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
						},
					},
				},
			},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should aggregate workergroups from multiple clusters in same namespace",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
						},
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-2",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-2",
							},
						},
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-3",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-3",
							},
						},
					},
				},
			},
			expectedComps:     []string{"group-1", "group-2", "group-3"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:              "should return empty when no clusters exist",
			namespace:         "default",
			allNamespaces:     false,
			clusterName:       "",
			args:              []string{},
			rayClusters:       []runtime.Object{},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should handle duplicate workergroup names across clusters",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			rayClusters: []runtime.Object{
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-1",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
						},
					},
				},
				&rayv1.RayCluster{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "cluster-2",
					},
					Spec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "group-1",
							},
						},
					},
				},
			},
			expectedComps:     []string{"group-1"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := genericclioptions.NewConfigFlags(true)
			cmdFactory := cmdutil.NewFactory(configFlags)
			kubeClientSet := kubefake.NewClientset()
			rayClient := clienttesting.NewRayClientset(tc.rayClusters...)
			k8sClient := client.NewClientForTesting(kubeClientSet, rayClient)

			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			cmd.Flags().String("ray-cluster", tc.clusterName, "")
			cmd.Flags().Bool("all-namespaces", tc.allNamespaces, "")

			comps, directive := workerGroupCompletionFunc(cmd, tc.args, tc.toComplete, k8sClient, cmdFactory)
			checkCompletion(t, comps, tc.expectedComps, directive, cobra.ShellCompDirectiveNoFileComp)
		})
	}
}

func TestNodeCompletionFunc(t *testing.T) {
	tests := []struct {
		name              string
		namespace         string
		clusterName       string
		toComplete        string
		args              []string
		pods              []runtime.Object
		expectedComps     []string
		expectedDirective cobra.ShellCompDirective
		allNamespaces     bool
	}{
		{
			name:          "should return node names in default namespace when no flag is set",
			namespace:     "",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			toComplete:    "",
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-2",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{"pod-1", "pod-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return empty when --all-namespaces flag is set",
			namespace:     "",
			allNamespaces: true,
			clusterName:   "",
			args:          []string{},
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return nodes in specified namespace",
			namespace:     "my-namespace",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "my-namespace",
						Name:      "pod-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "my-namespace",
						Name:      "pod-2",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-3",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-2",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{"pod-1", "pod-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should filter by --ray-cluster flag",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "cluster-1",
			args:          []string{},
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-2",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-3",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-2",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{"pod-1", "pod-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should handle prefix matching",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			toComplete:    "raycluster-worker",
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "raycluster-head-abc",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "raycluster-worker-xyz",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "raycluster-worker-ijk",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{"raycluster-worker-xyz", "raycluster-worker-ijk"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should return empty after first positional argument",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{"pod-1"},
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "pod-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:          "should only return pods with Ray labels",
			namespace:     "default",
			allNamespaces: false,
			clusterName:   "",
			args:          []string{},
			pods: []runtime.Object{
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "ray-node-1",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "non-ray-pod", // without ray label
					},
				},
				&corev1.Pod{
					ObjectMeta: v1.ObjectMeta{
						Namespace: "default",
						Name:      "ray-node-2",
						Labels: map[string]string{
							util.RayClusterLabelKey:   "cluster-1",
							util.RayIsRayNodeLabelKey: "yes",
						},
					},
				},
			},
			expectedComps:     []string{"ray-node-1", "ray-node-2"},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
		{
			name:              "should return empty when no pods exist",
			namespace:         "default",
			allNamespaces:     false,
			clusterName:       "",
			args:              []string{},
			pods:              []runtime.Object{},
			expectedComps:     []string{},
			expectedDirective: cobra.ShellCompDirectiveNoFileComp,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := genericclioptions.NewConfigFlags(true)
			cmdFactory := cmdutil.NewFactory(configFlags)
			kubeClientSet := kubefake.NewClientset(tc.pods...)
			rayClient := rayClientFake.NewClientset()
			k8sClient := client.NewClientForTesting(kubeClientSet, rayClient)

			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			cmd.Flags().String("ray-cluster", tc.clusterName, "")
			cmd.Flags().Bool("all-namespaces", tc.allNamespaces, "")

			comps, directive := nodeCompletionFunc(cmd, tc.args, tc.toComplete, k8sClient, cmdFactory)
			checkCompletion(t, comps, tc.expectedComps, directive, cobra.ShellCompDirectiveNoFileComp)
		})
	}
}
