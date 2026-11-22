package get

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func createTempKubeConfigFile(t *testing.T, currentNamespace string) (string, error) {
	tmpDir := t.TempDir()

	// Set up fake config for kubeconfig
	config := &api.Config{
		Clusters: map[string]*api.Cluster{
			"test-cluster": {
				Server:                "https://fake-kubernetes-cluster.example.com",
				InsecureSkipTLSVerify: true, // For testing purposes
			},
		},
		Contexts: map[string]*api.Context{
			"test-context": {
				Cluster:   "test-cluster",
				AuthInfo:  "my-fake-user",
				Namespace: currentNamespace,
			},
		},
		CurrentContext: "test-context",
		AuthInfos: map[string]*api.AuthInfo{
			"my-fake-user": {
				Token: "", // Empty for testing without authentication
			},
		},
	}

	fakeFile := filepath.Join(tmpDir, ".kubeconfig")

	return fakeFile, clientcmd.WriteToFile(*config, fakeFile)
}

// This is to test Complete() and ensure that it is setting the namespace and cluster correctly
// No validation test is done here
func TestRayClusterGetComplete(t *testing.T) {
	kubeConfigWithCurrentContext, err := createTempKubeConfigFile(t, "test-namespace")
	require.NoError(t, err)
	// Initialize members of the cluster get option struct and the struct itself
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	tests := []struct {
		name                  string
		namespace             string
		expectedCluster       string
		expectedNamespace     string
		args                  []string
		expectedAllNamespaces bool
	}{
		{
			name:                  "neither namespace nor args set",
			namespace:             "",
			args:                  []string{},
			expectedAllNamespaces: false,
			expectedCluster:       "",
			expectedNamespace:     "test-namespace",
		},
		{
			name:                  "namespace set, args not set",
			namespace:             "foo",
			args:                  []string{},
			expectedAllNamespaces: false,
			expectedCluster:       "",
			expectedNamespace:     "foo",
		},
		{
			name:                  "namespace not set, args set",
			namespace:             "",
			args:                  []string{"foo", "bar"},
			expectedAllNamespaces: false,
			expectedCluster:       "foo",
			expectedNamespace:     "test-namespace",
		},
		{
			name:                  "both namespace and args set",
			namespace:             "foo",
			args:                  []string{"bar", "qux"},
			expectedAllNamespaces: false,
			expectedCluster:       "bar",
			expectedNamespace:     "foo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := &genericclioptions.ConfigFlags{KubeConfig: &kubeConfigWithCurrentContext}
			cmdFactory := cmdutil.NewFactory(configFlags)
			fakeClusterGetOptions := NewGetClusterOptions(cmdFactory, testStreams)

			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())
			err := fakeClusterGetOptions.Complete(tc.args)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedAllNamespaces, fakeClusterGetOptions.allNamespaces)
			assert.Equal(t, tc.expectedCluster, fakeClusterGetOptions.cluster)
			assert.Equal(t, tc.expectedNamespace, fakeClusterGetOptions.namespace)
		})
	}
}

// Tests the Run() step of the command and ensure that the output is as expected.
func TestRayClusterGetRun(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := []struct {
		name                  string
		state                 rayv1.ClusterState
		expectedConditionType string
		expectedState         string
		conditions            []v1.Condition
	}{
		{
			name:                  "RayCluster with neither Conditions nor State should show nothing for those columns",
			expectedConditionType: "",
			expectedState:         "",
		},
		{
			name: "RayCluster with both Conditions and State should show the right values for those columns",
			conditions: []v1.Condition{
				{
					Type:    string(rayv1.RayClusterReplicaFailure),
					Status:  v1.ConditionFalse,
					Reason:  rayv1.HeadPodNotFound,
					Message: "Head Pod not found",
					LastTransitionTime: v1.Time{
						Time: time.Date(2024, 7, 21, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					Type:    string(rayv1.RayClusterProvisioned),
					Status:  v1.ConditionTrue,
					Reason:  rayv1.AllPodRunningAndReadyFirstTime,
					Message: "All Ray Pods are ready for the first time",
					LastTransitionTime: v1.Time{
						Time: time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC),
					},
				},
				{
					Type:    string(rayv1.HeadPodReady),
					Status:  v1.ConditionTrue,
					Reason:  rayv1.HeadPodRunningAndReady,
					Message: "Head Pod ready",
					LastTransitionTime: v1.Time{
						Time: time.Date(2024, 11, 5, 0, 0, 0, 0, time.UTC),
					},
				},
			},
			state:                 rayv1.Ready,
			expectedState:         string(rayv1.Ready),
			expectedConditionType: string(rayv1.RayClusterProvisioned),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, resBuf, _ := genericclioptions.NewTestIOStreams()
			fakeClusterGetOptions := NewGetClusterOptions(cmdFactory, testStreams)

			rayCluster := &rayv1.RayCluster{
				ObjectMeta: v1.ObjectMeta{
					Name:      "raycluster-kuberay",
					Namespace: "test",
				},
				Status: rayv1.RayClusterStatus{
					DesiredWorkerReplicas:   2,
					AvailableWorkerReplicas: 2,
					DesiredCPU:              resource.MustParse("6"),
					DesiredGPU:              resource.MustParse("1"),
					DesiredTPU:              resource.MustParse("1"),
					DesiredMemory:           resource.MustParse("24Gi"),
					Conditions:              tc.conditions,
					State:                   tc.state,
				},
			}

			kubeClientSet := kubefake.NewClientset()
			rayClient := clienttesting.NewRayClientset(rayCluster)
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			// Initialize the printer with an empty print options since we are setting the column definition later
			expectedTestResultTable := printers.NewTablePrinter(printers.PrintOptions{})

			// Define the column names and types
			testResTable := &v1.Table{
				ColumnDefinitions: []v1.TableColumnDefinition{
					{Name: "Name", Type: "string"},
					{Name: "Namespace", Type: "string"},
					{Name: "Desired Workers", Type: "string"},
					{Name: "Available Workers", Type: "string"},
					{Name: "CPUs", Type: "string"},
					{Name: "GPUs", Type: "string"},
					{Name: "TPUs", Type: "string"},
					{Name: "Memory", Type: "string"},
					{Name: "Condition", Type: "string"},
					{Name: "Status", Type: "string"},
					{Name: "Age", Type: "string"},
				},
			}

			testResTable.Rows = append(testResTable.Rows, v1.TableRow{
				Cells: []any{
					"raycluster-kuberay",
					"test",
					"2",
					"2",
					"6",
					"1",
					"1",
					"24Gi",
					tc.expectedConditionType,
					tc.expectedState,
					"<unknown>",
				},
			})

			// Result buffer for the expected table result
			var resbuffer bytes.Buffer
			err := expectedTestResultTable.PrintObj(testResTable, &resbuffer)
			require.NoError(t, err)

			err = fakeClusterGetOptions.Run(context.Background(), k8sClients)
			require.NoError(t, err)

			assert.Equal(t, resBuf.String(), resbuffer.String())
		})
	}
}

func TestGetRayClusters(t *testing.T) {
	testStreams := genericiooptions.NewTestIOStreamsDiscard()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	namespace := "my-namespace"
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: v1.ObjectMeta{
			Name:      "raycluster-kuberay",
			Namespace: namespace,
		},
	}

	defaultNamespace := "default"
	rayClusterInDefault := &rayv1.RayCluster{
		ObjectMeta: v1.ObjectMeta{
			Name:      "raycluster-kuberay-default",
			Namespace: defaultNamespace,
		},
	}

	tests := []struct {
		namespace           *string
		name                string
		expectedError       string
		expectedOutput      string
		cluster             string
		expectedRayClusters []runtime.Object
		allFakeRayClusters  []runtime.Object
		allNamespaces       bool
	}{
		{
			name:                "should not error if no cluster name is provided, searching all namespaces, and no clusters are found",
			namespace:           nil,
			expectedRayClusters: []runtime.Object{},
		},
		{
			name:                "should error if a cluster name is provided, searching all namespaces, and no clusters are found",
			cluster:             "my-cluster",
			namespace:           nil,
			expectedRayClusters: []runtime.Object{},
			expectedError:       "Ray cluster my-cluster not found",
		},
		{
			name:                "should not error if no cluster name is provided, searching one namespace, and no clusters are found",
			namespace:           &namespace,
			expectedRayClusters: []runtime.Object{},
		},
		{
			name:                "should error if a cluster name is provided, searching one namespace, and no clusters are found",
			cluster:             "my-cluster",
			namespace:           &namespace,
			expectedRayClusters: []runtime.Object{},
			expectedError:       fmt.Sprintf("Ray cluster my-cluster not found in namespace %s", namespace),
		},
		{
			name:                "should not error if no cluster name is provided, searching all namespaces, and clusters are found",
			namespace:           nil,
			allFakeRayClusters:  []runtime.Object{rayCluster},
			expectedRayClusters: []runtime.Object{rayCluster},
		},
		{
			name:                "should not error if a cluster name is provided, searching all namespaces, and clusters are found",
			cluster:             "raycluster-kuberay",
			namespace:           nil,
			allFakeRayClusters:  []runtime.Object{rayCluster},
			expectedRayClusters: []runtime.Object{rayCluster},
		},
		{
			name:                "should not error if no cluster name is provided, searching one namespace, and clusters are found",
			namespace:           &namespace,
			allFakeRayClusters:  []runtime.Object{rayCluster},
			expectedRayClusters: []runtime.Object{rayCluster},
		},
		{
			name:                "should not error if a cluster name is provided, searching one namespace, and clusters are found",
			cluster:             "raycluster-kuberay",
			namespace:           &namespace,
			allFakeRayClusters:  []runtime.Object{rayCluster},
			expectedRayClusters: []runtime.Object{rayCluster},
		},
		{
			name:                "should not error if no cluster name is provided, searching default namespace, and clusters are found",
			namespace:           &defaultNamespace,
			allFakeRayClusters:  []runtime.Object{rayCluster, rayClusterInDefault},
			expectedRayClusters: []runtime.Object{rayClusterInDefault},
		},
		{
			name:                "should not error if no cluster name is provided, searching all namespace, and clusters are found",
			allNamespaces:       true,
			allFakeRayClusters:  []runtime.Object{rayCluster, rayClusterInDefault},
			expectedRayClusters: []runtime.Object{rayCluster, rayClusterInDefault},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClusterGetOptions := GetClusterOptions{
				cmdFactory:    cmdFactory,
				ioStreams:     &testStreams,
				cluster:       tc.cluster,
				allNamespaces: tc.allNamespaces,
			}
			if tc.namespace != nil {
				fakeClusterGetOptions.namespace = *tc.namespace
			}

			kubeClientSet := kubefake.NewClientset()
			rayClient := clienttesting.NewRayClientset(tc.allFakeRayClusters...)
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			rayClusters, err := getRayClusters(context.Background(), &fakeClusterGetOptions, k8sClients)

			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			assert.Len(t, rayClusters.Items, len(tc.expectedRayClusters))
		})
	}
}

func TestPrintClusters(t *testing.T) {
	rayClusterList := &rayv1.RayClusterList{
		Items: []rayv1.RayCluster{
			{
				ObjectMeta: v1.ObjectMeta{
					Name:      "barista",
					Namespace: "cafe",
					CreationTimestamp: v1.Time{
						Time: time.Now().Add(-24 * time.Hour),
					},
				},
				Status: rayv1.RayClusterStatus{
					DesiredWorkerReplicas:   2,
					AvailableWorkerReplicas: 2,
					DesiredCPU:              resource.MustParse("6"),
					DesiredGPU:              resource.MustParse("1"),
					DesiredTPU:              resource.MustParse("1"),
					DesiredMemory:           resource.MustParse("24Gi"),
					State:                   rayv1.Ready,
				},
			},
			{
				ObjectMeta: v1.ObjectMeta{
					Name:      "bartender",
					Namespace: "speakeasy",
				},
				Status: rayv1.RayClusterStatus{
					DesiredWorkerReplicas:   3,
					AvailableWorkerReplicas: 4,
					DesiredCPU:              resource.MustParse("8"),
					DesiredGPU:              resource.MustParse("2"),
					DesiredTPU:              resource.MustParse("0"),
					DesiredMemory:           resource.MustParse("12Gi"),
					Conditions: []v1.Condition{
						{
							Type:   string(rayv1.RayClusterSuspending),
							Status: v1.ConditionTrue,
							LastTransitionTime: v1.Time{
								Time: time.Now().Add(-2 * time.Hour),
							},
						},
						{
							Type:   string(rayv1.HeadPodReady),
							Status: v1.ConditionTrue,
							LastTransitionTime: v1.Time{
								Time: time.Now().Add(-4 * time.Hour),
							},
						},
					},
					State: rayv1.Suspended,
				},
			},
		},
	}

	// Initialize the printer with an empty print options since we are setting the column definition later
	expectedTestResultTable := printers.NewTablePrinter(printers.PrintOptions{})

	// Define the column names and types
	testResTable := &v1.Table{
		ColumnDefinitions: []v1.TableColumnDefinition{
			{Name: "Name", Type: "string"},
			{Name: "Namespace", Type: "string"},
			{Name: "Desired Workers", Type: "string"},
			{Name: "Available Workers", Type: "string"},
			{Name: "CPUs", Type: "string"},
			{Name: "GPUs", Type: "string"},
			{Name: "TPUs", Type: "string"},
			{Name: "Memory", Type: "string"},
			{Name: "Condition", Type: "string"},
			{Name: "Status", Type: "string"},
			{Name: "Age", Type: "string"},
		},
	}

	testResTable.Rows = append(testResTable.Rows,
		v1.TableRow{
			Cells: []any{
				"barista",
				"cafe",
				"2",
				"2",
				"6",
				"1",
				"1",
				"24Gi",
				"",
				rayv1.Ready,
				"24h",
			},
		},
		v1.TableRow{
			Cells: []any{
				"bartender",
				"speakeasy",
				"3",
				"4",
				"8",
				"2",
				"0",
				"12Gi",
				rayv1.HeadPodReady,
				rayv1.Suspended,
				"<unknown>",
			},
		},
	)

	// Result buffer for the expected table result
	var expectedOutput bytes.Buffer
	err := expectedTestResultTable.PrintObj(testResTable, &expectedOutput)
	require.NoError(t, err)

	var output bytes.Buffer
	err = printClusters(rayClusterList, &output)
	require.NoError(t, err)

	assert.Equal(t, expectedOutput.String(), output.String())
}
