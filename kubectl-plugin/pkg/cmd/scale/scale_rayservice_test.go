package scale

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

const (
	testServiceVersion = "999"
	testClusterVersion = "999"
)

func newTestRayService(namespace, name, workerGroup string, replicas, minReplicas, maxReplicas *int32) *rayv1.RayService {
	return &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: testServiceVersion,
		},
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName:   workerGroup,
						Replicas:    replicas,
						MinReplicas: minReplicas,
						MaxReplicas: maxReplicas,
					},
				},
			},
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: name + "-cluster",
			},
		},
	}
}

func newTestRayCluster(namespace, name, workerGroup string, replicas, minReplicas, maxReplicas *int32) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: testClusterVersion,
		},
		Spec: rayv1.RayClusterSpec{
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   workerGroup,
					Replicas:    replicas,
					MinReplicas: minReplicas,
					MaxReplicas: maxReplicas,
				},
			},
		},
	}
}

func TestRayScaleRayServiceComplete(t *testing.T) {
	tests := []struct {
		name              string
		namespace         string
		expectedNamespace string
		args              []string
	}{
		{
			name:              "namespace should be set to 'default' if not specified",
			args:              []string{"my-service"},
			expectedNamespace: "default",
		},
		{
			name:              "namespace and service should be set correctly",
			args:              []string{"my-service"},
			namespace:         "DEADBEEF",
			expectedNamespace: "DEADBEEF",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
			configFlags := genericclioptions.NewConfigFlags(true)
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			cmdFactory := cmdutil.NewFactory(configFlags)

			fakeScaleRayServiceOptions := NewScaleRayServiceOptions(cmdFactory, testStreams)

			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())
			err := fakeScaleRayServiceOptions.Complete(tc.args)

			require.NoError(t, err)
			assert.Equal(t, tc.expectedNamespace, fakeScaleRayServiceOptions.namespace)
			assert.Equal(t, tc.args[0], fakeScaleRayServiceOptions.service)
		})
	}
}

func TestRayScaleRayServiceValidate(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := []struct {
		name        string
		opts        *ScaleRayServiceOptions
		expectError string
	}{
		{
			name: "should error when no worker group is set",
			opts: &ScaleRayServiceOptions{
				cmdFactory: cmdFactory,
			},
			expectError: "must specify -w/--worker-group",
		},
		{
			name: "should error when no parameters are set",
			opts: &ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				workerGroup: "test-worker-group",
			},
			expectError: "must specify at least one of --min-replicas or --max-replicas (non-negative integers)",
		},
		{
			name: "should error when min-replicas is negative",
			opts: &ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				workerGroup: "test-worker-group",
				minReplicas: new(int32(-2)),
			},
			expectError: "--min-replicas must be a non-negative integer",
		},
		{
			name: "should error when max-replicas is negative",
			opts: &ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				workerGroup: "test-worker-group",
				maxReplicas: new(int32(-2)),
			},
			expectError: "--max-replicas must be a non-negative integer",
		},
		{
			name: "should error when min-replicas is greater than max-replicas",
			opts: &ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				workerGroup: "test-worker-group",
				minReplicas: new(int32(5)),
				maxReplicas: new(int32(3)),
			},
			expectError: fmt.Sprintf("--min-replicas (%d) cannot be greater than --max-replicas (%d)", 5, 3),
		},
		{
			name: "successful validation call",
			opts: &ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				workerGroup: "test-worker-group",
				minReplicas: new(int32(2)),
				maxReplicas: new(int32(6)),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if tc.expectError != "" {
				assert.EqualError(t, err, tc.expectError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRayScaleRayServiceRun(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	testNamespace, workerGroup, service, cluster := "test-context", "worker-group-1", "my-service", "my-service-cluster"

	tests := []struct {
		name           string
		minReplicas    *int32
		maxReplicas    *int32
		rayService     *rayv1.RayService
		rayCluster     *rayv1.RayCluster
		expectedOutput []string
		expectedError  string
	}{
		{
			name:          "should error when the Ray service doesn't exist",
			rayService:    nil,
			expectedError: "failed to scale worker group",
		},
		{
			name:        "should error when an incremental upgrade is in progress",
			minReplicas: new(int32(2)),
			rayService: func() *rayv1.RayService {
				rayService := newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10)))
				rayService.Status.PendingServiceStatus = rayv1.RayServiceStatus{
					RayClusterName: cluster + "-pending",
				}
				return rayService
			}(),
			rayCluster:    newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedError: "an incremental upgrade is in progress",
		},
		{
			name:          "should error when the worker group doesn't exist in the Ray service",
			minReplicas:   new(int32(2)),
			rayService:    newTestRayService(testNamespace, service, "another-group", new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:    newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedError: fmt.Sprintf("worker group %s not found in Ray service %s in namespace %s", workerGroup, service, testNamespace),
		},
		{
			name:        "should error when the Ray service has no active Ray cluster",
			minReplicas: new(int32(2)),
			rayService: func() *rayv1.RayService {
				rayService := newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10)))
				rayService.Status.ActiveServiceStatus.RayClusterName = ""
				return rayService
			}(),
			rayCluster:    newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedError: "has no active Ray cluster yet",
		},
		{
			name:          "should error when the live Ray cluster doesn't exist",
			minReplicas:   new(int32(2)),
			rayService:    newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedError: "failed to get the live Ray cluster",
		},
		{
			name:          "should error when the worker group doesn't exist in the live Ray cluster",
			minReplicas:   new(int32(2)),
			rayService:    newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:    newTestRayCluster(testNamespace, cluster, "another-group", new(int32(5)), new(int32(1)), new(int32(10))),
			expectedError: fmt.Sprintf("worker group %s not found in the live Ray cluster %s in namespace %s", workerGroup, cluster, testNamespace),
		},
		{
			name:          "should error when the requested min-replicas exceeds the live cluster's current max",
			minReplicas:   new(int32(6)),
			rayService:    newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:    newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(5))),
			expectedError: fmt.Sprintf("cannot set --min-replicas (%d) greater than --max-replicas (%d)", 6, 5),
		},
		{
			name:        "should not do anything when the bounds already match",
			minReplicas: new(int32(1)),
			maxReplicas: new(int32(10)),
			rayService:  newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:  newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedOutput: []string{
				fmt.Sprintf("Worker group %s in Ray service %s in namespace %s already matches the requested configuration. Skipping.", workerGroup, service, testNamespace),
			},
		},
		{
			name:        "should successfully update only minReplicas on both objects",
			minReplicas: new(int32(3)),
			rayService:  newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:  newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedOutput: []string{
				fmt.Sprintf("Updated worker group %s in Ray service %s in namespace %s (Scaled minReplicas: 1 to 3)", workerGroup, service, testNamespace),
				fmt.Sprintf("Updated worker group %s in the live Ray cluster %s in namespace %s (Scaled minReplicas: 1 to 3)", workerGroup, cluster, testNamespace),
			},
		},
		{
			name:        "should successfully update only maxReplicas on both objects",
			maxReplicas: new(int32(20)),
			rayService:  newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:  newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedOutput: []string{
				fmt.Sprintf("Updated worker group %s in Ray service %s in namespace %s (Scaled maxReplicas: 10 to 20)", workerGroup, service, testNamespace),
				fmt.Sprintf("Updated worker group %s in the live Ray cluster %s in namespace %s (Scaled maxReplicas: 10 to 20)", workerGroup, cluster, testNamespace),
			},
		},
		{
			name:        "should successfully update both minReplicas and maxReplicas on both objects",
			minReplicas: new(int32(3)),
			maxReplicas: new(int32(8)),
			rayService:  newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			rayCluster:  newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedOutput: []string{
				fmt.Sprintf("Updated worker group %s in Ray service %s in namespace %s (Scaled minReplicas: 1 to 3, Scaled maxReplicas: 10 to 8)", workerGroup, service, testNamespace),
				fmt.Sprintf("Updated worker group %s in the live Ray cluster %s in namespace %s (Scaled minReplicas: 1 to 3, Scaled maxReplicas: 10 to 8)", workerGroup, cluster, testNamespace),
			},
		},
		{
			name:        "should skip the Ray service when its bounds already match and update only the live cluster",
			minReplicas: new(int32(3)),
			rayService:  newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(3)), new(int32(10))),
			rayCluster:  newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10))),
			expectedOutput: []string{
				fmt.Sprintf("Worker group %s in Ray service %s in namespace %s already matches the requested configuration. Skipping.", workerGroup, service, testNamespace),
				fmt.Sprintf("Updated worker group %s in the live Ray cluster %s in namespace %s (Scaled minReplicas: 1 to 3)", workerGroup, cluster, testNamespace),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeScaleRayServiceOptions := ScaleRayServiceOptions{
				cmdFactory:  cmdFactory,
				ioStreams:   &testStreams,
				namespace:   testNamespace,
				service:     service,
				minReplicas: tc.minReplicas,
				maxReplicas: tc.maxReplicas,
				workerGroup: workerGroup,
			}

			kubeClientSet := kubefake.NewClientset()
			var rayObjects []runtime.Object
			if tc.rayService != nil {
				rayObjects = append(rayObjects, tc.rayService)
			}
			if tc.rayCluster != nil {
				rayObjects = append(rayObjects, tc.rayCluster)
			}
			rayClient := rayClientFake.NewSimpleClientset(rayObjects...)
			k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

			var buf bytes.Buffer
			err := fakeScaleRayServiceOptions.Run(context.Background(), k8sClients, &buf)

			if tc.expectedError == "" {
				require.NoError(t, err)
				for _, expected := range tc.expectedOutput {
					assert.Contains(t, buf.String(), expected)
				}
			} else {
				assert.ErrorContains(t, err, tc.expectedError)
			}
		})
	}
}

func TestRayScaleRayServiceRunUpdatesBothObjects(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	testNamespace, workerGroup, service, cluster := "test-context", "worker-group-1", "my-service", "my-service-cluster"

	rayService := newTestRayService(testNamespace, service, workerGroup, new(int32(5)), new(int32(1)), new(int32(10)))
	rayCluster := newTestRayCluster(testNamespace, cluster, workerGroup, new(int32(5)), new(int32(1)), new(int32(10)))

	kubeClientSet := kubefake.NewClientset()
	rayClient := rayClientFake.NewSimpleClientset(rayService, rayCluster)
	k8sClients := client.NewClientForTesting(kubeClientSet, rayClient)

	fakeScaleRayServiceOptions := ScaleRayServiceOptions{
		cmdFactory:  cmdFactory,
		ioStreams:   &testStreams,
		namespace:   testNamespace,
		service:     service,
		minReplicas: new(int32(3)),
		maxReplicas: new(int32(8)),
		workerGroup: workerGroup,
	}

	var buf bytes.Buffer
	require.NoError(t, fakeScaleRayServiceOptions.Run(context.Background(), k8sClients, &buf))

	updatedService, err := rayClient.RayV1().RayServices(testNamespace).Get(context.Background(), service, metav1.GetOptions{})
	require.NoError(t, err)
	updatedCluster, err := rayClient.RayV1().RayClusters(testNamespace).Get(context.Background(), cluster, metav1.GetOptions{})
	require.NoError(t, err)

	assert.Equal(t, int32(3), *updatedService.Spec.RayClusterSpec.WorkerGroupSpecs[0].MinReplicas)
	assert.Equal(t, int32(8), *updatedService.Spec.RayClusterSpec.WorkerGroupSpecs[0].MaxReplicas)
	assert.Equal(t, int32(5), *updatedService.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas, "replicas should be untouched")
	assert.Equal(t, int32(3), *updatedCluster.Spec.WorkerGroupSpecs[0].MinReplicas)
	assert.Equal(t, int32(8), *updatedCluster.Spec.WorkerGroupSpecs[0].MaxReplicas)
	assert.Equal(t, int32(5), *updatedCluster.Spec.WorkerGroupSpecs[0].Replicas, "replicas should be untouched")
}
