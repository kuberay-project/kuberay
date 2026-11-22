package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestResourceManagerCreateCluster(t *testing.T) {
	ctx := context.Background()
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	clientManager := manager.NewClientManager()
	resourceManager := manager.NewResourceManager(&clientManager)

	tests := []struct {
		name                string
		cluster             *api.Cluster
		expectedErrorString string
	}{
		{
			name: "Create a valid cluster",
			cluster: &api.Cluster{
				Name:      tCtx.GetNextName(),
				Namespace: tCtx.GetNamespaceName(),
				ClusterSpec: &api.ClusterSpec{
					HeadGroupSpec: &api.HeadGroupSpec{
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
						RayStartParams: map[string]string{
							"dashboard-host":      "0.0.0.0",
							"metrics-export-port": "8080",
						},
					},
					WorkerGroupSpec: []*api.WorkerGroupSpec{
						{
							GroupName:       "small-wg",
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							RayStartParams: map[string]string{
								"node-ip-address": "$MY_POD_IP",
							},
						},
					},
				},
			},
			expectedErrorString: "",
		},
		{
			name: "Create a cluster with missing name",
			cluster: &api.Cluster{
				Name:      "",
				Namespace: tCtx.GetNamespaceName(),
				ClusterSpec: &api.ClusterSpec{
					HeadGroupSpec: &api.HeadGroupSpec{
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
						RayStartParams: map[string]string{
							"dashboard-host": "0.0.0.0",
						},
					},
				},
			},
			expectedErrorString: "Required value: name or generateName is required",
		},
		{
			name: "Create a cluster with missing namespace",
			cluster: &api.Cluster{
				Name:      tCtx.GetNextName(),
				Namespace: "",
				ClusterSpec: &api.ClusterSpec{
					HeadGroupSpec: &api.HeadGroupSpec{
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
					},
				},
			},
			expectedErrorString: "the server could not find the requested resource",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resourceManager.CreateCluster(ctx, tc.cluster)
			if tc.expectedErrorString == "" {
				require.NoError(t, err, "No error expected")
			} else {
				require.ErrorContains(t, err, tc.expectedErrorString)
			}
		})
	}
}

func TestListClusters(t *testing.T) {
	ctx := context.Background()
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	clusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: tCtx.GetComputeTemplateName(),
			Image:           tCtx.GetRayImage(),
			RayStartParams: map[string]string{
				"dashboard-host":      "0.0.0.0",
				"metrics-export-port": "8080",
			},
		},
	}

	// Create cluster
	clustersList := []*api.Cluster{
		{
			Name:        "test-cluster-1",
			Namespace:   tCtx.GetNamespaceName(),
			ClusterSpec: clusterSpec,
		},
		{
			Name:        "test-cluster-2",
			Namespace:   tCtx.GetNamespaceName(),
			ClusterSpec: clusterSpec,
		},
		{
			Name:        "test-cluster-3",
			Namespace:   tCtx.GetNamespaceName(),
			ClusterSpec: clusterSpec,
		},
		{
			Name:        "test-cluster-4",
			Namespace:   tCtx.GetNamespaceName(),
			ClusterSpec: clusterSpec,
		},
	}

	clientManager := manager.NewClientManager()
	resourceManager := manager.NewResourceManager(&clientManager)
	for _, cluster := range clustersList {
		_, err := resourceManager.CreateCluster(ctx, cluster)
		require.NoError(t, err)
	}

	// Verify returning 2 clusters each time using continue token
	resRayClusters, continueToken, err := resourceManager.ListClusters(ctx, tCtx.GetNamespaceName(), "" /*continueToken*/, 2 /*limit*/)
	require.NoError(t, err)
	assert.Len(t, resRayClusters, 2)

	resRayClusters, _, err = resourceManager.ListClusters(ctx, tCtx.GetNamespaceName(), continueToken, 2)
	require.NoError(t, err)
	assert.Len(t, resRayClusters, 2)
}

func TestResourceManagerDeleteCluster(t *testing.T) {
	ctx := context.Background()
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	}, []rayv1api.RayClusterConditionType{})

	clientManager := manager.NewClientManager()
	resourceManager := manager.NewResourceManager(&clientManager)

	type clusterInfo struct {
		clusterName string
		namespace   string
	}

	tests := []struct {
		name                string
		input               *clusterInfo
		expectedErrorString string
	}{
		{
			name: "Delete an existing cluster",
			input: &clusterInfo{
				clusterName: tCtx.GetRayClusterName(),
				namespace:   tCtx.GetNamespaceName(),
			},
			expectedErrorString: "",
		},
		{
			name: "Delete a non existing cluster",
			input: &clusterInfo{
				clusterName: "bogus-cluster-name",
				namespace:   tCtx.GetNamespaceName(),
			},
			expectedErrorString: "NotFoundError",
		},
		{
			name: "Delete a cluster with no namespace",
			input: &clusterInfo{
				clusterName: "bogus-cluster-name",
				namespace:   "",
			},
			expectedErrorString: "NotFoundError",
		},
		{
			name: "Delete a cluster with no name",
			input: &clusterInfo{
				clusterName: "",
				namespace:   tCtx.GetNamespaceName(),
			},
			expectedErrorString: "resource name may not be empty",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := resourceManager.DeleteCluster(ctx, tc.input.clusterName, tc.input.namespace)
			if tc.expectedErrorString == "" {
				require.NoError(t, err, "No error expected")

				// Verify the cluster is deleted
				_, err := resourceManager.GetCluster(ctx, tc.input.clusterName, tc.input.namespace)
				require.Error(t, err, "Cluster should not exist after deletion")
			} else {
				require.ErrorContains(t, err, tc.expectedErrorString)
			}
		})
	}
}
