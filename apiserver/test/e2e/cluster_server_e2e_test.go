package e2e

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	kuberayHTTP "github.com/ray-project/kuberay/apiserver/pkg/http"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// TestCreateClusterEndpoint sequentially iterates over the create cluster endpoint
// with valid and invalid requests
func TestCreateClusterEndpoint(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	// create config map and register a cleanup hook upon success
	configMapName := tCtx.CreateConfigMap(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
	})
	t.Cleanup(func() {
		tCtx.DeleteConfigMap(t, configMapName)
	})

	tests := []GenericEnd2EndTest[*api.CreateClusterRequest]{
		{
			Name: "Create a cluster without volumes",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "3cpo",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{
							{
								GroupName:       "small-wg",
								ComputeTemplate: tCtx.GetComputeTemplateName(),
								Image:           tCtx.GetRayImage(),
								Replicas:        1,
								MinReplicas:     1,
								MaxReplicas:     5,
								RayStartParams: map[string]string{
									"node-ip-address": "$MY_POD_IP",
								},
							},
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create cluster with config map volume",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
							Volumes: []*api.Volume{
								{
									MountPath:  "/home/ray/samples",
									VolumeType: api.Volume_CONFIGMAP,
									Name:       "code-sample",
									Source:     tCtx.GetConfigMapName(),
									Items: map[string]string{
										"counter_sample.py": "counter_sample.py",
									},
								},
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{
							{
								GroupName:       "small-wg",
								ComputeTemplate: tCtx.GetComputeTemplateName(),
								Image:           tCtx.GetRayImage(),
								Replicas:        1,
								MinReplicas:     1,
								MaxReplicas:     5,
								RayStartParams: map[string]string{
									"node-ip-address": "$MY_POD_IP",
								},
								Volumes: []*api.Volume{
									{
										MountPath:  "/home/ray/samples",
										VolumeType: api.Volume_CONFIGMAP,
										Name:       "code-sample",
										Source:     tCtx.GetConfigMapName(),
										Items: map[string]string{
											"counter_sample.py": "counter_sample.py",
										},
									},
								},
							},
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create cluster with no workers",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
							Volumes: []*api.Volume{
								{
									MountPath:  "/home/ray/samples",
									VolumeType: api.Volume_CONFIGMAP,
									Name:       "code-sample",
									Source:     tCtx.GetConfigMapName(),
									Items: map[string]string{
										"counter_sample.py": "counter_sample.py",
									},
								},
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create cluster with head service annotations",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:      tCtx.GetNextName(),
					Namespace: tCtx.GetNamespaceName(),
					User:      "kuberay",
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							RayStartParams: map[string]string{
								"dashboard-host": "0.0.0.0",
							},
						},
						HeadServiceAnnotations: map[string]string{
							"foo": "bar",
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create cluster with no namespace in request",
			Input: &api.CreateClusterRequest{
				Cluster:   &api.Cluster{},
				Namespace: "",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Create cluster with no cluster object",
			Input: &api.CreateClusterRequest{
				Cluster:   nil,
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no namespace in the cluster object",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   "",
					User:        "",
					Version:     "",
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no name in the cluster object",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        "",
					Namespace:   tCtx.GetNamespaceName(),
					User:        "",
					Version:     "",
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no user name in the cluster object",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "",
					Version:     "",
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with nil cluster spec in the cluster object",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "bullwinkle",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: nil,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no head group spec in the cluster object",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "bullwinkle",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no compute template in the head group spec",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: "",
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no ray start parameters in the head group spec",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no group name in the worker group spec",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{
							{},
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with no compute template in the worker group spec",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{
							{
								GroupName:       "small-wg",
								ComputeTemplate: "",
								Image:           "",
							},
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create cluster with invalid replica count in the worker group spec",
			Input: &api.CreateClusterRequest{
				Cluster: &api.Cluster{
					Name:        tCtx.GetNextName(),
					Namespace:   tCtx.GetNamespaceName(),
					User:        "kuberay",
					Version:     tCtx.GetRayVersion(),
					Environment: api.Cluster_DEV,
					ClusterSpec: &api.ClusterSpec{
						HeadGroupSpec: &api.HeadGroupSpec{
							ComputeTemplate: tCtx.GetComputeTemplateName(),
							Image:           tCtx.GetRayImage(),
							ServiceType:     "NodePort",
							RayStartParams: map[string]string{
								"dashboard-host":      "0.0.0.0",
								"metrics-export-port": "8080",
							},
						},
						WorkerGroupSpec: []*api.WorkerGroupSpec{
							{
								GroupName:       "small-wg",
								ComputeTemplate: tCtx.GetComputeTemplateName(),
								Image:           tCtx.GetRayImage(),
							},
						},
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualCluster, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateCluster(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.NotNil(t, actualCluster, "A cluster is expected")
				require.True(t, clusterSpecEqual(tc.Input.Cluster.ClusterSpec, actualCluster.ClusterSpec), "Cluster spec is not as expected. Expected: %v, Actual: %v", tc.Input.Cluster.ClusterSpec, actualCluster.ClusterSpec)
				waitForRunningCluster(t, tCtx, actualCluster.Name)
				tCtx.DeleteRayCluster(t, actualCluster.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestDeleteCluster(t *testing.T) {
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
	tests := []GenericEnd2EndTest[*api.DeleteClusterRequest]{
		{
			Name: "Delete an existing cluster",
			Input: &api.DeleteClusterRequest{
				Name:      tCtx.GetRayClusterName(),
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Delete a non existing cluster",
			Input: &api.DeleteClusterRequest{
				Name:      "bogus-cluster-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Delete a cluster with no namespace",
			Input: &api.DeleteClusterRequest{
				Name:      "bogus-cluster-name",
				Namespace: "",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Delete a cluster with no name",
			Input: &api.DeleteClusterRequest{
				Name:      "",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualRPCStatus, err := tCtx.GetRayAPIServerClient().DeleteCluster(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				waitForClusterToDisappear(t, tCtx, tc.Input.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func createOneClusterInEachNamespaces(t *testing.T, numberOfNamespaces int, expectedConditions []rayv1api.RayClusterConditionType) []*End2EndTestingContext {
	tCtxs := make([]*End2EndTestingContext, numberOfNamespaces)
	for i := range numberOfNamespaces {
		tCtx, err := NewEnd2EndTestingContext(t)
		require.NoError(t, err, "No error expected when creating testing context")

		tCtx.CreateComputeTemplate(t)
		t.Cleanup(func() {
			tCtx.DeleteComputeTemplate(t)
		})
		actualCluster, configMapName := tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
			"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
			"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
		}, expectedConditions)
		t.Cleanup(func() {
			tCtx.DeleteRayCluster(t, actualCluster.Name)
			tCtx.DeleteConfigMap(t, configMapName)
		})
		tCtxs[i] = tCtx
	}
	return tCtxs
}

func isMatchingCluster(tCtx *End2EndTestingContext, cluster *api.Cluster) bool {
	return tCtx.GetRayClusterName() == cluster.Name && tCtx.GetNamespaceName() == cluster.Namespace
}

// TestGetAllClusters tests gets all Ray clusters from k8s cluster
func TestGetAllClusters(t *testing.T) {
	numberOfNamespaces := 3
	tCtxs := createOneClusterInEachNamespaces(t, numberOfNamespaces, []rayv1api.RayClusterConditionType{})
	tCtx := tCtxs[0]
	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllClusters(&api.ListAllClustersRequest{})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.Empty(t, response.Continue, "No continue token is expected")
	require.NotEmpty(t, response.Clusters, "A list of clusters is required")
	require.Len(t, response.Clusters, numberOfNamespaces, "Number of clusters returned is not as expected")
	gotClusters := make([]bool, numberOfNamespaces)
	for _, cluster := range response.Clusters {
		for i := range numberOfNamespaces {
			if isMatchingCluster(tCtxs[i], cluster) {
				gotClusters[i] = true
				break
			}
		}
	}
	for i := range numberOfNamespaces {
		if !gotClusters[i] {
			t.Errorf("ListAllClusters did not return expected clusters %s", tCtxs[i].GetRayClusterName())
		}
	}
}

// TestGetAllClustersByPagination tests gets all Ray clusters from k8s cluster with pagination
func TestGetAllClustersByPagination(t *testing.T) {
	numberOfNamespaces := 3
	tCtxs := createOneClusterInEachNamespaces(t, numberOfNamespaces, []rayv1api.RayClusterConditionType{})
	tCtx := tCtxs[0]
	gotClusters := make([]bool, numberOfNamespaces)
	continueToken := ""
	for i := range numberOfNamespaces {
		limit := 1
		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllClusters(&api.ListAllClustersRequest{
			Limit:    int64(limit),
			Continue: continueToken,
		})
		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		if i != numberOfNamespaces-1 {
			require.NotEmpty(t, response.Continue, "A continue token is expected")
		} else {
			require.Empty(t, response.Continue, "No continue token is expected")
		}
		require.NotEmpty(t, response.Clusters, "A list of clusters is required")
		require.Len(t, response.Clusters, limit, "Number of clusters returned is not as expected")
		for _, cluster := range response.Clusters {
			for i := range numberOfNamespaces {
				if isMatchingCluster(tCtxs[i], cluster) {
					gotClusters[i] = true
					break
				}
			}
		}
		continueToken = response.Continue
	}
	for i := range numberOfNamespaces {
		if !gotClusters[i] {
			t.Errorf("ListAllClusters did not return expected clusters %s", tCtxs[i].GetRayClusterName())
		}
	}
}

// TestGetAllClustersByPaginationWithAllResults tests gets all Ray clusters from k8s cluster with pagination returning all results
func TestGetAllClustersByPaginationWithAllResults(t *testing.T) {
	numberOfNamespaces := 3
	tCtxs := createOneClusterInEachNamespaces(t, numberOfNamespaces, []rayv1api.RayClusterConditionType{})
	tCtx := tCtxs[0]
	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllClusters(&api.ListAllClustersRequest{
		Limit: 10,
	})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.Empty(t, response.Continue, "No continue token is expected")
	require.NotEmpty(t, response.Clusters, "A list of clusters is required")
	require.Len(t, response.Clusters, numberOfNamespaces, "Number of clusters returned is not as expected")
	gotClusters := make([]bool, numberOfNamespaces)
	for _, cluster := range response.Clusters {
		for i := range numberOfNamespaces {
			if isMatchingCluster(tCtxs[i], cluster) {
				gotClusters[i] = true
				break
			}
		}
	}
	for i := range numberOfNamespaces {
		if !gotClusters[i] {
			t.Errorf("ListAllClusters did not return expected clusters %s", tCtxs[i].GetRayClusterName())
		}
	}
}

// TestGetClustersInNamespace validates t
func TestGetClustersInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	cluster, configMapName := tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	}, []rayv1api.RayClusterConditionType{})
	t.Cleanup(func() {
		tCtx.DeleteRayCluster(t, cluster.Name)
		tCtx.DeleteConfigMap(t, configMapName)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListClusters(
		&api.ListClustersRequest{
			Namespace: tCtx.GetNamespaceName(),
		})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.Clusters, "A list of compute templates is required")
	gotCluster := false
	for _, cluster := range response.Clusters {
		if tCtx.GetRayClusterName() == cluster.Name && tCtx.GetNamespaceName() == cluster.Namespace {
			gotCluster = true
			break
		}
	}
	if !gotCluster {
		t.Error("Getting clusters din namespace did not return expected one")
	}
}

func TestGetClustersByPaginationInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	cluster1, configMapName1 := tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	}, []rayv1api.RayClusterConditionType{}, "cluster1")
	cluster2, configMapName2 := tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	}, []rayv1api.RayClusterConditionType{}, "cluster2")
	t.Cleanup(func() {
		tCtx.DeleteRayCluster(t, cluster1.Name)
		tCtx.DeleteRayCluster(t, cluster2.Name)
		tCtx.DeleteConfigMap(t, configMapName1)
		tCtx.DeleteConfigMap(t, configMapName2)
	})

	continueToken := ""
	for i := 1; i <= 2; i++ {
		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListClusters(
			&api.ListClustersRequest{
				Namespace: tCtx.GetNamespaceName(),
				Limit:     1,
				Continue:  continueToken,
			})
		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.Len(t, response.Clusters, 1)
		require.Equal(t, response.Clusters[0].Name, fmt.Sprintf("cluster%d", i))
		require.Equal(t, response.Clusters[0].Namespace, tCtx.GetNamespaceName())
		continueToken = response.Continue
	}
}

// TestDeleteTemplate sequentially iterates over the delete compute template endpoint
// to validate various input scenarios
func TestGetClustersByNameInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	cluster, configMapName := tCtx.CreateRayClusterWithConfigMaps(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	}, []rayv1api.RayClusterConditionType{})
	t.Cleanup(func() {
		tCtx.DeleteRayCluster(t, cluster.Name)
		tCtx.DeleteConfigMap(t, configMapName)
	})

	tests := []GenericEnd2EndTest[*api.GetClusterRequest]{
		{
			Name: "Get cluster by name in a namespace",
			Input: &api.GetClusterRequest{
				Name:      tCtx.GetRayClusterName(),
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Get non existing cluster",
			Input: &api.GetClusterRequest{
				Name:      "a-bogus-cluster-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Get a cluster with no Name",
			Input: &api.GetClusterRequest{
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Get a cluster with no namespace",
			Input: &api.GetClusterRequest{
				Name: "some-Name",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualCluster, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetCluster(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.Equal(t, tCtx.GetRayClusterName(), actualCluster.Name)
				require.Equal(t, tCtx.GetNamespaceName(), actualCluster.Namespace)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}
