package e2e

import (
	"testing"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// TestCreateClusterAutoscalerEndpoint sequentially iterates over the create cluster endpoint
// with valid and invalid requests
func TestCreateClusterAutoscaler(t *testing.T) {
	g := gomega.NewWithT(t)

	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	// create config map and register a cleanup hook upon success
	configMapName := tCtx.CreateConfigMap(t, map[string]string{
		"detached_actor.py":           ReadFileAsString(t, "resources/create_detached_actor.py"),
		"terminate_detached_actor.py": ReadFileAsString(t, "resources/terminate_detached_actor.py"),
	})
	t.Cleanup(func() {
		tCtx.DeleteConfigMap(t, configMapName)
	})

	volumes := []*api.Volume{
		{
			MountPath:  "/home/ray/samples",
			VolumeType: api.Volume_CONFIGMAP,
			Name:       "detached-actor",
			Source:     tCtx.GetConfigMapName(),
			Items: map[string]string{
				"detached_actor.py":           "detached_actor.py",
				"terminate_detached_actor.py": "terminate_detached_actor.py",
			},
		},
	}

	clusterReq := api.CreateClusterRequest{
		Namespace: tCtx.GetNamespaceName(),
		Cluster: &api.Cluster{
			Name:        tCtx.GetNextName(),
			Namespace:   tCtx.GetNamespaceName(),
			User:        "kuberay",
			Version:     tCtx.GetRayVersion(),
			Environment: api.Cluster_DEV,
			ClusterSpec: &api.ClusterSpec{
				EnableInTreeAutoscaling: true,
				AutoscalerOptions: &api.AutoscalerOptions{
					UpscalingMode:      "Default",
					IdleTimeoutSeconds: 30,
					Cpu:                "500m",
					Memory:             "512Mi",
				},
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: tCtx.GetComputeTemplateName(),
					Image:           tCtx.GetRayImage(),
					ServiceType:     "NodePort",
					RayStartParams: map[string]string{
						"dashboard-host":      "0.0.0.0",
						"metrics-export-port": "8080",
						"num-cpus":            "0",
					},
					Volumes: volumes,
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "small-wg",
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
						Replicas:        0,
						MinReplicas:     0,
						MaxReplicas:     5,
						RayStartParams: map[string]string{
							"node-ip-address": "$MY_POD_IP",
						},
						Volumes: volumes,
					},
				},
			},
		},
	}

	// Create cluster
	actualCluster, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateCluster(&clusterReq)
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, actualCluster, "A cluster is expected")
	require.True(t, clusterSpecEqual(clusterReq.Cluster.ClusterSpec, actualCluster.ClusterSpec), "Cluster spec should match the request. Expected: %v, Actual: %v", clusterReq.Cluster.ClusterSpec, actualCluster.ClusterSpec)
	waitForRunningCluster(t, tCtx, actualCluster.Name)

	// Get number of workers
	rayCluster, err := tCtx.GetRayClusterByName(actualCluster.Name)
	require.NoError(t, err, "No error expected")
	require.Equal(t, int32(0), rayCluster.Status.AvailableWorkerReplicas)

	// Create actor
	createActorRequest := api.CreateRayJobRequest{
		Namespace: tCtx.GetNamespaceName(),
		Job: &api.RayJob{
			Name:       tCtx.GetNextName(),
			Namespace:  tCtx.GetNamespaceName(),
			User:       "natacha",
			Entrypoint: "python /home/ray/samples/detached_actor.py",
			ClusterSelector: map[string]string{
				"ray.io/cluster": actualCluster.Name,
			},
		},
	}

	actualJob, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayJob(&createActorRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, actualJob, "A job is expected")
	require.True(t, jobSpecEqual(createActorRequest.Job, actualJob), "Job spec should match the request. Expected: %v, Actual: %v", createActorRequest.Job, actualJob)
	waitForRayJobInExpectedStatuses(t, tCtx, createActorRequest.Job.Name, []rayv1api.JobStatus{rayv1api.JobStatusSucceeded})

	// worker pod should be created as part of job execution
	g.Eventually(func() int32 {
		rayCluster, err := tCtx.GetRayClusterByName(actualCluster.Name)
		if err != nil {
			t.Logf("Error getting ray cluster: %v", err)
			return -1 // Return -1 to indicate error condition
		}
		t.Logf("Found ray cluster with %d available worker replicas", rayCluster.Status.AvailableWorkerReplicas)
		return rayCluster.Status.AvailableWorkerReplicas
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.Equal(int32(1)))
	// Delete actor
	deleteActorRequest := api.CreateRayJobRequest{
		Namespace: tCtx.GetNamespaceName(),
		Job: &api.RayJob{
			Name:       tCtx.GetNextName(),
			Namespace:  tCtx.GetNamespaceName(),
			User:       "natacha",
			Entrypoint: "python /home/ray/samples/terminate_detached_actor.py",
			ClusterSelector: map[string]string{
				"ray.io/cluster": actualCluster.Name,
			},
		},
	}
	actualJob, actualRPCStatus, err = tCtx.GetRayAPIServerClient().CreateRayJob(&deleteActorRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, actualJob, "A job is expected")
	require.True(t, jobSpecEqual(deleteActorRequest.Job, actualJob), "Job spec should match the request. Expected: %v, Actual: %v", deleteActorRequest.Job, actualJob)
	waitForRayJobInExpectedStatuses(t, tCtx, deleteActorRequest.Job.Name, []rayv1api.JobStatus{rayv1api.JobStatusSucceeded})
	g.Eventually(func() int32 {
		rayCluster, err := tCtx.GetRayClusterByName(actualCluster.Name)
		if err != nil {
			t.Logf("Error getting ray cluster: %v", err)
			return -1 // Return -1 to indicate error condition
		}
		t.Logf("Found ray cluster with %d available worker replicas", rayCluster.Status.AvailableWorkerReplicas)
		return rayCluster.Status.AvailableWorkerReplicas
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.Equal(int32(0)))
	require.NoError(t, err, "No error expected")
}
