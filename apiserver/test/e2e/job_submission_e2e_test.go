package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/ray-project/kuberay/proto/go_client"
)

func TestCreateJobSubmission(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	// create config map and register a cleanup hook upon success
	configMapName := tCtx.CreateConfigMap(t, map[string]string{
		"sample_code.py": ReadFileAsString(t, "resources/counter_sample.py"),
	})
	t.Cleanup(func() {
		tCtx.DeleteConfigMap(t, configMapName)
	})

	volumes := []*api.Volume{
		{
			MountPath:  "/home/ray/samples",
			VolumeType: api.Volume_CONFIGMAP,
			Name:       "code-sample",
			Source:     tCtx.GetConfigMapName(),
			Items: map[string]string{
				"sample_code.py": "sample_code.py",
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
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: tCtx.GetComputeTemplateName(),
					Image:           tCtx.GetRayImage(),
					ServiceType:     "NodePort",
					RayStartParams: map[string]string{
						"dashboard-host":      "0.0.0.0",
						"metrics-export-port": "8080",
					},
					Volumes: volumes,
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "small-wg",
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     1,
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
	require.True(t, clusterSpecEqual(clusterReq.Cluster.ClusterSpec, actualCluster.ClusterSpec), "Cluster specs should be equal. Expected: %v, Actual: %v", clusterReq.Cluster.ClusterSpec, actualCluster.ClusterSpec)
	waitForRunningCluster(t, tCtx, actualCluster.Name)

	// Submit job
	submitJobRequest := api.SubmitRayJobRequest{
		Namespace:   "foo",
		Clustername: actualCluster.Name,
		Jobsubmission: &api.RayJobSubmission{
			Entrypoint:        "python /home/ray/samples/sample_code.py",
			RuntimeEnv:        "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
			EntrypointNumCpus: .5,
		},
	}
	// Request for the wrong namespace, should fail
	_, jobSubmissionStatus, err := tCtx.GetRayAPIServerClient().SubmitRayJob(&submitJobRequest)
	require.Error(t, err, "An error is expected")
	require.NotNil(t, jobSubmissionStatus, "A not nill RPC status is required")

	// Fix request and resubmit
	submitJobRequest.Namespace = tCtx.GetNamespaceName()
	jobSubmission, jobSubmissionStatus, err := tCtx.GetRayAPIServerClient().SubmitRayJob(&submitJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobSubmissionStatus, "No RPC status expected")
	require.NotNil(t, jobSubmission, "A job  submission is expected")

	// Get Job details
	jobDetailsRequest := api.GetJobDetailsRequest{
		Namespace:    tCtx.GetNamespaceName(),
		Clustername:  actualCluster.Name,
		Submissionid: "1234567", // Not valid submission ID
	}
	// Request with the wrong submission ID should fail
	_, jobDetailsStatus, err := tCtx.GetRayAPIServerClient().GetRayJobDetails(&jobDetailsRequest)
	require.Error(t, err, "An error is expected")
	require.NotNil(t, jobDetailsStatus, "A not nill RPC status is required")

	// Set the correct submission ID and resubmit
	jobDetailsRequest.Submissionid = jobSubmission.SubmissionId
	jobDetails, jobDetailsStatus, err := tCtx.GetRayAPIServerClient().GetRayJobDetails(&jobDetailsRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobDetailsStatus, "No RPC status expected")
	require.NotNil(t, jobDetails, "Job  details are expected")

	// List jobs
	listJobRequest := api.ListJobDetailsRequest{
		Namespace:   tCtx.GetNamespaceName(),
		Clustername: actualCluster.Name,
	}
	jobList, jobListStatus, err := tCtx.GetRayAPIServerClient().ListRayJobsCluster(&listJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobListStatus, "No RPC status expected")
	require.NotNil(t, jobList, "List of Job details is expected")

	// Get job log
	logJobRequest := api.GetJobLogRequest{
		Namespace:    tCtx.GetNamespaceName(),
		Clustername:  actualCluster.Name,
		Submissionid: "1234567", // Not valid submission ID
	}
	_, jobLogStatus, err := tCtx.GetRayAPIServerClient().GetRayJobLog(&logJobRequest)
	require.Error(t, err, "An error is expected")
	require.NotNil(t, jobLogStatus, "A not nill RPC status is required")

	// Set the correct submission ID and resubmit
	logJobRequest.Submissionid = jobSubmission.SubmissionId
	jobLog, jobLogStatus, err := tCtx.GetRayAPIServerClient().GetRayJobLog(&logJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobLogStatus, "No RPC status expected")
	require.NotNil(t, jobLog, "Job log is expected")

	// Stop job
	stopJobRequest := api.StopRayJobSubmissionRequest{
		Namespace:    tCtx.GetNamespaceName(),
		Clustername:  actualCluster.Name,
		Submissionid: "1234567", // Not valid submission ID
	}
	jobStopStatus, err := tCtx.GetRayAPIServerClient().StopRayJob(&stopJobRequest)
	require.Error(t, err, "An error is expected")
	require.NotNil(t, jobStopStatus, "A not nill RPC status is required")
	// Set the correct submission ID and resubmit
	stopJobRequest.Submissionid = jobSubmission.SubmissionId
	jobStopStatus, err = tCtx.GetRayAPIServerClient().StopRayJob(&stopJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobStopStatus, "No RPC status expected")

	// Delete job
	deleteJobRequest := api.DeleteRayJobSubmissionRequest{
		Namespace:    tCtx.GetNamespaceName(),
		Clustername:  actualCluster.Name,
		Submissionid: jobSubmission.SubmissionId,
	}
	jobDeleteStatus, err := tCtx.GetRayAPIServerClient().DeleteRayJobCluster(&deleteJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, jobDeleteStatus, "No RPC status expected")
}
