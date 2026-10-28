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

func TestCreateJobWithDisposableClusters(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	// create config map and register a cleanup hook upon success
	configMapName := tCtx.CreateConfigMap(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	})
	t.Cleanup(func() {
		tCtx.DeleteConfigMap(t, configMapName)
	})

	items := map[string]string{
		"counter_sample.py": "counter_sample.py",
		"fail_fast.py":      "fail_fast_sample.py",
	}

	startParams := map[string]string{
		"dashboard-host":      "0.0.0.0",
		"metrics-export-port": "8080",
	}
	volume := &api.Volume{
		MountPath:  "/home/ray/samples",
		VolumeType: api.Volume_CONFIGMAP,
		Name:       "code-sample",
		Source:     tCtx.GetConfigMapName(),
		Items:      items,
	}

	clusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: tCtx.GetComputeTemplateName(),
			Image:           tCtx.GetRayImage(),
			ServiceType:     "NodePort",
			EnableIngress:   false,
			RayStartParams:  startParams,
			Volumes:         []*api.Volume{volume},
		},
		WorkerGroupSpec: []*api.WorkerGroupSpec{
			{
				GroupName:       "small-wg",
				ComputeTemplate: tCtx.GetComputeTemplateName(),
				Image:           tCtx.GetRayImage(),
				Replicas:        1,
				MinReplicas:     1,
				MaxReplicas:     5,
				RayStartParams:  startParams,
				Volumes:         []*api.Volume{volume},
			},
		},
	}

	tests := []struct {
		Name              string
		Input             *api.CreateRayJobRequest
		ExpectedError     error
		ExpectedJobStatus rayv1api.JobStatus
	}{
		{
			Name: "Create a running sample job",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Name:                     tCtx.GetNextName(),
					Namespace:                tCtx.GetNamespaceName(),
					User:                     "natacha",
					Version:                  tCtx.GetRayVersion(),
					Entrypoint:               "python /home/ray/samples/counter_sample.py",
					RuntimeEnv:               "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
					ShutdownAfterJobFinishes: true,
					ClusterSpec:              clusterSpec,
					TtlSecondsAfterFinished:  60,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError:     nil,
			ExpectedJobStatus: rayv1api.JobStatusSucceeded,
		},
		{
			Name: "Create a failing sample job",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Name:                     tCtx.GetNextName(),
					Namespace:                tCtx.GetNamespaceName(),
					User:                     "natacha",
					Version:                  tCtx.GetRayVersion(),
					Entrypoint:               "python /home/ray/samples/fail_fast.py",
					ShutdownAfterJobFinishes: true,
					ClusterSpec:              clusterSpec,
					TtlSecondsAfterFinished:  60,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError:     nil,
			ExpectedJobStatus: rayv1api.JobStatusFailed,
		},
		{
			Name: "Create a job request without providing a namespace",
			Input: &api.CreateRayJobRequest{
				Job:       nil,
				Namespace: "",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Create a job request with nil job spec",
			Input: &api.CreateRayJobRequest{
				Job:       nil,
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a job request with no namespace in the job spec",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Name: tCtx.GetNextName(),
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a job request with no name",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Namespace: tCtx.GetNamespaceName(),
					Name:      "",
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a job request with no user name",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Namespace: tCtx.GetNamespaceName(),
					Name:      tCtx.GetNextName(),
					User:      "",
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a job request with entrypoint",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Namespace: tCtx.GetNamespaceName(),
					Name:      tCtx.GetNextName(),
					User:      "bullwinkle",
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a job request with nil cluster spec",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Namespace:                tCtx.GetNamespaceName(),
					Name:                     tCtx.GetNextName(),
					User:                     "bullwinkle",
					Version:                  tCtx.GetRayVersion(),
					Entrypoint:               "python /home/ray/samples/counter_sample.py",
					RuntimeEnv:               "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
					ShutdownAfterJobFinishes: true,
					ClusterSpec:              nil,
					TtlSecondsAfterFinished:  60,
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
			actualJob, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayJob(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.NotNil(t, actualJob, "A job is expected")
				require.True(t, jobSpecEqual(tc.Input.Job, actualJob), "Job spec should match the input spec. Expected: %v, Actual: %v", tc.Input.Job, actualJob)
				waitForRayJobInExpectedStatuses(t, tCtx, tc.Input.Job.Name, []rayv1api.JobStatus{tc.ExpectedJobStatus})
				tCtx.DeleteRayJobByName(t, actualJob.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestDeleteJob(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testJobRequest := createTestJob(t, tCtx, []rayv1api.JobStatus{rayv1api.JobStatusNew, rayv1api.JobStatusPending, rayv1api.JobStatusRunning, rayv1api.JobStatusSucceeded})

	tests := []GenericEnd2EndTest[*api.DeleteRayJobRequest]{
		{
			Name: "Delete an existing job",
			Input: &api.DeleteRayJobRequest{
				Name:      testJobRequest.Job.Name,
				Namespace: testJobRequest.Namespace,
			},
			ExpectedError: nil,
		},
		{
			Name: "Delete a non existing job",
			Input: &api.DeleteRayJobRequest{
				Name:      "a-bogus-job-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Delete a job without providing a namespace",
			Input: &api.DeleteRayJobRequest{
				Name:      testJobRequest.Job.Name,
				Namespace: "",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualRPCStatus, err := tCtx.GetRayAPIServerClient().DeleteRayJob(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				waitForRayJobToDisappear(t, tCtx, testJobRequest.Job.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestGetAllJobs(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testJobRequest := createTestJob(t, tCtx, []rayv1api.JobStatus{rayv1api.JobStatusNew, rayv1api.JobStatusPending, rayv1api.JobStatusRunning, rayv1api.JobStatusSucceeded})
	t.Cleanup(func() {
		tCtx.DeleteRayJobByName(t, testJobRequest.Job.Name)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllRayJobs(&api.ListAllRayJobsRequest{})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.Jobs, "A list of jobs is required")
	require.Empty(t, response.Continue, "No continue token is expected")
	foundName := false
	for _, job := range response.Jobs {
		if testJobRequest.Job.Name == job.Name && tCtx.GetNamespaceName() == job.Namespace {
			foundName = true
			break
		}
	}
	require.True(t, foundName)
}

func TestGetAllJobsWithPagination(t *testing.T) {
	const numberOfNamespaces = 3
	testContexts := make([]*End2EndTestingContext, 0, numberOfNamespaces)

	for range numberOfNamespaces {
		tCtx, err := NewEnd2EndTestingContext(t)
		require.NoError(t, err, "No error expected when creating testing context")

		tCtx.CreateComputeTemplate(t)
		t.Cleanup(func() {
			tCtx.DeleteComputeTemplate(t)
		})
		testContexts = append(testContexts, tCtx)
		createTestJob(t, tCtx, rayv1api.AllJobStatuses)
	}

	// Test pagination with limit 1, which is less than the total number of jobs.
	t.Run("Test pagination return part of the result jobs", func(t *testing.T) {
		// Used to check all jobs have been returned.
		gotJob := []bool{false, false, false}

		continueToken := ""
		for i := range numberOfNamespaces {
			response, actualRPCStatus, err := testContexts[i].GetRayAPIServerClient().ListAllRayJobs(&api.ListAllRayJobsRequest{
				Limit:    int64(1),
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
			require.NotEmpty(t, response.Jobs, "A list of jobs is required")
			require.Len(t, response.Jobs, 1, "Number of jobs returned is not as expected")
			for _, curJob := range response.Jobs {
				for j := range numberOfNamespaces {
					if testContexts[j].GetCurrentName() == curJob.Name && testContexts[j].GetNamespaceName() == curJob.Namespace {
						gotJob[j] = true
						break
					}
				}
			}
			continueToken = response.Continue
		}
		for i := range numberOfNamespaces {
			if !gotJob[i] {
				t.Errorf("ListAllJobs did not return expected jobs %s", testContexts[i].GetCurrentName())
			}
		}
	})

	// Test pagination with limit 4, which is larger than the total number of jobs.
	t.Run("Test pagination return all result jobs", func(t *testing.T) {
		// Used to check all jobs have been returned.
		gotJob := []bool{false, false, false}

		continueToken := ""
		response, actualRPCStatus, err := testContexts[0].GetRayAPIServerClient().ListAllRayJobs(&api.ListAllRayJobsRequest{
			Limit:    numberOfNamespaces + 1,
			Continue: continueToken,
		})
		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.Empty(t, response.Continue, "No continue token is expected")
		require.NotEmpty(t, response.Jobs, "A list of jobs is required")
		require.Len(t, response.Jobs, numberOfNamespaces, "Number of jobs returned is not as expected")
		for _, curJob := range response.Jobs {
			for j := range numberOfNamespaces {
				if testContexts[j].GetCurrentName() == curJob.Name && testContexts[j].GetNamespaceName() == curJob.Namespace {
					gotJob[j] = true
					break
				}
			}
		}

		for i := range numberOfNamespaces {
			if !gotJob[i] {
				t.Errorf("ListAllJobs did not return expected jobs %s", testContexts[i].GetCurrentName())
			}
		}
	})
}

func TestGetJobsInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testJobRequest := createTestJob(t, tCtx, []rayv1api.JobStatus{rayv1api.JobStatusNew, rayv1api.JobStatusPending, rayv1api.JobStatusRunning, rayv1api.JobStatusSucceeded})
	t.Cleanup(func() {
		tCtx.DeleteRayJobByName(t, testJobRequest.Job.Name)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayJobs(&api.ListRayJobsRequest{
		Namespace: tCtx.GetNamespaceName(),
	})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.Jobs, "A list of jobs is required")
	foundName := false
	for _, job := range response.Jobs {
		if testJobRequest.Job.Name == job.Name && tCtx.GetNamespaceName() == job.Namespace {
			foundName = true
			break
		}
	}
	require.True(t, foundName)
}

func TestGetJobByPaginationInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testJobNum := 10
	testJobs := make([]*api.CreateRayJobRequest, 0, testJobNum)
	for i := range testJobNum {
		tCtx.currentName = fmt.Sprintf("job%d", i)
		tCtx.configMapName = fmt.Sprintf("job%d-cm", i)
		testJobs = append(testJobs, createTestJob(t, tCtx, []rayv1api.JobStatus{rayv1api.JobStatusNew, rayv1api.JobStatusPending, rayv1api.JobStatusRunning, rayv1api.JobStatusSucceeded}))
	}

	t.Cleanup(func() {
		for i := range testJobNum {
			tCtx.DeleteRayJobByName(t, testJobs[i].Job.Name)
		}
	})

	// Test pagination with limit 1
	t.Run("Test pagination return part of the result jobs", func(t *testing.T) {
		continueToken := ""
		for i := range testJobNum {
			response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayJobs(&api.ListRayJobsRequest{
				Namespace: tCtx.GetNamespaceName(),
				Limit:     1,
				Continue:  continueToken,
			})
			require.NoError(t, err, "No error expected")
			require.Nil(t, actualRPCStatus, "No RPC status expected")
			require.NotNil(t, response, "A response is expected")
			require.Len(t, response.Jobs, 1)
			require.Equal(t, response.Jobs[0].Namespace, tCtx.GetNamespaceName())
			require.Equal(t, response.Jobs[0].Name, testJobs[i].Job.Name)
			continueToken = response.Continue
		}
		require.Empty(t, continueToken) // Continue token should be empty because this is the last page
	})

	// Test pagination return all jobs
	t.Run("Test pagination return all jobs", func(t *testing.T) {
		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayJobs(&api.ListRayJobsRequest{
			Namespace: tCtx.GetNamespaceName(),
			Limit:     int64(testJobNum),
			Continue:  "",
		})
		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.Equal(t, len(response.Jobs), testJobNum)
		require.Empty(t, response.Continue) // Continue token should be empty because this is the last page
	})

	t.Run("Test no pagination", func(t *testing.T) {
		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayJobs(&api.ListRayJobsRequest{
			Namespace: tCtx.GetNamespaceName(),
		})
		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.Equal(t, len(response.Jobs), testJobNum)
		require.Empty(t, response.Continue) // Continue token should be empty because this is the last page
	})
}

func TestGetJob(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testJobRequest := createTestJob(t, tCtx, []rayv1api.JobStatus{rayv1api.JobStatusNew, rayv1api.JobStatusPending, rayv1api.JobStatusRunning, rayv1api.JobStatusSucceeded})
	t.Cleanup(func() {
		tCtx.DeleteRayJobByName(t, testJobRequest.Job.Name)
	})
	tests := []GenericEnd2EndTest[*api.GetRayJobRequest]{
		{
			Name: "Get job by name in a namespace",
			Input: &api.GetRayJobRequest{
				Name:      testJobRequest.Job.Name,
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Get non existing job",
			Input: &api.GetRayJobRequest{
				Name:      "a-bogus-cluster-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Get a job with no Name",
			Input: &api.GetRayJobRequest{
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Get a job with no namespace",
			Input: &api.GetRayJobRequest{
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
			actualJob, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetRayJob(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.Equal(t, tc.Input.Name, actualJob.Name)
				require.Equal(t, tCtx.GetNamespaceName(), actualJob.Namespace)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestCreateJobWithClusterSelector(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	cluster, configMapName := tCtx.CreateRayClusterWithConfigMaps(t,
		map[string]string{
			"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
			"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
		}, []rayv1api.RayClusterConditionType{rayv1api.RayClusterProvisioned})
	t.Cleanup(func() {
		tCtx.DeleteRayCluster(t, cluster.Name)
		tCtx.DeleteConfigMap(t, configMapName)
	})

	tests := []struct {
		Name              string
		Input             *api.CreateRayJobRequest
		ExpectedError     error
		ExpectedJobStatus rayv1api.JobStatus
	}{
		{
			Name: "Submit a correct job on an already running cluster",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Name:                     tCtx.GetNextName(),
					Namespace:                tCtx.GetNamespaceName(),
					User:                     "r2d2",
					Version:                  tCtx.GetRayVersion(),
					Entrypoint:               "python /home/ray/samples/counter_sample.py",
					Metadata:                 map[string]string{},
					RuntimeEnv:               "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
					ClusterSelector:          map[string]string{"ray.io/cluster": cluster.Name},
					ShutdownAfterJobFinishes: true,
					TtlSecondsAfterFinished:  60,
					JobSubmitter: &api.RayJobSubmitter{
						Image: cluster.ClusterSpec.HeadGroupSpec.Image,
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedJobStatus: rayv1api.JobStatusSucceeded,
			ExpectedError:     nil,
		},
		{
			Name: "Submit a failing job on an already running cluster",
			Input: &api.CreateRayJobRequest{
				Job: &api.RayJob{
					Name:                     tCtx.GetNextName(),
					Namespace:                tCtx.GetNamespaceName(),
					User:                     "r2d2",
					Version:                  tCtx.GetRayVersion(),
					Entrypoint:               "python /home/ray/samples/fail_fast.py",
					RuntimeEnv:               "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
					ShutdownAfterJobFinishes: true,
					TtlSecondsAfterFinished:  60,
					ClusterSelector:          map[string]string{"ray.io/cluster": cluster.Name},
					JobSubmitter: &api.RayJobSubmitter{
						Image: cluster.ClusterSpec.HeadGroupSpec.Image,
					},
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedJobStatus: rayv1api.JobStatusFailed,
			ExpectedError:     nil,
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualJob, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayJob(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.NotNil(t, actualJob, "A job is expected")
				require.True(t, jobSpecEqual(tc.Input.Job, actualJob), "Job spec should match the input spec. Expected: %v, Actual: %v", tc.Input.Job, actualJob)
				waitForRayJobInExpectedStatuses(t, tCtx, tc.Input.Job.Name, []rayv1api.JobStatus{tc.ExpectedJobStatus})
				tCtx.DeleteRayJobByName(t, actualJob.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func createTestJob(t *testing.T, tCtx *End2EndTestingContext, expectedJobStatues []rayv1api.JobStatus) *api.CreateRayJobRequest {
	// `expectedJobStatues` is a slice of job statuses that we expect the job to be in
	// create config map and register a cleanup hook upon success
	configMapName := tCtx.CreateConfigMap(t, map[string]string{
		"counter_sample.py": ReadFileAsString(t, "resources/counter_sample.py"),
		"fail_fast.py":      ReadFileAsString(t, "resources/fail_fast_sample.py"),
	})
	t.Cleanup(func() {
		tCtx.DeleteConfigMap(t, configMapName)
	})

	items := map[string]string{
		"counter_sample.py": "counter_sample.py",
		"fail_fast.py":      "fail_fast_sample.py",
	}

	startParams := map[string]string{
		"dashboard-host":      "0.0.0.0",
		"metrics-export-port": "8080",
	}
	volume := &api.Volume{
		MountPath:  "/home/ray/samples",
		VolumeType: api.Volume_CONFIGMAP,
		Name:       "code-sample",
		Source:     configMapName,
		Items:      items,
	}

	testJobRequest := &api.CreateRayJobRequest{
		Job: &api.RayJob{
			Name:                     tCtx.currentName,
			Namespace:                tCtx.GetNamespaceName(),
			User:                     "natacha",
			Version:                  tCtx.GetRayVersion(),
			Entrypoint:               "python /home/ray/samples/counter_sample.py",
			RuntimeEnv:               "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
			ShutdownAfterJobFinishes: true,
			ClusterSpec: &api.ClusterSpec{
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: tCtx.GetComputeTemplateName(),
					Image:           tCtx.GetRayImage(),
					ServiceType:     "NodePort",
					EnableIngress:   false,
					RayStartParams:  startParams,
					Volumes:         []*api.Volume{volume},
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "small-wg",
						ComputeTemplate: tCtx.GetComputeTemplateName(),
						Image:           tCtx.GetRayImage(),
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     5,
						RayStartParams:  startParams,
						Volumes:         []*api.Volume{volume},
					},
				},
			},
			TtlSecondsAfterFinished: 60,
		},
		Namespace: tCtx.GetNamespaceName(),
	}

	actualJob, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayJob(testJobRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, actualJob, "A job is expected")
	require.True(t, jobSpecEqual(testJobRequest.Job, actualJob), "Job spec should match the input spec. Expected: %v, Actual: %v", testJobRequest.Job, actualJob)
	waitForRayJobInExpectedStatuses(t, tCtx, testJobRequest.Job.Name, expectedJobStatues)
	return testJobRequest
}
