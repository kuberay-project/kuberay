package e2e

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	kuberayHTTP "github.com/ray-project/kuberay/apiserver/pkg/http"
	api "github.com/ray-project/kuberay/proto/go_client"
)

// TestServiceServerV2 sequentially iterates over the endpoints of the service endpoints using
// V2 configurations (yaml)
func TestCreateServiceV2(t *testing.T) {
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
			ServiceType:     "NodePort",
			EnableIngress:   false,
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
					"dashboard-host":      "0.0.0.0",
					"metrics-export-port": "8080",
				},
			},
		},
	}

	tests := []GenericEnd2EndTest[*api.CreateRayServiceRequest]{
		{
			Name: "Create a fruit stand ray service using V2 configuration",
			Input: &api.CreateRayServiceRequest{
				Service: &api.RayService{
					Name:                               tCtx.GetNextName(),
					Namespace:                          tCtx.GetNamespaceName(),
					User:                               "user1",
					Version:                            tCtx.GetRayVersion(),
					ServeConfig_V2:                     "applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
					ServiceUnhealthySecondThreshold:    10,
					DeploymentUnhealthySecondThreshold: 20,
					ClusterSpec:                        clusterSpec,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create a service request with no namespace value",
			Input: &api.CreateRayServiceRequest{
				Service:   &api.RayService{},
				Namespace: "",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Create a service request with mismatching namespaces",
			Input: &api.CreateRayServiceRequest{
				Service: &api.RayService{
					Namespace: "another-namespace-name",
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a service request with no name",
			Input: &api.CreateRayServiceRequest{
				Service: &api.RayService{
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
			Name: "Create a service request with no user",
			Input: &api.CreateRayServiceRequest{
				Service: &api.RayService{
					Namespace: tCtx.GetNamespaceName(),
					Name:      tCtx.GetNextName(),
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a service request with no cluster spec",
			Input: &api.CreateRayServiceRequest{
				Service: &api.RayService{
					Namespace:   tCtx.GetNamespaceName(),
					Name:        tCtx.GetNextName(),
					ClusterSpec: nil,
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
			actualService, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayService(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.NotNil(t, actualService, "A service is expected")
				require.True(t, serviceSpecEqual(tc.Input.Service, actualService), "Service spec should match the request. Expected: %v, Actual: %v", tc.Input.Service, actualService)
				tCtx.DeleteRayService(t, actualService.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestDeleteService(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testServiceRequest := createTestServiceV2(t, tCtx)

	tests := []GenericEnd2EndTest[*api.DeleteRayServiceRequest]{
		{
			Name: "Delete an existing service",
			Input: &api.DeleteRayServiceRequest{
				Name:      testServiceRequest.Service.Name,
				Namespace: testServiceRequest.Namespace,
			},
			ExpectedError: nil,
		},
		{
			Name: "Delete a non existing service",
			Input: &api.DeleteRayServiceRequest{
				Name:      "a-bogus-job-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Delete a service without providing a namespace",
			Input: &api.DeleteRayServiceRequest{
				Name:      testServiceRequest.Service.Name,
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
			actualRPCStatus, err := tCtx.GetRayAPIServerClient().DeleteRayService(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				waitForServiceToDisappear(t, tCtx, testServiceRequest.Service.Name)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func TestGetAllServices(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testServiceRequest := createTestServiceV2(t, tCtx)
	t.Cleanup(func() {
		tCtx.DeleteRayService(t, testServiceRequest.Service.Name)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllRayServices(&api.ListAllRayServicesRequest{})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.Services, "A list of services is required")
	require.Equal(t, testServiceRequest.Service.Name, response.Services[0].Name)
	require.Equal(t, tCtx.GetNamespaceName(), response.Services[0].Namespace)
}

func TestGetAllServicesWithPagination(t *testing.T) {
	const numberOfNamespaces = 3
	const numberOfService = 2
	const totalServices = numberOfNamespaces * numberOfService

	type targetService struct {
		namespace string
		service   string
	}

	tCtxs := make([]*End2EndTestingContext, 0, numberOfNamespaces)
	expectedServices := make([]targetService, 0, totalServices)

	// Create services for each namespace
	for range numberOfNamespaces {
		tCtx, err := NewEnd2EndTestingContext(t)
		require.NoError(t, err, "No error expected when creating testing context")

		tCtx.CreateComputeTemplate(t)
		t.Cleanup(func() {
			tCtx.DeleteComputeTemplate(t)
		})

		for range numberOfService {
			testServiceRequest := createTestServiceV2(t, tCtx)
			t.Cleanup(func() {
				tCtx.DeleteRayService(t, testServiceRequest.Service.Name)
			})
			expectedServices = append(expectedServices, targetService{
				namespace: tCtx.GetNamespaceName(),
				service:   testServiceRequest.Service.Name,
			})
		}

		tCtxs = append(tCtxs, tCtx)
	}

	var pageToken string
	tCtx := tCtxs[0]

	// Test pagination with limit less than the total number of services in all namespaces.
	t.Run("Test pagination return part of the result services", func(t *testing.T) {
		pageToken = ""
		gotServices := make(map[targetService]bool, totalServices)
		for _, expectedService := range expectedServices {
			gotServices[expectedService] = false
		}

		for i := range totalServices {
			response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllRayServices(&api.ListAllRayServicesRequest{
				PageToken: pageToken,
				PageSize:  int32(1),
			})
			require.NoError(t, err, "No error expected")
			require.Nil(t, actualRPCStatus, "No RPC status expected")
			require.NotNil(t, response, "A response is expected")
			require.NotEmpty(t, response.Services, "A list of service is required")
			require.Len(t, response.Services, 1, "Got %d services in response, expected %d", len(response.Services), 1)

			pageToken = response.NextPageToken
			if i == totalServices-1 {
				require.Empty(t, pageToken, "No continue token is expected")
			} else {
				require.NotEmpty(t, pageToken, "A continue token is expected")
			}

			for _, service := range response.Services {
				key := targetService{namespace: service.Namespace, service: service.Name}
				seen, exist := gotServices[key]

				// Check if this service is in expectedServices list
				require.True(t, exist,
					"ListAllRayServices returned an unexpected service: namespace=%s, name=%s",
					key.namespace, key.service)

				// Check if we've already seen this service before (duplicate)
				require.False(t, seen,
					"ListAllRayServices returned duplicated service: namespace=%s, name=%s",
					key.namespace, key.service)

				gotServices[key] = true
			}
		}

		// Check all services were found
		for _, expectedService := range expectedServices {
			require.True(t, gotServices[expectedService],
				"ListAllRayServices did not return expected service %s from namespace %s",
				expectedService.service, expectedService.namespace)
		}
	})

	// Test pagination with limit larger than the total number of services in all namespaces.
	t.Run("Test pagination return all result services", func(t *testing.T) {
		pageToken = ""
		gotServices := make(map[targetService]bool, totalServices)
		for _, expectedService := range expectedServices {
			gotServices[expectedService] = false
		}

		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListAllRayServices(&api.ListAllRayServicesRequest{
			PageToken: pageToken,
			PageSize:  int32(totalServices + 1),
		})

		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.NotEmpty(t, response.Services, "A list of services is required")
		require.Len(t, response.Services, totalServices, "Got %d services in response, expected %d", len(response.Services), totalServices)
		require.Empty(t, response.NextPageToken, "Page token should be empty")

		for _, service := range response.Services {
			key := targetService{namespace: service.Namespace, service: service.Name}
			seen, exist := gotServices[key]

			// Check if this service is in expectedServices list
			require.True(t, exist,
				"ListAllRayServices returned an unexpected service: namespace=%s, name=%s",
				key.namespace, key.service)

			// Check if we've already seen this service before (duplicate)
			require.False(t, seen,
				"ListAllRayServices returned duplicated service: namespace=%s, name=%s",
				key.namespace, key.service)

			gotServices[key] = true
		}

		// Check all services were found
		for _, expectedService := range expectedServices {
			require.True(t, gotServices[expectedService],
				"ListAllRayServices did not return expected service %s from namespace %s",
				expectedService.service, expectedService.namespace)
		}
	})
}

func TestGetServicesInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testServiceRequest := createTestServiceV2(t, tCtx)
	t.Cleanup(func() {
		tCtx.DeleteRayService(t, testServiceRequest.Service.Name)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayServices(&api.ListRayServicesRequest{
		Namespace: tCtx.GetNamespaceName(),
	})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.Services, "A list of compute templates is required")
	require.Equal(t, testServiceRequest.Service.Name, response.Services[0].Name)
	require.Equal(t, tCtx.GetNamespaceName(), response.Services[0].Namespace)
}

func TestGetServicesInNamespaceWithPagination(t *testing.T) {
	const serviceCount = 2
	expectedServiceNames := make([]string, 0, serviceCount)

	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	for range serviceCount {
		testServiceRequest := createTestServiceV2(t, tCtx)
		t.Cleanup(func() {
			tCtx.DeleteRayService(t, testServiceRequest.Service.Name)
		})
		expectedServiceNames = append(expectedServiceNames, testServiceRequest.Service.Name)
	}

	// Test pagination with limit 1, which is less than the total number of services.
	t.Run("Test pagination return part of the result services", func(t *testing.T) {
		// Used to check all services have been returned.
		gotServices := []bool{false, false}

		pageToken := ""
		for ii := range serviceCount {
			response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayServices(&api.ListRayServicesRequest{
				Namespace: tCtx.GetNamespaceName(),
				PageToken: pageToken,
				PageSize:  int32(1),
			})

			require.NoError(t, err, "No error expected")
			require.Nil(t, actualRPCStatus, "No RPC status expected")
			require.NotNil(t, response, "A response is expected")
			require.NotEmpty(t, response.Services, "A list of service is required")
			require.Len(t, response.Services, 1)

			for _, curService := range response.Services {
				for jj := range serviceCount {
					if expectedServiceNames[jj] == curService.Name {
						gotServices[jj] = true
						break
					}
				}
			}

			// Check next page token.
			pageToken = response.NextPageToken
			if ii == serviceCount-1 {
				require.Empty(t, pageToken, "Last page token should be empty")
			} else {
				require.NotEmpty(t, pageToken, "Non-last page token should be non empty")
			}
		}

		// Check all services created have been returned.
		for idx := range serviceCount {
			require.True(t, gotServices[idx],
				"ListServices did not return expected services %s",
				expectedServiceNames[idx])
		}
	})

	// Test pagination with limit 3, which is larger than the total number of services.
	t.Run("Test pagination return all result services", func(t *testing.T) {
		// Used to check all services have been returned.
		gotServices := []bool{false, false}

		pageToken := ""
		response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().ListRayServices(&api.ListRayServicesRequest{
			Namespace: tCtx.GetNamespaceName(),
			PageToken: pageToken,
			PageSize:  serviceCount + 1,
		})

		require.NoError(t, err, "No error expected")
		require.Nil(t, actualRPCStatus, "No RPC status expected")
		require.NotNil(t, response, "A response is expected")
		require.NotEmpty(t, response.Services, "A list of services is required")
		require.Len(t, response.Services, serviceCount)
		require.Empty(t, pageToken, "Page token should be empty")
		for _, curService := range response.Services {
			for jj := range serviceCount {
				if expectedServiceNames[jj] == curService.Name {
					gotServices[jj] = true
					break
				}
			}
		}

		// Check all services created have been returned.
		for idx := range serviceCount {
			require.True(t, gotServices[idx],
				"ListServices did not return expected services %s",
				expectedServiceNames[idx])
		}
	})
}

func TestGetService(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})
	testServiceRequest := createTestServiceV2(t, tCtx)
	t.Cleanup(func() {
		tCtx.DeleteRayService(t, testServiceRequest.Service.Name)
	})
	tests := []GenericEnd2EndTest[*api.GetRayServiceRequest]{
		{
			Name: "Get job by name in a namespace",
			Input: &api.GetRayServiceRequest{
				Name:      testServiceRequest.Service.Name,
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Get non existing job",
			Input: &api.GetRayServiceRequest{
				Name:      "a-bogus-cluster-name",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Get a job with no Name",
			Input: &api.GetRayServiceRequest{
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Get a job with no namespace",
			Input: &api.GetRayServiceRequest{
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
			actualService, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetRayService(tc.Input)
			if tc.ExpectedError == nil {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.Equal(t, tc.Input.Name, actualService.Name)
				require.Equal(t, tCtx.GetNamespaceName(), actualService.Namespace)
			} else {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			}
		})
	}
}

func createTestServiceV2(t *testing.T, tCtx *End2EndTestingContext) *api.CreateRayServiceRequest {
	clusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: tCtx.GetComputeTemplateName(),
			Image:           tCtx.GetRayImage(),
			ServiceType:     "NodePort",
			EnableIngress:   false,
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
					"dashboard-host":      "0.0.0.0",
					"metrics-export-port": "8080",
				},
			},
		},
	}

	testServiceRequest := &api.CreateRayServiceRequest{
		Service: &api.RayService{
			Name:                               tCtx.GetNextName(),
			Namespace:                          tCtx.GetNamespaceName(),
			User:                               "user1",
			Version:                            tCtx.GetRayVersion(),
			ServeConfig_V2:                     "applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
			ServiceUnhealthySecondThreshold:    10,
			DeploymentUnhealthySecondThreshold: 20,
			ClusterSpec:                        clusterSpec,
		},
		Namespace: tCtx.GetNamespaceName(),
	}
	actualService, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateRayService(testServiceRequest)
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, actualService, "A service is expected")
	require.True(t, serviceSpecEqual(testServiceRequest.Service, actualService), "The service spec should be equal. Expected: %v, Actual: %v", testServiceRequest.Service, actualService)
	checkRayServiceCreatedSuccessfully(t, tCtx, actualService.Name)
	return testServiceRequest
}

func checkRayServiceCreatedSuccessfully(t *testing.T, tCtx *End2EndTestingContext, serviceName string) {
	rayService, err := tCtx.GetRayServiceByName(serviceName)
	require.NoError(t, err)
	require.NotNil(t, rayService)
}
