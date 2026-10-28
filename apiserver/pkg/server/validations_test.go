package server_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ray-project/kuberay/apiserver/pkg/server"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

func TestValidateClusterSpec(t *testing.T) {
	// A valid `ClusterSpec` template for test cases.
	// Each test clones and modifies a field to verify specific validations.
	baseClusterSpec := &api.ClusterSpec{
		HeadGroupSpec: &api.HeadGroupSpec{
			ComputeTemplate: "a template",
			RayStartParams: map[string]string{
				"dashboard-host":      "0.0.0.0",
				"metrics-export-port": "8080",
			},
		},
		WorkerGroupSpec: []*api.WorkerGroupSpec{
			{
				GroupName:       "group-1",
				ComputeTemplate: "group-1-template",
				Replicas:        1,
				MinReplicas:     1,
				MaxReplicas:     1,
			},
			{
				GroupName:       "group-2",
				ComputeTemplate: "group-2-template",
				Replicas:        1,
				MinReplicas:     1,
				MaxReplicas:     1,
			},
		},
	}

	tests := []struct {
		expectedError error
		mutate        func(base *api.ClusterSpec) *api.ClusterSpec // mutate applies a change to the base request to simulate each test scenario
		name          string
	}{
		{
			name: "A valid cluster spec",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				return proto.Clone(base).(*api.ClusterSpec)
			},
			expectedError: nil,
		},
		{
			name:          "A nill cluster spec",
			mutate:        func(_ *api.ClusterSpec) *api.ClusterSpec { return nil },
			expectedError: util.NewInvalidInputError("A ClusterSpec object is required. Please specify one."),
		},
		{
			name: "An empty cluster spec",
			mutate: func(_ *api.ClusterSpec) *api.ClusterSpec {
				return &api.ClusterSpec{}
			},
			expectedError: util.NewInvalidInputError("Cluster Spec Object requires HeadGroupSpec to be populated. Please specify one."),
		},
		{
			name: "An empty head group cluster spec",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.HeadGroupSpec = &api.HeadGroupSpec{}
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{}
				return clone
			},
			expectedError: util.NewInvalidInputError("HeadGroupSpec compute template is empty. Please specify a valid value."),
		},
		{
			name: "A head group without ray start parameters",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.HeadGroupSpec.RayStartParams = nil
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{}
				return clone
			},
			expectedError: util.NewInvalidInputError("HeadGroupSpec RayStartParams is empty. Please specify values."),
		},
		{
			name: "A head group with A wrong image pull policy",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.HeadGroupSpec.ImagePullPolicy = "foo"
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{}
				return clone
			},
			expectedError: util.NewInvalidInputError("HeadGroupSpec unsupported value for Image pull policy. Please specify Always or IfNotPresent"),
		},
		{
			name: "An empty worker group",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{}
				return clone
			},
			expectedError: nil,
		},
		{
			name: "Two empty worker group specs",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{
					{},
					{},
				}
				return clone
			},
			expectedError: util.NewInvalidInputError("WorkerNodeSpec 0 group name is empty. Please specify a valid value."),
		},
		{
			name: "A worker group spec without a group name",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{
					{
						GroupName:       "",
						ComputeTemplate: "group-1-template",
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     1,
					},
				}
				return clone
			},
			expectedError: util.NewInvalidInputError("WorkerNodeSpec 0 group name is empty. Please specify a valid value."),
		},
		{
			name: "A worker group spec without a template",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{
					{
						GroupName:       "group 1",
						ComputeTemplate: "",
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     1,
					},
				}
				return clone
			},
			expectedError: util.NewInvalidInputError("WorkerNodeSpec 0 compute template is empty. Please specify a valid value."),
		},
		{
			name: "A worker group spec with 0 max replicas",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{
					{
						GroupName:       "group 1",
						ComputeTemplate: "a template",
						MaxReplicas:     0,
					},
				}
				return clone
			},
			expectedError: util.NewInvalidInputError("WorkerNodeSpec 0 MaxReplicas can not be 0. Please specify a valid value."),
		},
		{
			name: "A worker group spec with invalid min replicas",
			mutate: func(base *api.ClusterSpec) *api.ClusterSpec {
				clone := proto.Clone(base).(*api.ClusterSpec)
				clone.WorkerGroupSpec = []*api.WorkerGroupSpec{
					{
						GroupName:       "group 1",
						ComputeTemplate: "a template",
						MinReplicas:     5,
						MaxReplicas:     1,
					},
				}
				return clone
			},
			expectedError: util.NewInvalidInputError("WorkerNodeSpec 0 MinReplica > MaxReplicas. Please specify a valid value."),
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			specToTest := tc.mutate(baseClusterSpec)
			actualError := server.ValidateClusterSpec(specToTest)
			if tc.expectedError == nil {
				require.NoError(t, actualError, "No error expected.")
			} else {
				require.EqualError(t, actualError, tc.expectedError.Error(), "A matching error is expected")
			}
		})
	}
}

func TestValidateCreateServiceRequest(t *testing.T) {
	// A valid `CreateRayServiceRequest` template for test cases.
	// Each test clones and modifies a field to verify specific validations.
	baseCreateRequest := &api.CreateRayServiceRequest{
		Namespace: "a-namespace",
		Service: &api.RayService{
			Name:                               "a-name",
			Namespace:                          "a-namespace",
			User:                               "a-user",
			ServeConfig_V2:                     "some yaml",
			ServiceUnhealthySecondThreshold:    900,
			DeploymentUnhealthySecondThreshold: 300,
			ClusterSpec: &api.ClusterSpec{
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: "a compute template name",
					EnableIngress:   false,
					RayStartParams: map[string]string{
						"dashboard-host":      "0.0.0.0",
						"metrics-export-port": "8080",
					},
					Volumes: []*api.Volume{},
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "group 1",
						ComputeTemplate: "a-template",
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     1,
					},
				},
			},
		},
	}

	tests := []struct {
		expectedError error
		mutate        func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest // mutate applies a change to the base request to simulate each test scenario
		name          string
	}{
		{
			name: "A valid create service request V2",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				return proto.Clone(base).(*api.CreateRayServiceRequest)
			},
			expectedError: nil,
		},
		{
			name:          "A nil name create service request",
			mutate:        func(_ *api.CreateRayServiceRequest) *api.CreateRayServiceRequest { return nil },
			expectedError: util.NewInvalidInputError("A non nill request is expected"),
		},
		{
			name: "An empty create service request",
			mutate: func(_ *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				return &api.CreateRayServiceRequest{}
			},
			expectedError: util.NewInvalidInputError("Namespace is empty. Please specify a valid value."),
		},
		{
			name: "A create service request with a nill service spec",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service = nil
				return clone
			},
			expectedError: util.NewInvalidInputError("Service is empty, please input a valid payload."),
		},
		{
			name: "A create service request with mismatching namespaces",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service.Namespace = "another-namespace"
				return clone
			},
			expectedError: util.NewInvalidInputError("The namespace in the request is different from the namespace in the service definition."),
		},
		{
			name: "A create service request with no name",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service.Name = ""
				return clone
			},
			expectedError: util.NewInvalidInputError("Service name is empty. Please specify a valid value."),
		},
		{
			name: "A create service request with no user name",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service.User = ""
				return clone
			},
			expectedError: util.NewInvalidInputError("User who created the Service is empty. Please specify a valid value."),
		},
		{
			name: "A create service with no service graph or V2 config",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service.ServeConfig_V2 = ""
				clone.Service.ClusterSpec = nil
				return clone
			},
			expectedError: util.NewInvalidInputError("A ClusterSpec object is required. Please specify one."),
		},
		{
			name: "A create request with no cluster spec",
			mutate: func(base *api.CreateRayServiceRequest) *api.CreateRayServiceRequest {
				clone := proto.Clone(base).(*api.CreateRayServiceRequest)
				clone.Service.ClusterSpec = nil
				return clone
			},
			expectedError: util.NewInvalidInputError("A ClusterSpec object is required. Please specify one."),
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reqToTest := tc.mutate(baseCreateRequest)
			actualError := server.ValidateCreateServiceRequest(reqToTest)
			if tc.expectedError == nil {
				require.NoError(t, actualError, "No error expected.")
			} else {
				require.EqualError(t, actualError, tc.expectedError.Error(), "A matching error is expected")
			}
		})
	}
}

func TestValidateUpdateServiceRequest(t *testing.T) {
	// A valid `UpdateRayServiceRequest` template for test cases.
	// Each test clones and modifies a field to verify specific validations.
	base := &api.UpdateRayServiceRequest{
		Name:      "a-name",
		Namespace: "a-namespace",
		Service: &api.RayService{
			Name:                               "a-name",
			Namespace:                          "a-namespace",
			User:                               "a-user",
			ServeConfig_V2:                     "some yaml",
			ServiceUnhealthySecondThreshold:    900,
			DeploymentUnhealthySecondThreshold: 300,
			ClusterSpec: &api.ClusterSpec{
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: "a compute template name",
					EnableIngress:   false,
					RayStartParams: map[string]string{
						"dashboard-host":      "0.0.0.0",
						"metrics-export-port": "8080",
					},
					Volumes: []*api.Volume{},
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "group 1",
						ComputeTemplate: "a-template",
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     1,
					},
				},
			},
		},
	}

	tests := []struct {
		expectedError error
		mutate        func(r *api.UpdateRayServiceRequest) // mutate applies a change to the base request to simulate each test scenario
		name          string
	}{
		{
			name:          "A valid update service request V2",
			mutate:        func(_ *api.UpdateRayServiceRequest) {},
			expectedError: nil,
		},
		{
			name: "An update service request with no name",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Name = ""
			},
			expectedError: util.NewInvalidInputError("Name is empty. Please specify a valid value."),
		},
		{
			name: "An empty update service request",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Namespace = ""
			},
			expectedError: util.NewInvalidInputError("Namespace is empty. Please specify a valid value."),
		},
		{
			name: "An update service request with a nill service spec",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Service = nil
			},
			expectedError: util.NewInvalidInputError("Service is empty, please input a valid payload."),
		},
		{
			name: "An update service request with mismatching namespaces",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Service.Namespace = "another-namespace"
			},
			expectedError: util.NewInvalidInputError("The namespace in the request is different from the namespace in the service definition."),
		},
		{
			name: "An update service request with no name",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Service.Name = ""
			},
			expectedError: util.NewInvalidInputError("Service name is empty. Please specify a valid value."),
		},
		{
			name: "An update service request with no user name",
			mutate: func(r *api.UpdateRayServiceRequest) {
				r.Service.User = ""
			},
			expectedError: util.NewInvalidInputError("User who updated the Service is empty. Please specify a valid value."),
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := proto.Clone(base).(*api.UpdateRayServiceRequest)
			tc.mutate(req)

			err := server.ValidateUpdateServiceRequest(req)
			if tc.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedError.Error())
			}
		})
	}
}
