package e2e

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	kuberayHTTP "github.com/ray-project/kuberay/apiserver/pkg/http"
	api "github.com/ray-project/kuberay/proto/go_client"
)

// TestCreateTemplate sequentially iterates over the create compute endpoint
// to validate various input scenarios
func TestCreateTemplate(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	tests := []GenericEnd2EndTest[*api.CreateComputeTemplateRequest]{
		{
			Name: "Create a valid compute template",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      tCtx.GetComputeTemplateName(),
					Namespace: tCtx.GetNamespaceName(),
					Cpu:       2,
					Memory:    4,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Create an invalid template with no Name",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      "",
					Namespace: tCtx.GetNamespaceName(),
					Cpu:       2,
					Memory:    4,
					Gpu:       0,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create an invalid template with different namespace",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      tCtx.GetComputeTemplateName(),
					Namespace: "another",
					Cpu:       2,
					Memory:    4,
					Gpu:       0,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create an invalid template with zero cpu",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      tCtx.GetComputeTemplateName(),
					Namespace: tCtx.GetNamespaceName(),
					Cpu:       0,
					Memory:    4,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create an invalid template with zero memory",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      tCtx.GetComputeTemplateName(),
					Namespace: tCtx.GetNamespaceName(),
					Cpu:       2,
					Memory:    0,
				},
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Create a duplicate invalid",
			Input: &api.CreateComputeTemplateRequest{
				ComputeTemplate: &api.ComputeTemplate{
					Name:      tCtx.GetComputeTemplateName(),
					Namespace: tCtx.GetNamespaceName(),
					Cpu:       2,
					Memory:    0,
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
			actualTemplate, actualRPCStatus, err := tCtx.GetRayAPIServerClient().CreateComputeTemplate(tc.Input)
			if tc.ExpectedError != nil {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			} else {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				if tc.Input.ComputeTemplate.MemoryUnit == "" {
					require.Equal(t, "Gi", actualTemplate.MemoryUnit, "Default MemoryUnit should be Gi")
					// Copy tc.Input.ComputeTemplate to the expected template with the default MemoryUnit
					expected := &api.ComputeTemplate{
						Name:              tc.Input.ComputeTemplate.Name,
						Cpu:               tc.Input.ComputeTemplate.Cpu,
						Memory:            tc.Input.ComputeTemplate.Memory,
						Namespace:         tc.Input.ComputeTemplate.Namespace,
						Gpu:               tc.Input.ComputeTemplate.Gpu,
						GpuAccelerator:    tc.Input.ComputeTemplate.GpuAccelerator,
						Tolerations:       tc.Input.ComputeTemplate.Tolerations,
						ExtendedResources: tc.Input.ComputeTemplate.ExtendedResources,
						MemoryUnit:        "Gi",
					}
					t.Logf("Expected template: %+v", expected)
					t.Logf("Actual template: %+v", actualTemplate)
					require.Truef(t, reflect.DeepEqual(expected, actualTemplate), "Equal templates expected (with default MemoryUnit)")
				} else {
					require.Truef(t, reflect.DeepEqual(tc.Input.ComputeTemplate, actualTemplate), "Equal templates expected")
				}
			}
		})
	}
}

// TestDeleteTemplate sequentially iterates over the delete compute template endpoint
// to validate various input scenarios
func TestDeleteTemplate(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)

	tests := []GenericEnd2EndTest[*api.DeleteComputeTemplateRequest]{
		{
			Name: "Delete existing template",
			Input: &api.DeleteComputeTemplateRequest{
				Name:      tCtx.GetComputeTemplateName(),
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Delete non existing template",
			Input: &api.DeleteComputeTemplateRequest{
				Name:      "another-template",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Delete a template with no name",
			Input: &api.DeleteComputeTemplateRequest{
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Delete a template with no namespace",
			Input: &api.DeleteComputeTemplateRequest{
				Name: "some-name",
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
	}
	// Execute tests sequentially
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			actualRPCStatus, err := tCtx.GetRayAPIServerClient().DeleteComputeTemplate(tc.Input)
			if tc.ExpectedError != nil {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			} else {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
			}
		})
	}
}

// TestGetAllTheTemplates tests gets all compute templates endpoint
// to validate various input scenarios
func TestGetAllComputeTemplates(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetAllComputeTemplates()
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.ComputeTemplates, "A list of compute templates is required")
	foundName := false
	for _, template := range response.ComputeTemplates {
		if tCtx.GetComputeTemplateName() == template.Name && tCtx.GetNamespaceName() == template.Namespace {
			foundName = true
			break
		}
	}
	require.True(t, foundName)
}

// TestGetTemplatesInNamespace get all compute templates in namespace endpoint
// to validate various input scenarios
func TestGetTemplatesInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)
	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t)
	})

	response, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetAllComputeTemplatesInNamespace(
		&api.ListComputeTemplatesRequest{
			Namespace: tCtx.GetNamespaceName(),
		})
	require.NoError(t, err, "No error expected")
	require.Nil(t, actualRPCStatus, "No RPC status expected")
	require.NotNil(t, response, "A response is expected")
	require.NotEmpty(t, response.ComputeTemplates, "A list of compute templates is required")
	foundName := false
	for _, template := range response.ComputeTemplates {
		if tCtx.GetComputeTemplateName() == template.Name && tCtx.GetNamespaceName() == template.Namespace {
			foundName = true
			break
		}
	}
	require.True(t, foundName)
}

// TestDeleteTemplate sequentially iterates over the delete compute template endpoint
// to validate various input scenarios
func TestGetTemplateByNameInNamespace(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	tCtx.CreateComputeTemplate(t)

	tests := []GenericEnd2EndTest[*api.GetComputeTemplateRequest]{
		{
			Name: "Get template by Name in a namespace",
			Input: &api.GetComputeTemplateRequest{
				Name:      tCtx.GetComputeTemplateName(),
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: nil,
		},
		{
			Name: "Get non existing template",
			Input: &api.GetComputeTemplateRequest{
				Name:      "another-template",
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusNotFound,
			},
		},
		{
			Name: "Get a template with no Name",
			Input: &api.GetComputeTemplateRequest{
				Namespace: tCtx.GetNamespaceName(),
			},
			ExpectedError: &kuberayHTTP.KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusBadRequest,
			},
		},
		{
			Name: "Get a template with no namespace",
			Input: &api.GetComputeTemplateRequest{
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
			actualTemplate, actualRPCStatus, err := tCtx.GetRayAPIServerClient().GetComputeTemplate(tc.Input)
			if tc.ExpectedError != nil {
				require.EqualError(t, err, tc.ExpectedError.Error(), "Matching error expected")
				require.NotNil(t, actualRPCStatus, "A not nill RPC status is required")
			} else {
				require.NoError(t, err, "No error expected")
				require.Nil(t, actualRPCStatus, "No RPC status expected")
				require.Equal(t, tCtx.GetComputeTemplateName(), actualTemplate.Name)
				require.Equal(t, tCtx.GetNamespaceName(), actualTemplate.Namespace)
			}
		})
	}
}
