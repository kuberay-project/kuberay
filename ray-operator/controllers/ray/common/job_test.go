package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func rayJobTemplate() *rayv1.RayJob {
	return &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RuntimeEnvYAML: "test: test",
			Metadata: map[string]string{
				"testKey": "testValue",
			},
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion: "2.6.0",
			},
			Entrypoint:          "echo no quote 'single quote' \"double quote\"",
			EntrypointNumCpus:   1,
			EntrypointNumGpus:   0.5,
			EntrypointResources: `{"Custom_1": 1, "Custom_2": 5.5}`,
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "testJobId",
		},
	}
}

func TestGetRuntimeEnvJsonFromBase64(t *testing.T) {
	testRayJob := rayJobTemplate()
	expected := `{"test":"test"}`
	jsonOutput, err := getRuntimeEnvJson(testRayJob)
	require.NoError(t, err)
	assert.JSONEq(t, expected, jsonOutput)
}

func TestGetRuntimeEnvJsonFromYAML(t *testing.T) {
	rayJobWithYAML := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RuntimeEnvYAML: `
working_dir: "https://github.com/ray-project/serve_config_examples/archive/b393e77bbd6aba0881e3d94c05f968f05a387b96.zip"
pip: ["python-multipart==0.0.6"]
`,
		},
	}
	expectedJSON := `{"working_dir":"https://github.com/ray-project/serve_config_examples/archive/b393e77bbd6aba0881e3d94c05f968f05a387b96.zip","pip":["python-multipart==0.0.6"]}`
	jsonOutput, err := getRuntimeEnvJson(rayJobWithYAML)
	require.NoError(t, err)

	var expectedMap map[string]any
	var actualMap map[string]any

	// Convert the JSON strings into map types to avoid errors due to ordering
	require.NoError(t, json.Unmarshal([]byte(expectedJSON), &expectedMap))
	require.NoError(t, json.Unmarshal([]byte(jsonOutput), &actualMap))

	// Now compare the maps
	assert.Equal(t, expectedMap, actualMap)
}

func TestBuildJobSubmitCommandWithK8sJobMode(t *testing.T) {
	testRayJob := rayJobTemplate()
	expected := []string{
		"until",
		fmt.Sprintf(utils.K8sJobDashboardHealthCommand, utils.RayDashboardGCSHealthPath, utils.RayDashboardGCSHealthCheckTimeoutSeconds, "http://127.0.0.1:8265"),
		">/dev/null", "2>&1", ";",
		"do", "echo", strconv.Quote("Waiting for Ray Dashboard GCS to become healthy at http://127.0.0.1:8265 ..."), ";", "sleep", "2", ";", "done", ";",
		"if",
		"!", "ray", "job", "status", "--address", "http://127.0.0.1:8265", "testJobId", ">/dev/null", "2>&1",
		";", "then",
		"ray", "job", "submit", "--address", "http://127.0.0.1:8265", "--no-wait",
		"--runtime-env-json", strconv.Quote(`{"test":"test"}`),
		"--metadata-json", strconv.Quote(`{"testKey":"testValue"}`),
		"--submission-id", "testJobId",
		"--entrypoint-num-cpus", "1.000000",
		"--entrypoint-num-gpus", "0.500000",
		"--entrypoint-resources", strconv.Quote(`{"Custom_1": 1, "Custom_2": 5.5}`),
		"--",
		"echo no quote 'single quote' \"double quote\"",
		";", "fi", ";",
		"ray", "job", "logs", "--address", "http://127.0.0.1:8265", "--follow", "testJobId",
	}
	command, err := BuildJobSubmitCommand(testRayJob, rayv1.K8sJobMode)
	require.NoError(t, err)
	assert.Equal(t, expected, command)
}

func TestBuildJobSubmitCommandWithSidecarMode(t *testing.T) {
	testRayJob := rayJobTemplate()
	testRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{
		{
			Ports: []corev1.ContainerPort{
				{
					Name:          utils.DashboardPortName,
					ContainerPort: utils.DefaultDashboardPort,
				},
			},
		},
	}

	expected := []string{
		"until",
		fmt.Sprintf(
			utils.BasePythonHealthCommand,
			fmt.Sprintf("http://localhost:%d/%s", utils.DefaultDashboardPort, utils.RayDashboardGCSHealthPath),
			utils.RayDashboardGCSHealthCheckTimeoutSeconds,
		),
		">/dev/null", "2>&1", ";",
		"do", "echo", strconv.Quote("Waiting for Ray Dashboard GCS to become healthy at http://127.0.0.1:8265 ..."), ";", "sleep", "2", ";", "done", ";",
		"ray", "job", "submit", "--address", "http://127.0.0.1:8265",
		"--runtime-env-json", strconv.Quote(`{"test":"test"}`),
		"--metadata-json", strconv.Quote(`{"testKey":"testValue"}`),
		"--submission-id", "testJobId",
		"--entrypoint-num-cpus", "1.000000",
		"--entrypoint-num-gpus", "0.500000",
		"--entrypoint-resources", strconv.Quote(`{"Custom_1": 1, "Custom_2": 5.5}`),
		"--",
		"echo no quote 'single quote' \"double quote\"",
		";",
	}
	command, err := BuildJobSubmitCommand(testRayJob, rayv1.SidecarMode)
	require.NoError(t, err)
	assert.Equal(t, expected, command)
}

func TestBuildJobSubmitCommandWithSidecarModeVersionSwitch(t *testing.T) {
	tests := []struct {
		name       string
		rayVersion string
	}{
		{
			name:       "uses python health command for ray >= 2.53",
			rayVersion: "2.53.0",
		},
		{
			name:       "uses python health command for ray < 2.53",
			rayVersion: "2.52.1",
		},
		{
			name:       "uses python health command when rayVersion is invalid",
			rayVersion: "invalid-version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRayJob := rayJobTemplate()
			testRayJob.Spec.RayClusterSpec.RayVersion = tt.rayVersion
			// Avoid metadata-json version parsing failure; this test only checks health command selection.
			testRayJob.Spec.Metadata = nil
			testRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{
				{
					Ports: []corev1.ContainerPort{
						{
							Name:          utils.DashboardPortName,
							ContainerPort: utils.DefaultDashboardPort,
						},
					},
				},
			}
			command, err := BuildJobSubmitCommand(testRayJob, rayv1.SidecarMode)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(command), 2)
			assert.Equal(t, "until", command[0])
			assert.Contains(t, command[1], "python -c")
			assert.Contains(t, command[1], utils.RayDashboardGCSHealthPath)
			assert.NotContains(t, command[1], "wget")
		})
	}
}

func TestBuildJobSubmitCommandWithSidecarModeCustomDashboardPort(t *testing.T) {
	testRayJob := rayJobTemplate()
	const customPort = 9000
	testRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{
		{
			Ports: []corev1.ContainerPort{
				{
					Name:          utils.DashboardPortName,
					ContainerPort: customPort,
				},
			},
		},
	}
	command, err := BuildJobSubmitCommand(testRayJob, rayv1.SidecarMode)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(command), 2)
	assert.Equal(t, "until", command[0])
	assert.Contains(t, command[1], fmt.Sprintf("localhost:%d/%s", customPort, utils.RayDashboardGCSHealthPath))
	assert.Contains(t, command[1], "python -c")
	assert.NotContains(t, command[1], "wget")
}

func TestBuildJobSubmitCommandWithK8sJobModeHealthWaitLoop(t *testing.T) {
	testRayJob := rayJobTemplate()
	command, err := BuildJobSubmitCommand(testRayJob, rayv1.K8sJobMode)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(command), 2)
	assert.Equal(t, "until", command[0])
	assert.Contains(t, command[1], "python -c")
	assert.Contains(t, command[1], utils.RayDashboardGCSHealthPath)
	assert.Contains(t, command[1], "127.0.0.1:8265")
	assert.NotContains(t, command[1], "wget")
}

func TestBuildJobSubmitCommandWithK8sJobModeDashboardAddressOverrides(t *testing.T) {
	_, err := exec.LookPath("python")
	require.NoError(t, err, "python is required to execute the generated health probe")

	tests := []struct {
		name         string
		env          map[string]string
		expectedPath string
	}{
		{"fallback", nil, "/fallback/api/gcs_healthz"},
		{"Ray address", map[string]string{"RAY_ADDRESS": "/ray"}, "/ray/api/gcs_healthz"},
		{"API server address", map[string]string{"RAY_API_SERVER_ADDRESS": "/api"}, "/api/api/gcs_healthz"},
		{"API server takes precedence", map[string]string{"RAY_ADDRESS": "/ray", "RAY_API_SERVER_ADDRESS": "/api"}, "/api/api/gcs_healthz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc(tt.expectedPath, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("success"))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			t.Setenv("RAY_ADDRESS", "")
			t.Setenv("RAY_API_SERVER_ADDRESS", "")
			t.Setenv("no_proxy", "*")
			for name, path := range tt.env {
				t.Setenv(name, server.URL+path)
			}

			// Only the expected address returns success; other paths return HTTP 404.
			rayJob := rayJobTemplate()
			rayJob.Status.DashboardURL = server.URL + "/fallback"
			command, err := BuildJobSubmitCommand(rayJob, rayv1.K8sJobMode)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(command), 2)
			//nolint:gosec // G204: intentionally execute the generated probe using only a fixed template and the local test server URL.
			cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", "exec "+command[1])
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestBuildJobSubmitCommandWithSidecarModeAndFeatureGate(t *testing.T) {
	// Enable the SidecarSubmitterRestart feature gate for this test
	features.SetFeatureGateDuringTest(t, features.SidecarSubmitterRestart, true)

	testRayJob := rayJobTemplate()
	testRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers = []corev1.Container{
		{
			Ports: []corev1.ContainerPort{
				{
					Name:          utils.DashboardPortName,
					ContainerPort: utils.DefaultDashboardPort,
				},
			},
		},
	}

	// With SidecarSubmitterRestart enabled, the command should include:
	// - status check (if ! ray job status ...)
	// - --no-wait flag
	// - job logs follow at the end
	healthURL := fmt.Sprintf("http://localhost:%d/%s", utils.DefaultDashboardPort, utils.RayDashboardGCSHealthPath)
	expected := []string{
		"until",
		fmt.Sprintf(
			utils.BasePythonHealthCommand,
			healthURL,
			utils.RayDashboardGCSHealthCheckTimeoutSeconds,
		),
		">/dev/null", "2>&1", ";",
		"do", "echo", strconv.Quote("Waiting for Ray Dashboard GCS to become healthy at http://127.0.0.1:8265 ..."), ";", "sleep", "2", ";", "done", ";",
		"if", "!", "ray", "job", "status", "--address", "http://127.0.0.1:8265", "testJobId", ">/dev/null", "2>&1", ";", "then",
		"ray", "job", "submit", "--address", "http://127.0.0.1:8265", "--no-wait",
		"--runtime-env-json", strconv.Quote(`{"test":"test"}`),
		"--metadata-json", strconv.Quote(`{"testKey":"testValue"}`),
		"--submission-id", "testJobId",
		"--entrypoint-num-cpus", "1.000000",
		"--entrypoint-num-gpus", "0.500000",
		"--entrypoint-resources", strconv.Quote(`{"Custom_1": 1, "Custom_2": 5.5}`),
		"--",
		"echo no quote 'single quote' \"double quote\"",
		";", "fi", ";",
		"ray", "job", "logs", "--address", "http://127.0.0.1:8265", "--follow", "testJobId",
	}
	command, err := BuildJobSubmitCommand(testRayJob, rayv1.SidecarMode)
	require.NoError(t, err)
	assert.Equal(t, expected, command)
}

func TestBuildJobSubmitCommandWithK8sJobModeAndYAML(t *testing.T) {
	rayJobWithYAML := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RuntimeEnvYAML: `
working_dir: "https://github.com/ray-project/serve_config_examples/archive/b393e77bbd6aba0881e3d94c05f968f05a387b96.zip"
pip: ["python-multipart==0.0.6"]
`,
			Metadata: map[string]string{
				"testKey": "testValue",
			},
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion: "2.6.0",
			},
			Entrypoint: "echo no quote 'single quote' \"double quote\"",
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "testJobId",
		},
	}
	expected := []string{
		"until",
		fmt.Sprintf(utils.K8sJobDashboardHealthCommand, utils.RayDashboardGCSHealthPath, utils.RayDashboardGCSHealthCheckTimeoutSeconds, "http://127.0.0.1:8265"),
		">/dev/null", "2>&1", ";",
		"do", "echo", strconv.Quote("Waiting for Ray Dashboard GCS to become healthy at http://127.0.0.1:8265 ..."), ";", "sleep", "2", ";", "done", ";",
		"if",
		"!", "ray", "job", "status", "--address", "http://127.0.0.1:8265", "testJobId", ">/dev/null", "2>&1",
		";", "then",
		"ray", "job", "submit", "--address", "http://127.0.0.1:8265", "--no-wait",
		"--runtime-env-json", strconv.Quote(`{"working_dir":"https://github.com/ray-project/serve_config_examples/archive/b393e77bbd6aba0881e3d94c05f968f05a387b96.zip","pip":["python-multipart==0.0.6"]}`),
		"--metadata-json", strconv.Quote(`{"testKey":"testValue"}`),
		"--submission-id", "testJobId",
		"--",
		"echo no quote 'single quote' \"double quote\"",
		";", "fi", ";",
		"ray", "job", "logs", "--address", "http://127.0.0.1:8265", "--follow", "testJobId",
	}
	command, err := BuildJobSubmitCommand(rayJobWithYAML, rayv1.K8sJobMode)
	require.NoError(t, err)

	// Ensure the slices are the same length.
	assert.Len(t, command, len(expected))

	for i := 0; i < len(expected); i++ {
		// For non-JSON elements, compare them directly.
		assert.Equal(t, expected[i], command[i])
		if expected[i] == "--runtime-env-json" {
			// Decode the JSON string from the next element.
			var expectedMap, actualMap map[string]any
			//nolint:gosec // G602: test invariant guarantees "--runtime-env-json" is followed by a value.
			unquoteExpected, err1 := strconv.Unquote(expected[i+1])
			require.NoError(t, err1)

			unquotedCommand, err2 := strconv.Unquote(command[i+1])
			require.NoError(t, err2)
			err1 = json.Unmarshal([]byte(unquoteExpected), &expectedMap)
			err2 = json.Unmarshal([]byte(unquotedCommand), &actualMap)

			// If there's an error decoding either JSON string, it's an error in the test.
			require.NoError(t, err1)
			require.NoError(t, err2)

			// Compare the maps directly to avoid errors due to ordering.
			assert.Equal(t, expectedMap, actualMap)

			// Skip the next element because we've just checked it.
			i++
		}
	}
}

// TestBuildJobSubmitCommandWithClusterSelector verifies that metadata is included
// when submitting a RayJob to an existing cluster via clusterSelector (no RayClusterSpec,
// so we can't know the Ray version without looking up the RayCluster; we assume >= 2.6).
func TestBuildJobSubmitCommandWithClusterSelector(t *testing.T) {
	rayJobWithClusterSelector := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			ClusterSelector: map[string]string{
				"ray.io/cluster": "existing-cluster",
			},
			Metadata: map[string]string{
				"tenant": "tenant1",
				"team":   "ml-platform",
			},
			Entrypoint: "python /app/batch_inference.py",
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://existing-cluster-head-svc:8265",
			JobId:        "cluster-selector-job-id",
		},
	}

	command, err := BuildJobSubmitCommand(rayJobWithClusterSelector, rayv1.K8sJobMode)
	require.NoError(t, err)

	hasMetadataFlag := false
	for i, arg := range command {
		if arg == "--metadata-json" {
			hasMetadataFlag = true
			require.Greater(t, len(command), i+1)
			unquoted, err := strconv.Unquote(command[i+1])
			require.NoError(t, err)
			var metadata map[string]string
			require.NoError(t, json.Unmarshal([]byte(unquoted), &metadata))
			assert.Equal(t, "tenant1", metadata["tenant"])
			assert.Equal(t, "ml-platform", metadata["team"])
			break
		}
	}
	assert.True(t, hasMetadataFlag, "metadata-json flag should be present when using clusterSelector with metadata")
}

// TestBuildJobSubmitCommandWithUnparseableRayVersion verifies that metadata is
// rejected when RayJob.Spec.RayClusterSpec.RayVersion is set to a non-semver string. A user
// who went to the trouble of typing a version should fail fast rather than silently proceed.
func TestBuildJobSubmitCommandWithUnparseableRayVersion(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion: "not-a-version",
			},
			Metadata: map[string]string{
				"testKey": "testValue",
			},
			Entrypoint: "echo hello",
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "testJobId",
		},
	}
	_, err := BuildJobSubmitCommand(rayJob, rayv1.K8sJobMode)
	require.Error(t, err)
}

// TestBuildJobSubmitCommandWithOldRayVersion verifies that metadata is
// rejected when RayJob.Spec.RayClusterSpec.RayVersion is explicitly set below 2.6.0.
func TestBuildJobSubmitCommandWithOldRayVersion(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion: "2.5.0",
			},
			Metadata: map[string]string{
				"testKey": "testValue",
			},
			Entrypoint: "echo hello",
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "testJobId",
		},
	}
	_, err := BuildJobSubmitCommand(rayJob, rayv1.K8sJobMode)
	require.Error(t, err)
}

// TestBuildJobSubmitCommandWithUnsetRayVersion verifies that
// metadata is still included when RayClusterSpec is present but RayVersion is empty. We assume
// the cluster is >= 2.6.0 unless the user explicitly sets a lower version.
func TestBuildJobSubmitCommandWithUnsetRayVersion(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{},
			Metadata: map[string]string{
				"testKey": "testValue",
			},
			Entrypoint: "echo hello",
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "testJobId",
		},
	}
	command, err := BuildJobSubmitCommand(rayJob, rayv1.K8sJobMode)
	require.NoError(t, err)

	hasMetadataFlag := false
	for i, arg := range command {
		if arg == "--metadata-json" {
			hasMetadataFlag = true
			require.Greater(t, len(command), i+1)
			unquoted, err := strconv.Unquote(command[i+1])
			require.NoError(t, err)
			var metadata map[string]string
			require.NoError(t, json.Unmarshal([]byte(unquoted), &metadata))
			assert.Equal(t, "testValue", metadata["testKey"])
			break
		}
	}
	assert.True(t, hasMetadataFlag, "metadata-json flag should be present when RayVersion is unset")
}

func TestGetSubmitterTemplate(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{},
	}
	rayCluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "head-sa",
						ImagePullSecrets: []corev1.LocalObjectReference{
							{Name: "head-secret"},
						},
						Containers: []corev1.Container{
							{
								Image:           "rayproject/ray:test-submitter-template",
								ImagePullPolicy: corev1.PullAlways,
							},
						},
					},
				},
			},
		},
	}
	template := GetSubmitterTemplate(&rayJob.Spec, &rayCluster.Spec)
	assert.Equal(t, template.Spec.Containers[0].Image, rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Image)
	assert.Equal(t, corev1.PullAlways, template.Spec.Containers[0].ImagePullPolicy)
	assert.Equal(t, "head-secret", template.Spec.ImagePullSecrets[0].Name)
}

func TestGetSubmitterPodTemplate(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{},
	}
	rayCluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: "head-sa",
						ImagePullSecrets: []corev1.LocalObjectReference{
							{Name: "head-secret"},
						},
						Containers: []corev1.Container{
							{
								Image:           "my-custom-image:latest",
								ImagePullPolicy: corev1.PullAlways,
							},
						},
					},
				},
			},
		},
	}
	rayJob.Spec.SubmitterPodTemplate = &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"my-label": "true"},
			Annotations: map[string]string{"my-annotation": "true"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Image: "rayproject/ray:test-submitter-template",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("250m"),
						},
					},
				},
			},
		},
	}
	template := GetSubmitterTemplate(&rayJob.Spec, &rayCluster.Spec)
	assert.Equal(t, "rayproject/ray:test-submitter-template", template.Spec.Containers[0].Image)
	assert.Equal(t, "true", template.ObjectMeta.Labels["my-label"])
	assert.Equal(t, "true", template.ObjectMeta.Annotations["my-annotation"])
	assert.Equal(t, resource.MustParse("250m"), template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU])

	// Verify that custom SubmitterPodTemplate is returned as-is without merging/overriding from Head Pod
	assert.Equal(t, corev1.PullPolicy(""), template.Spec.Containers[0].ImagePullPolicy)
	assert.Empty(t, template.Spec.ImagePullSecrets)
}
