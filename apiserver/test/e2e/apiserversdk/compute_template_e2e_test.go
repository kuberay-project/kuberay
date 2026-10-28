package apiserversdk

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

// TestComputeTemplateMiddleware tests that the middleware correctly applies compute template
// resources to RayCluster, RayJob, and RayService resources
func TestComputeTemplateMiddleware(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")

	// Create a compute template with specific resources and tolerations
	templateName := tCtx.GetNextName()
	computeTemplate := &api.ComputeTemplate{
		Name:           templateName,
		Namespace:      tCtx.GetNamespaceName(),
		Cpu:            2,
		Memory:         4,
		Gpu:            1,
		GpuAccelerator: "nvidia.com/gpu",
		ExtendedResources: map[string]uint32{
			"custom.io/special-resource": 2,
		},
		Tolerations: []*api.PodToleration{
			{
				Key:      "ray.io/node-type",
				Operator: "Equal",
				Value:    "worker",
				Effect:   "NoSchedule",
			},
		},
	}

	// Create compute template
	_, _, err = tCtx.GetRayAPIServerClient().CreateComputeTemplate(&api.CreateComputeTemplateRequest{
		ComputeTemplate: computeTemplate,
		Namespace:       tCtx.GetNamespaceName(),
	})
	require.NoError(t, err, "No error expected when creating compute template")

	t.Cleanup(func() {
		tCtx.DeleteComputeTemplate(t, templateName)
	})

	t.Run("RayCluster with compute template", func(t *testing.T) {
		testRayClusterWithComputeTemplate(t, tCtx, templateName, computeTemplate)
	})

	t.Run("RayJob with compute template", func(t *testing.T) {
		testRayJobWithComputeTemplate(t, tCtx, templateName, computeTemplate)
	})

	t.Run("RayService with compute template", func(t *testing.T) {
		testRayServiceWithComputeTemplate(t, tCtx, templateName, computeTemplate)
	})
}

func testRayClusterWithComputeTemplate(t *testing.T, tCtx *End2EndTestingContext, templateName string, computeTemplate *api.ComputeTemplate) {
	clusterName := tCtx.GetNextName()

	// Create RayCluster YAML with compute template references
	rayClusterYAML := fmt.Sprintf(`
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: %s
  namespace: %s
spec:
  headGroupSpec:
    computeTemplate: %s
    template:
      spec:
        containers:
        - name: ray-head
          image: %s
  workerGroupSpecs:
  - groupName: worker-group
    computeTemplate: %s
    replicas: 1
    minReplicas: 1
    maxReplicas: 3
    template:
      spec:
        containers:
        - name: ray-worker
          image: %s
`, clusterName, tCtx.GetNamespaceName(), templateName, tCtx.GetRayImage(), templateName, tCtx.GetRayImage())

	// Send HTTP POST request to apiserver proxy to create RayCluster
	_, err := tCtx.SendYAMLRequest("POST", fmt.Sprintf("/apis/ray.io/v1/namespaces/%s/rayclusters", tCtx.GetNamespaceName()), rayClusterYAML)
	require.NoError(t, err, "No error expected when sending YAML request")

	t.Cleanup(func() {
		tCtx.DeleteRayCluster(t, clusterName)
	})

	// Verify the actual RayCluster was created with correct resources applied by middleware
	actualCluster, err := tCtx.GetRayClusterByName(clusterName)
	require.NoError(t, err, "No error expected when getting ray cluster")

	// Verify head group has correct resources and annotations
	verifyPodSpecResources(t, &actualCluster.Spec.HeadGroupSpec.Template.Spec, "head", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualCluster.Spec.HeadGroupSpec.Template.ObjectMeta, templateName)

	// Verify worker group has correct resources and annotations
	require.Len(t, actualCluster.Spec.WorkerGroupSpecs, 1, "Expected one worker group")
	verifyPodSpecResources(t, &actualCluster.Spec.WorkerGroupSpecs[0].Template.Spec, "worker", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualCluster.Spec.WorkerGroupSpecs[0].Template.ObjectMeta, templateName)
}

func testRayJobWithComputeTemplate(t *testing.T, tCtx *End2EndTestingContext, templateName string, computeTemplate *api.ComputeTemplate) {
	jobName := tCtx.GetNextName()

	// Create RayJob YAML with compute template references
	rayJobYAML := fmt.Sprintf(`
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: %s
  namespace: %s
spec:
  entrypoint: "python -c \"import ray; ray.init(); print('Hello from Ray Job')\""
  rayClusterSpec:
    headGroupSpec:
      computeTemplate: %s
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        spec:
          containers:
          - name: ray-head
            image: %s
    workerGroupSpecs:
    - groupName: worker-group
      computeTemplate: %s
      replicas: 1
      minReplicas: 1
      maxReplicas: 1
      rayStartParams:
        node-ip-address: "$MY_POD_IP"
      template:
        spec:
          containers:
          - name: ray-worker
            image: %s
`, jobName, tCtx.GetNamespaceName(), templateName, tCtx.GetRayImage(), templateName, tCtx.GetRayImage())

	// Send HTTP POST request to apiserver proxy to create RayJob
	_, err := tCtx.SendYAMLRequest("POST", fmt.Sprintf("/apis/ray.io/v1/namespaces/%s/rayjobs", tCtx.GetNamespaceName()), rayJobYAML)
	require.NoError(t, err, "No error expected when sending YAML request")

	t.Cleanup(func() {
		tCtx.DeleteRayJobByName(t, jobName)
	})

	// Verify the actual RayJob was created with correct resources applied by middleware
	actualRayJob, err := tCtx.GetRayJobByName(jobName)
	require.NoError(t, err, "No error expected when getting ray job")

	// Verify head group has correct resources and annotations
	verifyPodSpecResources(t, &actualRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec, "head", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualRayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta, templateName)

	// Verify worker group has correct resources and annotations
	require.Len(t, actualRayJob.Spec.RayClusterSpec.WorkerGroupSpecs, 1, "Expected one worker group")
	verifyPodSpecResources(t, &actualRayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.Spec, "worker", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualRayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.ObjectMeta, templateName)
}

func testRayServiceWithComputeTemplate(t *testing.T, tCtx *End2EndTestingContext, templateName string, computeTemplate *api.ComputeTemplate) {
	serviceName := tCtx.GetNextName()

	// Create RayService YAML with compute template references
	rayServiceYAML := fmt.Sprintf(`
apiVersion: ray.io/v1
kind: RayService
metadata:
  name: %s
  namespace: %s
spec:
  rayClusterConfig:
    headGroupSpec:
      computeTemplate: %s
      rayStartParams:
        dashboard-host: "0.0.0.0"
      template:
        spec:
          containers:
          - name: ray-head
            image: %s
    workerGroupSpecs:
    - groupName: worker-group
      computeTemplate: %s
      replicas: 1
      minReplicas: 1
      maxReplicas: 1
      rayStartParams:
        node-ip-address: "$MY_POD_IP"
      template:
        spec:
          containers:
          - name: ray-worker
            image: %s
`, serviceName, tCtx.GetNamespaceName(), templateName, tCtx.GetRayImage(), templateName, tCtx.GetRayImage())

	// Send HTTP POST request to apiserver proxy to create RayService
	_, err := tCtx.SendYAMLRequest("POST", fmt.Sprintf("/apis/ray.io/v1/namespaces/%s/rayservices", tCtx.GetNamespaceName()), rayServiceYAML)
	require.NoError(t, err, "No error expected when sending YAML request")

	t.Cleanup(func() {
		tCtx.DeleteRayService(t, serviceName)
	})

	// Verify the actual RayService was created with correct resources applied by middleware
	actualRayService, err := tCtx.GetRayServiceByName(serviceName)
	require.NoError(t, err, "No error expected when getting ray service")

	// Verify head group has correct resources and annotations
	verifyPodSpecResources(t, &actualRayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec, "head", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualRayService.Spec.RayClusterSpec.HeadGroupSpec.Template.ObjectMeta, templateName)

	// Verify worker group has correct resources and annotations
	require.Len(t, actualRayService.Spec.RayClusterSpec.WorkerGroupSpecs, 1, "Expected one worker group")
	verifyPodSpecResources(t, &actualRayService.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.Spec, "worker", computeTemplate)
	verifyComputeTemplateAnnotation(t, actualRayService.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.ObjectMeta, templateName)
}

// verifyPodSpecResources verifies that the PodSpec has the expected resources from the compute template
func verifyPodSpecResources(t *testing.T, podSpec *corev1.PodSpec, groupType string, computeTemplate *api.ComputeTemplate) {
	require.NotEmpty(t, podSpec.Containers, "Expected at least one container")

	// Find the ray container (ray-head or ray-worker)
	expectedContainerName := fmt.Sprintf("ray-%s", groupType)
	var rayContainer *corev1.Container
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == expectedContainerName {
			rayContainer = &podSpec.Containers[i]
			break
		}
	}

	require.NotNil(t, rayContainer, "Expected to find ray container with name %s", expectedContainerName)

	// Verify CPU and memory resources
	require.NotNil(t, rayContainer.Resources.Limits, "Expected resource limits")
	require.NotNil(t, rayContainer.Resources.Requests, "Expected resource requests")

	cpuLimit := rayContainer.Resources.Limits[corev1.ResourceCPU]
	cpuRequest := rayContainer.Resources.Requests[corev1.ResourceCPU]
	require.Equal(t, fmt.Sprint(computeTemplate.GetCpu()), cpuLimit.String(), "CPU limit mismatch")
	require.Equal(t, fmt.Sprint(computeTemplate.GetCpu()), cpuRequest.String(), "CPU request mismatch")

	// Check Memory
	expectedMemory := fmt.Sprintf("%dGi", computeTemplate.GetMemory())
	memoryLimit := rayContainer.Resources.Limits[corev1.ResourceMemory]
	memoryRequest := rayContainer.Resources.Requests[corev1.ResourceMemory]
	require.Equal(t, expectedMemory, memoryLimit.String(), "Expected memory limit to be 4Gi")
	require.Equal(t, expectedMemory, memoryRequest.String(), "Expected memory request to be 4Gi")

	// Check GPU
	gpuLimit := rayContainer.Resources.Limits["nvidia.com/gpu"]
	gpuRequest := rayContainer.Resources.Requests["nvidia.com/gpu"]
	require.Equal(t, fmt.Sprint(computeTemplate.GetGpu()), gpuLimit.String(), "Expected GPU limit to be 1")
	require.Equal(t, fmt.Sprint(computeTemplate.GetGpu()), gpuRequest.String(), "Expected GPU request to be 1")

	// Check extended resources
	for name, val := range computeTemplate.ExtendedResources {
		extResourceLimit := rayContainer.Resources.Limits[corev1.ResourceName(name)]
		extResourceRequest := rayContainer.Resources.Requests[corev1.ResourceName(name)]
		require.Equal(t, fmt.Sprint(val), extResourceLimit.String(), "Expected extended resource limit to be 2")
		require.Equal(t, fmt.Sprint(val), extResourceRequest.String(), "Expected extended resource request to be 2")
	}

	// Verify tolerations are applied to the pod spec
	require.Len(t, podSpec.Tolerations, len(computeTemplate.Tolerations), "Toleration count mismatch")
	for i, toleration := range podSpec.Tolerations {
		expectedToleration := computeTemplate.Tolerations[i]
		require.Equal(t, expectedToleration.Key, toleration.Key, "Expected toleration key to be ray.io/node-type")
		require.Equal(t, corev1.TolerationOperator(expectedToleration.Operator), toleration.Operator, "Expected toleration operator to be Equal")
		require.Equal(t, expectedToleration.Value, toleration.Value, "Expected toleration value to be worker")
		require.Equal(t, corev1.TaintEffect(expectedToleration.Effect), toleration.Effect, "Expected toleration effect to be NoSchedule")

	}
}

// verifyComputeTemplateAnnotation verifies that the compute template annotation is set correctly
func verifyComputeTemplateAnnotation(t *testing.T, objMeta metav1.ObjectMeta, expectedTemplateName string) {
	require.NotNil(t, objMeta.Annotations, "Expected annotations to be set")
	actualTemplateName := objMeta.Annotations[util.RayClusterComputeTemplateAnnotationKey]
	require.Equal(t, expectedTemplateName, actualTemplateName, "Expected compute template annotation to be set correctly")
}
