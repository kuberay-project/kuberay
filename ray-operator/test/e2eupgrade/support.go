package e2eupgrade

import (
	"os/exec"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	e2e "github.com/ray-project/kuberay/ray-operator/test/e2erayservice"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func ProcessStateSuccess(cmd *exec.Cmd) bool {
	if cmd.ProcessState == nil {
		return false
	}
	return cmd.ProcessState.Success()
}

func requestRayService(t Test, rayService *rayv1.RayService, curlPod *corev1.Pod, curlContainerName string) string {
	stdout1, _ := e2e.CurlRayServicePod(t, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	stdout2, _ := e2e.CurlRayServicePod(t, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)

	return stdout1.String() + ", " + stdout2.String()
}

func rayServiceSampleYamlApplyConfigurationWithWorker() *rayv1ac.RayServiceSpecApplyConfiguration {
	return rayv1ac.RayServiceSpec().WithServeConfigV2(`applications:
      - name: fruit_app
        import_path: fruit.deployment_graph
        route_prefix: /fruit
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
        deployments:
          - name: MangoStand
            num_replicas: 2
            max_replicas_per_node: 1
            user_config:
              price: 3
            ray_actor_options:
              num_cpus: 0.1
          - name: OrangeStand
            num_replicas: 1
            user_config:
              price: 2
            ray_actor_options:
              num_cpus: 0.1
          - name: FruitMarket
            num_replicas: 1
            ray_actor_options:
              num_cpus: 0.1
      - name: math_app
        import_path: conditional_dag.serve_dag
        route_prefix: /calc
        runtime_env:
          working_dir: "https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip"
        deployments:
          - name: Adder
            num_replicas: 1
            user_config:
              increment: 3
            ray_actor_options:
              num_cpus: 0.1
          - name: Multiplier
            num_replicas: 1
            user_config:
              factor: 5
            ray_actor_options:
              num_cpus: 0.1
          - name: Router
            ray_actor_options:
              num_cpus: 0.1
            num_replicas: 1`).
		WithRayClusterSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(corev1ac.PodTemplateSpec().
					WithSpec(corev1ac.PodSpec().
						WithContainers(corev1ac.Container().
							WithName("ray-head").
							WithImage(GetRayImage()).
							WithPorts(
								corev1ac.ContainerPort().WithName(utils.GcsServerPortName).WithContainerPort(utils.DefaultGcsServerPort),
								corev1ac.ContainerPort().WithName(utils.ServingPortName).WithContainerPort(utils.DefaultServingPort),
								corev1ac.ContainerPort().WithName(utils.DashboardPortName).WithContainerPort(utils.DefaultDashboardPort),
								corev1ac.ContainerPort().WithName(utils.ClientPortName).WithContainerPort(utils.DefaultClientPort),
							).
							WithResources(corev1ac.ResourceRequirements().
								WithRequests(corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								}).
								WithLimits(corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("2"),
									corev1.ResourceMemory: resource.MustParse("3Gi"),
								})))))).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(1).
				WithGroupName("ray-worker-1").
				WithMinReplicas(1).
				WithMaxReplicas(1).
				WithRayStartParams(map[string]string{"num-cpus": "1"}).
				WithTemplate(corev1ac.PodTemplateSpec().
					WithSpec(corev1ac.PodSpec().
						WithContainers(corev1ac.Container().
							WithName("ray-worker").
							WithImage(GetRayImage()).
							WithResources(corev1ac.ResourceRequirements().
								WithRequests(corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("1Gi"),
								}).
								WithLimits(corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: resource.MustParse("2Gi"),
								}),
							),
						),
					),
				),
			))
}
