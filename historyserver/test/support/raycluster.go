package support

import (
	"fmt"
	"strings"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	RayClusterManifestPath = "../testdata/raycluster.yaml"

	// RayVersionForTokenAuth is the minimum rayVersion the operator accepts for token auth.
	RayVersionForTokenAuth = "2.52.0"
)

// ApplyRayClusterWithCollectorWithEnvs deploys a Ray cluster with the collector sidecar into the test namespace,
// adding the specified environment variables to the head pod.
func ApplyRayClusterWithCollectorWithEnvs(test Test, g *WithT, namespace *corev1.Namespace, envs map[string]string) *rayv1.RayCluster {
	return applyRayClusterWithCollector(test, g, namespace, func(rayCluster *rayv1.RayCluster) {
		headContainer := &rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex]
		if len(headContainer.Env) == 0 {
			headContainer.Env = []corev1.EnvVar{}
		}

		for key, value := range envs {
			env := corev1.EnvVar{
				Name:  key,
				Value: value,
			}
			headContainer.Env = append(headContainer.Env, env)
		}
	})
}

// ApplyRayClusterWithCollectorTokenAuth deploys a Ray cluster whose Dashboard requires token
// authentication, and wires the collector sidecars to the auth token.
func ApplyRayClusterWithCollectorTokenAuth(test Test, g *WithT, namespace *corev1.Namespace) *rayv1.RayCluster {
	return applyRayClusterWithCollector(test, g, namespace, func(rayCluster *rayv1.RayCluster) {
		rayCluster.Spec.RayVersion = RayVersionForTokenAuth
		rayCluster.Spec.AuthOptions = &rayv1.AuthOptions{Mode: rayv1.AuthModeToken}

		// reconcileAuthSecret generates this Secret when authOptions.secretName is unset.
		secretName := utils.CheckName(rayCluster.Name)
		injectCollectorAuthToken(rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers, secretName)
		for wg := range rayCluster.Spec.WorkerGroupSpecs {
			injectCollectorAuthToken(rayCluster.Spec.WorkerGroupSpecs[wg].Template.Spec.Containers, secretName)
		}
	})
}

func applyRayClusterWithCollector(test Test, g *WithT, namespace *corev1.Namespace, mutate func(*rayv1.RayCluster)) *rayv1.RayCluster {
	rayClusterFromYaml := DeserializeRayClusterYAML(test, RayClusterManifestPath)
	rayClusterFromYaml.Namespace = namespace.Name

	mutate(rayClusterFromYaml)

	// Inject namespace name as the ray-cluster-namespace for head group collector
	injectCollectorRayClusterNamespaceAndEnvVar(rayClusterFromYaml.Spec.HeadGroupSpec.Template.Spec.Containers, rayClusterFromYaml.Name, namespace.Name)

	// Inject namespace name as the ray-cluster-namespace for worker group collectors
	for wg := range rayClusterFromYaml.Spec.WorkerGroupSpecs {
		injectCollectorRayClusterNamespaceAndEnvVar(rayClusterFromYaml.Spec.WorkerGroupSpecs[wg].Template.Spec.Containers, rayClusterFromYaml.Name, namespace.Name)
	}

	rayCluster, err := test.Client().Ray().RayV1().
		RayClusters(namespace.Name).
		Create(test.Ctx(), rayClusterFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Waiting for head pod of RayCluster %s/%s to be running and ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		Should(WithTransform(IsPodRunningAndReady, BeTrue()))

	return rayCluster
}

// injectCollectorRayClusterNamespaceAndEnvVar injects the ray-cluster-namespace and required environment variables (RAY_CLUSTER_NAMESPACE, POD_IP, FQ_RAY_IP) into all collector containers.
func injectCollectorRayClusterNamespaceAndEnvVar(containers []corev1.Container, rayClusterName string, rayClusterNamespace string) {
	fqdnRayIP := fmt.Sprintf("%s-head-svc.%s.svc.cluster.local", rayClusterName, rayClusterNamespace)
	for i := range containers {
		if containers[i].Name == "collector" {
			if containers[i].Env == nil {
				containers[i].Env = []corev1.EnvVar{}
			}
			setOrAppendEnv(&containers[i], "RAY_CLUSTER_NAMESPACE", rayClusterNamespace, nil)
			setOrAppendEnv(&containers[i], "POD_IP", "", &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			})
			setOrAppendEnv(&containers[i], "FQ_RAY_IP", fqdnRayIP, nil)
		}
	}
}

// injectCollectorAuthToken points every collector sidecar at the auth token in the given Secret.
func injectCollectorAuthToken(containers []corev1.Container, secretName string) {
	for i := range containers {
		if containers[i].Name != "collector" {
			continue
		}
		setOrAppendEnv(&containers[i], utils.RAY_AUTH_TOKEN_ENV_VAR, "", &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  utils.RAY_AUTH_TOKEN_SECRET_KEY,
			},
		})
	}
}

// setOrAppendEnv updates an environment variable in-place if it already exists (e.g., from static YAML manifests)
// or appends a new entry if missing. This prevents duplicate environment variable entries in the pod spec.
func setOrAppendEnv(container *corev1.Container, name string, val string, valFrom *corev1.EnvVarSource) {
	for i := range container.Env {
		if container.Env[i].Name == name {
			container.Env[i].Value = val
			container.Env[i].ValueFrom = valFrom
			return
		}
	}
	container.Env = append(container.Env, corev1.EnvVar{
		Name:      name,
		Value:     val,
		ValueFrom: valFrom,
	})
}

// GetSessionIDFromHeadPod retrieves the sessionID from the Ray head pod by reading the symlink
// /tmp/ray/session_latest and getting its basename.
func GetSessionIDFromHeadPod(test Test, g *WithT, rayCluster *rayv1.RayCluster) string {
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())

	getSessionIDCmd := `ray_tmp_root="${RAY_TMP_ROOT:-/tmp/ray}"
if [ -L "${ray_tmp_root}/session_latest" ]; then
  session_path=$(readlink "${ray_tmp_root}/session_latest")
  basename "$session_path"
else
  echo "session_latest is not a symlink"
  exit 1
fi`
	output, _ := ExecPodCmd(test, headPod, "ray-head", []string{"sh", "-c", getSessionIDCmd})

	// Parse output to extract the sessionID.
	sessionID := strings.TrimSpace(output.String())
	LogWithTimestamp(test.T(), "Retrieved sessionID: %s", sessionID)
	g.Expect(sessionID).NotTo(BeEmpty(), "sessionID should not be empty")

	return sessionID
}

// GetNodeIDFromPod retrieves the nodeID from the Ray head or worker pod by extracting
// the --node_id argument from the active raylet process command line.
func GetNodeIDFromPod(test Test, g *WithT, getPod func() (*corev1.Pod, error), containerName string) string {
	pod, err := getPod()
	g.Expect(err).NotTo(HaveOccurred())

	// Extract node_id from raylet process arguments as history server retrieves node_id programmatically via HTTP API.
	getNodeIDCmd := `ps -ef | grep raylet | grep -v grep | sed -n 's/.*--node_id=\([^ ]*\).*/\1/p'`
	output, _ := ExecPodCmd(test, pod, containerName, []string{"sh", "-c", getNodeIDCmd})

	// Parse output to extract the nodeID.
	nodeID := strings.TrimSpace(output.String())
	LogWithTimestamp(test.T(), "Retrieved nodeID: %s", nodeID)
	g.Expect(nodeID).NotTo(BeEmpty(), "nodeID should not be empty")

	return nodeID
}

// FirstWorkerPod returns a function that gets the first worker pod from the Ray cluster.
// It adapts the WorkerPods function to be used with functions expecting a single pod.
func FirstWorkerPod(test Test, rayCluster *rayv1.RayCluster) func() (*corev1.Pod, error) {
	return func() (*corev1.Pod, error) {
		workerPods, err := GetWorkerPods(test, rayCluster)
		if err != nil {
			return nil, err
		}
		if len(workerPods) == 0 {
			return nil, fmt.Errorf("no worker pods found")
		}
		return &workerPods[0], nil
	}
}
