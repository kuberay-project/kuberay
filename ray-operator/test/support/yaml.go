package support

import (
	"os"
	"os/exec"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayscheme "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/scheme"
)

func deserializeYAML(filename string, into runtime.Object) error {
	yamlFileContent, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	decoder := rayscheme.Codecs.UniversalDecoder()
	if _, _, err = decoder.Decode(yamlFileContent, nil, into); err != nil {
		return err
	}
	return nil
}

func DeserializeRayClusterYAML(t Test, filename string) *rayv1.RayCluster {
	t.T().Helper()
	rayCluster := &rayv1.RayCluster{}
	err := deserializeYAML(filename, rayCluster)
	require.NoError(t.T(), err, "Fail to deserialize yaml file %s", filename)
	return rayCluster
}

func DeserializeRayJobYAML(t Test, filename string) *rayv1.RayJob {
	t.T().Helper()
	rayJob := &rayv1.RayJob{}
	err := deserializeYAML(filename, rayJob)
	require.NoError(t.T(), err, "Fail to deserialize yaml file %s", filename)
	return rayJob
}

func DeserializeRayServiceYAML(t Test, filename string) *rayv1.RayService {
	t.T().Helper()
	rayService := &rayv1.RayService{}
	err := deserializeYAML(filename, rayService)
	require.NoError(t.T(), err, "Fail to deserialize yaml file %s", filename)
	return rayService
}

func KubectlApplyYAML(t Test, filename string, namespace string) {
	t.T().Helper()
	kubectlCmd := exec.CommandContext(t.Ctx(), "kubectl", "apply", "-f", filename, "-n", namespace)
	err := kubectlCmd.Run()
	require.NoError(t.T(), err, "Failed to apply %s to namespace %s", filename, namespace)
	LogWithTimestamp(t.T(), "Successfully applied %s to namespace %s", filename, namespace)
}

func KubectlApplyQuota(t Test, namespace, quota string) {
	t.T().Helper()
	kubectlCmd := exec.CommandContext(t.Ctx(), "kubectl", "create", "quota", namespace, "-n", namespace, quota)
	err := kubectlCmd.Run()
	require.NoError(t.T(), err, "Failed to apply quota %s in %s", quota, namespace)
	LogWithTimestamp(t.T(), "Successfully applied quota %s in %s", quota, namespace)
}

func KubectlDeleteAllPods(t Test, namespace string) {
	t.T().Helper()
	kubectlCmd := exec.CommandContext(t.Ctx(), "kubectl", "delete", "--all", "pods", "-n", namespace)
	err := kubectlCmd.Run()
	require.NoError(t.T(), err, "Failed to delete pods in %s", namespace)
	LogWithTimestamp(t.T(), "Successfully delete pods in %s", namespace)
}
