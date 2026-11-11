package sampleyaml

import (
	"path"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJob(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ray-job.custom-head-svc.yaml",
		},
		{
			name: "ray-job.resources.yaml",
		},
		{
			name: "ray-job.sample.yaml",
		},
		{
			name: "ray-job.shutdown.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			g.ConfigureWithT(WithRayJobResourceLogger(test))
			yamlFilePath := path.Join(GetSampleYAMLDir(test), tt.name)
			namespace := test.NewTestNamespace()
			rayJobFromYaml := DeserializeRayJobYAML(test, yamlFilePath)
			KubectlApplyYAML(test, yamlFilePath, namespace.Name)

			checkRayJobCluster(test, g, rayJobFromYaml, namespace)

			g.Eventually(RayJob(test, namespace.Name, rayJobFromYaml.Name), TestTimeoutMedium).Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))
			g.Eventually(RayJob(test, namespace.Name, rayJobFromYaml.Name), TestTimeoutMedium).Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))
		})
	}
}

func TestRayJobUseExistingRayClusterSampleConsistency(t *testing.T) {
	test := With(t)
	g := NewWithT(t)
	sampleYAMLDir := GetSampleYAMLDir(test)
	rayCluster := DeserializeRayClusterYAML(test, path.Join(sampleYAMLDir, "ray-cluster.sample.yaml"))
	rayJob := DeserializeRayJobYAML(test, path.Join(sampleYAMLDir, "ray-job.use-existing-raycluster.yaml"))

	g.Expect(rayJob.Spec.ClusterSelector).To(HaveKeyWithValue(utils.RayJobClusterSelectorKey, rayCluster.Name))
}

func TestRayJobInteractiveMode(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ray-job.interactive-mode.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			g.ConfigureWithT(WithRayJobResourceLogger(test))
			yamlFilePath := path.Join(GetSampleYAMLDir(test), tt.name)
			namespace := test.NewTestNamespace()
			rayJobFromYaml := DeserializeRayJobYAML(test, yamlFilePath)
			KubectlApplyYAML(test, yamlFilePath, namespace.Name)
			checkRayJobCluster(test, g, rayJobFromYaml, namespace)
		})
	}
}

func checkRayJobCluster(test Test, g *WithT, rayJobFromYaml *rayv1.RayJob, namespace *corev1.Namespace) {
	rayJob, err := GetRayJob(test, namespace.Name, rayJobFromYaml.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayJob).NotTo(BeNil())

	// Wait for RayCluster name to be populated
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
		Should(WithTransform(RayJobClusterName, Not(BeEmpty())))

	rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayJob).NotTo(BeNil())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to be ready", namespace.Name, rayJob.Status.RayClusterName)
	g.Eventually(RayCluster(test, namespace.Name, rayJob.Status.RayClusterName), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())

	// Check if the RayCluster created correct number of pods
	var desiredWorkerPods int32
	if rayCluster.Spec.WorkerGroupSpecs != nil {
		for _, workerGroupSpec := range rayCluster.Spec.WorkerGroupSpecs {
			desiredWorkerPods += (*workerGroupSpec.Replicas * workerGroupSpec.NumOfHosts)
		}
	}
	g.Eventually(WorkerPods(test, rayCluster), TestTimeoutShort).Should(HaveLen(int(desiredWorkerPods)))
	g.Expect(GetRayCluster(test, namespace.Name, rayCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(desiredWorkerPods)))

	// Check if the head pod is ready
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutShort).Should(WithTransform(IsPodRunningAndReady, BeTrue()))

	// Check if all worker pods are ready
	g.Eventually(WorkerPods(test, rayCluster), TestTimeoutShort).Should(WithTransform(AllPodsRunningAndReady, BeTrue()))
}
