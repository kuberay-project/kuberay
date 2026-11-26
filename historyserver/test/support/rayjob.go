package support

import (
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	rayJobManifestPath = "../testdata/rayjob.yaml"
	// Self-contained; the generated cluster name is only known from the RayJob status.
	rayDataManifestPath = "../testdata/ray-data.yaml"
)

// ApplyRayJobAndWaitForCompletion applies a Ray job to the existing Ray cluster and waits for it to complete successfully.
// In the RayJob manifest, the clusterSelector is set to the existing Ray cluster, raycluster-historyserver.
func ApplyRayJobAndWaitForCompletion(test Test, g *WithT, namespace *corev1.Namespace, _ *rayv1.RayCluster) *rayv1.RayJob {
	rayJobFromYaml := DeserializeRayJobYAML(test, rayJobManifestPath)
	rayJobFromYaml.Namespace = namespace.Name

	rayJob, err := test.Client().Ray().RayV1().
		RayJobs(namespace.Name).
		Create(test.Ctx(), rayJobFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete successfully", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(SatisfyAll(
			WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)),
			WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)),
		))
	LogWithTimestamp(test.T(), "RayJob %s/%s completed successfully", rayJob.Namespace, rayJob.Name)

	return rayJob
}

// ApplyRayDataJobAndWaitForCompletion applies the Ray Data RayJob and waits for success;
// the returned Status.RayClusterName is the only way to learn the generated cluster name.
func ApplyRayDataJobAndWaitForCompletion(test Test, g *WithT, namespace *corev1.Namespace) *rayv1.RayJob {
	rayJobFromYaml := DeserializeRayJobYAML(test, rayDataManifestPath)
	rayJobFromYaml.Namespace = namespace.Name

	rayJob, err := test.Client().Ray().RayV1().
		RayJobs(namespace.Name).
		Create(test.Ctx(), rayJobFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete successfully", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

	rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
	LogWithTimestamp(test.T(), "RayJob %s/%s succeeded on cluster %s", rayJob.Namespace, rayJob.Name, rayJob.Status.RayClusterName)

	return rayJob
}
