package sampleyaml

import (
	"path"
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayCluster(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ray-cluster.autoscaler-v2.yaml",
		},
		{
			name: "ray-cluster.autoscaler.yaml",
		},
		{
			name: "ray-cluster.complete.yaml",
		},
		{
			name: "ray-cluster.custom-head-service.yaml",
		},
		{
			name: "ray-cluster.deprecate-gcs-ft.yaml",
		},
		{
			name: "ray-cluster.persistent-redis.yaml",
		},
		{
			name: "ray-cluster.embed-grafana.yaml",
		},
		{
			name: "ray-cluster.external-redis-uri.yaml",
		},
		{
			name: "ray-cluster.external-redis.yaml",
		},
		{
			name: "ray-cluster.head-command.yaml",
		},
		{
			name: "ray-cluster.overwrite-command.yaml",
		},
		{
			name: "ray-cluster.py-spy.yaml",
		},
		{
			name: "ray-cluster.sample.yaml",
		},
		{
			name: "ray-cluster.separate-ingress.yaml",
		},
		{
			name: "ray-cluster.tls.yaml",
		},
		{
			name: "ray-cluster.fluentbit.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			g.ConfigureWithT(WithRayClusterResourceLogger(test))

			yamlFilePath := path.Join(GetSampleYAMLDir(test), tt.name)
			namespace := test.NewTestNamespace()
			rayClusterFromYaml := DeserializeRayClusterYAML(test, yamlFilePath)
			KubectlApplyYAML(test, yamlFilePath, namespace.Name)

			rayCluster, err := GetRayCluster(test, namespace.Name, rayClusterFromYaml.Name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rayCluster).NotTo(BeNil())

			LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to be ready", namespace.Name, rayCluster.Name)
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionTrue, rayv1.HeadPodRunningAndReady)))
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
			rayCluster, err = GetRayCluster(test, namespace.Name, rayCluster.Name)
			g.Expect(err).NotTo(HaveOccurred())

			// Check if the RayCluster created correct number of pods
			var desiredWorkerReplicas int32
			if rayCluster.Spec.WorkerGroupSpecs != nil {
				for _, workerGroupSpec := range rayCluster.Spec.WorkerGroupSpecs {
					desiredWorkerReplicas += *workerGroupSpec.Replicas
				}
			}
			g.Eventually(WorkerPods(test, rayCluster), TestTimeoutShort).Should(HaveLen(int(desiredWorkerReplicas)))
			g.Expect(GetRayCluster(test, namespace.Name, rayCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(desiredWorkerReplicas)))

			// Check if the head pod is ready
			g.Eventually(HeadPod(test, rayCluster), TestTimeoutShort).Should(WithTransform(IsPodRunningAndReady, BeTrue()))

			// Check if all worker pods are ready
			g.Eventually(WorkerPods(test, rayCluster), TestTimeoutShort).Should(WithTransform(AllPodsRunningAndReady, BeTrue()))

			// Check that all pods can submit jobs
			g.Eventually(SubmitJobsToAllPods(test, rayCluster), TestTimeoutShort).Should(Succeed())

			// Delete all pods after setting quota to 0 to avoid recreating pods
			KubectlApplyQuota(test, namespace.Name, "--hard=cpu=0,memory=0G,pods=0")
			KubectlDeleteAllPods(test, namespace.Name)
			// The HeadPodReady condition should now be False with a HeadPodNotFound reason.
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionFalse, rayv1.HeadPodNotFound)))
			// The RayClusterProvisioned condition should still be True.
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))
			// The RayClusterReplicaFailure condition now be True with a FailedCreateHeadPod reason due to the quota limit.
			g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.RayClusterReplicaFailure), MatchCondition(metav1.ConditionTrue, "FailedCreateHeadPod")))
		})
	}
}
