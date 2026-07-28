package e2erayjob

import (
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// RayJob without clusterSelector, under NetworkPolicy DenyAll. The operator owns
// the RayCluster so it adds the submitter ingress rule itself; we add DNS egress
// and operator dashboard ingress (the reconciler polls job status directly).
// Kindnet enforces NetworkPolicy on k8s >= 1.32 (CI runs on 1.35), so a wrong
// policy means the pods can't reach each other and the job never finishes.
func TestRayJobWithNetworkPolicy(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	// Job scripts.
	jobsAC := NewConfigMap(namespace.Name, Files(test, "counter.py"))
	jobs, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	// DenyAll: intra-cluster and submitter ingress are handled by the operator.
	// We add DNS egress, plus operator ingress on the dashboard port because the
	// RayJob reconciler polls job status (GetJobInfo) by dialing the head
	// dashboard directly, not through the submitter.
	networkPolicy := rayv1ac.NetworkPolicyConfig().
		WithMode(rayv1.NetworkPolicyDenyAll).
		WithHead(rayv1ac.NetworkPolicyRules().
			WithIngressRules(OperatorIngressRule(utils.DefaultDashboardPort)).
			WithEgressRules(DNSEgressRule())).
		WithWorker(rayv1ac.NetworkPolicyRules().WithEgressRules(DNSEgressRule()))

	rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")).
				WithNetworkPolicy(networkPolicy)).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

	rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	// Wait for the operator to spin up the RayCluster.
	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to start its RayCluster", rayJob.Namespace, rayJob.Name)
	g.Eventually(func(gg Gomega) string {
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		return rayJob.Status.RayClusterName
	}, TestTimeoutShort).ShouldNot(BeEmpty())

	rayClusterName := rayJob.Status.RayClusterName
	// Both policies should exist before the cluster can become ready.
	g.Eventually(func(gg Gomega) {
		_, err := test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayClusterName+"-head", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
		_, err = test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayClusterName+"-workers", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
	}, TestTimeoutShort).Should(Succeed())
	LogWithTimestamp(test.T(), "Head and worker NetworkPolicies created for RayCluster %s/%s", namespace.Name, rayClusterName)

	// If the policies are right the cluster starts and the job completes.
	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutLong).
		Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

	g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
		To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))
}

// RayJob with clusterSelector pointing at a RayCluster we create ourselves, under
// NetworkPolicy DenyAll. Since the RayCluster isn't owned by the RayJob, the
// operator won't add the submitter ingress rule for us, so we add it by hand
// (matching the submitter pod's labels) plus DNS egress.
func TestRayJobWithClusterSelectorAndNetworkPolicy(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	jobsAC := NewConfigMap(namespace.Name, Files(test, "counter.py"))
	jobs, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	rayJobName := "counter"

	// Head needs the submitter ingress rule (dashboard) so the submitter can
	// reach it, operator ingress on the dashboard (the reconciler polls job
	// status via GetJobInfo directly), and DNS egress. Workers just need DNS.
	networkPolicy := rayv1ac.NetworkPolicyConfig().
		WithMode(rayv1.NetworkPolicyDenyAll).
		WithHead(rayv1ac.NetworkPolicyRules().
			WithIngressRules(
				SubmitterIngressRule(rayJobName, utils.DefaultDashboardPort),
				OperatorIngressRule(utils.DefaultDashboardPort),
			).
			WithEgressRules(DNSEgressRule())).
		WithWorker(rayv1ac.NetworkPolicyRules().WithEgressRules(DNSEgressRule()))

	rayClusterAC := rayv1ac.RayCluster("raycluster", namespace.Name).
		WithSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")).
			WithNetworkPolicy(networkPolicy))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	// Sanity check that both policies landed.
	g.Eventually(func(gg Gomega) {
		_, err := test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayCluster.Name+"-head", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
		_, err = test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayCluster.Name+"-workers", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// RayJob targeting the existing cluster by label.
	rayJobAC := rayv1ac.RayJob(rayJobName, namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithClusterSelector(map[string]string{utils.RayClusterLabelKey: rayCluster.Name}).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

	rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	// The submitter has to get through the head policy to reach the dashboard, so
	// the job succeeding is what proves our ingress rule is correct.
	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutLong).
		Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

	g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
		To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))
}
