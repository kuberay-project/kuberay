package e2erayjob

import (
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobHeadFailurePolicy(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	test.T().Run("Fail with backoffLimit > 0 is ValidationFailed", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("head-fail-backoff", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithBackoffLimit(1).
				WithEntrypoint("python -c 'print(1)'").
				WithRayClusterSpec(NewRayClusterSpec()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusValidationFailed)))
	})

	test.T().Run("Fail with clusterSelector is ValidationFailed", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("head-fail-selector", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithEntrypoint("python -c 'print(1)'").
				WithClusterSelector(map[string]string{
					utils.RayJobClusterSelectorKey: "nonexistent-cluster",
				}))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusValidationFailed)))
	})

	test.T().Run("Fail with unset head restartPolicy forces Never on RayCluster", func(_ *testing.T) {
		// NewRayClusterSpec leaves restartPolicy empty; the controller should set Never for Fail.
		rayJobAC := rayv1ac.RayJob("head-fail-restart-unset", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithBackoffLimit(0).
				WithEntrypoint("python -c 'print(1)'").
				WithShutdownAfterJobFinishes(true).
				WithRayClusterSpec(NewRayClusterSpec()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(And(
				WithTransform(RayJobDeploymentStatus, Not(Equal(rayv1.JobDeploymentStatusValidationFailed))),
				WithTransform(RayJobClusterName, Not(BeEmpty())),
			))
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayCluster.Spec.HeadGroupSpec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
	})

	test.T().Run("Fail with head restartPolicy Always is ValidationFailed", func(_ *testing.T) {
		clusterSpec := NewRayClusterSpec()
		clusterSpec.HeadGroupSpec.Template.Spec.WithRestartPolicy(corev1.RestartPolicyAlways)

		rayJobAC := rayv1ac.RayJob("head-fail-restart-always", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithBackoffLimit(0).
				WithEntrypoint("python -c 'print(1)'").
				WithRayClusterSpec(clusterSpec))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusValidationFailed)))
	})

	test.T().Run("Fail policy retains failed head Pod and fails RayJob", func(_ *testing.T) {
		// Force the head Pod to fail via a failing init container with RestartPolicy Never.
		clusterSpec := failingHeadClusterSpec()

		rayJobAC := rayv1ac.RayJob("head-fail-policy", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithBackoffLimit(0).
				WithEntrypoint("python -c 'print(1)'").
				WithShutdownAfterJobFinishes(false).
				WithRayClusterSpec(clusterSpec))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))

		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayJob.Status.Reason).To(Equal(rayv1.HeadPodFailedReason))
		g.Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())

		rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayCluster.Annotations[utils.HeadFailurePolicyAnnotationKey]).To(Equal(string(rayv1.HeadFailurePolicyFail)))
		g.Expect(meta.IsStatusConditionTrue(rayCluster.Status.Conditions, string(rayv1.HeadPodFailed))).To(BeTrue())

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod.DeletionTimestamp).To(BeNil())
		g.Expect(headPod.Status.Phase).To(Equal(corev1.PodFailed))

		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
	})

	test.T().Run("Fail policy with SidecarMode and SidecarSubmitterRestart retains failed head and fails RayJob", func(t *testing.T) {
		// CI (make deploy-with-override) enables SidecarSubmitterRestart. Local Helm installs may
		// not expose the same Deployment labels / gate; skip instead of failing in that case.
		if !isSidecarSubmitterRestartEnabled(t, test) {
			t.Skip("SidecarSubmitterRestart feature gate is not active; skipping")
		}

		clusterSpec := failingHeadClusterSpec()
		rayJobAC := rayv1ac.RayJob("head-fail-sidecar", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithHeadFailurePolicy(rayv1.HeadFailurePolicyFail).
				WithBackoffLimit(0).
				WithEntrypoint("python -c 'print(1)'").
				WithShutdownAfterJobFinishes(false).
				WithRayClusterSpec(clusterSpec))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created SidecarMode RayJob %s/%s with headFailurePolicy=Fail", rayJob.Namespace, rayJob.Name)

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))

		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayJob.Status.Reason).To(Equal(rayv1.HeadPodFailedReason))
		g.Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())

		rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayCluster.Labels[utils.RayJobSubmissionModeLabelKey]).To(Equal(string(rayv1.SidecarMode)))
		g.Expect(rayCluster.Annotations[utils.HeadFailurePolicyAnnotationKey]).To(Equal(string(rayv1.HeadFailurePolicyFail)))
		g.Expect(meta.IsStatusConditionTrue(rayCluster.Status.Conditions, string(rayv1.HeadPodFailed))).To(BeTrue())

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod.DeletionTimestamp).To(BeNil())
		g.Expect(headPod.Status.Phase).To(Equal(corev1.PodFailed))

		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
	})

	// Recreate regression: default / explicit Recreate must still delete and recreate a failed head Pod.
	for _, tc := range []struct {
		name              string
		jobName           string
		headFailurePolicy *rayv1.HeadFailurePolicy
	}{
		{
			name:              "unset headFailurePolicy recreates failed head Pod",
			jobName:           "head-recreate-unset",
			headFailurePolicy: nil,
		},
		{
			name:              "explicit Recreate recreates failed head Pod",
			jobName:           "head-recreate-explicit",
			headFailurePolicy: ptr.To(rayv1.HeadFailurePolicyRecreate),
		},
	} {
		test.T().Run(tc.name, func(_ *testing.T) {
			clusterSpec := failingHeadClusterSpec()
			spec := rayv1ac.RayJobSpec().
				WithBackoffLimit(0).
				WithEntrypoint("python -c 'print(1)'").
				WithShutdownAfterJobFinishes(false).
				WithRayClusterSpec(clusterSpec)
			if tc.headFailurePolicy != nil {
				spec = spec.WithHeadFailurePolicy(*tc.headFailurePolicy)
			}

			rayJobAC := rayv1ac.RayJob(tc.jobName, namespace.Name).WithSpec(spec)
			rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayJob %s/%s for Recreate regression", rayJob.Namespace, rayJob.Name)

			g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
				Should(WithTransform(RayJobClusterName, Not(BeEmpty())))

			rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
			g.Expect(err).NotTo(HaveOccurred())
			rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
			g.Expect(err).NotTo(HaveOccurred())

			if tc.headFailurePolicy == nil {
				g.Expect(rayCluster.Annotations[utils.HeadFailurePolicyAnnotationKey]).To(BeEmpty())
			} else {
				g.Expect(rayCluster.Annotations[utils.HeadFailurePolicyAnnotationKey]).To(Equal(string(rayv1.HeadFailurePolicyRecreate)))
			}

			// Under Recreate the failed head is deleted quickly, so do not wait for Phase=Failed
			// (sampling usually only sees Pending). Capture the first head UID, then wait for a new one.
			var firstUID types.UID
			g.Eventually(func(gomega Gomega) {
				headPod, err := GetHeadPod(test, rayCluster)
				gomega.Expect(err).NotTo(HaveOccurred())
				firstUID = headPod.UID
			}, TestTimeoutMedium, 2*time.Second).Should(Succeed())

			g.Eventually(func(gomega Gomega) {
				headPod, err := GetHeadPod(test, rayCluster)
				gomega.Expect(err).NotTo(HaveOccurred())
				gomega.Expect(headPod.UID).NotTo(Equal(firstUID), "head Pod should be recreated under Recreate policy")
			}, TestTimeoutMedium, 2*time.Second).Should(Succeed())

			// New feature must not mark the RayJob Failed with HeadPodFailedReason under Recreate.
			g.Consistently(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort, 2*time.Second).
				Should(WithTransform(RayJobReason, Not(Equal(rayv1.HeadPodFailedReason))))
			g.Consistently(func(gomega Gomega) {
				rc, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
				gomega.Expect(err).NotTo(HaveOccurred())
				gomega.Expect(meta.IsStatusConditionTrue(rc.Status.Conditions, string(rayv1.HeadPodFailed))).To(BeFalse())
			}, TestTimeoutShort, 2*time.Second).Should(Succeed())

			err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}

func failingHeadClusterSpec() *rayv1ac.RayClusterSpecApplyConfiguration {
	clusterSpec := NewRayClusterSpec()
	clusterSpec.HeadGroupSpec.Template.Spec.
		WithRestartPolicy(corev1.RestartPolicyNever).
		WithInitContainers(corev1ac.Container().
			WithName("fail-init").
			WithImage(GetRayImage()).
			WithCommand("sh", "-c", "exit 1"))
	clusterSpec.WorkerGroupSpecs[0].WithReplicas(0).WithMinReplicas(0)
	return clusterSpec
}

// isSidecarSubmitterRestartEnabled reports whether the running operator has
// SidecarSubmitterRestart=true. Returns false (caller should Skip) when the
// operator Deployment cannot be found or the gate is off — do not Fail, so
// local Helm installs without the Kustomize component label still pass.
func isSidecarSubmitterRestartEnabled(t *testing.T, test Test) bool {
	t.Helper()

	operatorDeploymentList, err := test.Client().Core().AppsV1().Deployments("").List(t.Context(), metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=kuberay-operator",
	})
	if err != nil {
		t.Logf("warning: failed to list kuberay-operator deployments: %v; skipping SidecarSubmitterRestart case", err)
		return false
	}
	if len(operatorDeploymentList.Items) == 0 {
		t.Log("warning: no Deployment with label app.kubernetes.io/component=kuberay-operator " +
			"(common with Helm installs); skipping SidecarSubmitterRestart case")
		return false
	}
	if len(operatorDeploymentList.Items) > 1 {
		t.Logf("warning: found %d kuberay-operator deployments; using the first", len(operatorDeploymentList.Items))
	}

	for _, container := range operatorDeploymentList.Items[0].Spec.Template.Spec.Containers {
		if container.Name != "kuberay-operator" {
			continue
		}
		for _, arg := range container.Args {
			if strings.Contains(arg, "SidecarSubmitterRestart=true") {
				return true
			}
		}
	}
	t.Log("warning: SidecarSubmitterRestart=true not found in operator args; skipping")
	return false
}
