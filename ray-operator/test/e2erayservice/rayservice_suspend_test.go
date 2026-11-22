package e2erayservice

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// TestRayServiceSuspendResume covers the happy path: set Spec.Suspend=true on a
// Running RayService, observe that all owned resources are deleted and the
// status reaches Suspended, then set Spec.Suspend=false and observe that the
// service is recreated and serves traffic again.
func TestRayServiceSuspendResume(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-suspend"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(RayServiceSampleYamlApplyConfiguration())
	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be Ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	curlPod, err := CreateCurlPod(g, test, "curl-pod", "curl-container", namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	stdout, _ := CurlRayServicePod(test, rayService, curlPod, "curl-container", "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=true")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = true
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for the Suspended condition to be True")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceSuspended, BeTrue()))

	// Once Suspended is reached, Suspending / UpgradeInProgress /
	// RollbackInProgress must all be present as False with reason
	// SuspendComplete (not removed, and not stuck on the stale
	// SuspendInProgress reason from when suspend started).
	LogWithTimestamp(test.T(), "Asserting Suspending / UpgradeInProgress / RollbackInProgress are all retained as False with SuspendComplete reason")
	g.Eventually(func(gg Gomega) {
		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		gg.Expect(err).NotTo(HaveOccurred())
		for _, ctype := range []rayv1.RayServiceConditionType{
			rayv1.RayServiceSuspending,
			rayv1.UpgradeInProgress,
			rayv1.RollbackInProgress,
		} {
			cond := meta.FindStatusCondition(rs.Status.Conditions, string(ctype))
			gg.Expect(cond).NotTo(BeNil(), "%s condition should be retained as False, not removed", ctype)
			gg.Expect(cond.Status).To(Equal(metav1.ConditionFalse), "%s should be False", ctype)
			gg.Expect(cond.Reason).To(Equal(string(rayv1.SuspendComplete)), "%s reason should be SuspendComplete", ctype)
		}
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Asserting status fields are reset and all owned resources are gone")
	g.Eventually(func(gg Gomega) {
		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rs.Status.ActiveServiceStatus.RayClusterName).To(BeEmpty())
		gg.Expect(rs.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
		gg.Expect(rs.Status.NumServeEndpoints).To(BeEquivalentTo(0))
		gg.Expect(rs.Status.ServiceStatus).To(BeEquivalentTo(""))
		gg.Expect(IsRayServiceReady(rs)).To(BeFalse())

		rcList, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rcList.Items).To(BeEmpty())

		svcList, err := test.Client().Core().CoreV1().Services(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(svcList.Items).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=false to resume")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = false
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be Ready again", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).
		Should(WithTransform(IsRayServiceSuspended, BeFalse()))

	// The Suspended condition must be retained as False with reason
	// RayServiceResumed, not removed. Verifies that exiting the Suspended
	// state preserves lastTransitionTime / reason / message instead of
	// dropping the condition.
	LogWithTimestamp(test.T(), "Asserting Suspended condition is retained as False with RayServiceResumed reason")
	g.Eventually(func(gg Gomega) {
		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		gg.Expect(err).NotTo(HaveOccurred())
		cond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceSuspended))
		gg.Expect(cond).NotTo(BeNil(), "Suspended condition should be retained as False, not removed")
		gg.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		gg.Expect(cond.Reason).To(Equal(string(rayv1.RayServiceResumed)))
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Sending requests to verify the resumed RayService serves traffic again")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	// --connect-timeout/--max-time keep each curl attempt short so Eventually
	// can actually retry — without them a single attempt can hang on TCP
	// retransmits longer than TestTimeoutShort, fooling Eventually into
	// reporting a timeout after one attempt.
	curlCmd := []string{
		"curl", "-sS", "--connect-timeout", "3", "--max-time", "5",
		"-X", "POST", "-H", "Content-Type: application/json",
		fmt.Sprintf("%s-serve-svc.%s.svc.cluster.local:8000/fruit", rayService.Name, rayService.Namespace),
		"-d", `["MANGO", 2]`,
	}
	g.Eventually(func(gg Gomega) {
		stdout, _, err := ExecPodCmdWithError(test, curlPod, "curl-container", curlCmd)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(stdout.String()).To(Equal("6"))
	}, TestTimeoutShort).Should(Succeed())
}

// TestRayServiceSuspendAtomic verifies that once the Suspending condition has
// been committed, the suspend operation completes atomically regardless of
// subsequent flips on Spec.Suspend. The pattern mirrors the RayJob atomic
// suspend test: we pin the underlying RayCluster with a synthetic finalizer
// so deletion can't complete, then flip Spec.Suspend back and forth and
// assert via Consistently that Suspending stays True throughout. After the
// finalizer is removed, the deletion drains and the RayService comes back up.
func TestRayServiceSuspendAtomic(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-suspend-atomic"
	const deletionBlocker = "ray.io/test-suspend-block"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(RayServiceSampleYamlApplyConfiguration())
	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be Ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	originalClusterName := rayService.Status.ActiveServiceStatus.RayClusterName
	g.Expect(originalClusterName).NotTo(BeEmpty())

	LogWithTimestamp(test.T(), "Adding a synthetic finalizer to RayCluster %s so deletion blocks", originalClusterName)
	rayCluster, err := GetRayCluster(test, namespace.Name, originalClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	rayCluster.Finalizers = append(rayCluster.Finalizers, deletionBlocker)
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(test.Ctx(), rayCluster, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=true to enter Suspending")
	rayService.Spec.Suspend = true
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for the Suspending condition to be True")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).
		Should(WithTransform(IsRayServiceSuspending, BeTrue()))

	// Flipping Spec.Suspend back to false while a suspend is in flight must
	// not unwind the Suspending state — the deletion has to complete first.
	LogWithTimestamp(test.T(), "Flipping Spec.Suspend=false; Suspending must stay True")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = false
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Consistently(RayService(test, rayService.Namespace, rayService.Name), 5*time.Second, 500*time.Millisecond).
		Should(WithTransform(IsRayServiceSuspending, BeTrue()))

	// Flipping back to true is also a no-op — Suspending is already in flight.
	LogWithTimestamp(test.T(), "Flipping Spec.Suspend=true; Suspending must stay True")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = true
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Consistently(RayService(test, rayService.Namespace, rayService.Name), 5*time.Second, 500*time.Millisecond).
		Should(WithTransform(IsRayServiceSuspending, BeTrue()))

	// Settle on Spec.Suspend=false so the controller resumes once the
	// finalizer is removed and deletion finishes.
	LogWithTimestamp(test.T(), "Settling on Spec.Suspend=false and removing the deletion-blocker finalizer")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = false
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	rayCluster, err = GetRayCluster(test, namespace.Name, originalClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	rayCluster.Finalizers = nil
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(test.Ctx(), rayCluster, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Original RayCluster %s must be deleted after the finalizer is removed", originalClusterName)
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, originalClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(errors.IsNotFound, BeTrue()))

	LogWithTimestamp(test.T(), "RayService should become Ready again, backed by a different RayCluster")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService.Status.ActiveServiceStatus.RayClusterName).NotTo(BeEmpty())
	g.Expect(rayService.Status.ActiveServiceStatus.RayClusterName).NotTo(Equal(originalClusterName))
}

// TestRayServiceCreatedSuspended verifies that a RayService created with
// Spec.Suspend=true never spins up its owned resources and reaches Suspended
// directly. Flipping Spec.Suspend=false afterwards must then bring the service
// up normally.
func TestRayServiceCreatedSuspended(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-born-suspended"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).
		WithSpec(RayServiceSampleYamlApplyConfiguration().WithSuspend(true))
	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for the Suspended condition to be True")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceSuspended, BeTrue()))

	LogWithTimestamp(test.T(), "Asserting no owned resources were ever created and status is empty")
	g.Consistently(func(gg Gomega) {
		rcList, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rcList.Items).To(BeEmpty())

		svcList, err := test.Client().Core().CoreV1().Services(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(svcList.Items).To(BeEmpty())

		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rs.Status.ActiveServiceStatus.RayClusterName).To(BeEmpty())
		gg.Expect(rs.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
		gg.Expect(rs.Status.NumServeEndpoints).To(BeEquivalentTo(0))
	}, TestTimeoutShort/2).Should(Succeed())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=false; RayService should now come up normally")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = false
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).
		Should(WithTransform(IsRayServiceSuspended, BeFalse()))
}

// TestRayServiceSuspendDuringUpgrade triggers a zero-downtime upgrade so both
// the active and pending RayClusters exist, then sets Spec.Suspend=true. Both
// clusters must be deleted and the RayService must transition to Suspended.
// After resuming, the new spec is applied and the service serves traffic.
func TestRayServiceSuspendDuringUpgrade(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-suspend-upgrade"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(RayServiceSampleYamlApplyConfiguration())
	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be Ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	LogWithTimestamp(test.T(), "Triggering a zero-downtime upgrade by bumping RayVersion")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.RayClusterSpec.RayVersion = rayService.Spec.RayClusterSpec.RayVersion + "-upgrade"
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting until a pending RayCluster is created")
	g.Eventually(func() string {
		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		if err != nil {
			return ""
		}
		return rs.Status.PendingServiceStatus.RayClusterName
	}, TestTimeoutMedium).ShouldNot(BeEmpty())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=true while upgrade is in progress")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = true
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Both active and pending RayClusters must be deleted and Suspended must reach True")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceSuspended, BeTrue()))
	g.Eventually(func(gg Gomega) {
		rcList, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rcList.Items).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Resuming by setting Spec.Suspend=false; RayService should become Ready with the upgraded spec")
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.Suspend = false
	_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService.Spec.RayClusterSpec.RayVersion).To(HaveSuffix("-upgrade"))
}
