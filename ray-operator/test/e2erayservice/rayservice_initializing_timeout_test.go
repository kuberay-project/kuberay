package e2erayservice

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// TestRayServiceBecomesReadyBeforeTimeout tests that a RayService with a timeout annotation
// successfully becomes ready within the timeout period.
func TestRayServiceBecomesReadyBeforeTimeout(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-sample"

	// Create RayService with a generous timeout that should not be hit
	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).
		WithAnnotations(map[string]string{
			utils.RayServiceInitializingTimeoutAnnotation: "10m",
		}).
		WithSpec(RayServiceSampleYamlApplyConfiguration())

	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())
	g.Expect(rayService.Annotations[utils.RayServiceInitializingTimeoutAnnotation]).To(Equal("10m"))

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	// Get the latest RayService
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify the service is Ready and not in InitializingTimeout state
	g.Expect(IsRayServiceReady(rayService)).To(BeTrue())

	// Check that RayServiceReady condition exists and is True
	readyCondition := meta.FindStatusCondition(rayService.Status.Conditions, string(rayv1.RayServiceReady))
	g.Expect(readyCondition).NotTo(BeNil())
	g.Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
	g.Expect(readyCondition.Reason).NotTo(Equal(string(rayv1.RayServiceInitializingTimeout)))
}

// TestRayServiceInitializingTimeoutTerminalFailure tests that a RayService fails with InitializingTimeout
// when it cannot become ready within the configured timeout, and remains in a terminal failure state
// even after spec updates. The user must delete and recreate the RayService to recover.
func TestRayServiceInitializingTimeoutTerminalFailure(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-timeout-test"

	// Create RayService with a short timeout and an invalid container image
	// to ensure the head Pod never starts (ImagePullBackOff).
	// This guarantees the RayService stays in Initializing state and will timeout.
	headPodTemplate := HeadPodTemplateApplyConfiguration()
	// Override the image with a non-existent one to cause ImagePullBackOff
	headPodTemplate.Spec.Containers[0].Image = new("invalid-image-does-not-exist:v1.0.0")

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).
		WithAnnotations(map[string]string{
			utils.RayServiceInitializingTimeoutAnnotation: "10s",
		}).
		WithSpec(RayServiceSampleYamlApplyConfiguration().
			WithRayClusterSpec(rayv1ac.RayClusterSpec().
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
					WithTemplate(headPodTemplate))))

	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())

	initialGeneration := rayService.Generation
	LogWithTimestamp(test.T(), "Created RayService %s/%s at generation %d with 10s timeout",
		rayService.Namespace, rayService.Name, initialGeneration)

	// Step 1: Wait for the timeout to trigger and verify terminal failure state
	LogWithTimestamp(test.T(), "Waiting for RayService to timeout and enter terminal failure state")
	g.Eventually(func(g Gomega) {
		rayService, err = GetRayService(test, namespace.Name, rayServiceName)
		g.Expect(err).NotTo(HaveOccurred())

		// Verify RayServiceReady condition is False with InitializingTimeout reason
		readyCondition := meta.FindStatusCondition(rayService.Status.Conditions, string(rayv1.RayServiceReady))
		g.Expect(readyCondition).NotTo(BeNil())
		g.Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(readyCondition.Reason).To(Equal(string(rayv1.RayServiceInitializingTimeout)))
		g.Expect(readyCondition.Message).To(ContainSubstring("RayService failed to become ready within the configured timeout"))
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "RayService entered terminal failure state with InitializingTimeout")

	// Step 2: Re-fetch and verify complete terminal failure state
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())

	// Verify cluster names are cleared after timeout
	g.Expect(rayService.Status.ActiveServiceStatus.RayClusterName).To(BeEmpty(),
		"ActiveServiceStatus.RayClusterName should be cleared after timeout")
	g.Expect(rayService.Status.PendingServiceStatus.RayClusterName).To(BeEmpty(),
		"PendingServiceStatus.RayClusterName should be cleared after timeout")

	// Verify ObservedGeneration is set
	expectedGeneration := initialGeneration + 1
	g.Expect(rayService.Status.ObservedGeneration).To(Equal(expectedGeneration),
		"ObservedGeneration should be the initial generation + 1 after adding finalizer")

	// Step 3: Update the RayService with valid config to test terminal failure behavior
	LogWithTimestamp(test.T(), "Updating RayService spec with valid configuration to test terminal failure persistence")

	// Update with valid config (correct image and longer timeout)
	rayServiceUpdateAC := rayv1ac.RayService(rayServiceName, namespace.Name).
		WithAnnotations(map[string]string{
			utils.RayServiceInitializingTimeoutAnnotation: "10m",
		}).
		WithSpec(RayServiceSampleYamlApplyConfiguration())

	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceUpdateAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService.Generation).To(BeNumerically(">", initialGeneration),
		"Generation should increment after spec update")

	updatedGeneration := rayService.Generation
	LogWithTimestamp(test.T(), "Updated RayService spec to generation %d with valid config", updatedGeneration)

	// Step 4: Verify that the terminal failure state persists despite spec update
	LogWithTimestamp(test.T(), "Verifying that terminal failure state persists after spec update")

	// Wait briefly for reconciliation to occur, then verify timeout state persists
	g.Consistently(func(g Gomega) {
		rayService, err = GetRayService(test, namespace.Name, rayServiceName)
		g.Expect(err).NotTo(HaveOccurred())

		// Verify RayServiceReady condition remains False with InitializingTimeout reason
		readyCondition := meta.FindStatusCondition(rayService.Status.Conditions, string(rayv1.RayServiceReady))
		g.Expect(readyCondition).NotTo(BeNil())
		g.Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
		g.Expect(readyCondition.Reason).To(Equal(string(rayv1.RayServiceInitializingTimeout)))

		// Verify cluster names remain empty (no new cluster created)
		g.Expect(rayService.Status.ActiveServiceStatus.RayClusterName).To(BeEmpty())
		g.Expect(rayService.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
	}, 15*time.Second, 2*time.Second).Should(Succeed())

	LogWithTimestamp(test.T(), "Confirmed: RayService remains in terminal failure state despite spec update")
}
