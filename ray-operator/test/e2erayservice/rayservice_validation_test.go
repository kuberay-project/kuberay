package e2erayservice

import (
	"strings"
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

func TestRayServiceValidation(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	test.T().Run("RayService name too long with 48 characters", func(_ *testing.T) {
		rayServiceAC := rayv1ac.RayService(strings.Repeat("a", 48), namespace.Name).
			WithSpec(RayServiceSampleYamlApplyConfiguration())

		rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayService %s/%s successfully", rayService.Namespace, rayService.Name)

		g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).
			Should(WithTransform(func(rs *rayv1.RayService) bool {
				condition := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
				if condition == nil {
					return false
				}
				return condition.Status == "False" && condition.Reason == string(rayv1.RayServiceValidationFailed)
			}, BeTrue()))
	})
}

func TestRayServiceManagedBy(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	test.T().Run("Successful creation of RayService, managed by Kuberay Operator", func(t *testing.T) {
		t.Parallel()

		rayServiceAC := rayv1ac.RayService("rayservice-ok", namespace.Name).
			WithSpec(RayServiceSampleYamlApplyConfiguration().
				WithManagedBy(utils.KubeRayController))

		rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayService %s/%s successfully", rayService.Namespace, rayService.Name)

		LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to become ready", rayService.Namespace, rayService.Name)
		g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
			Should(WithTransform(IsRayServiceReady, BeTrue()))
	})

	test.T().Run("Creation of RayService skipped, managed by Kueue", func(t *testing.T) {
		t.Parallel()

		rayServiceAC := rayv1ac.RayService("rayservice-skip", namespace.Name).
			WithSpec(RayServiceSampleYamlApplyConfiguration().
				WithManagedBy("kueue.x-k8s.io/multikueue"))

		rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayService %s/%s successfully", rayService.Namespace, rayService.Name)

		LogWithTimestamp(test.T(), "RayService %s/%s will not become ready - not reconciled", rayService.Namespace, rayService.Name)
		// Assert the RayService status has not been updated
		g.Consistently(func(gg Gomega) {
			rs, err := GetRayService(test, rayService.Namespace, rayService.Name)
			gg.Expect(err).NotTo(HaveOccurred())
			// RayService should not have Ready condition set when managed externally
			condition := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
			gg.Expect(condition).To(BeNil())
			// ServiceStatus should remain empty/NotRunning
			gg.Expect(rs.Status.ServiceStatus).To(Equal(rayv1.NotRunning))
			// ActiveServiceStatus should remain empty
			gg.Expect(rs.Status.ActiveServiceStatus.RayClusterName).To(BeEmpty())
			// PendingServiceStatus should remain empty
			gg.Expect(rs.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
		}, time.Second*3, time.Millisecond*500).Should(Succeed())

		// Should not be able to change managedBy field as it's immutable
		rayServiceAC.Spec.WithManagedBy(utils.KubeRayController)
		_, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Eventually(RayService(test, *rayServiceAC.Namespace, *rayServiceAC.Name)).
			Should(WithTransform(RayServiceManagedBy, Equal(new("kueue.x-k8s.io/multikueue"))))

		// Assert the associated RayCluster has not been created
		rcList, err := test.Client().Ray().RayV1().RayClusters(rayService.Namespace).List(test.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		for _, rc := range rcList.Items {
			g.Expect(rc.Name).NotTo(HavePrefix(*rayServiceAC.Name))
		}

		// Delete the RayService
		err = test.Client().Ray().RayV1().RayServices(namespace.Name).Delete(test.Ctx(), *rayServiceAC.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayService %s/%s successfully", *rayServiceAC.Namespace, *rayServiceAC.Name)
	})

	test.T().Run("Failed creation of RayService, managed by external non supported controller", func(t *testing.T) {
		t.Parallel()

		rayServiceAC := rayv1ac.RayService("rayservice-fail", namespace.Name).
			WithSpec(RayServiceSampleYamlApplyConfiguration().
				WithManagedBy("controller.com/not-supported"))

		_, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Expect(errors.IsInvalid(err)).To(BeTrue(), "error: %v", err)
	})
}
