package e2eraycronjob

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	rayclientset "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func rayCronJobACTemplate(name, namespace, schedule string) *rayv1ac.RayCronJobApplyConfiguration {
	return rayv1ac.RayCronJob(name, namespace).
		WithSpec(
			rayv1ac.RayCronJobSpec().
				WithSchedule(schedule).
				WithJobTemplate(
					rayv1ac.RayJobSpec().
						WithEntrypoint("sleep 1").
						WithRayClusterSpec(NewRayClusterSpec()),
				),
		)
}

func TestRayCronJobSuspend(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	test.T().Run("RayCronJob respects suspend flag across schedule and resumes job creation.", func(_ *testing.T) {
		rayCronJobAC := rayCronJobACTemplate("suspended-raycronjob", namespace.Name, "*/1 * * * *")
		rayCronJobAC.Spec.WithSuspend(true)
		rayCronJob, err := test.Client().Ray().RayV1().RayCronJobs(namespace.Name).Apply(test.Ctx(), rayCronJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayCronJob %s/%s successfully", rayCronJob.Namespace, rayCronJob.Name)

		rayCronJob, err = GetRayCronJob(test, rayCronJob.Namespace, rayCronJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		ownerUID := rayCronJob.UID

		// No RayJob should be created
		LogWithTimestamp(test.T(), "Waiting to ensure no RayJobs are created while suspended")
		g.Consistently(func() (int, error) {
			return countRayJobsOwnedByUID(test.Ctx(), test.Client().Ray(), namespace.Name, ownerUID)
		}, 130*time.Second, 5*time.Second).Should(BeZero())

		// Resume
		LogWithTimestamp(test.T(), "Resuming RayCronJob %s/%s", rayCronJob.Namespace, rayCronJob.Name)
		patch := []byte(`{"spec":{"suspend":false}}`)
		_, err = test.Client().Ray().RayV1().RayCronJobs(namespace.Name).Patch(test.Ctx(), rayCronJob.Name, types.MergePatchType, patch, metav1.PatchOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		// Spec.Suspend should be false
		g.Eventually(RayCronJob(test, namespace.Name, rayCronJob.Name), TestTimeoutShort).
			Should(WithTransform(func(rayCronJob *rayv1.RayCronJob) bool {
				return rayCronJob.Spec.Suspend == nil || !*rayCronJob.Spec.Suspend
			}, BeTrue()))

		// Jobs must start appearing now
		g.Eventually(func() (int, error) {
			return countRayJobsOwnedByUID(test.Ctx(), test.Client().Ray(), namespace.Name, ownerUID)
		}, TestTimeoutMedium).Should(BeNumerically(">", 0))

		// Delete the RayCronJob
		err = test.Client().Ray().RayV1().RayCronJobs(namespace.Name).Delete(test.Ctx(), rayCronJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayCronJob %s/%s successfully", rayCronJob.Namespace, rayCronJob.Name)
	})
}

func TestRayCronJobNoDuplicateRayJob(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	test.T().Run("RayCronJob should not create duplicate RayJobs for the same schedule tick.", func(_ *testing.T) {
		rayCronJobAC := rayCronJobACTemplate("no-duplicate-raycronjob", namespace.Name, "*/1 * * * *")
		rayCronJob, err := test.Client().Ray().RayV1().RayCronJobs(namespace.Name).Apply(test.Ctx(), rayCronJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayCronJob %s/%s successfully", rayCronJob.Namespace, rayCronJob.Name)

		rayCronJob, err = GetRayCronJob(test, rayCronJob.Namespace, rayCronJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		ownerUID := rayCronJob.UID

		// Wait for the first RayJob to be created
		g.Eventually(func() (int, error) {
			return countRayJobsOwnedByUID(test.Ctx(), test.Client().Ray(), namespace.Name, ownerUID)
		}, TestTimeoutMedium, 5*time.Second).Should(BeNumerically(">", 0))

		// Consistently check that no duplicate RayJob is created within the same schedule tick
		g.Consistently(func() (int, error) {
			return countRayJobsOwnedByUID(test.Ctx(), test.Client().Ray(), namespace.Name, ownerUID)
		}, 30*time.Second, 5*time.Second).Should(BeNumerically("<=", 1))

		err = test.Client().Ray().RayV1().RayCronJobs(namespace.Name).Delete(test.Ctx(), rayCronJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayCronJob %s/%s successfully", rayCronJob.Namespace, rayCronJob.Name)
	})
}

// countRayJobsOwnedByUID counts RayJobs in namespace whose ownerReferences contains ownerUID.
func countRayJobsOwnedByUID(ctx context.Context, rayClient rayclientset.Interface, namespace string, ownerUID types.UID) (int, error) {
	list, err := rayClient.RayV1().RayJobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	n := 0
	for i := range list.Items {
		owners := list.Items[i].OwnerReferences
		for _, or := range owners {
			if or.UID == ownerUID {
				n++
				break
			}
		}
	}
	return n, nil
}
