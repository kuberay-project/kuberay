package e2erayjob

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobRecovery(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Job scripts
	jobsAC := NewConfigMap(namespace.Name, Files(test, "long_running_counter.py"))
	jobs, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	test.T().Run("RayJob should recover after pod deletion", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running_counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to start running", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))
		LogWithTimestamp(test.T(), "Find RayJob %s/%s running", rayJob.Namespace, rayJob.Name)
		// wait for the job to run a bit
		LogWithTimestamp(test.T(), "Sleep RayJob %s/%s 15 seconds", rayJob.Namespace, rayJob.Name)
		time.Sleep(15 * time.Second)

		// get the running jobpods
		jobpods, err := test.Client().Core().CoreV1().Pods(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", rayJob.Name),
		})
		g.Expect(err).NotTo(HaveOccurred())

		// remove the running jobpods
		propagationPolicy := metav1.DeletePropagationBackground
		for _, pod := range jobpods.Items {
			LogWithTimestamp(test.T(), "Delete Pod %s from namespace  %s", pod.Name, rayJob.Namespace)
			err = test.Client().Core().CoreV1().Pods(namespace.Name).Delete(test.Ctx(), pod.Name, metav1.DeleteOptions{
				PropagationPolicy: &propagationPolicy,
			})
			g.Expect(err).NotTo(HaveOccurred())
		}

		LogWithTimestamp(test.T(), "Waiting for new pod to be created and running for RayJob %s/%s", namespace.Name, rayJob.Name)
		g.Eventually(func() ([]corev1.Pod, error) {
			pods, err := test.Client().Core().CoreV1().Pods(namespace.Name).List(
				test.Ctx(),
				metav1.ListOptions{
					LabelSelector: fmt.Sprintf("job-name=%s", rayJob.Name),
				},
			)
			g.Expect(err).NotTo(HaveOccurred())
			return pods.Items, nil
		}, TestTimeoutMedium).Should(
			WithTransform(func(pods []corev1.Pod) bool {
				for _, pod := range pods {
					if pod.Status.Phase == corev1.PodRunning {
						for _, oldPod := range jobpods.Items {
							if pod.Name == oldPod.Name {
								continue
							}
						}
						LogWithTimestamp(test.T(), "Found new running pod %s/%s", pod.Namespace, pod.Name)
						return true
					}
				}
				return false
			}, BeTrue()),
		)

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		g.Eventually(RayJob(test, namespace.Name, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))
	})
}
