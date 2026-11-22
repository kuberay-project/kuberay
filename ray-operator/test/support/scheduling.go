package support

import (
	"github.com/onsi/gomega"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Workload(t Test, namespace, name string) func() (*schedulingv1alpha3.Workload, error) {
	return func() (*schedulingv1alpha3.Workload, error) {
		return GetWorkload(t, namespace, name)
	}
}

func GetWorkload(t Test, namespace, name string) (*schedulingv1alpha3.Workload, error) {
	return t.Client().Core().SchedulingV1alpha3().Workloads(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func PodGroup(t Test, namespace, name string) func() (*schedulingv1alpha3.PodGroup, error) {
	return func() (*schedulingv1alpha3.PodGroup, error) {
		return GetPodGroup(t, namespace, name)
	}
}

func GetPodGroup(t Test, namespace, name string) (*schedulingv1alpha3.PodGroup, error) {
	return t.Client().Core().SchedulingV1alpha3().PodGroups(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func Workloads(t Test, namespace string) func(g gomega.Gomega) []schedulingv1alpha3.Workload {
	return func(g gomega.Gomega) []schedulingv1alpha3.Workload {
		workloads, err := t.Client().Core().SchedulingV1alpha3().Workloads(namespace).List(t.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		return workloads.Items
	}
}

func PodGroups(t Test, namespace string) func(g gomega.Gomega) []schedulingv1alpha3.PodGroup {
	return func(g gomega.Gomega) []schedulingv1alpha3.PodGroup {
		podGroups, err := t.Client().Core().SchedulingV1alpha3().PodGroups(namespace).List(t.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		return podGroups.Items
	}
}
