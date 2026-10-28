package support

import (
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Jobs(t Test, namespace string) func(g gomega.Gomega) []batchv1.Job {
	return func(g gomega.Gomega) []batchv1.Job {
		jobs, err := t.Client().Core().BatchV1().Jobs(namespace).List(t.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		return jobs.Items
	}
}

func Job(t Test, namespace, name string) func(g gomega.Gomega) *batchv1.Job {
	return func(g gomega.Gomega) *batchv1.Job {
		job, err := t.Client().Core().BatchV1().Jobs(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		return job
	}
}
