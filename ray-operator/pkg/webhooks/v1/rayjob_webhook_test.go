package v1

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var _ = Describe("RayJob validating webhook", func() {
	Context("when name is too long", func() {
		It("should return error", func() {
			longName := "this-name-is-tooooooooooooooooooooooooooooooooooooooooooo-long-and-should-be-invalid"
			rayJob := rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      longName,
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{},
								},
							},
						},
					},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayJob)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("RayJob.ray.io \"%s\" is invalid: metadata.name", longName)))
		})
	})

	Context("when name isn't a DNS1035 label", func() {
		It("should return error", func() {
			rayJob := rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "invalid.name",
				},
				Spec: rayv1.RayJobSpec{
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{},
								},
							},
						},
					},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayJob)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring("RayJob.ray.io \"invalid.name\" is invalid: metadata.name:"))
		})
	})
})
