package v1

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var _ = Describe("RayCluster validating webhook", func() {
	Context("when name is too long", func() {
		It("should return error", func() {
			longName := "this-name-is-tooooooooooooooooooooooooooooooooooooooooooo-long-and-should-be-invalid"
			rayCluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      longName,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring(fmt.Sprintf("RayCluster.ray.io \"%s\" is invalid: metadata.name", longName)))
		})
	})

	Context("when name isn't a DNS1035 label", func() {
		It("should return error", func() {
			rayCluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "invalid.name",
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring("RayCluster.ray.io \"invalid.name\" is invalid: metadata.name:"))
		})
	})

	Context("when groupNames are not unique", func() {
		var name, namespace string
		var rayCluster rayv1.RayCluster

		BeforeEach(func() {
			namespace = "default"
			name = fmt.Sprintf("test-raycluster-%d", rand.IntnRange(1000, 9000))
		})

		It("should return error", func() {
			rayCluster = rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName: "group1",
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{},
								},
							},
						},
						{
							GroupName: "group1",
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{},
								},
							},
						},
					},
				},
			}

			err := k8sClient.Create(context.TODO(), &rayCluster)
			Expect(err).To(HaveOccurred())

			Expect(err.Error()).To(ContainSubstring("worker group names must be unique"))
		})
	})
})
