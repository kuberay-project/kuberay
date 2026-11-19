package v1

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1alpha1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1alpha1" //nolint:staticcheck // ray.io/v1alpha1 is still a served RayCluster version; this suite intentionally exercises it to prove the worker group removal guard also applies there.
)

var _ = Describe("RayCluster CRD validation", func() {
	It("allows worker groups to be added and modified", func() {
		rayCluster := newRayClusterForCRDValidation("raycluster-update", "small-group")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		replicas := int32(2)
		rayCluster.Spec.WorkerGroupSpecs[0].Replicas = &replicas
		rayCluster.Spec.WorkerGroupSpecs = append(rayCluster.Spec.WorkerGroupSpecs, newWorkerGroupForCRDValidation("large-group"))
		Expect(k8sClient.Update(context.Background(), rayCluster)).To(Succeed())
	})

	It("allows worker groups to be reordered", func() {
		rayCluster := newRayClusterForCRDValidation("raycluster-reorder", "small-group", "large-group")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.WorkerGroupSpecs[0], rayCluster.Spec.WorkerGroupSpecs[1] = rayCluster.Spec.WorkerGroupSpecs[1], rayCluster.Spec.WorkerGroupSpecs[0]
		Expect(k8sClient.Update(context.Background(), rayCluster)).To(Succeed())
	})

	It("rejects worker group removal", func() {
		rayCluster := newRayClusterForCRDValidation("raycluster-remove", "small-group", "large-group")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.WorkerGroupSpecs = rayCluster.Spec.WorkerGroupSpecs[:1]
		err := k8sClient.Update(context.Background(), rayCluster)
		Expect(err).To(MatchError(ContainSubstring("workerGroupSpecs entries cannot be removed")))
	})

	It("allows updates to a head-only cluster that has no worker groups", func() {
		rayCluster := newRayClusterForCRDValidation("raycluster-head-only")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.RayVersion = "2.9.0"
		Expect(k8sClient.Update(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Labels = map[string]string{"foo": "bar"}
		Expect(k8sClient.Update(context.Background(), rayCluster)).To(Succeed())
	})

	It("allows updates that omit workerGroupSpecs on a cluster stored with an empty list", func() {
		name := uniqueNameForCRDValidation("raycluster-empty-list")
		created := &unstructured.Unstructured{}
		created.SetGroupVersionKind(rayv1.GroupVersion.WithKind("RayCluster"))
		created.SetNamespace("default")
		created.SetName(name)
		Expect(unstructured.SetNestedField(created.Object,
			map[string]any{"containers": []any{map[string]any{"name": "ray-head", "image": "rayproject/ray"}}},
			"spec", "headGroupSpec", "template", "spec")).To(Succeed())
		Expect(unstructured.SetNestedSlice(created.Object, []any{}, "spec", "workerGroupSpecs")).To(Succeed())
		Expect(k8sClient.Create(context.Background(), created)).To(Succeed())

		// A typed update drops the empty slice via omitempty, mirroring controller finalizer writes.
		typed := &rayv1.RayCluster{}
		Expect(k8sClient.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, typed)).To(Succeed())
		typed.Labels = map[string]string{"foo": "bar"}
		Expect(k8sClient.Update(context.Background(), typed)).To(Succeed())
	})

	It("rejects removing the only worker group", func() {
		rayCluster := newRayClusterForCRDValidation("raycluster-drain-all", "small-group")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.WorkerGroupSpecs = nil
		err := k8sClient.Update(context.Background(), rayCluster)
		Expect(err).To(MatchError(ContainSubstring("workerGroupSpecs entries cannot be removed")))
	})

	It("rejects worker group removal through the served v1alpha1 version", func() {
		rayCluster := newV1alpha1RayClusterForCRDValidation("raycluster-v1alpha1-remove", "small-group", "large-group")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.WorkerGroupSpecs = rayCluster.Spec.WorkerGroupSpecs[:1]
		err := k8sClient.Update(context.Background(), rayCluster)
		Expect(err).To(MatchError(ContainSubstring("workerGroupSpecs entries cannot be removed")))
	})

	It("allows updates to a head-only v1alpha1 cluster that has no worker groups", func() {
		rayCluster := newV1alpha1RayClusterForCRDValidation("raycluster-v1alpha1-head-only")
		Expect(k8sClient.Create(context.Background(), rayCluster)).To(Succeed())

		rayCluster.Spec.RayVersion = "2.9.0"
		Expect(k8sClient.Update(context.Background(), rayCluster)).To(Succeed())
	})

	It("allows worker group removal from a RayService cluster template", func() {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      uniqueNameForCRDValidation("rayservice-remove"),
				Namespace: "default",
			},
			Spec: rayv1.RayServiceSpec{
				RayClusterSpec: newRayClusterSpecForCRDValidation("small-group", "large-group"),
			},
		}
		Expect(k8sClient.Create(context.Background(), rayService)).To(Succeed())

		rayService.Spec.RayClusterSpec.WorkerGroupSpecs = rayService.Spec.RayClusterSpec.WorkerGroupSpecs[:1]
		Expect(k8sClient.Update(context.Background(), rayService)).To(Succeed())
	})
})

func newRayClusterForCRDValidation(name string, groupNames ...string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniqueNameForCRDValidation(name),
			Namespace: "default",
		},
		Spec: newRayClusterSpecForCRDValidation(groupNames...),
	}
}

func newRayClusterSpecForCRDValidation(groupNames ...string) rayv1.RayClusterSpec {
	workerGroups := make([]rayv1.WorkerGroupSpec, 0, len(groupNames))
	for _, groupName := range groupNames {
		workerGroups = append(workerGroups, newWorkerGroupForCRDValidation(groupName))
	}
	return rayv1.RayClusterSpec{
		HeadGroupSpec: rayv1.HeadGroupSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{}},
			},
		},
		WorkerGroupSpecs: workerGroups,
	}
}

func newWorkerGroupForCRDValidation(groupName string) rayv1.WorkerGroupSpec {
	return rayv1.WorkerGroupSpec{
		GroupName: groupName,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{}},
		},
	}
}

func uniqueNameForCRDValidation(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.IntnRange(1000, 9000))
}

func newV1alpha1RayClusterForCRDValidation(name string, groupNames ...string) *rayv1alpha1.RayCluster { //nolint:staticcheck // builds a v1alpha1 RayCluster on purpose to test the removal guard on the deprecated-but-served version.
	minReplicas := int32(0)
	maxReplicas := int32(1)
	workerGroups := make([]rayv1alpha1.WorkerGroupSpec, 0, len(groupNames))
	for _, groupName := range groupNames {
		workerGroups = append(workerGroups, rayv1alpha1.WorkerGroupSpec{
			GroupName:      groupName,
			MinReplicas:    &minReplicas,
			MaxReplicas:    &maxReplicas,
			RayStartParams: map[string]string{},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{}},
			},
		})
	}
	return &rayv1alpha1.RayCluster{ //nolint:staticcheck // builds a v1alpha1 RayCluster on purpose to test the removal guard on the deprecated-but-served version.
		ObjectMeta: metav1.ObjectMeta{
			Name:      uniqueNameForCRDValidation(name),
			Namespace: "default",
		},
		Spec: rayv1alpha1.RayClusterSpec{
			HeadGroupSpec: rayv1alpha1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{}},
				},
			},
			WorkerGroupSpecs: workerGroups,
		},
	}
}
