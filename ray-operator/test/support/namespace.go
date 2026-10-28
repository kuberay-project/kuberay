package support

import (
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func createTestNamespace(t Test, options ...Option[*corev1.Namespace]) *corev1.Namespace {
	t.T().Helper()
	namespace := &corev1.Namespace{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Namespace",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "test-ns-",
		},
	}

	for _, option := range options {
		require.NoError(t.T(), option.applyTo(namespace))
	}

	namespace, err := t.Client().Core().CoreV1().Namespaces().Create(t.Ctx(), namespace, metav1.CreateOptions{})
	require.NoError(t.T(), err)

	return namespace
}

func deleteTestNamespace(t Test, namespace *corev1.Namespace) {
	t.T().Helper()
	propagationPolicy := metav1.DeletePropagationBackground
	err := t.Client().Core().CoreV1().Namespaces().Delete(t.Ctx(), namespace.Name, metav1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	})
	require.NoError(t.T(), err)
}

func CreateResourceQuota(t Test, namespace string, name string, cpu string, memory string) *corev1.ResourceQuota {
	t.T().Helper()
	require.NotEmpty(t.T(), namespace, "Namespace name must be set prior to creating a resource quota")

	resourceQuota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsCPU:    resource.MustParse(cpu),
				corev1.ResourceLimitsMemory: resource.MustParse(memory),
			},
		},
	}

	createdResourceQuota, err := t.Client().Core().CoreV1().ResourceQuotas(namespace).Create(t.Ctx(), resourceQuota, metav1.CreateOptions{})
	require.NoError(t.T(), err, "Failed to create ResourceQuota %s", resourceQuota.Name)

	t.T().Cleanup(func() {
		err := t.Client().Core().CoreV1().ResourceQuotas(namespace).Delete(t.Ctx(), resourceQuota.Name, metav1.DeleteOptions{})
		require.NoError(t.T(), err, "Failed to delete ResourceQuota %s", resourceQuota.Name)
	})

	return createdResourceQuota
}
