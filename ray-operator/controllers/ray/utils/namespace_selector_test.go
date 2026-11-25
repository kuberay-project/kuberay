package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestNamespaceMatchesSelector(t *testing.T) {
	c := fake.NewClientBuilder().WithObjects(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "watched", Labels: map[string]string{"ray.io/watch": "true"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ignored", Labels: map[string]string{"ray.io/watch": "false"}}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "unlabeled"}},
	).Build()
	selector, err := labels.Parse("ray.io/watch=true")
	require.NoError(t, err)

	tests := []struct {
		selector  labels.Selector
		name      string
		namespace string
		want      bool
	}{
		{name: "nil selector matches any namespace", selector: nil, namespace: "ignored", want: true},
		{name: "nil selector matches a missing namespace", selector: nil, namespace: "missing", want: true},
		{name: "matching labels", selector: selector, namespace: "watched", want: true},
		{name: "non-matching labels", selector: selector, namespace: "ignored", want: false},
		{name: "no labels", selector: selector, namespace: "unlabeled", want: false},
		{name: "missing namespace", selector: selector, namespace: "missing", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NamespaceMatchesSelector(context.Background(), c, tc.selector, tc.namespace)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNamespaceStartsMatching(t *testing.T) {
	selector, err := labels.Parse("ray.io/watch=true")
	require.NoError(t, err)
	matching := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Labels: map[string]string{"ray.io/watch": "true"}}}
	nonMatching := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}}
	p := namespaceStartsMatching(selector)

	require.True(t, p.Create(event.CreateEvent{Object: matching}), "create matching")
	require.False(t, p.Create(event.CreateEvent{Object: nonMatching}), "create non-matching")
	require.True(t, p.Update(event.UpdateEvent{ObjectOld: nonMatching, ObjectNew: matching}), "update false->true")
	require.False(t, p.Update(event.UpdateEvent{ObjectOld: matching, ObjectNew: matching}), "update true->true")
	require.False(t, p.Update(event.UpdateEvent{ObjectOld: matching, ObjectNew: nonMatching}), "update true->false")
	require.False(t, p.Delete(event.DeleteEvent{Object: matching}), "delete")
	require.False(t, p.Generic(event.GenericEvent{Object: matching}), "generic")
}

func TestEnqueueCustomResourcesInNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "a"}},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "b"}},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns2", Name: "c"}},
	).Build()

	mapFunc := enqueueCustomResourcesInNamespace(c, &rayv1.RayClusterList{})
	requests := mapFunc(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}})
	require.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "a"}},
		{NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "b"}},
	}, requests)
	require.Empty(t, mapFunc(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "empty"}}))
}
