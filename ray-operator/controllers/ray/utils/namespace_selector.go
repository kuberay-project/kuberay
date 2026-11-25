package utils

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// NamespaceMatchesSelector reports whether the namespace's labels match the selector.
// A nil selector matches every namespace. A namespace that does not exist matches nothing.
// The Namespace is read as metadata only, which the manager serves from a metadata-only informer.
func NamespaceMatchesSelector(ctx context.Context, c client.Reader, selector labels.Selector, namespace string) (bool, error) {
	if selector == nil {
		return true, nil
	}
	ns := &metav1.PartialObjectMetadata{}
	ns.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Namespace"))
	if err := c.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return selector.Matches(labels.Set(ns.GetLabels())), nil
}

// WatchMatchingNamespaces makes the controller built by b enqueue every custom resource of the type held by
// emptyList in a namespace as soon as that namespace starts matching the selector, so those resources are
// reconciled immediately instead of waiting for their next event. It is a no-op if selector is nil.
func WatchMatchingNamespaces(b *builder.Builder, c client.Reader, selector labels.Selector, emptyList client.ObjectList) *builder.Builder {
	if selector == nil {
		return b
	}
	return b.WatchesMetadata(
		&corev1.Namespace{},
		handler.EnqueueRequestsFromMapFunc(enqueueCustomResourcesInNamespace(c, emptyList)),
		builder.WithPredicates(namespaceStartsMatching(selector)),
	)
}

// namespaceStartsMatching passes Namespace events where the namespace newly matches the selector.
// Create events are included so that a namespace that appears after its custom resources were skipped is not missed.
func namespaceStartsMatching(selector labels.Selector) predicate.Funcs {
	matches := func(obj client.Object) bool { return selector.Matches(labels.Set(obj.GetLabels())) }
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return matches(e.Object) },
		UpdateFunc:  func(e event.UpdateEvent) bool { return !matches(e.ObjectOld) && matches(e.ObjectNew) },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// enqueueCustomResourcesInNamespace returns a map function that lists the custom resources of the type held by
// emptyList in the given Namespace and returns a reconcile request for each.
func enqueueCustomResourcesInNamespace(c client.Reader, emptyList client.ObjectList) handler.MapFunc {
	return func(ctx context.Context, ns client.Object) []reconcile.Request {
		list := emptyList.DeepCopyObject().(client.ObjectList)
		if err := c.List(ctx, list, client.InNamespace(ns.GetName())); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to list custom resources in namespace", "namespace", ns.GetName())
			return nil
		}
		var requests []reconcile.Request
		_ = meta.EachListItem(list, func(o runtime.Object) error {
			if accessor, err := meta.Accessor(o); err == nil {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: accessor.GetNamespace(), Name: accessor.GetName()}})
			}
			return nil
		})
		return requests
	}
}
