package managercache

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// K8sControllerRuntimeCacheSelectors returns a map[client.Object]cache.ByObject that scopes the manager's
// informer cache to only watch KubeRay-managed Jobs (filtered by app.kubernetes.io/created-by=kuberay-operator)
// and Ray node Pods (filtered by ray.io/node-type in head|worker|redis-cleanup).
func K8sControllerRuntimeCacheSelectors() (map[client.Object]cache.ByObject, error) {
	createByLabel, err := labels.NewRequirement(utils.KubernetesCreatedByLabelKey, selection.Equals, []string{utils.ComponentName})
	if err != nil {
		return nil, err
	}
	rayNodeTypeLabel, err := labels.NewRequirement(
		utils.RayNodeTypeLabelKey,
		selection.In,
		[]string{string(rayv1.HeadNode), string(rayv1.WorkerNode), string(rayv1.RedisCleanupNode)},
	)
	if err != nil {
		return nil, err
	}

	jobSelector := labels.NewSelector().Add(*createByLabel)
	podSelector := labels.NewSelector().Add(*rayNodeTypeLabel)
	return map[client.Object]cache.ByObject{
		&batchv1.Job{}: {Label: jobSelector},
		&corev1.Pod{}:  {Label: podSelector},
	}, nil
}

// EventForwarderCacheByObject returns the cache.ByObject scoping the Event informer
// used by the Selective Event Forwarder. The field selector makes the API server
// filter the watch to Node events server-side, so the operator never receives or
// caches the (high-churn) Events of other objects.
//
// If types contains exactly one value (e.g. ["Warning"]), a field selector for "type"
// is also applied to filter out other event types at the API server level. When types
// is empty or contains multiple values, type filtering is handled client-side because
// Kubernetes field selectors do not support OR/IN expressions.
//
// Per Kubernetes validation, Events involving
// cluster-scoped objects like Nodes (where involvedObject.namespace is empty)
// must be recorded in either "default" or "kube-system". Scoping the cache's
// Namespaces to these two namespaces ensures controller-runtime starts namespace-scoped
// informers rather than an all-namespaces cluster-scoped watch, allowing RBAC
// permissions to be restricted to "default" and "kube-system".
func EventForwarderCacheByObject(types []string) cache.ByObject {
	fieldSelector := fields.OneTermEqualSelector("involvedObject.kind", "Node")
	if len(types) == 1 {
		fieldSelector = fields.AndSelectors(
			fieldSelector,
			fields.OneTermEqualSelector("type", types[0]),
		)
	}
	return cache.ByObject{
		Field: fieldSelector,
		Namespaces: map[string]cache.Config{
			metav1.NamespaceDefault: {},
			metav1.NamespaceSystem:  {},
		},
	}
}
