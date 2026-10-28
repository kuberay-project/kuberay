package expectations

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const HeadGroup = ""

// ScaleAction is the action of scale, like create and delete.
type ScaleAction string

const (
	// Create action
	Create ScaleAction = "create"
	// Delete action
	Delete ScaleAction = "delete"
)

const (
	// GroupIndex indexes pods within the specified group in RayCluster
	GroupIndex = "group"
	// RayClusterIndex indexes pods within the RayCluster
	RayClusterIndex = "raycluster"
)

var ExpectationsTimeout = time.Second * 30

// RayClusterScaleExpectation is an interface that to set and wait on expectations of RayCluster groups scale.
type RayClusterScaleExpectation interface {
	ExpectScalePod(namespace, rayClusterName, group, podName string, action ScaleAction)
	IsSatisfied(ctx context.Context, namespace, rayClusterName, group string) bool
	Delete(rayClusterName, namespace string)
}

func NewRayClusterScaleExpectation(client client.Client) RayClusterScaleExpectation {
	return &rayClusterScaleExpectationImpl{
		Client:     client,
		itemsCache: cache.NewIndexer(rayPodKey, cache.Indexers{GroupIndex: groupIndexFunc, RayClusterIndex: rayClusterIndexFunc}),
	}
}

type rayClusterScaleExpectationImpl struct {
	client.Client
	// itemsCache is only used to cache rayPod.
	itemsCache cache.Indexer
}

func (r *rayClusterScaleExpectationImpl) ExpectScalePod(namespace, rayClusterName, group, name string, action ScaleAction) {
	// Strictly limit the data type stored in itemsCache to rayPod.
	// If an error occurs, it must be due to an issue with our usage. We should panic immediately instead of returning an error.
	if err := r.itemsCache.Add(&rayPod{
		name:            name,
		namespace:       namespace,
		group:           group,
		rayCluster:      rayClusterName,
		action:          action,
		recordTimestamp: time.Now(),
	}); err != nil {
		// If an error occurs, it indicates that there is an issue with our KeyFunc.
		// This is a fatal error, panic it.
		panic(err)
	}
}

func (r *rayClusterScaleExpectationImpl) isPodScaled(ctx context.Context, rp *rayPod) bool {
	pod := &corev1.Pod{}
	switch rp.action {
	case Create:
		if err := r.Get(ctx, types.NamespacedName{Name: rp.name, Namespace: rp.namespace}, pod); err == nil {
			return true
		}
		// Tolerating extreme case:
		//   The first reconciliation created a Pod. If the Pod was quickly deleted from etcd by another component
		//   before the second reconciliation. This would lead to never satisfying the expected condition.
		//   Avoid this by setting a timeout.
		return rp.recordTimestamp.Add(ExpectationsTimeout).Before(time.Now())
	case Delete:
		if err := r.Get(ctx, types.NamespacedName{Name: rp.name, Namespace: rp.namespace}, pod); err != nil {
			return errors.IsNotFound(err)
		}
		// Tolerating extreme case:
		//   A Pod was requested to be deleted but is stuck (e.g. its node is NotReady, so the kubelet
		//   cannot confirm termination, leaving the Pod with a deletionTimestamp that never clears).
		//   Without a timeout this expectation stays unsatisfied forever, and because IsSatisfied
		//   short-circuits, it freezes ALL scaling (both scale-up and scale-down) for the whole worker
		//   group indefinitely. Once Kubernetes has marked the Pod for deletion, let the expectation
		//   expire after a timeout so reconciliation can make progress instead of deadlocking.
		return !pod.DeletionTimestamp.IsZero() &&
			rp.recordTimestamp.Add(ExpectationsTimeout).Before(time.Now())
	}
	return false
}

func (r *rayClusterScaleExpectationImpl) IsSatisfied(ctx context.Context, namespace, rayClusterName, group string) (isSatisfied bool) {
	items, err := r.itemsCache.ByIndex(GroupIndex, fmt.Sprintf("%s/%s/%s", namespace, rayClusterName, group))
	if err != nil {
		// An error occurs when there is no corresponding IndexFunc for GroupIndex. This should be a fatal error.
		panic(err)
	}
	for i := range items {
		rp := items[i].(*rayPod)
		isPodSatisfied := r.isPodScaled(ctx, rp)

		if !isPodSatisfied {
			// Log which Pod and action is blocking the group so the cause is diagnosable from logs.
			// Without this, callers only see "NotSatisfiedGroupExpectations" and cannot tell which
			// Pod is stuck (e.g. a Pod requested for deletion that never terminates).
			ctrl.LoggerFrom(ctx).Info("scale expectation not satisfied, blocking group reconcile",
				"namespace", namespace, "rayCluster", rayClusterName, "group", group,
				"pod", rp.name, "action", rp.action, "waitingFor", time.Since(rp.recordTimestamp).String())
			return false
		}

		// delete satisfied item in cache
		if err := r.itemsCache.Delete(items[i]); err != nil {
			// Fatal error in KeyFunc.
			panic(err)
		}
	}
	return true
}

func (r *rayClusterScaleExpectationImpl) Delete(rayClusterName, namespace string) {
	items, err := r.itemsCache.ByIndex(RayClusterIndex, fmt.Sprintf("%s/%s", namespace, rayClusterName))
	if err != nil {
		// An error occurs when there is no corresponding IndexFunc for RayClusterIndex. This should be a fatal error.
		panic(err)
	}
	for _, item := range items {
		if err := r.itemsCache.Delete(item); err != nil {
			// Fatal error in KeyFunc.
			panic(err)
		}
	}
}

type rayPod struct {
	recordTimestamp time.Time
	action          ScaleAction
	name            string
	namespace       string
	rayCluster      string
	group           string
}

func (p *rayPod) Key() string {
	return fmt.Sprintf("%s/%s", p.namespace, p.name)
}

func (p *rayPod) GroupKey() string {
	return fmt.Sprintf("%s/%s/%s", p.namespace, p.rayCluster, p.group)
}

func (p *rayPod) ClusterKey() string {
	return fmt.Sprintf("%s/%s", p.namespace, p.rayCluster)
}

// rayPodKey is used only for getting rayPod.Key(). The type of obj must be rayPod.
func rayPodKey(obj any) (string, error) {
	return obj.(*rayPod).Key(), nil
}

// groupIndexFunc is used only for getting rayPod.GroupKey(). The type of obj must be rayPod.
func groupIndexFunc(obj any) ([]string, error) {
	return []string{obj.(*rayPod).GroupKey()}, nil
}

// rayClusterIndexFunc is used only for getting rayPod.ClusterKey(). The type of obj must be rayPod.
func rayClusterIndexFunc(obj any) ([]string, error) {
	return []string{obj.(*rayPod).ClusterKey()}, nil
}

func NewFakeRayClusterScaleExpectation() RayClusterScaleExpectation {
	return &fakeRayClusterScaleExpectation{}
}

type fakeRayClusterScaleExpectation struct{}

func (r *fakeRayClusterScaleExpectation) ExpectScalePod(_, _, _, _ string, _ ScaleAction) {
}

func (r *fakeRayClusterScaleExpectation) IsSatisfied(_ context.Context, _, _, _ string) bool {
	return true
}

func (r *fakeRayClusterScaleExpectation) Delete(_, _ string) {}
