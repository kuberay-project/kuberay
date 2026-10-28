package ray

import (
	"context"
	"fmt"
	"sync"
	"unicode/utf8"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	// forwardedEventReason is the fallback reason set on Events re-emitted onto RayClusters
	// if the source Node Event has an empty Reason.
	forwardedEventReason = "NodeEvent"
	// maxEventNoteLength is the note length that events.k8s.io/v1 API server
	// validation enforces. An Event whose note exceeds it is rejected outright,
	// so a verbose source message must be truncated rather than dropped.
	maxEventNoteLength = 1024
	// maxEventReasonLength is the maximum reason length enforced by events.k8s.io/v1.
	maxEventReasonLength = 128
	// podNodeNameIndexField is the cache field-index key for a Pod's spec.nodeName.
	podNodeNameIndexField = "spec.nodeName"
	// nodeInvolvedObjectKind is the involvedObject.kind of the source Events we forward.
	nodeInvolvedObjectKind = "Node"
)

// EventForwarderOptions configures which Node Events are forwarded.
// An empty Sources, Reasons, or Types list means "allow all".
type EventForwarderOptions struct {
	// Sources is the allowed set of event emitters, matched against both the
	// legacy Source.Component and the new-style ReportingController fields
	// (e.g. "node-problem-detector", "nvidia-gpu-device-plugin"). Empty means all sources.
	Sources []string
	// Reasons is the allowed set of event reasons (e.g. "XIDError", "KernelDeadlock"). Empty means all reasons.
	Reasons []string
	// Types is the allowed set of event types ("Warning", "Normal"). Empty means all types.
	Types []string
}

// Validate reports whether the options are usable. types is checked because an
// unrecognized value matches no Event and would otherwise silently disable forwarding altogether.
func (o EventForwarderOptions) Validate() error {
	for _, t := range o.Types {
		if t != corev1.EventTypeNormal && t != corev1.EventTypeWarning {
			return fmt.Errorf("invalid event type %q: must be one of %q, %q",
				t, corev1.EventTypeNormal, corev1.EventTypeWarning)
		}
	}
	return nil
}

// eventFilter is the compiled form of EventForwarderOptions.
type eventFilter struct {
	sources sets.Set[string]
	reasons sets.Set[string]
	types   sets.Set[string]
}

func newEventFilter(options EventForwarderOptions) eventFilter {
	return eventFilter{
		sources: sets.New(options.Sources...),
		reasons: sets.New(options.Reasons...),
		types:   sets.New(options.Types...),
	}
}

// forwardedRecord remembers the last occurrence of a source Event that was
// forwarded, so that cache resyncs and requeues do not re-forward it while
// count bumps (recurrences of the same fault) do.
type forwardedRecord struct {
	uid   types.UID
	count int32
}

// EventForwarderReconciler watches Kubernetes Events involving Nodes and
// re-emits them onto the RayCluster custom resources whose Pods are scheduled
// on those Nodes, so they surface in the Ray Dashboard's Platform Events tab.
//
// The node->cluster join is served by a Pod field index on spec.nodeName: on
// each Node Event we list the Ray Pods on that node (filtered by the
// ray.io/cluster label), dedupe by cluster, and emit one Event per
// RayCluster with the involvedObject set to that RayCluster.
type EventForwarderReconciler struct {
	client.Client
	Recorder events.EventRecorder

	filter eventFilter

	mu sync.Mutex

	// forwarded tracks, per source Event object, the occurrence that was last
	// forwarded. Keyed by object name so entries can be dropped when the API
	// server expires the Event (default TTL is 1h), keeping the map bounded by
	// the number of live Node Events.
	forwarded map[types.NamespacedName]forwardedRecord
}

// eventPredicate filters Node Events and handles initial list deduplication.
func (r *EventForwarderReconciler) eventPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			ev, ok := e.Object.(*corev1.Event)
			if !ok || ev.InvolvedObject.Kind != nodeInvolvedObjectKind || !r.filter.matches(ev) {
				return false
			}
			if e.IsInInitialList {
				// Record baseline occurrence count so future count increments (fault recurrences)
				// are recognized as new occurrences and forwarded.
				r.markForwarded(client.ObjectKeyFromObject(ev), ev)
				return false
			}
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			ev, ok := e.ObjectNew.(*corev1.Event)
			return ok && ev.InvolvedObject.Kind == nodeInvolvedObjectKind && r.filter.matches(ev)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			ev, ok := e.Object.(*corev1.Event)
			return ok && ev.InvolvedObject.Kind == nodeInvolvedObjectKind && r.filter.matches(ev)
		},
	}
}

// NewEventForwarderReconciler returns a new EventForwarderReconciler or an error
// if the options are invalid.
func NewEventForwarderReconciler(mgr manager.Manager, options EventForwarderOptions) (*EventForwarderReconciler, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	return &EventForwarderReconciler{
		Client:    mgr.GetClient(),
		Recorder:  mgr.GetEventRecorder("kuberay-node-event-forwarder"),
		filter:    newEventFilter(options),
		forwarded: make(map[types.NamespacedName]forwardedRecord),
	}, nil
}

// SetupWithManager registers the Pod spec.nodeName field index and the Event watch.
func (r *EventForwarderReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	// The cache cannot serve a MatchingFields List unless the field is indexed;
	// without this the List in Reconcile returns an error, not empty results.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{},
		podNodeNameIndexField, func(o client.Object) []string {
			pod, ok := o.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("node-event-forwarder").
		For(&corev1.Event{}, builder.WithPredicates(r.eventPredicate())).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("NodeEventForwarder")
				if request != nil {
					logger = logger.WithValues("Event", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

// Note: RBAC for watching Node events (get, list, watch) is intentionally scoped
// to the "default" and "kube-system" namespaces via dedicated Roles rather than
// being granted cluster-wide in the base ClusterRole, enforcing least privilege.
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch

// [WARNING]: There MUST be a newline after kubebuilder markers.

// Reconcile forwards a single Node Event onto every RayCluster with Pods on that Node.
func (r *EventForwarderReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	src := &corev1.Event{}
	if err := r.Get(ctx, request.NamespacedName, src); err != nil {
		if errors.IsNotFound(err) {
			// The API server expired or deleted the source Event; drop its
			// tracking entry so the forwarded map stays bounded.
			r.forget(request.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// The cache field selector and watch predicate already enforce these;
	// re-check here so behavior does not depend on how we are driven.
	if src.InvolvedObject.Kind != nodeInvolvedObjectKind || !r.filter.matches(src) {
		return ctrl.Result{}, nil
	}
	if !r.shouldForward(request.NamespacedName, src) {
		return ctrl.Result{}, nil
	}

	nodeName := src.InvolvedObject.Name

	// Served from the spec.nodeName field index. The manager cache only holds
	// Ray node Pods, and the label filter narrows to those with a cluster label.
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.MatchingFields{podNodeNameIndexField: nodeName},
		client.HasLabels{utils.RayClusterLabelKey},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Many Ray Pods may share one node; collapse to one Event per RayCluster.
	clusterKeys := make(map[types.NamespacedName]struct{})
	for i := range pods.Items {
		pod := &pods.Items[i]
		if clusterName := pod.Labels[utils.RayClusterLabelKey]; clusterName != "" {
			clusterKeys[types.NamespacedName{Namespace: pod.Namespace, Name: clusterName}] = struct{}{}
		}
	}

	targets, err := r.resolveTargets(ctx, clusterKeys)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(targets) > 0 {
		// The affected Node is attached as the related object so a reader of the
		// forwarded Event can get back to the source Node.
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName, UID: src.InvolvedObject.UID}}
		reason := forwardedReason(src)
		note := forwardedNote(nodeName, src)

		for _, target := range targets {
			r.Recorder.Eventf(target, node, src.Type, reason, "Forward", "%s", note)
			logger.V(1).Info("forwarded node event to RayCluster",
				"node", nodeName, "target", client.ObjectKeyFromObject(target), "sourceReason", src.Reason)
		}
	}

	// Mark the event as forwarded (even if no targets were resolved, e.g. no Ray workload
	// on the node or clusters already deleted) so an informer resync or requeue does not
	// retroactively forward it if a Ray pod is scheduled onto this node later.
	r.markForwarded(request.NamespacedName, src)
	return ctrl.Result{}, nil
}

// resolveTargets returns the RayClusters in clusterKeys that still exist.
// Events are forwarded strictly to RayCluster (and not parent RayJob or RayService)
// to prevent duplicate events in the Ray Dashboard's Platform Events tab, where
// watchers for both the cluster and parent resources run concurrently.
func (r *EventForwarderReconciler) resolveTargets(ctx context.Context, clusterKeys map[types.NamespacedName]struct{}) ([]client.Object, error) {
	var targets []client.Object

	for clusterKey := range clusterKeys {
		cluster := &rayv1.RayCluster{}
		if err := r.Get(ctx, clusterKey, cluster); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		targets = append(targets, cluster)
	}

	return targets, nil
}

// forwardedReason builds the Reason for the forwarded Event, combining the source
// reason with the event source (e.g. "XIDError/node-problem-detector"), falling back
// to forwardedEventReason if Reason is empty, and truncating to maxEventReasonLength.
func forwardedReason(src *corev1.Event) string {
	reason := src.Reason
	if reason == "" {
		reason = forwardedEventReason
	}
	if source := eventSource(src); source != "" {
		reason = fmt.Sprintf("%s/%s", reason, source)
	}
	return truncateString(reason, maxEventReasonLength, "")
}

// forwardedNote formats the message for the forwarded Event, capturing the affected Node,
// reason, source component, and original message, truncated to maxEventNoteLength.
func forwardedNote(nodeName string, src *corev1.Event) string {
	reason := src.Reason
	if reason == "" {
		reason = forwardedEventReason
	}
	return truncateString(fmt.Sprintf(
		"Node event observed on Node %q (reason: %s, source: %s): %s",
		nodeName, reason, eventSource(src), src.Message), maxEventNoteLength, "...")
}

// truncateString truncates s to at most maxLen bytes, cutting at a UTF-8 rune
// boundary and appending suffix if truncation occurred.
func truncateString(s string, maxLen int, suffix string) string {
	if len(s) <= maxLen {
		return s
	}

	cut := maxLen - len(suffix)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + suffix
}

// shouldForward reports whether this occurrence of the source Event is new.
// Kubernetes aggregates recurring events by bumping count (or series.count) on
// the same object, so a higher count than last forwarded means the fault
// recurred. A different UID under the same name means a brand-new Event.
func (r *EventForwarderReconciler) shouldForward(key types.NamespacedName, e *corev1.Event) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	last, ok := r.forwarded[key]
	if !ok || last.uid != e.UID {
		return true
	}
	return occurrenceCount(e) > last.count
}

func (r *EventForwarderReconciler) markForwarded(key types.NamespacedName, e *corev1.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := occurrenceCount(e)
	if last, ok := r.forwarded[key]; !ok || last.uid != e.UID || count > last.count {
		r.forwarded[key] = forwardedRecord{uid: e.UID, count: count}
	}
}

func (r *EventForwarderReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.forwarded, key)
}

// occurrenceCount returns how many times the source Event has occurred,
// handling both legacy (count) and new-style (series.count) aggregation.
func occurrenceCount(e *corev1.Event) int32 {
	if e.Series != nil && e.Series.Count > 0 {
		return e.Series.Count
	}
	if e.Count > 0 {
		return e.Count
	}
	return 1
}

// eventSource returns the component that emitted the Event, handling both
// legacy (source.component) and new-style (reportingController) Events.
func eventSource(e *corev1.Event) string {
	if e.Source.Component != "" {
		return e.Source.Component
	}
	return e.ReportingController
}

func (f eventFilter) matches(e *corev1.Event) bool {
	if f.types.Len() > 0 && !f.types.Has(e.Type) {
		return false
	}
	if f.reasons.Len() > 0 && !f.reasons.Has(e.Reason) {
		return false
	}
	if f.sources.Len() > 0 {
		if !f.sources.Has(e.Source.Component) && !f.sources.Has(e.ReportingController) {
			return false
		}
	}
	return true
}
