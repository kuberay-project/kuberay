package ray

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestEventForwarder_OneEventPerClusterAcrossPods(t *testing.T) {
	// Two Pods of cluster-a and one Pod of cluster-b all on node-1 => two Events.
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-b", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		rayPodOnNode("a-worker", "cluster-a"),
		rayPodOnNode("b-head", "cluster-b"),
		warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79"),
	)

	reconcileForwarderEvent(t, r, "evt-1")

	assert.ElementsMatch(t,
		[]string{"RayCluster/default/cluster-a", "RayCluster/default/cluster-b"},
		recorder.targets(t))
	for _, e := range recorder.events {
		assert.Equal(t, corev1.EventTypeWarning, e.eventtype)
		assert.Equal(t, "XIDError/node-problem-detector", e.reason)
		assert.Contains(t, e.message, `Node "node-1"`)
		assert.Contains(t, e.message, "XIDError")
		assert.Contains(t, e.message, "node-problem-detector")
		assert.Contains(t, e.message, "Caught XID error, XID=79")
	}
}

func TestEventForwarder_SkipsDeletedRayCluster(t *testing.T) {
	// Pods outlive their RayCluster briefly during deletion.
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79"),
	)

	reconcileForwarderEvent(t, r, "evt-1")

	assert.Empty(t, recorder.events)
}

func TestEventForwarder_RelatesForwardedEventToNode(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "kubelet", "XIDError", "Caught XID error, XID=79"),
	)

	reconcileForwarderEvent(t, r, "evt-1")

	require.Len(t, recorder.events, 1)
	node, ok := recorder.events[0].related.(*corev1.Node)
	require.True(t, ok, "the related object must be the affected Node, got %T", recorder.events[0].related)
	assert.Equal(t, "node-1", node.Name)
}

func TestEventForwarder_TruncatesLongMessages(t *testing.T) {
	// events.k8s.io/v1 rejects a note longer than maxEventNoteLength, so a verbose
	// source message must be trimmed rather than lost.
	evt := warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", strings.Repeat("x", 4000))

	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		evt,
	)

	reconcileForwarderEvent(t, r, "evt-1")

	require.Len(t, recorder.events, 1)
	message := recorder.events[0].message
	assert.LessOrEqual(t, len(message), maxEventNoteLength)
	assert.Contains(t, message, `Node "node-1"`, "the prefix must survive truncation")
}

func TestEventForwarder_TruncatesLongReason(t *testing.T) {
	// 127 ASCII bytes followed by a 2-byte UTF-8 rune ("é" = \u00e9).
	// A naive slice at maxEventReasonLength (128) would split the rune, producing invalid UTF-8.
	evt := warningNodeEvent("evt-1", "node-1", "", strings.Repeat("a", 127)+"é", "reason truncation test")

	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		evt,
	)

	reconcileForwarderEvent(t, r, "evt-1")

	require.Len(t, recorder.events, 1)
	reason := recorder.events[0].reason
	assert.LessOrEqual(t, len(reason), maxEventReasonLength)
	assert.True(t, utf8.ValidString(reason), "truncated reason must be valid UTF-8")
	assert.Equal(t, strings.Repeat("a", 127), reason)
}

func TestEventForwarder_FallbackReasonWhenEmpty(t *testing.T) {
	recorder := &capturingRecorder{}
	evt := normalNodeEvent("evt-1", "node-1", "kubelet", "", "Node node-1 status is now: NodeReady")

	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		evt,
	)

	reconcileForwarderEvent(t, r, "evt-1")

	require.Len(t, recorder.events, 1)
	e := recorder.events[0]
	assert.Equal(t, corev1.EventTypeNormal, e.eventtype)
	assert.Equal(t, "NodeEvent/kubelet", e.reason)
	assert.Equal(t, `Node event observed on Node "node-1" (reason: NodeEvent, source: kubelet): Node node-1 status is now: NodeReady`, e.message)
}

func TestEventForwarderOptions_Validate(t *testing.T) {
	tests := []struct {
		name        string
		errContains string
		types       []string
	}{
		{name: "empty is valid"},
		{name: "both types", types: []string{corev1.EventTypeWarning, corev1.EventTypeNormal}},
		{name: "wrong case", types: []string{"warning"}, errContains: `invalid event type "warning"`},
		{name: "unknown type", types: []string{corev1.EventTypeWarning, "Critical"}, errContains: `invalid event type "Critical"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := EventForwarderOptions{Types: tc.types}.Validate()
			if tc.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestEventForwarder_SkipsNodesWithoutRayPods(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-2", "node-2", "kubelet", "XIDError", "Caught XID error, XID=79"), // event for a node with no Ray pods
	)

	reconcileForwarderEvent(t, r, "evt-2")

	assert.Empty(t, recorder.events)

	// An informer resync occurs after a Ray pod is later scheduled onto node-2.
	// Because the event was already observed and marked forwarded, the resync must not
	// retroactively forward it.
	podOnNode2 := rayPodOnNode("a-worker", "cluster-a")
	podOnNode2.Spec.NodeName = "node-2"
	require.NoError(t, r.Create(context.Background(), podOnNode2))

	reconcileForwarderEvent(t, r, "evt-2") // informer resync
	assert.Empty(t, recorder.events, "resync must not retroactively forward old event to newly scheduled pod")

	// If the fault recurs (count bump), it must now be forwarded to cluster-a.
	ctx := context.Background()
	evt := &corev1.Event{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "evt-2", Namespace: "default"}, evt))
	evt.Count = 2
	evt.LastTimestamp = metav1.NewTime(time.Now())
	require.NoError(t, r.Update(ctx, evt))

	reconcileForwarderEvent(t, r, "evt-2")
	assert.Len(t, recorder.events, 1, "fault recurrence on node-2 should now be forwarded")
}

func TestEventForwarder_ResyncDoesNotReforward(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "nvidia-gpu-device-plugin", "XIDError", "Caught XID error, XID=79"),
	)

	reconcileForwarderEvent(t, r, "evt-1")
	reconcileForwarderEvent(t, r, "evt-1") // resync/requeue of the unchanged Event

	assert.Len(t, recorder.events, 1, "an unchanged source Event must be forwarded only once")
}

func TestEventForwarder_CountBumpReforwards(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79"),
	)
	ctx := context.Background()

	reconcileForwarderEvent(t, r, "evt-1")

	// The fault recurs: Kubernetes aggregates it into the same Event object by
	// bumping count and lastTimestamp.
	evt := &corev1.Event{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "evt-1", Namespace: "default"}, evt))
	evt.Count = 3
	evt.LastTimestamp = metav1.NewTime(time.Now())
	require.NoError(t, r.Update(ctx, evt))

	reconcileForwarderEvent(t, r, "evt-1")
	reconcileForwarderEvent(t, r, "evt-1") // no further occurrences

	assert.Len(t, recorder.events, 2, "a recurrence (count bump) must be forwarded again")
}

func TestEventForwarder_NewUIDUnderSameNameForwards(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79"),
	)
	ctx := context.Background()

	reconcileForwarderEvent(t, r, "evt-1")

	// The Event expires and a new one is created under the same name.
	evt := &corev1.Event{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: "evt-1", Namespace: "default"}, evt))
	require.NoError(t, r.Delete(ctx, evt))
	replacement := warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79")
	replacement.UID = "evt-1-uid-2"
	require.NoError(t, r.Create(ctx, replacement))

	reconcileForwarderEvent(t, r, "evt-1")

	assert.Len(t, recorder.events, 2, "a new source Event reusing the name must be forwarded")
}

func TestEventForwarder_ForgetsDeletedEvents(t *testing.T) {
	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		warningNodeEvent("evt-1", "node-1", "kubelet", "XIDError", "Caught XID error, XID=79"),
	)
	ctx := context.Background()
	key := types.NamespacedName{Name: "evt-1", Namespace: "default"}

	reconcileForwarderEvent(t, r, "evt-1")
	assert.Contains(t, r.forwarded, key)

	evt := &corev1.Event{}
	require.NoError(t, r.Get(ctx, key, evt))
	require.NoError(t, r.Delete(ctx, evt))
	reconcileForwarderEvent(t, r, "evt-1") // delete notification

	assert.NotContains(t, r.forwarded, key, "tracking entries of expired Events must be dropped")
}

func TestEventForwarder_Filters(t *testing.T) {
	tests := []struct {
		mutate  func(*corev1.Event)
		name    string
		options EventForwarderOptions
		want    int
	}{
		{
			name:   "normal events are forwarded when Types is empty",
			mutate: func(e *corev1.Event) { e.Type = corev1.EventTypeNormal },
			want:   1,
		},
		{
			name:    "normal events dropped when configured for Warning only",
			options: EventForwarderOptions{Types: []string{corev1.EventTypeWarning}},
			mutate:  func(e *corev1.Event) { e.Type = corev1.EventTypeNormal },
			want:    0,
		},
		{
			name:    "normal events forwarded when configured",
			options: EventForwarderOptions{Types: []string{corev1.EventTypeNormal, corev1.EventTypeWarning}},
			mutate:  func(e *corev1.Event) { e.Type = corev1.EventTypeNormal },
			want:    1,
		},
		{
			name:    "matching source component",
			options: EventForwarderOptions{Sources: []string{"node-problem-detector"}},
			mutate:  func(_ *corev1.Event) {},
			want:    1,
		},
		{
			name:    "matching reportingController",
			options: EventForwarderOptions{Sources: []string{"nvidia-gpu-device-plugin"}},
			mutate: func(e *corev1.Event) {
				e.Source = corev1.EventSource{}
				e.ReportingController = "nvidia-gpu-device-plugin"
			},
			want: 1,
		},
		{
			name:    "non-matching source",
			options: EventForwarderOptions{Sources: []string{"nvidia-gpu-device-plugin"}},
			mutate:  func(_ *corev1.Event) {},
			want:    0,
		},
		{
			name:    "matching reason",
			options: EventForwarderOptions{Reasons: []string{"XIDError", "KernelDeadlock"}},
			mutate:  func(_ *corev1.Event) {},
			want:    1,
		},
		{
			name:    "non-matching reason",
			options: EventForwarderOptions{Reasons: []string{"KernelDeadlock"}},
			mutate:  func(_ *corev1.Event) {},
			want:    0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evt := warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79")
			tc.mutate(evt)
			recorder := &capturingRecorder{}
			r := newEventForwarder(t, recorder, tc.options,
				&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
				rayPodOnNode("a-head", "cluster-a"),
				evt,
			)

			reconcileForwarderEvent(t, r, "evt-1")

			assert.Len(t, recorder.events, tc.want)
		})
	}
}

func TestEventForwarder_RecurrenceOfOldEventForwards(t *testing.T) {
	// The Event object was seen in the initial list, but the fault recurs after
	// startup: count increments and it must be forwarded.
	evt := warningNodeEvent("evt-1", "node-1", "node-problem-detector", "XIDError", "Caught XID error, XID=79")
	evt.Count = 2

	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		evt,
	)
	// Simulate evt-1 having been recorded during initial list at count 1
	key := types.NamespacedName{Namespace: "default", Name: "evt-1"}
	r.forwarded[key] = forwardedRecord{uid: types.UID("evt-1-uid"), count: 1}

	reconcileForwarderEvent(t, r, "evt-1")

	assert.Len(t, recorder.events, 1)
}

func TestEventForwarder_IgnoresNonNodeEvents(t *testing.T) {
	podEvent := warningNodeEvent("evt-pod", "a-head", "kubelet", "XIDError", "Caught XID error, XID=79")
	podEvent.InvolvedObject = corev1.ObjectReference{Kind: "Pod", Name: "a-head", Namespace: "default"}

	recorder := &capturingRecorder{}
	r := newEventForwarder(t, recorder, EventForwarderOptions{},
		&rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", Namespace: "default"}},
		rayPodOnNode("a-head", "cluster-a"),
		podEvent,
	)

	reconcileForwarderEvent(t, r, "evt-pod")

	assert.Empty(t, recorder.events)
}

// capturedEvent records one emission through the capturingRecorder.
type capturedEvent struct {
	object    runtime.Object
	related   runtime.Object
	eventtype string
	reason    string
	action    string
	message   string
}

// capturingRecorder implements record.EventRecorder and captures the target
// object and annotations, which record.FakeRecorder discards.
type capturingRecorder struct {
	events []capturedEvent
}

func (c *capturingRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	c.events = append(c.events, capturedEvent{
		object:    regarding,
		related:   related,
		eventtype: eventtype,
		reason:    reason,
		action:    action,
		message:   fmt.Sprintf(note, args...),
	})
}

// targets renders the involved objects of all captured events as
// "Kind/namespace/name" strings for order-independent assertions.
func (c *capturingRecorder) targets(t *testing.T) []string {
	t.Helper()
	out := make([]string, 0, len(c.events))
	for _, e := range c.events {
		switch o := e.object.(type) {
		case *rayv1.RayCluster:
			out = append(out, "RayCluster/"+o.Namespace+"/"+o.Name)
		default:
			t.Fatalf("unexpected event target type %T", e.object)
		}
	}
	return out
}

func forwarderScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	return scheme
}

func rayPodOnNode(name, cluster string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{utils.RayClusterLabelKey: cluster},
		},
		Spec: corev1.PodSpec{NodeName: "node-1"},
	}
}

// warningNodeEvent returns a legacy-style Warning Node event, as emitted by
// components like node-problem-detector.
func warningNodeEvent(name, node, source, reason, message string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
		InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: node},
		Source:         corev1.EventSource{Component: source},
		Reason:         reason,
		Message:        message,
		Type:           corev1.EventTypeWarning,
		Count:          1,
		LastTimestamp:  metav1.NewTime(time.Now()),
	}
}

// normalNodeEvent returns an informational Normal Node event, as emitted by
// components like kubelet.
func normalNodeEvent(name, node, source, reason, message string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")},
		InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: node},
		Source:         corev1.EventSource{Component: source},
		Reason:         reason,
		Message:        message,
		Type:           corev1.EventTypeNormal,
		Count:          1,
		LastTimestamp:  metav1.NewTime(time.Now()),
	}
}

// newEventForwarder builds a reconciler over a fake client that mirrors the
// production cache: a spec.nodeName field index over Pods.
func newEventForwarder(t *testing.T, recorder *capturingRecorder, options EventForwarderOptions, objs ...client.Object) *EventForwarderReconciler {
	t.Helper()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(forwarderScheme(t)).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, podNodeNameIndexField, func(o client.Object) []string {
			pod, ok := o.(*corev1.Pod)
			if !ok || pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		Build()
	return &EventForwarderReconciler{
		Client:    fakeClient,
		Recorder:  recorder,
		filter:    newEventFilter(options),
		forwarded: make(map[types.NamespacedName]forwardedRecord),
	}
}

func reconcileForwarderEvent(t *testing.T, r *EventForwarderReconciler, eventName string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: eventName, Namespace: "default"},
	})
	require.NoError(t, err)
}
