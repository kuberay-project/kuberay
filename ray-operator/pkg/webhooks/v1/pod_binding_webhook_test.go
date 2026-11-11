package v1

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func newBinding(podName, nodeName string) *corev1.Binding {
	return &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: "default"},
		Target:     corev1.ObjectReference{Kind: "Node", Name: nodeName},
	}
}

func newBindingRequest(t *testing.T, binding *corev1.Binding, dryRun bool) admission.Request {
	binding.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Binding"}
	raw, err := json.Marshal(binding)
	require.NoError(t, err)
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   admissionv1.Create,
		Name:        binding.Name,
		Namespace:   binding.Namespace,
		Kind:        metav1.GroupVersionKind{Version: "v1", Kind: "Binding"},
		Resource:    metav1.GroupVersionResource{Version: "v1", Resource: "pods"},
		SubResource: "binding",
		Object:      runtime.RawExtension{Raw: raw},
		DryRun:      &dryRun,
	}}
}

// deliveredLabels returns the labels the response writes into the Binding annotation, or nil
func deliveredLabels(t *testing.T, resp admission.Response) map[string]string {
	for _, patch := range resp.Patches {
		if patch.Path != "/metadata/annotations" {
			continue
		}
		encoded, ok := patch.Value.(map[string]any)[utils.RayNodeLabelsAnnotationKey].(string)
		require.True(t, ok, "annotations patch should carry the labels annotation")
		labels := map[string]string{}
		require.NoError(t, json.Unmarshal([]byte(encoded), &labels))
		return labels
	}
	return nil
}

func drainEvents(recorder *events.FakeRecorder) []string {
	var drained []string
	for {
		select {
		case event := <-recorder.Events:
			drained = append(drained, event)
		default:
			return drained
		}
	}
}

func TestPodBindingWebhookHandle(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, true)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	topoPod := newWorkerPod("test")
	plainPod := newWorkerPod("plain")
	otherPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nginx", Namespace: "default"}}
	zoneNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}}}
	bareNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}
	// prod/ray-gpu and prod-ray/gpu both set labelRefs and both name their pods prod-ray-gpu-worker-*
	prod := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "default"},
		Spec:       rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "ray-gpu", LabelRefs: []rayv1.LabelRef{nodeLabelRef("nvidia.com/gpu.clique", "")}}}},
	}
	prodRay := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod-ray", Namespace: "default"},
		Spec:       rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "gpu", LabelRefs: []rayv1.LabelRef{nodeLabelRef("topology.kubernetes.io/zone", "")}}}},
	}
	collidingPod := newWorkerPod("gpu")
	collidingPod.Name = "prod-ray-gpu-worker-abcde"
	collidingPod.Labels[utils.RayClusterLabelKey] = "prod-ray"
	// multi-host groups with a long cluster+group name use MultiHostWorkerPodNamePrefix (45 chars), which is shorter than PodName (50 chars)
	longMultiHostCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: strings.Repeat("a", 40), Namespace: "default"},
		Spec:       rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "tpu-v6e-256", NumOfHosts: 4, LabelRefs: []rayv1.LabelRef{nodeLabelRef("topology.kubernetes.io/zone", "")}}}},
	}
	multiHostPod := newWorkerPod("tpu-v6e-256")
	multiHostPod.Name = utils.FormatMultiHostWorkerPodName(longMultiHostCluster.Name, "tpu-v6e-256", "tpu-v6e-256-x9k2m", 0)
	multiHostPod.Labels[utils.RayClusterLabelKey] = longMultiHostCluster.Name
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(newLabelRefsCluster(), prod, prodRay, longMultiHostCluster, topoPod, plainPod, otherPod, collidingPod, multiHostPod, zoneNode, bareNode).Build()
	apiReader := &countingReader{Reader: c}

	newHandler := func(allowed ...string) (*PodBindingWebhook, *events.FakeRecorder) {
		recorder := events.NewFakeRecorder(10)
		return &PodBindingWebhook{
			Client:            c,
			APIReader:         apiReader,
			Recorder:          recorder,
			Decoder:           admission.NewDecoder(scheme),
			AllowedNodeLabels: sets.New(allowed...),
		}, recorder
	}
	passThrough := func(t *testing.T, resp admission.Response, recorder *events.FakeRecorder) {
		require.True(t, resp.Allowed, resp.Result)
		assert.Empty(t, resp.Patches)
		assert.Empty(t, drainEvents(recorder))
	}
	withheld := func(t *testing.T, resp admission.Response, recorder *events.FakeRecorder, cause string) {
		require.True(t, resp.Allowed, resp.Result)
		assert.Nil(t, deliveredLabels(t, resp))
		recorded := drainEvents(recorder)
		require.Len(t, recorded, 1)
		assert.Contains(t, recorded[0], FailedToDeliverNodeLabels)
		assert.Contains(t, recorded[0], cause)
	}

	t.Run("delivers the mapped labels on the binding from the informer cache without an API read", func(t *testing.T) {
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		before := apiReader.gets
		resp := handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-a"), false))
		require.True(t, resp.Allowed, resp.Result)
		assert.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}, deliveredLabels(t, resp))
		assert.Empty(t, drainEvents(recorder))
		assert.Equal(t, before, apiReader.gets, "cached node read must not hit APIReader")
	})

	t.Run("falls back to APIReader when the bound node is not yet in the informer cache", func(t *testing.T) {
		uncachedNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-uncached", Labels: map[string]string{"topology.kubernetes.io/zone": "us-east1-b"}}}
		apiBackend := fake.NewClientBuilder().WithScheme(scheme).WithObjects(uncachedNode).Build()
		fallbackReader := &countingReader{Reader: apiBackend}
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		handler.APIReader = fallbackReader

		resp := handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-uncached"), false))
		require.True(t, resp.Allowed, resp.Result)
		assert.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-east1-b"}, deliveredLabels(t, resp))
		assert.Empty(t, drainEvents(recorder))
		assert.Equal(t, 1, fallbackReader.gets, "uncached node must fall back to APIReader")
	})

	t.Run("falls back to APIReader when a cached node lacks an allowlisted label that the API server already has", func(t *testing.T) {
		freshNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"topology.kubernetes.io/zone": "us-west1-c"}}}
		apiBackend := fake.NewClientBuilder().WithScheme(scheme).WithObjects(freshNode).Build()
		fallbackReader := &countingReader{Reader: apiBackend}
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		handler.APIReader = fallbackReader

		resp := handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-b"), false))
		require.True(t, resp.Allowed, resp.Result)
		assert.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-west1-c"}, deliveredLabels(t, resp))
		assert.Empty(t, drainEvents(recorder))
		assert.Equal(t, 1, fallbackReader.gets, "stale cached node lacking an allowlisted label must recheck APIReader once")
	})

	t.Run("does not read APIReader twice when an uncached node also lacks the label", func(t *testing.T) {
		uncachedBare := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-uncached-bare"}}
		apiBackend := fake.NewClientBuilder().WithScheme(scheme).WithObjects(uncachedBare).Build()
		fallbackReader := &countingReader{Reader: apiBackend}
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		handler.APIReader = fallbackReader

		withheld(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-uncached-bare"), false)), recorder, `node lacks label "topology.kubernetes.io/zone"`)
		assert.Equal(t, 1, fallbackReader.gets, "uncached unlabeled node must only hit APIReader once")
	})

	t.Run("delivers the mapped labels to a deterministically named multi-host worker pod", func(t *testing.T) {
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		resp := handler.Handle(ctx, newBindingRequest(t, newBinding(multiHostPod.Name, "node-a"), false))
		require.True(t, resp.Allowed, resp.Result)
		assert.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}, deliveredLabels(t, resp))
		assert.Empty(t, drainEvents(recorder))
	})

	t.Run("resolves the group from the pod labels when name prefixes collide", func(t *testing.T) {
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		resp := handler.Handle(ctx, newBindingRequest(t, newBinding(collidingPod.Name, "node-a"), false))
		require.True(t, resp.Allowed, resp.Result)
		assert.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}, deliveredLabels(t, resp))
		assert.Empty(t, drainEvents(recorder))
	})

	t.Run("withholds and records an event after checking APIReader once when the node lacks an allowlisted label", func(t *testing.T) {
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		before := apiReader.gets
		withheld(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-b"), false)), recorder, `node lacks label "topology.kubernetes.io/zone"`)
		assert.Equal(t, before+1, apiReader.gets, "cached unlabeled node should fall back to APIReader once")
	})

	t.Run("withholds and records the cached missing-label error when fallback APIReader.Get fails", func(t *testing.T) {
		emptyBackend := fake.NewClientBuilder().WithScheme(scheme).Build()
		fallbackReader := &countingReader{Reader: emptyBackend}
		handler, recorder := newHandler("topology.kubernetes.io/zone")
		handler.APIReader = fallbackReader

		withheld(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-b"), false)), recorder, `node lacks label "topology.kubernetes.io/zone"`)
		assert.Equal(t, 1, fallbackReader.gets, "cached unlabeled node should attempt APIReader once")
	})

	t.Run("withholds when a mapped label is not allowlisted without hitting APIReader", func(t *testing.T) {
		handler, recorder := newHandler()
		before := apiReader.gets
		withheld(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-a"), false)), recorder, "not in the operator's allowedNodeLabels")
		withheld(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-b"), false)), recorder, "not in the operator's allowedNodeLabels")
		assert.Equal(t, before, apiReader.gets, "non-allowlisted label must not hit APIReader even when absent from cached node")

		// A missing allowlisted label paired with a non-allowlisted label must still skip APIReader.
		multiCluster := &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "mixed", Namespace: "default"},
			Spec: rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
				GroupName: "workers",
				LabelRefs: []rayv1.LabelRef{
					nodeLabelRef("topology.kubernetes.io/zone", ""),
					nodeLabelRef("nvidia.com/gpu.clique", ""),
				},
			}}},
		}
		mixedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mixed-workers-worker-abcde",
				Namespace: "default",
				Labels: map[string]string{
					utils.RayClusterLabelKey:   "mixed",
					utils.RayNodeGroupLabelKey: "workers",
					utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
				},
			},
		}
		mixedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(multiCluster, mixedPod, bareNode).Build()
		mixedReader := &countingReader{Reader: mixedClient}
		mixedRecorder := events.NewFakeRecorder(10)
		mixedHandler := &PodBindingWebhook{
			Client:            mixedClient,
			APIReader:         mixedReader,
			Recorder:          mixedRecorder,
			Decoder:           admission.NewDecoder(scheme),
			AllowedNodeLabels: sets.New("topology.kubernetes.io/zone"),
		}
		withheld(t, mixedHandler.Handle(ctx, newBindingRequest(t, newBinding(mixedPod.Name, "node-b"), false)), mixedRecorder, "not in the operator's allowedNodeLabels")
		assert.Zero(t, mixedReader.gets, "mixed missing allowlisted + disallowed labelRefs must not hit APIReader")
	})

	t.Run("dry run records no event", func(t *testing.T) {
		handler, recorder := newHandler()
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-b"), true)), recorder)
	})

	t.Run("disabled feature gate passes through", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, false)
		handler, recorder := newHandler()
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding(topoPod.Name, "node-a"), false)), recorder)
	})

	t.Run("passes through a group without label mappings", func(t *testing.T) {
		handler, recorder := newHandler()
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding(plainPod.Name, "node-a"), false)), recorder)
	})

	t.Run("passes through non-Ray and unknown pods without an API read", func(t *testing.T) {
		handler, recorder := newHandler()
		before := apiReader.gets
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding(otherPod.Name, "node-a"), false)), recorder)
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding("ghost", "node-a"), false)), recorder)
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, newBinding(plainPod.Name, "node-a"), false)), recorder)
		assert.Equal(t, before, apiReader.gets, "unrelated bindings must be decided from the cache")
	})

	t.Run("passes through a binding that already carries labels", func(t *testing.T) {
		handler, recorder := newHandler()
		binding := newBinding(topoPod.Name, "node-a")
		binding.Annotations = map[string]string{utils.RayNodeLabelsAnnotationKey: "{}"}
		passThrough(t, handler.Handle(ctx, newBindingRequest(t, binding, false)), recorder)
	})
}

// countingReader counts Get calls, to prove bindings of unrelated pods cost no API read
type countingReader struct {
	client.Reader
	gets int
}

func (r *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	r.gets++
	return r.Reader.Get(ctx, key, obj, opts...)
}
