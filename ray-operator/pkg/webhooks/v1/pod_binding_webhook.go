package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// FailedToDeliverNodeLabels is the reason of the Warning event recorded on a pod whose node labels were not delivered
const FailedToDeliverNodeLabels = "FailedToDeliverNodeLabels"

var podBindingLog = logf.Log.WithName("pod-binding-webhook")

//+kubebuilder:webhook:path=/mutate-v1-pod-binding,mutating=true,failurePolicy=ignore,sideEffects=NoneOnDryRun,groups="",resources=pods/binding,verbs=create,versions=v1,name=mpodbinding.kb.io,admissionReviewVersions=v1,timeoutSeconds=5
//+kubebuilder:rbac:groups=core,resources=nodes,verbs=get

// SetupPodBindingWebhookWithManager registers the pods/binding CREATE mutating webhook, which writes the bound node's
// mapped labels into the Binding annotations. The API server copies them onto the pod together with spec.nodeName.
func SetupPodBindingWebhookWithManager(mgr ctrl.Manager, config configapi.Configuration) error {
	mgr.GetWebhookServer().Register("/mutate-v1-pod-binding", &webhook.Admission{Handler: &PodBindingWebhook{
		Client:            mgr.GetClient(),
		APIReader:         mgr.GetAPIReader(),
		Recorder:          mgr.GetEventRecorder("kuberay-pod-binding-webhook"),
		Decoder:           admission.NewDecoder(mgr.GetScheme()),
		AllowedNodeLabels: sets.New(config.AllowedNodeLabels...),
	}})
	return nil
}

type PodBindingWebhook struct {
	// Client reads pods and RayClusters from the manager cache
	Client client.Reader
	// APIReader reads pods the cache does not hold and node metadata from the API server
	APIReader client.Reader
	// Recorder emits the Warning event when labels are withheld, may be nil
	Recorder events.EventRecorder
	Decoder  admission.Decoder
	// AllowedNodeLabels is the operator allowlist for labelRefs
	AllowedNodeLabels sets.Set[string]
}

func (w *PodBindingWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	if !features.Enabled(features.NodeLabelDelivery) {
		return admission.Allowed("NodeLabelDelivery feature gate is disabled")
	}
	binding := &corev1.Binding{}
	if err := w.Decoder.Decode(req, binding); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if _, done := binding.Annotations[utils.RayNodeLabelsAnnotationKey]; done {
		// already delivered e.g. on a webhook reinvocation
		return admission.Allowed("node labels already set on the binding")
	}
	nodeName := binding.Target.Name
	podName := req.Name
	if nodeName == "" {
		return admission.Allowed("binding has no target node")
	}
	log := podBindingLog.WithValues("pod", req.Namespace+"/"+podName, "node", nodeName)

	candidate, err := w.labelRefsCandidate(ctx, req.Namespace, podName)
	if err != nil {
		log.Error(err, "cannot list RayClusters, admitting the binding without node labels")
		return admission.Allowed("RayCluster lookup failed")
	}
	if !candidate {
		return admission.Allowed("not a worker of a group with labelRefs")
	}

	podMeta, err := w.getPodMetadata(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName})
	if err != nil {
		// a prepared pod exits at container start without the annotation, so admitting is still loud
		log.Error(err, "cannot read pod, admitting the binding without node labels")
		return admission.Allowed("pod lookup failed")
	}
	if podMeta == nil {
		return admission.Allowed("pod not found")
	}
	group, err := w.labelRefsGroup(ctx, req.Namespace, podMeta.Labels[utils.RayClusterLabelKey], podMeta.Labels[utils.RayNodeGroupLabelKey])
	if err != nil {
		log.Error(err, "cannot read RayCluster, admitting the binding without node labels")
		return admission.Allowed("RayCluster lookup failed")
	}
	if group == nil {
		return admission.Allowed("not a worker of a group with labelRefs")
	}
	// metadata-only GET
	node := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}}
	if err := w.APIReader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		w.withhold(req, podMeta, nodeName, fmt.Errorf("cannot read node %s: %w", nodeName, err))
		return admission.Allowed("node labels withheld")
	}
	labels, err := buildNodeLabels(node.Labels, group.LabelRefs, w.AllowedNodeLabels)
	if err != nil {
		w.withhold(req, podMeta, nodeName, err)
		return admission.Allowed("node labels withheld")
	}
	encoded, err := json.Marshal(labels)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if binding.Annotations == nil {
		binding.Annotations = map[string]string{}
	}
	binding.Annotations[utils.RayNodeLabelsAnnotationKey] = string(encoded)
	patched, err := json.Marshal(binding)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	log.Info("delivering node labels", "labels", labels)
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// labelRefsCandidate reports whether podName carries the <cluster>-<group>-worker- prefix of any cached group with
// labelRefs in the namespace. Prefixes can collide, so the pod's labels decide the group
func (w *PodBindingWebhook) labelRefsCandidate(ctx context.Context, namespace, podName string) (bool, error) {
	clusters := &rayv1.RayClusterList{}
	if err := w.Client.List(ctx, clusters, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range clusters.Items {
		for _, group := range clusters.Items[i].Spec.WorkerGroupSpecs {
			if len(group.LabelRefs) > 0 && strings.HasPrefix(podName, utils.PodName(clusters.Items[i].Name+"-"+group.GroupName, rayv1.WorkerNode, true)) {
				return true, nil
			}
		}
	}
	return false, nil
}

// labelRefsGroup returns the named worker group of the cached cluster if it sets labelRefs
func (w *PodBindingWebhook) labelRefsGroup(ctx context.Context, namespace, clusterName, groupName string) (*rayv1.WorkerGroupSpec, error) {
	cluster := &rayv1.RayCluster{}
	if err := w.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: clusterName}, cluster); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if group := findWorkerGroupSpec(&cluster.Spec, groupName); group != nil && len(group.LabelRefs) > 0 {
		return group, nil
	}
	return nil, nil
}

func buildNodeLabels(nodeLabels map[string]string, refs []rayv1.LabelRef, allowed sets.Set[string]) (map[string]string, error) {
	labels := make(map[string]string, len(refs))
	var errList []string
	for _, ref := range refs {
		nodeLabel, err := utils.NodeLabelKey(ref.ValueFrom.NodeRef.FieldPath)
		if err != nil {
			errList = append(errList, err.Error())
			continue
		}
		value, ok := nodeLabels[nodeLabel]
		switch {
		case !allowed.Has(nodeLabel):
			errList = append(errList, fmt.Sprintf("node label %q is not in the operator's allowedNodeLabels", nodeLabel))
		case !ok:
			errList = append(errList, fmt.Sprintf("node lacks label %q", nodeLabel))
		default:
			// name defaults to the node label key
			key := ref.Name
			if key == "" {
				key = nodeLabel
			}
			labels[key] = value
		}
	}
	if len(errList) > 0 {
		return nil, errors.New(strings.Join(errList, "; "))
	}
	return labels, nil
}

func (w *PodBindingWebhook) getPodMetadata(ctx context.Context, key types.NamespacedName) (*metav1.ObjectMeta, error) {
	pod := &corev1.Pod{}
	err := w.Client.Get(ctx, key, pod)
	if err == nil {
		return &pod.ObjectMeta, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	podMeta := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}}
	if err := w.APIReader.Get(ctx, key, podMeta); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &podMeta.ObjectMeta, nil
}

// Records why the labels were not delivered. The binding proceeds without the annotation and the pod exits before ray start
func (w *PodBindingWebhook) withhold(req admission.Request, podMeta *metav1.ObjectMeta, nodeName string, cause error) {
	message := fmt.Sprintf("node labels not delivered for the binding to node %s: %v", nodeName, cause)
	podBindingLog.Info(message, "pod", podMeta.Namespace+"/"+podMeta.Name)
	if w.Recorder == nil || (req.DryRun != nil && *req.DryRun) {
		return
	}
	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: *podMeta,
	}
	w.Recorder.Eventf(pod, nil, corev1.EventTypeWarning, FailedToDeliverNodeLabels, "DeliverNodeLabels", "%s", message)
}
