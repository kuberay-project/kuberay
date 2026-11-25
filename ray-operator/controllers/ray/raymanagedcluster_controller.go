package ray

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	// The labels Ray reads a worker pod's instance by.
	rayNodeReplicaLabelKey = "ray.io/replica"
	rayNodeHostLabelKey    = "ray.io/host"
	rayNodeIDLabelKey      = "ray.io/node-id"
	// The instance's ID, which ray start gives its Ray node.
	requestedNodeIDEnvKey = "RAY_REQUESTED_NODE_ID"
	// The head's node config naming the cluster object, which has Ray use KubeRayProvider.
	kuberayCRNameEnvKey = "RAY_KUBERAY_CR_NAME"
	// The node config naming the file with the cluster's token.
	authTokenPathEnvKey = "RAY_AUTH_TOKEN_PATH" // #nosec G101
	// Where the token Secret is mounted in each Ray container.
	authTokenVolumeName = "ray-auth-token" // #nosec G101
	authTokenMountPath  = "/etc/ray/auth"  // #nosec G101
	// The bytes of a Ray node ID.
	rayNodeIDBytes = 28
	// The longest pod name: Kubernetes uses it as the hostname.
	maxPodNameLength = 63
	// How soon a replica whose pod's name another pod has is tried again.
	nameTakenRetry = 2 * time.Second
	// The init container KubeRay injects to wait for the head (common.DefaultWorkerPodTemplate).
	waitGCSReadyContainerName = "wait-gcs-ready"
)

// errNameTaken: another pod has the name of a replica's pod, or the replica's own is going.
var errNameTaken = errors.New("another pod has the pod's name")

// RayManagedClusterReconciler runs RayManagedClusters: the head, each group's initial replicas, and then
// exactly the replicas Ray asks for in status.requests, deleting the pods Ray gives up. It never
// replaces a pod and doesn't check maxReplicas: Ray does.
type RayManagedClusterReconciler struct {
	client.Client
	// Reads the API server itself, for what the cache may not have caught up with yet.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
}

func NewRayManagedClusterReconciler(mgr manager.Manager) *RayManagedClusterReconciler {
	return &RayManagedClusterReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorder("raymanagedcluster-controller"),
	}
}

// +kubebuilder:rbac:groups=ray.io,resources=raymanagedclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=raymanagedclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *RayManagedClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	var cluster rayv1.RayManagedCluster
	if err := r.Get(ctx, request.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cluster.DeletionTimestamp.IsZero() {
		// Its pods and service go with it, as it owns them.
		return ctrl.Result{}, nil
	}
	// KubeRay has answered nothing yet: the cluster was just created.
	if cluster.Status.Provisioning == nil {
		return ctrl.Result{}, r.start(ctx, &cluster)
	}
	// Its head stopped, so the cluster is over: its workers go, and nothing is made again.
	if meta.IsStatusConditionTrue(cluster.Status.Conditions, rayv1.RayManagedClusterEnded) {
		return ctrl.Result{}, r.deleteWorkers(ctx, &cluster)
	}
	if err := r.reconcileAuthSecret(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	rayCluster := asRayCluster(&cluster)
	head, stopped, err := r.reconcileHead(ctx, &cluster, rayCluster)
	if errors.Is(err, errNameTaken) {
		logger.Info("The head waits for another pod with its pod's name to go")
		return ctrl.Result{RequeueAfter: nameTakenRetry}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if stopped != nil {
		logger.Info("The cluster's head stopped: the cluster ends, and its workers are deleted", "reason", stopped.Reason, "message", stopped.Message)
		conditions := slices.Clone(cluster.Status.Conditions)
		meta.SetStatusCondition(&conditions, *stopped)
		if err := r.patchStatus(ctx, &cluster, map[string]any{"status": map[string]any{"conditions": conditions, "head": head}}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.deleteWorkers(ctx, &cluster)
	}
	provisioning := cluster.Status.Provisioning.DeepCopy()
	requests := cluster.Status.Requests
	if requests == nil {
		requests = &rayv1.RayManagedClusterRequests{}
	}
	// What fails is tried again after the rest is done and its progress is recorded, so one
	// failure holds up only itself.
	var errs []error
	retry := false
	for _, group := range requests.Groups {
		answer := provisioningOf(provisioning, group.Name)
		spec := workerGroup(&cluster, group.Name)
		newReplicas := slices.Clone(group.NewReplicas)
		slices.SortFunc(newReplicas, func(a, b rayv1.RayManagedNewReplica) int { return int(a.Replica - b.Replica) })
		for _, replica := range newReplicas {
			if replica.Replica <= answer.CreatedReplicas {
				continue
			}
			var err error
			if spec == nil {
				err = &refusedError{fmt.Errorf("the cluster has no worker group named %q", group.Name)}
			} else {
				err = r.createReplica(ctx, &cluster, rayCluster, spec, replica)
			}
			var refusal *refusedError
			if err != nil && !errors.As(err, &refusal) {
				// Nothing is recorded, and createdReplicas only rises: the group's later replicas
				// wait for this one.
				var clash *nameClashError
				if errors.As(err, &clash) {
					r.Recorder.Eventf(&cluster, nil, corev1.EventTypeWarning, "PodNameTaken", "CreatePod",
						"Group %q's replica %d waits: %v. Pods are named <cluster>-<group>-<replica>-<host>, so rename this cluster or the group",
						group.Name, replica.Replica, clash)
				}
				if errors.Is(err, errNameTaken) {
					logger.Info("A replica waits for another pod with its pod's name to go", "group", group.Name, "replica", replica.Replica, "why", err.Error())
					retry = true
				} else {
					errs = append(errs, fmt.Errorf("creating group %q's replica %d: %w", group.Name, replica.Replica, err))
				}
				break
			}
			if refusal != nil {
				logger.Info("A replica can't be created", "group", group.Name, "replica", replica.Replica, "error", refusal.Error())
				answer.FailedReplicas = append(answer.FailedReplicas, rayv1.RayManagedFailedReplica{Replica: replica.Replica, Message: refusal.Error()})
			}
			answer.CreatedReplicas = replica.Replica
		}
		for _, deleted := range group.DeletePods {
			if err := r.deletePods(ctx, &cluster, spec, group.Name, deleted); err != nil {
				errs = append(errs, fmt.Errorf("deleting group %q's replica %d's pods: %w", group.Name, deleted.Replica, err))
			}
		}
	}
	// A failed replica's answer stays until Ray drops it from newReplicas.
	for i := range provisioning.Groups {
		answer := &provisioning.Groups[i]
		asked := map[int64]bool{}
		for _, group := range requests.Groups {
			if group.Name == answer.Name {
				for _, replica := range group.NewReplicas {
					asked[replica.Replica] = true
				}
			}
		}
		answer.FailedReplicas = slices.DeleteFunc(answer.FailedReplicas, func(f rayv1.RayManagedFailedReplica) bool { return !asked[f.Replica] })
	}
	result := ctrl.Result{}
	if retry {
		result.RequeueAfter = nameTakenRetry
	}
	if !equality.Semantic.DeepEqual(provisioning, cluster.Status.Provisioning) || head != cluster.Status.Head ||
		cluster.Status.ObservedGeneration != cluster.Generation {
		// Only KubeRay's part: Ray's requests stay as Ray wrote them.
		status := map[string]any{"provisioning": provisioning, "head": head, "observedGeneration": cluster.Generation}
		errs = append(errs, r.patchStatus(ctx, &cluster, map[string]any{"status": status}))
	}
	return result, errors.Join(errs...)
}

// start writes each group's initial replicas into status.requests, with instance IDs KubeRay
// picks, and KubeRay's first answer, so a later reconcile makes their pods along with the head's.
// It's conditional on the object's version, so it's written once. A status dropped while the
// cluster's pods run (a restore) isn't a new cluster: its initial replicas aren't asked for again,
// and each group's createdReplicas starts from its pods' highest replica.
func (r *RayManagedClusterReconciler) start(ctx context.Context, cluster *rayv1.RayManagedCluster) error {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), client.MatchingLabels{utils.RayClusterLabelKey: cluster.Name}); err != nil {
		return err
	}
	restored := false
	created := map[string]int64{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, cluster) {
			continue
		}
		restored = true
		if replica, err := strconv.ParseInt(pod.Labels[rayNodeReplicaLabelKey], 10, 64); err == nil {
			group := pod.Labels[utils.RayNodeGroupLabelKey]
			created[group] = max(created[group], replica)
		}
	}
	requests := rayv1.RayManagedClusterRequests{}
	provisioning := rayv1.RayManagedClusterProvisioning{Groups: []rayv1.RayManagedGroupProvisioning{}}
	for _, group := range cluster.Spec.WorkerGroupSpecs {
		provisioning.Groups = append(provisioning.Groups, rayv1.RayManagedGroupProvisioning{Name: group.GroupName, CreatedReplicas: created[group.GroupName]})
		initial := ptr.Deref(group.InitialReplicas, 0)
		if initial == 0 || restored {
			continue
		}
		asked := rayv1.RayManagedGroupRequests{Name: group.GroupName}
		for replica := int64(1); replica <= int64(initial); replica++ {
			nodes := make([]string, max(group.NumOfHosts, 1))
			for host := range nodes {
				id, err := newNodeID()
				if err != nil {
					return err
				}
				nodes[host] = id
			}
			asked.NewReplicas = append(asked.NewReplicas, rayv1.RayManagedNewReplica{Replica: replica, Nodes: nodes})
		}
		requests.Groups = append(requests.Groups, asked)
	}
	patch := map[string]any{
		"metadata": map[string]any{"resourceVersion": cluster.ResourceVersion},
		"status":   map[string]any{"requests": requests, "provisioning": provisioning},
	}
	return r.patchStatus(ctx, cluster, patch)
}

func (r *RayManagedClusterReconciler) patchStatus(ctx context.Context, cluster *rayv1.RayManagedCluster, patch map[string]any) error {
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	return r.Status().Patch(ctx, cluster, client.RawPatch(types.MergePatchType, data))
}

// reconcileHead creates the head's service, and its pod once: a head that stops (it fails, exits,
// or its pod is deleted) isn't made again, and the Ended condition saying why is returned. A pod
// with its name that isn't the cluster's (an earlier cluster's, being deleted) has it wait
// (errNameTaken).
func (r *RayManagedClusterReconciler) reconcileHead(ctx context.Context, cluster *rayv1.RayManagedCluster, rayCluster rayv1.RayCluster) (rayv1.HeadInfo, *metav1.Condition, error) {
	service, err := common.BuildServiceForHeadPod(ctx, rayCluster, nil, nil)
	if err != nil {
		return rayv1.HeadInfo{}, nil, err
	}
	if err := r.createIfMissing(ctx, cluster, service); err != nil {
		return rayv1.HeadInfo{}, nil, err
	}
	pod := r.buildHeadPod(ctx, cluster, rayCluster)
	head := rayv1.HeadInfo{PodName: pod.Name, ServiceName: service.Name}
	var existing corev1.Pod
	err = r.Get(ctx, client.ObjectKeyFromObject(&pod), &existing)
	// Made (the status names it) and missing: deleted, unless the cache hasn't caught up.
	if apierrors.IsNotFound(err) && cluster.Status.Head.PodName != "" {
		err = r.APIReader.Get(ctx, client.ObjectKeyFromObject(&pod), &existing)
		if apierrors.IsNotFound(err) {
			return head, endedBy(rayv1.RayManagedHeadPodDeleted, fmt.Sprintf("the head pod %s was deleted", pod.Name)), nil
		}
	}
	if apierrors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(cluster, &pod, r.Scheme); err != nil {
			return head, nil, err
		}
		if err := r.Create(ctx, &pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return head, nil, err
		}
		return head, nil, nil
	}
	if err != nil {
		return head, nil, err
	}
	if !metav1.IsControlledBy(&existing, cluster) {
		return head, nil, errNameTaken
	}
	head.PodIP = existing.Status.PodIP
	switch {
	case existing.DeletionTimestamp != nil:
		return head, endedBy(rayv1.RayManagedHeadPodDeleted, fmt.Sprintf("the head pod %s is being deleted", pod.Name)), nil
	case existing.Status.Phase == corev1.PodFailed:
		return head, endedBy(rayv1.RayManagedHeadPodFailed, headExit(&existing)), nil
	case existing.Status.Phase == corev1.PodSucceeded:
		return head, endedBy(rayv1.RayManagedHeadPodSucceeded, headExit(&existing)), nil
	}
	return head, nil, nil
}

// endedBy is the Ended condition: the head stopped, for `reason`.
func endedBy(reason, message string) *metav1.Condition {
	return &metav1.Condition{
		Type:    rayv1.RayManagedClusterEnded,
		Status:  metav1.ConditionTrue,
		Reason:  reason,
		Message: message + ". Ray can't restart its head yet, so the cluster ended and its workers are deleted: recreate it to start over",
	}
}

// headExit says how the head pod ended: its Ray container's exit (with the termination message
// ray start writes when Ray fails to start), or else the pod's status (an eviction's, say).
func headExit(pod *corev1.Pod) string {
	ray := pod.Spec.Containers[utils.RayContainerIndex].Name
	for _, status := range pod.Status.ContainerStatuses {
		if terminated := status.State.Terminated; status.Name == ray && terminated != nil {
			message := fmt.Sprintf("the head pod %s's Ray container exited with code %d (%s)", pod.Name, terminated.ExitCode, terminated.Reason)
			if text := strings.TrimSpace(terminated.Message); text != "" {
				message += ": " + text
			}
			return message
		}
	}
	return strings.TrimSuffix(fmt.Sprintf("the head pod %s ended (%s): %s", pod.Name, pod.Status.Reason, pod.Status.Message), ": ")
}

// deleteWorkers deletes the cluster's worker pods: its head stopped, so they can't rejoin anything,
// and nothing else would ever stop them.
func (r *RayManagedClusterReconciler) deleteWorkers(ctx context.Context, cluster *rayv1.RayManagedCluster) error {
	var pods corev1.PodList
	selector := client.MatchingLabels{utils.RayClusterLabelKey: cluster.Name, utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode)}
	if err := r.List(ctx, &pods, client.InNamespace(cluster.Namespace), selector); err != nil {
		return err
	}
	var errs []error
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !metav1.IsControlledBy(pod, cluster) || pod.DeletionTimestamp != nil {
			continue
		}
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *RayManagedClusterReconciler) createIfMissing(ctx context.Context, cluster *rayv1.RayManagedCluster, object client.Object) error {
	if err := controllerutil.SetControllerReference(cluster, object, r.Scheme); err != nil {
		return err
	}
	existing := object.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(object), existing)
	if !apierrors.IsNotFound(err) {
		return err
	}
	if err := r.Create(ctx, object); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (r *RayManagedClusterReconciler) buildHeadPod(ctx context.Context, cluster *rayv1.RayManagedCluster, rayCluster rayv1.RayCluster) corev1.Pod {
	name := utils.PodName(cluster.Name, rayv1.HeadNode, false)
	fqdn := utils.GenerateFQDNServiceName(ctx, rayCluster, cluster.Namespace)
	headSpec := rayCluster.Spec.HeadGroupSpec
	headPort := common.GetHeadPort(headSpec.RayStartParams)
	template := common.DefaultHeadPodTemplate(ctx, rayCluster, headSpec, name, headPort)
	pod := common.BuildPod(ctx, template, rayv1.HeadNode, headSpec.RayStartParams, headPort, false, utils.RayClusterCRD, fqdn, nil, rayCluster.Spec.RayVersion)
	pod.Name, pod.GenerateName = name, ""
	fitToRay3(&pod, cluster.Spec.HeadGroupSpec.Template, rayv1.HeadNode, headSpec.RayStartParams)
	// Ray can't restart its head in place yet: a head started again is a new, empty cluster, which
	// the workers can't rejoin. So a head that stops isn't restarted, and neither is its pod
	// replaced: the cluster ends with it.
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	container := &pod.Spec.Containers[utils.RayContainerIndex]
	container.Env = append(container.Env, corev1.EnvVar{Name: kuberayCRNameEnvKey, Value: cluster.Name})
	return pod
}

// fitToRay3 fits a pod RayCluster's builders made to Ray 3.0, which runs no dashboard agent and
// doesn't connect by address. The probes KubeRay injects call the agent, so they're replaced:
// the head's call its dashboard's health checks, a worker's its raylet's metrics endpoint. Probes
// the template sets are kept.
func fitToRay3(pod *corev1.Pod, template corev1.PodTemplateSpec, nodeType rayv1.RayNodeType, rayStartParams map[string]string) {
	container := &pod.Spec.Containers[utils.RayContainerIndex]
	container.Env = slices.DeleteFunc(container.Env, func(env corev1.EnvVar) bool { return env.Name == utils.RAY_ADDRESS })
	port := func(param string, fallback int) int {
		if port, err := strconv.Atoi(rayStartParams[param]); err == nil {
			return port
		}
		return fallback
	}
	var handler corev1.ProbeHandler
	if nodeType == rayv1.HeadNode {
		dashboard := port("dashboard-port", utils.DefaultDashboardPort)
		check := fmt.Sprintf("wget --tries 1 -T 2 -q -O- http://localhost:%d/api/local_raylet_healthz | grep success && "+
			"wget --tries 1 -T 10 -q -O- http://localhost:%d/api/gcs_healthz | grep success", dashboard, dashboard)
		handler = corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"bash", "-c", check}}}
	} else {
		metrics := port("metrics-export-port", utils.DefaultMetricsPort)
		handler = corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/metrics", Port: intstr.FromInt(metrics)}}
	}
	given := template.Spec.Containers[utils.RayContainerIndex]
	if given.LivenessProbe == nil && container.LivenessProbe != nil {
		container.LivenessProbe.ProbeHandler = handler
	}
	if given.ReadinessProbe == nil && container.ReadinessProbe != nil {
		container.ReadinessProbe.ProbeHandler = handler
	}
}

// createReplica makes a replica's pods, one per host, each with its instance's ID. A replica is
// all or nothing: if the API server refuses a create, the pods made are deleted, and it returns a
// refusedError once they all are. Any other error (one that may pass if tried again, or a pod
// that couldn't be deleted) leaves it to be tried again, keeping the pods made: a create repeated
// then finds them.
func (r *RayManagedClusterReconciler) createReplica(ctx context.Context, cluster *rayv1.RayManagedCluster, rayCluster rayv1.RayCluster, group *rayv1.RayManagedWorkerGroupSpec, replica rayv1.RayManagedNewReplica) error {
	var made []*corev1.Pod
	for host, nodeID := range replica.Nodes {
		pod := r.buildWorkerPod(ctx, cluster, rayCluster, group, replica.Replica, host, nodeID)
		if err := controllerutil.SetControllerReference(cluster, &pod, r.Scheme); err != nil {
			return err
		}
		err := r.Create(ctx, &pod)
		if apierrors.IsAlreadyExists(err) {
			err = r.isOwnPod(ctx, cluster, &pod, nodeID)
		}
		if err == nil {
			made = append(made, &pod)
			continue
		}
		if !refused(err) {
			return err
		}
		for _, made := range made {
			if deleteErr := r.Delete(ctx, made); client.IgnoreNotFound(deleteErr) != nil {
				// It hasn't failed until its pods are gone: it's tried again.
				return fmt.Errorf("the API server refused pod %s (%w), and pod %s can't be deleted: %w", pod.Name, err, made.Name, deleteErr)
			}
		}
		return &refusedError{err}
	}
	return nil
}

// isOwnPod checks the pod with `pod`'s name, from the API server: nil if it's this replica's
// host's, from a create repeated after a crash. Otherwise the replica waits, and is made once the
// other pod is gone: errNameTaken if it will go (it's being deleted, or an earlier cluster of this
// name made it), or a nameClashError if it's another cluster's, which only renaming resolves.
func (r *RayManagedClusterReconciler) isOwnPod(ctx context.Context, cluster *rayv1.RayManagedCluster, pod *corev1.Pod, nodeID string) error {
	var existing corev1.Pod
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pod), &existing); err != nil {
		if apierrors.IsNotFound(err) {
			return errNameTaken
		}
		return err
	}
	owner := metav1.GetControllerOf(&existing)
	switch {
	case existing.DeletionTimestamp != nil:
		return errNameTaken
	case metav1.IsControlledBy(&existing, cluster):
		if existing.Labels[rayNodeIDLabelKey] != nodeID {
			return errNameTaken
		}
		return nil
	case owner != nil && owner.Kind == "RayManagedCluster" && owner.Name == cluster.Name:
		return errNameTaken
	case owner != nil:
		return &nameClashError{pod: pod.Name, holder: fmt.Sprintf("%s %s", owner.Kind, owner.Name)}
	}
	return &nameClashError{pod: pod.Name, holder: "a pod no controller owns"}
}

// nameClashError: a pod of another cluster's (or no cluster's) has the name of a replica's pod,
// which waits for it to go.
type nameClashError struct {
	pod, holder string
}

func (e *nameClashError) Error() string {
	return fmt.Sprintf("pod name %s is taken by %s", e.pod, e.holder)
}

func (e *nameClashError) Is(target error) bool { return target == errNameTaken }

// refused says whether the API server won't create the pod: a quota, an admission webhook or the
// pod's validation refuses it. Anything else (a timeout, an overloaded or restarting API server, a
// webhook that's down) may pass if tried again.
func refused(err error) bool {
	return apierrors.IsForbidden(err) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err)
}

// refusedError: the API server refused a replica's pod, and the pods made for it are gone.
type refusedError struct{ err error }

func (e *refusedError) Error() string { return e.err.Error() }
func (e *refusedError) Unwrap() error { return e.err }

func (r *RayManagedClusterReconciler) buildWorkerPod(ctx context.Context, cluster *rayv1.RayManagedCluster, rayCluster rayv1.RayCluster, group *rayv1.RayManagedWorkerGroupSpec, replica int64, host int, nodeID string) corev1.Pod {
	worker := asWorkerGroupSpec(cluster, group)
	fqdn := utils.GenerateFQDNServiceName(ctx, rayCluster, cluster.Namespace)
	headPort := common.GetHeadPort(rayCluster.Spec.HeadGroupSpec.RayStartParams)
	replicaName := fmt.Sprintf("%s-%d", group.GroupName, replica)
	template := common.DefaultWorkerPodTemplate(ctx, rayCluster, worker, "", fqdn, headPort, replicaName, int(replica), host)
	pod := common.BuildPod(ctx, template, rayv1.WorkerNode, worker.RayStartParams, headPort, false, utils.RayClusterCRD, fqdn, nil, rayCluster.Spec.RayVersion)
	pod.Name, pod.GenerateName = workerPodName(cluster.Name, group.GroupName, replica, host), ""
	fitToRay3(&pod, group.Template, rayv1.WorkerNode, worker.RayStartParams)
	// Ray asks for replicas from its head, so the head is up: only initial replicas, made along
	// with it, wait for it. Ray numbers its replicas above the initial ones it adopted.
	if replica > int64(ptr.Deref(group.InitialReplicas, 0)) {
		pod.Spec.InitContainers = slices.DeleteFunc(pod.Spec.InitContainers, func(c corev1.Container) bool { return c.Name == waitGCSReadyContainerName })
	}
	pod.Labels[rayNodeReplicaLabelKey] = strconv.FormatInt(replica, 10)
	pod.Labels[rayNodeHostLabelKey] = strconv.Itoa(host)
	pod.Labels[rayNodeIDLabelKey] = nodeID
	// A pod that ends is never restarted: Ray replaces the instance if it wants to.
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	container := &pod.Spec.Containers[utils.RayContainerIndex]
	container.Env = append(container.Env, corev1.EnvVar{Name: requestedNodeIDEnvKey, Value: nodeID})
	return pod
}

// deletePods deletes the pods Ray gave up, and, if the group says so, their replicas' others. They're
// found by their labels, not their names, so a change to the naming rule strands none.
func (r *RayManagedClusterReconciler) deletePods(ctx context.Context, cluster *rayv1.RayManagedCluster, group *rayv1.RayManagedWorkerGroupSpec, groupName string, deleted rayv1.RayManagedDeletePods) error {
	var list corev1.PodList
	selector := client.MatchingLabels{
		utils.RayClusterLabelKey:   cluster.Name,
		utils.RayNodeGroupLabelKey: groupName,
		rayNodeReplicaLabelKey:     strconv.FormatInt(deleted.Replica, 10),
	}
	if err := r.List(ctx, &list, client.InNamespace(cluster.Namespace), selector); err != nil {
		return err
	}
	whole := group != nil && group.DeleteWholeReplicas
	// Each pod is deleted whatever happens to the others.
	var errs []error
	for i := range list.Items {
		pod := &list.Items[i]
		host, err := strconv.ParseInt(pod.Labels[rayNodeHostLabelKey], 10, 64)
		listed := err == nil && slices.Contains(deleted.Hosts, host)
		if !metav1.IsControlledBy(pod, cluster) || pod.DeletionTimestamp != nil || (!whole && !listed) {
			continue
		}
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// asRayCluster is the RayCluster whose pod and service builders make the cluster's pods.
func asRayCluster(cluster *rayv1.RayManagedCluster) rayv1.RayCluster {
	headSpec := *cluster.Spec.HeadGroupSpec.DeepCopy()
	if headSpec.RayStartParams == nil {
		headSpec.RayStartParams = map[string]string{}
	}
	configureAuth(cluster, &headSpec.Template)
	return rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        cluster.Name,
			Namespace:   cluster.Namespace,
			UID:         cluster.UID,
			Labels:      cluster.Labels,
			Annotations: cluster.Annotations,
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: headSpec,
			RayVersion:    cluster.Spec.RayVersion,
		},
	}
}

// asWorkerGroupSpec is a copy of the group as RayCluster's builders take it.
func asWorkerGroupSpec(cluster *rayv1.RayManagedCluster, group *rayv1.RayManagedWorkerGroupSpec) rayv1.WorkerGroupSpec {
	group = group.DeepCopy()
	if group.RayStartParams == nil {
		group.RayStartParams = map[string]string{}
	}
	configureAuth(cluster, &group.Template)
	return rayv1.WorkerGroupSpec{
		GroupName:      group.GroupName,
		MinReplicas:    ptr.To[int32](0),
		MaxReplicas:    group.MaxReplicas,
		NumOfHosts:     max(group.NumOfHosts, 1),
		Resources:      group.Resources,
		Labels:         group.Labels,
		RayStartParams: group.RayStartParams,
		Template:       group.Template,
	}
}

// authMode is the cluster's authentication: Ray 3's default, token, unless authOptions says.
func authMode(cluster *rayv1.RayManagedCluster) rayv1.AuthMode {
	if options := cluster.Spec.AuthOptions; options != nil && options.Mode != "" {
		return options.Mode
	}
	return rayv1.AuthModeToken
}

// authSecretName is the Secret with the cluster's token: the one authOptions names, else the one
// KubeRay generates, named after the cluster.
func authSecretName(cluster *rayv1.RayManagedCluster) string {
	if options := cluster.Spec.AuthOptions; options != nil && options.SecretName != nil && *options.SecretName != "" {
		return *options.SecretName
	}
	return utils.CheckName(cluster.Name)
}

// reconcileAuthSecret generates the cluster's token, before its first pod, unless authentication
// is off or authOptions names a Secret of the user's.
func (r *RayManagedClusterReconciler) reconcileAuthSecret(ctx context.Context, cluster *rayv1.RayManagedCluster) error {
	options := cluster.Spec.AuthOptions
	if authMode(cluster) != rayv1.AuthModeToken || (options != nil && options.SecretName != nil && *options.SecretName != "") {
		return nil
	}
	token, err := generateRandomToken(32)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authSecretName(cluster),
			Namespace: cluster.Namespace,
			Labels:    map[string]string{utils.RayClusterLabelKey: cluster.Name},
		},
		StringData: map[string]string{utils.RAY_AUTH_TOKEN_SECRET_KEY: token},
	}
	return r.createIfMissing(ctx, cluster, secret)
}

// configureAuth sets the authentication of a pod template's Ray container: RAY_AUTH_MODE, and with
// token authentication, the token Secret mounted as a file it reads (RAY_AUTH_TOKEN_PATH), unless
// the template gives the token itself. A file, not an environment variable: every process Ray
// starts, the user's code's included, would inherit a variable. Set before RayCluster's builders
// run, so the init container that waits for the head, which copies the Ray container's
// environment and mounts, has it too.
func configureAuth(cluster *rayv1.RayManagedCluster, template *corev1.PodTemplateSpec) {
	container := &template.Spec.Containers[utils.RayContainerIndex]
	mode := authMode(cluster)
	if !utils.EnvVarExists(utils.RAY_AUTH_MODE_ENV_VAR, container.Env) {
		container.Env = append(container.Env, corev1.EnvVar{Name: utils.RAY_AUTH_MODE_ENV_VAR, Value: string(mode)})
	}
	if mode != rayv1.AuthModeToken || utils.EnvVarExists(utils.RAY_AUTH_TOKEN_ENV_VAR, container.Env) ||
		utils.EnvVarExists(authTokenPathEnvKey, container.Env) {
		return
	}
	if !utils.VolumeExists(authTokenVolumeName, template.Spec.Volumes) {
		template.Spec.Volumes = append(template.Spec.Volumes, corev1.Volume{
			Name: authTokenVolumeName,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: authSecretName(cluster),
				Items:      []corev1.KeyToPath{{Key: utils.RAY_AUTH_TOKEN_SECRET_KEY, Path: utils.RAY_AUTH_TOKEN_SECRET_KEY}},
			}},
		})
	}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: authTokenVolumeName, MountPath: authTokenMountPath, ReadOnly: true})
	container.Env = append(container.Env, corev1.EnvVar{Name: authTokenPathEnvKey, Value: authTokenMountPath + "/" + utils.RAY_AUTH_TOKEN_SECRET_KEY})
}

func workerGroup(cluster *rayv1.RayManagedCluster, name string) *rayv1.RayManagedWorkerGroupSpec {
	for i := range cluster.Spec.WorkerGroupSpecs {
		if cluster.Spec.WorkerGroupSpecs[i].GroupName == name {
			return &cluster.Spec.WorkerGroupSpecs[i]
		}
	}
	return nil
}

func provisioningOf(provisioning *rayv1.RayManagedClusterProvisioning, name string) *rayv1.RayManagedGroupProvisioning {
	for i := range provisioning.Groups {
		if provisioning.Groups[i].Name == name {
			return &provisioning.Groups[i]
		}
	}
	provisioning.Groups = append(provisioning.Groups, rayv1.RayManagedGroupProvisioning{Name: name})
	return &provisioning.Groups[len(provisioning.Groups)-1]
}

// workerPodName is a replica's host's pod's name: the same for a repeated create, which then gets
// AlreadyExists, and a hostname (at most 63 lowercase letters, digits and dashes). Plainly
// `<cluster>-<group>-<replica>-<host>` when that is one. Otherwise the cluster and group part is
// cleaned up and cut, and a hash of their names keeps groups apart. The replica and host are never
// cut, so a group's pods never share a name, however many there are. Clusters whose plain names run
// together (`a-b` and `c`, `a` and `b-c`) would share them: the second waits, and says why.
func workerPodName(cluster, group string, replica int64, host int) string {
	prefix, suffix := cluster+"-"+group, fmt.Sprintf("-%d-%d", replica, host)
	isHostname := func(c rune) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' }
	if len(prefix)+len(suffix) <= maxPodNameLength && !strings.ContainsFunc(prefix, func(c rune) bool { return !isHostname(c) }) {
		return prefix + suffix
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(cluster + "/" + group))
	suffix = fmt.Sprintf("-%08x%s", hash.Sum32(), suffix)
	cleaned := strings.Map(func(c rune) rune {
		if isHostname(c) {
			return c
		}
		if c >= 'A' && c <= 'Z' {
			return c - 'A' + 'a'
		}
		return '-'
	}, prefix)
	return strings.TrimRight(cleaned[:min(len(cleaned), maxPodNameLength-len(suffix))], "-") + suffix
}

// newNodeID is a random Ray node ID, in hex.
func newNodeID() (string, error) {
	id := make([]byte, rayNodeIDBytes)
	if _, err := rand.Read(id); err != nil {
		return "", err
	}
	return hex.EncodeToString(id), nil
}

func (r *RayManagedClusterReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayManagedCluster{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayManagedCluster")
				if request != nil {
					logger = logger.WithValues("RayManagedCluster", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}
