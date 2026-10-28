package ray

import (
	"context"
	errstd "errors"
	"fmt"
	"maps"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	cmap "github.com/orcaman/concurrent-map/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/lru"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const (
	ServiceDefaultRequeueDuration   = 2 * time.Second
	RayClusterDeletionDelayDuration = 60 * time.Second
	ENABLE_ZERO_DOWNTIME            = "ENABLE_ZERO_DOWNTIME"
)

// RayServiceReconciler reconciles a RayService object
type RayServiceReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	// Currently, the Ray dashboard doesn't cache the Serve application config.
	// To avoid reapplying the same config repeatedly, cache the config in this map.
	// Cache key is the combination of RayService namespace and name.
	// Cache value is map of RayCluster name to Serve application config.
	ServeConfigs                 *lru.Cache
	RayClusterDeletionTimestamps cmap.ConcurrentMap[string, time.Time]
	dashboardClientFunc          func(rayCluster *rayv1.RayCluster, url string) (dashboardclient.RayDashboardClientInterface, error)
	httpProxyClientFunc          func(hostIp, podNamespace, podName string, port int) utils.RayHttpProxyClientInterface
}

// NewRayServiceReconciler returns a new reconcile.Reconciler
func NewRayServiceReconciler(ctx context.Context, mgr manager.Manager, provider utils.ClientProvider) *RayServiceReconciler {
	dashboardClientFunc := provider.GetDashboardClient(ctx, mgr)
	httpProxyClientFunc := provider.GetHttpProxyClient(mgr)
	return &RayServiceReconciler{
		Client:                       mgr.GetClient(),
		Scheme:                       mgr.GetScheme(),
		Recorder:                     mgr.GetEventRecorder("rayservice-controller"),
		ServeConfigs:                 lru.New(utils.ServeConfigLRUSize),
		RayClusterDeletionTimestamps: cmap.New[time.Time](),

		dashboardClientFunc: dashboardClientFunc,
		httpProxyClientFunc: httpProxyClientFunc,
	}
}

// +kubebuilder:rbac:groups=ray.io,resources=rayservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/proxy,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services/proxy,verbs=get;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;create;update
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="gateway.networking.k8s.io",resources=gateways,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="gateway.networking.k8s.io",resources=httproutes,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=get;list;watch;create;delete;update
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

// [WARNING]: There MUST be a newline after kubebuilder markers.
// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// This the top level reconciliation flow for RayService.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.2/pkg/reconcile
func (r *RayServiceReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	rayServiceInstance, err := r.getRayServiceInstance(ctx, request)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	originalRayServiceInstance := rayServiceInstance.DeepCopy()

	if manager := utils.ManagedByExternalController(rayServiceInstance.Spec.ManagedBy); manager != nil {
		logger.Info("Skipping RayService managed by a custom controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	r.cleanUpServeConfigCache(ctx, rayServiceInstance)
	hasRayClustersToClean, err := r.cleanUpRayClusterInstance(ctx, rayServiceInstance)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !rayServiceInstance.DeletionTimestamp.IsZero() {
		if !hasRayClustersToClean {
			logger.Info("No RayClusters exist, removing finalizer")
			controllerutil.RemoveFinalizer(rayServiceInstance, utils.RayServiceFinalizer)
			if err := r.Update(ctx, rayServiceInstance); err != nil {
				logger.Error(err, "Failed to remove finalizer for RayService")
				return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
			}
			return ctrl.Result{}, nil
		}
		logger.Info("Requeue RayService, waiting for RayClusters to be deleted")
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
	}

	if !controllerutil.ContainsFinalizer(rayServiceInstance, utils.RayServiceFinalizer) {
		logger.Info("Adding finalizer to RayService", "finalizer", utils.RayServiceFinalizer)
		controllerutil.AddFinalizer(rayServiceInstance, utils.RayServiceFinalizer)
		if err := r.Update(ctx, rayServiceInstance); err != nil {
			logger.Error(err, "Failed to add finalizer to RayService")
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
	}

	// Perform all validations and directly fail the RayService if any of the validation fails
	errType, err := validateRayService(ctx, rayServiceInstance)
	// Immediately update the status after validation
	if err != nil {
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(errType), string(utils.ValidateAction),
			"%s/%s: %v", rayServiceInstance.Namespace, rayServiceInstance.Name, err)

		setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceValidationFailed, err.Error())
		rayServiceInstance.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}

		if updateErr := r.Status().Update(ctx, rayServiceInstance); updateErr != nil {
			logger.Info("Failed to update RayService status", "error", updateErr)
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, updateErr
		}
		return ctrl.Result{}, nil
	}

	result, err := r.handleSuspend(ctx, rayServiceInstance)
	if updateErr := r.updateStatusIfChanged(ctx, originalRayServiceInstance, rayServiceInstance); updateErr != nil && err == nil {
		err = updateErr
		if (result == ctrl.Result{}) {
			result = ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}
		}
	}
	if err != nil || suspendIsOperative(rayServiceInstance) {
		return result, err
	}

	// If the RayService has timed out during initialization, skip the rest of the reconciliation.
	// The service is in a terminal failure state - only cleanup (above) is needed.
	// The user must delete and recreate the RayService to recover.
	if isInitializingTimeout(rayServiceInstance) {
		// Requeue only if there are still RayClusters to clean up
		// This avoids unnecessary reconciliations after all resources are deleted
		if hasRayClustersToClean {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
		}
		logger.Info("RayService in terminal failure state, all RayClusters cleaned up")
		return ctrl.Result{}, nil
	}

	// Find active and pending ray cluster objects given current service name.
	var activeRayClusterInstance, pendingRayClusterInstance *rayv1.RayCluster
	if activeRayClusterInstance, pendingRayClusterInstance, err = r.reconcileRayCluster(ctx, rayServiceInstance); err != nil {
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, client.IgnoreNotFound(err)
	}

	// Determine the rollback state immediately before making serving decisions.
	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		// If an upgrade is in progress, check if rollback is necessary.
		isUpgradeInProgress := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress))
		if isUpgradeInProgress {
			if activeRayClusterInstance == nil {
				logger.Info("Cannot initiate rollback: active cluster not found")
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.RollbackImpossible), string(utils.ReconcileAction), "Active cluster not found, rollback cannot be initiated")
			} else if pendingRayClusterInstance != nil {
				if err := r.reconcileRollbackState(ctx, rayServiceInstance, activeRayClusterInstance, pendingRayClusterInstance); err != nil {
					return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
				}
			}
		}
	}

	// Check both active and pending Ray clusters to see if the head Pod is ready to serve requests.
	// This is important to ensure the reliability of the serve service because the head Pod cannot
	// rely on readiness probes to determine serve readiness.
	if err := r.updateHeadPodServeLabel(ctx, rayServiceInstance, activeRayClusterInstance, rayServiceInstance.Spec.ExcludeHeadPodFromServeSvc); err != nil {
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
	}
	if err := r.updateHeadPodServeLabel(ctx, rayServiceInstance, pendingRayClusterInstance, rayServiceInstance.Spec.ExcludeHeadPodFromServeSvc); err != nil {
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
	}

	// Reconcile serve applications for active and/or pending clusters
	// 1. If there is a pending cluster, reconcile serve applications for the pending cluster.
	// 2. If there are both active and pending clusters, reconcile serve applications for the pending cluster only.
	// 3. If there is no pending cluster, reconcile serve applications for the active cluster.
	// 4. During NewClusterWithIncrementalUpgrade, reconcileServe will reconcile either the pending or active cluster
	// based on total TargetCapacity.
	isActiveClusterReady, isPendingClusterReady := false, false
	var activeClusterServeApplications, pendingClusterServeApplications map[string]rayv1.AppStatus = nil, nil
	if pendingRayClusterInstance != nil {
		logger.Info("Reconciling the Serve applications for pending cluster", "clusterName", pendingRayClusterInstance.Name)
		if isPendingClusterReady, pendingClusterServeApplications, err = r.reconcileServe(ctx, rayServiceInstance, pendingRayClusterInstance); err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
	}
	if activeRayClusterInstance != nil && pendingRayClusterInstance == nil &&
		!shouldPrepareNewCluster(ctx, rayServiceInstance, activeRayClusterInstance, nil, false) {
		// Only reconcile serve applications for the active cluster when there is no pending cluster. That is, during the upgrade process,
		// in-place update and updating the serve application status for the active cluster will not work.
		logger.Info("Reconciling the Serve applications for active cluster", "clusterName", activeRayClusterInstance.Name)
		if isActiveClusterReady, activeClusterServeApplications, err = r.reconcileServe(ctx, rayServiceInstance, activeRayClusterInstance); err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
	} else if activeRayClusterInstance != nil && pendingRayClusterInstance != nil && utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		logger.Info("Reconciling the Serve applications for active cluster during NewClusterWithIncrementalUpgrade", "clusterName", activeRayClusterInstance.Name)
		if isActiveClusterReady, activeClusterServeApplications, err = r.reconcileServe(ctx, rayServiceInstance, activeRayClusterInstance); err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
	}

	// Check if NewClusterWithIncrementalUpgrade is enabled, if so reconcile Gateway objects.
	var httpRouteInstance *gwv1.HTTPRoute
	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		// Ensure per-cluster Serve service exists for the active and pending RayClusters.
		if err = r.reconcilePerClusterServeService(ctx, rayServiceInstance, activeRayClusterInstance); err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
		if err = r.reconcilePerClusterServeService(ctx, rayServiceInstance, pendingRayClusterInstance); err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}
		// Creates or updates a Gateway CR that points to the Serve services of
		// the active and pending (if it exists) RayClusters. For incremental upgrades,
		// the Gateway endpoint is used rather than the Serve service.
		err = r.reconcileGateway(ctx, rayServiceInstance)
		if err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, client.IgnoreNotFound(err)
		}
		// Create or update the HTTPRoute for the Gateway, passing in the pending cluster readiness status.
		httpRouteInstance, err = r.reconcileHTTPRoute(ctx, rayServiceInstance, isPendingClusterReady)
		if err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, client.IgnoreNotFound(err)
		}
	}

	// Reconcile K8s services and make sure it points to the correct RayCluster.
	var headSvc, serveSvc *corev1.Service
	if isPendingClusterReady || isActiveClusterReady {
		targetCluster := activeRayClusterInstance
		logMsg := "Reconciling K8s services to point to the active Ray cluster."

		isIncrementalUpgradeInProgress := utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) && meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress))
		if isPendingClusterReady && !isIncrementalUpgradeInProgress {
			// This step is skipped for incremental upgrade, because the pending cluster is ready during the upgrade
			// and creates its own per-cluster Serve service.
			targetCluster = pendingRayClusterInstance
			logMsg = "Reconciling K8s services to point to the pending Ray cluster to switch traffic because it is ready."
		}

		logger.Info(logMsg)
		headSvc, serveSvc, err = r.reconcileServicesToReadyCluster(ctx, rayServiceInstance, targetCluster)
		if err != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
		}

		if headSvc == nil || serveSvc == nil {
			panic(fmt.Sprintf("Either head service (%v) or serve service (%v) is nil before calculating RayService status. "+
				"This should never happen. Please open a GitHub issue in the KubeRay repository.", headSvc, serveSvc))
		}
	}

	// Calculate the status of the RayService based on K8s resources.
	if err := r.calculateStatus(
		ctx,
		rayServiceInstance,
		headSvc,
		serveSvc,
		activeRayClusterInstance,
		pendingRayClusterInstance,
		activeClusterServeApplications,
		pendingClusterServeApplications,
		httpRouteInstance,
	); err != nil {
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
	}

	// Final status update for any CR modification.
	if utils.InconsistentRayServiceStatuses(originalRayServiceInstance.Status, rayServiceInstance.Status) {
		rayServiceInstance.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
		if errStatus := r.Status().Update(ctx, rayServiceInstance); errStatus != nil {
			return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, errStatus
		}
	}
	return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
}

func validateRayService(ctx context.Context, rayServiceInstance *rayv1.RayService) (utils.K8sEventType, error) {
	logger := ctrl.LoggerFrom(ctx)
	validationRules := []struct {
		validate func() error
		errType  utils.K8sEventType
	}{
		{func() error { return utils.ValidateRayServiceMetadata(rayServiceInstance.ObjectMeta) }, utils.InvalidRayServiceMetadata},
		{func() error { return utils.ValidateRayServiceSpec(rayServiceInstance) }, utils.InvalidRayServiceSpec},
	}

	for _, validation := range validationRules {
		if err := validation.validate(); err != nil {
			logger.Error(err, err.Error())
			return validation.errType, err
		}
	}
	return "", nil
}

// suspendIsOperative reports whether the suspend state machine currently
// owns this reconcile pass. When true, Reconcile must short-circuit after
// persisting status: running the rest of the loop would either re-create
// resources that handleSuspend tore down or overwrite the conditions it
// staged.
//
// Reading Suspending / Suspended is the canonical "is suspend driving?"
// check because those two conditions must only be mutated by handleSuspend
// (and are the source of truth for the suspend state machine after it
// returns). Do not write to them from anywhere else.
func suspendIsOperative(rayServiceInstance *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceSuspending)) ||
		meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceSuspended))
}

// handleSuspend implements the Spec.Suspend lifecycle. It mutates
// rayServiceInstance.Status in-place; the caller persists the changes via a
// single Status().Update.
//
// State machine:
//
//	(no suspend)  --Spec.Suspend=true-->  Suspending  --owned resources deleted-->  Suspended  --Spec.Suspend=false-->  (no suspend)
//
// Atomicity comes from a persisted Suspending condition as the commit point.
// The first reconcile that observes Spec.Suspend=true only stages the status
// transition (Suspending=True + reset of ActiveServiceStatus,
// PendingServiceStatus, NumServeEndpoints, ServiceStatus) in the same status
// update; deletion runs on the next reconcile, once Suspending is durable. If
// a later deletion attempt errors out or Spec.Suspend is flipped back to
// false, Suspending stays True in storage and subsequent reconciles continue
// the cleanup.
func (r *RayServiceReconciler) handleSuspend(ctx context.Context, rayServiceInstance *rayv1.RayService) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	isSuspending := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceSuspending))
	isSuspended := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceSuspended))

	// Case 1: already fully suspended.
	if isSuspended {
		if !rayServiceInstance.Spec.Suspend {
			logger.Info("Spec.Suspend is false; exiting Suspended state and resuming reconcile")
			rayServiceInstance.Status.ObservedGeneration = rayServiceInstance.ObjectMeta.Generation
			setCondition(rayServiceInstance, rayv1.RayServiceSuspended, metav1.ConditionFalse, rayv1.RayServiceResumed,
				"Spec.Suspend is false; RayService has resumed.")
			// Re-arm the initializing-timeout for the resumed attempt.
			// Remove + Set forces a fresh LastTransitionTime because
			// meta.SetStatusCondition only refreshes it when Status changes.
			meta.RemoveStatusCondition(&rayServiceInstance.Status.Conditions, string(rayv1.RayServiceReady))
			setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceInitializing,
				"RayService is initializing after resuming from suspend.")
			meta.RemoveStatusCondition(&rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress))
			setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.RayServiceInitializing,
				"RayService is initializing after resuming from suspend.")
			return ctrl.Result{}, nil
		}
		// Stay suspended; nothing to reconcile.
		return ctrl.Result{}, nil
	}

	// Case 2: Spec.Suspend just transitioned to true. Stage the status
	// transition (Suspending=True + reset status fields) and return so the
	// caller can persist it. Deletion runs on the next reconcile once
	// Suspending is durable.
	if !isSuspending {
		if !rayServiceInstance.Spec.Suspend {
			return ctrl.Result{}, nil
		}
		logger.Info("Spec.Suspend is true; committing transition to Suspending state")
		rayServiceInstance.Status.ObservedGeneration = rayServiceInstance.ObjectMeta.Generation
		setCondition(rayServiceInstance, rayv1.RayServiceSuspending, metav1.ConditionTrue, rayv1.SuspendRequested,
			"Spec.Suspend is true; will delete all Kubernetes resources owned by this RayService.")
		setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.SuspendInProgress, "RayService is suspending.")
		setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.SuspendInProgress,
			"No upgrade in progress.")
		setCondition(rayServiceInstance, rayv1.RollbackInProgress, metav1.ConditionFalse, rayv1.SuspendInProgress,
			"No rollback in progress.")
		rayServiceInstance.Status.ActiveServiceStatus = rayv1.RayServiceStatus{}
		rayServiceInstance.Status.PendingServiceStatus = rayv1.RayServiceStatus{}
		rayServiceInstance.Status.NumServeEndpoints = 0
		rayServiceInstance.Status.ServiceStatus = rayv1.NotRunning
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
	}

	// Case 3: Suspending is committed in storage. Delete owned resources.
	// Atomic: ignore Spec.Suspend here — once Suspending is persisted, the
	// deletion always runs to completion.
	allDeleted, err := r.deleteRayServiceOwnedResources(ctx, rayServiceInstance)
	if err != nil {
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, err
	}

	if !allDeleted {
		setCondition(rayServiceInstance, rayv1.RayServiceSuspending, metav1.ConditionTrue, rayv1.SuspendInProgress,
			"Waiting for all Kubernetes resources owned by this RayService to be deleted.")
		return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
	}

	// All resources deleted: transition to Suspended. Requeue so that the
	// next reconcile observes Spec.Suspend; this matters when the user
	// flipped Spec.Suspend back to false mid-suspend (atomic completion put
	// us here despite Spec.Suspend=false), since the status-only update
	// below would not otherwise wake the controller up.
	logger.Info("All RayService-owned resources deleted; transitioning to Suspended")
	setCondition(rayServiceInstance, rayv1.RayServiceSuspending, metav1.ConditionFalse, rayv1.SuspendComplete,
		"All owned resources have been deleted.")
	setCondition(rayServiceInstance, rayv1.RayServiceSuspended, metav1.ConditionTrue, rayv1.SuspendComplete, "All owned resources have been deleted.")
	setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.SuspendComplete, "RayService is suspended.")
	setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.SuspendComplete,
		"No upgrade in progress.")
	setCondition(rayServiceInstance, rayv1.RollbackInProgress, metav1.ConditionFalse, rayv1.SuspendComplete,
		"No rollback in progress.")
	return ctrl.Result{RequeueAfter: ServiceDefaultRequeueDuration}, nil
}

// deleteRayServiceOwnedResources deletes every Kubernetes resource that the
// RayService controller owns: RayClusters, head/serve Services, and — when
// RayServiceIncrementalUpgrade is enabled — the Gateway and HTTPRoute.
// Per-cluster serve Services used by incremental upgrade are owned by their
// RayCluster, so they are garbage-collected when the cluster is deleted above.
// Returns true when nothing remained to be deleted.
func (r *RayServiceReconciler) deleteRayServiceOwnedResources(ctx context.Context, rayServiceInstance *rayv1.RayService) (bool, error) {
	logger := ctrl.LoggerFrom(ctx)
	allDeleted := true

	// RayClusters.
	rayClusterList := rayv1.RayClusterList{}
	if err := r.List(ctx, &rayClusterList, common.RayServiceRayClustersAssociationOptions(rayServiceInstance).ToListOptions()...); err != nil {
		return false, err
	}
	for i := range rayClusterList.Items {
		cluster := &rayClusterList.Items[i]
		allDeleted = false
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}
		logger.Info("Deleting RayCluster for suspend", "name", cluster.Name)
		if err := r.Delete(ctx, cluster, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteRayCluster), string(utils.DeleteAction),
				"Failed to delete the RayCluster %s/%s during suspend: %v", cluster.Namespace, cluster.Name, err)
			return false, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.DeletedRayCluster), string(utils.DeleteAction),
			"Deleted the RayCluster %s/%s during suspend", cluster.Namespace, cluster.Name)
	}

	// Kubernetes Services (head + serve).
	svcList := corev1.ServiceList{}
	if err := r.List(ctx, &svcList, common.RayServiceRayClustersAssociationOptions(rayServiceInstance).ToListOptions()...); err != nil {
		return false, err
	}
	for i := range svcList.Items {
		svc := &svcList.Items[i]
		allDeleted = false
		if !svc.DeletionTimestamp.IsZero() {
			continue
		}
		logger.Info("Deleting Service for suspend", "name", svc.Name)
		if err := r.Delete(ctx, svc, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteService), string(utils.DeleteAction),
				"Failed to delete the Service %s/%s during suspend: %v", svc.Namespace, svc.Name, err)
			return false, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.DeletedService), string(utils.DeleteAction),
			"Deleted the Service %s/%s during suspend", svc.Namespace, svc.Name)
	}

	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		// Delete only the Gateway owned by the RayService, not one referenced via GatewayRef.
		gateway := &gwv1.Gateway{}
		if err := r.Get(ctx, common.RayServiceOwnedGatewayNamespacedName(rayServiceInstance), gateway); err == nil {
			if metav1.IsControlledBy(gateway, rayServiceInstance) {
				allDeleted = false
				if gateway.DeletionTimestamp.IsZero() {
					logger.Info("Deleting Gateway for suspend", "name", gateway.Name)
					if err := r.Delete(ctx, gateway, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
						r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteGateway), string(utils.DeleteAction),
							"Failed to delete the Gateway %s/%s during suspend: %v", gateway.Namespace, gateway.Name, err)
						return false, err
					}
					r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.DeletedGateway), string(utils.DeleteAction),
						"Deleted the Gateway %s/%s during suspend", gateway.Namespace, gateway.Name)
				}
			}
		} else if !errors.IsNotFound(err) {
			return false, err
		}

		httpRoute := &gwv1.HTTPRoute{}
		if err := r.Get(ctx, common.RayServiceHTTPRouteNamespacedName(rayServiceInstance), httpRoute); err == nil {
			allDeleted = false
			if httpRoute.DeletionTimestamp.IsZero() {
				logger.Info("Deleting HTTPRoute for suspend", "name", httpRoute.Name)
				if err := r.Delete(ctx, httpRoute, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
					r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteHTTPRoute), string(utils.DeleteAction),
						"Failed to delete the HTTPRoute %s/%s during suspend: %v", httpRoute.Namespace, httpRoute.Name, err)
					return false, err
				}
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.DeletedHTTPRoute), string(utils.DeleteAction),
					"Deleted the HTTPRoute %s/%s during suspend", httpRoute.Namespace, httpRoute.Name)
			}
		} else if !errors.IsNotFound(err) {
			return false, err
		}
	}

	return allDeleted, nil
}

// updateStatusIfChanged persists rayServiceInstance.Status if it differs from
// originalRayService.Status. It mirrors the status-update path used at the end
// of Reconcile so that handleSuspend can finalize a reconcile early.
func (r *RayServiceReconciler) updateStatusIfChanged(ctx context.Context, originalRayService, rayServiceInstance *rayv1.RayService) error {
	if !utils.InconsistentRayServiceStatuses(originalRayService.Status, rayServiceInstance.Status) {
		return nil
	}
	rayServiceInstance.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
	return r.Status().Update(ctx, rayServiceInstance)
}

func (r *RayServiceReconciler) reconcileServicesToReadyCluster(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster) (*corev1.Service, *corev1.Service, error) {
	// Create K8s services if they don't exist. If they do exist, update the services to point to the RayCluster passed in.
	headSvc, err := r.reconcileServices(ctx, rayServiceInstance, rayClusterInstance, utils.HeadService)
	if err != nil {
		return headSvc, nil, err
	}
	serveSvc, err := r.reconcileServices(ctx, rayServiceInstance, rayClusterInstance, utils.ServingService)
	if err != nil {
		return headSvc, serveSvc, err
	}
	return headSvc, serveSvc, nil
}

// reconcilePromotionAndServingStatus handles the promotion logic after an upgrade, returning
// isPendingClusterServing: True if the main Kubernetes services are pointing to the pending cluster.
func reconcilePromotionAndServingStatus(ctx context.Context, headSvc, serveSvc *corev1.Service, rayServiceInstance *rayv1.RayService, pendingCluster *rayv1.RayCluster) (isPendingClusterServing bool) {
	logger := ctrl.LoggerFrom(ctx)

	// Step 1: Service Consistency Check. Ensure head and serve services point to the
	// same cluster (active or pending).
	clusterSvcPointsTo := utils.GetRayClusterNameFromService(headSvc)
	if clusterSvcPointsTo != utils.GetRayClusterNameFromService(serveSvc) {
		// This indicates a broken state that the controller cannot recover from automatically.
		panic("headSvc and serveSvc are not pointing to the same cluster")
	}

	// Step 2: Cluster Switching Logic. Determine which cluster the services are currently pointing to and
	// determine if promotion should occur.
	pendingClusterName := rayServiceInstance.Status.PendingServiceStatus.RayClusterName
	activeClusterName := rayServiceInstance.Status.ActiveServiceStatus.RayClusterName

	// Verify that the service points to a known cluster (either active or pending).
	if clusterSvcPointsTo != pendingClusterName && clusterSvcPointsTo != activeClusterName {
		panic("clusterName from services is not equal to pendingCluster or activeCluster")
	}

	var shouldPromote bool
	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		// An incremental upgrade is complete when the active cluster has 0% capacity and the pending cluster has
		// 100% of the traffic. We can't promote the pending cluster until traffic has been fully migrated.
		if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress)) {
			isRollbackInProgress := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))

			// Do not promote the pending cluster during a rollback. Traffic is being
			// shifted back to the active cluster, so the pending cluster must not
			// become the new active even if its TargetCapacity and TrafficRoutedPercent
			// reach the promotion thresholds.
			if !isRollbackInProgress && utils.IsIncrementalUpgradeComplete(rayServiceInstance, pendingCluster) {
				shouldPromote = true
				logger.Info("Incremental upgrade completed, triggering promotion.", "rayService", rayServiceInstance.Name)
			}
		} else if activeClusterName == "" && pendingClusterName != "" {
			// The Active cluster is empty when the RayCluster is first scaling up.
			shouldPromote = true
		}
	} else {
		// For traditional blue/green upgrade, promotion is complete when the Service selector has switched.
		if activeClusterName != clusterSvcPointsTo {
			shouldPromote = true
		}
	}

	// Step 3: Promote the pending cluster if prior conditions are met.
	if shouldPromote {
		logger.Info("Promoting pending cluster to active.",
			"oldCluster", rayServiceInstance.Status.ActiveServiceStatus.RayClusterName,
			"newCluster", rayServiceInstance.Status.PendingServiceStatus.RayClusterName)

		rayServiceInstance.Status.ActiveServiceStatus = rayServiceInstance.Status.PendingServiceStatus
		rayServiceInstance.Status.PendingServiceStatus = rayv1.RayServiceStatus{}
	}

	return (clusterSvcPointsTo == pendingClusterName)
}

func (r *RayServiceReconciler) calculateStatus(
	ctx context.Context,
	rayServiceInstance *rayv1.RayService,
	headSvc, serveSvc *corev1.Service,
	activeCluster, pendingCluster *rayv1.RayCluster,
	activeClusterServeApplications, pendingClusterServeApplications map[string]rayv1.AppStatus,
	httpRoute *gwv1.HTTPRoute,
) error {
	logger := ctrl.LoggerFrom(ctx)

	rayServiceInstance.Status.ObservedGeneration = rayServiceInstance.ObjectMeta.Generation

	// Update RayClusterStatus in RayService status.
	var activeClusterStatus, pendingClusterStatus rayv1.RayClusterStatus
	if activeCluster != nil {
		activeClusterStatus = activeCluster.Status
	}
	if pendingCluster != nil {
		pendingClusterStatus = pendingCluster.Status
	}
	rayServiceInstance.Status.ActiveServiceStatus.RayClusterStatus = activeClusterStatus
	rayServiceInstance.Status.PendingServiceStatus.RayClusterStatus = pendingClusterStatus

	// Update serve application status in RayService status.
	rayServiceInstance.Status.ActiveServiceStatus.Applications = activeClusterServeApplications
	rayServiceInstance.Status.PendingServiceStatus.Applications = pendingClusterServeApplications

	var isPendingClusterServing bool
	if headSvc != nil && serveSvc != nil {
		if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
			logger.Info("Processing NewClusterWithIncrementalUpgrade strategy.", "rayService", rayServiceInstance.Name)
			oldActivePercent := ptr.Deref(rayServiceInstance.Status.ActiveServiceStatus.TrafficRoutedPercent, -1)
			oldPendingPercent := ptr.Deref(rayServiceInstance.Status.PendingServiceStatus.TrafficRoutedPercent, -1)

			// Update TrafficRoutedPercent to each RayService based on current weights from HTTPRoute.
			activeWeight, pendingWeight := utils.GetWeightsFromHTTPRoute(httpRoute, rayServiceInstance)
			now := metav1.Time{Time: time.Now()}
			if activeWeight >= 0 {
				rayServiceInstance.Status.ActiveServiceStatus.TrafficRoutedPercent = new(activeWeight)
				logger.Info("Updated active TrafficRoutedPercent from HTTPRoute", "activeClusterWeight", activeWeight)
				if activeWeight != oldActivePercent {
					rayServiceInstance.Status.ActiveServiceStatus.LastTrafficMigratedTime = &now
					logger.Info("Updated LastTrafficMigratedTime of Active Service.")
				}
			}
			if pendingWeight >= 0 && pendingCluster != nil {
				rayServiceInstance.Status.PendingServiceStatus.TrafficRoutedPercent = new(pendingWeight)
				logger.Info("Updated pending TrafficRoutedPercent from HTTPRoute", "pendingClusterWeight", pendingWeight)
				if pendingWeight != oldPendingPercent {
					rayServiceInstance.Status.PendingServiceStatus.LastTrafficMigratedTime = &now
					logger.Info("Updated LastTrafficMigratedTime of Pending Service.")
				}
			}
		}
		// Reconcile serving status and promotion logic for all upgrade strategies.
		isPendingClusterServing = reconcilePromotionAndServingStatus(ctx, headSvc, serveSvc, rayServiceInstance, pendingCluster)
	}

	if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress)) {
		activeStatus := &rayServiceInstance.Status.ActiveServiceStatus
		pendingStatus := &rayServiceInstance.Status.PendingServiceStatus

		if shouldCompleteIncrementalRollback(activeStatus, pendingStatus, pendingCluster) {
			logger.Info("Rollback to original cluster is complete. Cleaning up pending cluster from prior upgrade.")

			// Clear the RayService pending service status to clean up the pending cluster.
			rayServiceInstance.Status.PendingServiceStatus = rayv1.RayServiceStatus{}
			pendingCluster = nil

			meta.RemoveStatusCondition(&rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))

			// Ensure the upgrade state machine resets after a successful rollback.
			setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.NoPendingCluster, "Rollback complete, active Ray cluster exists and no pending Ray cluster")
		}
	}

	if shouldPrepareNewCluster(ctx, rayServiceInstance, activeCluster, pendingCluster, isPendingClusterServing) {
		rayServiceInstance.Status.PendingServiceStatus = rayv1.RayServiceStatus{
			RayClusterName: utils.GenerateRayClusterName(rayServiceInstance.Name),
		}
		logger.Info("Preparing a new pending RayCluster instance by setting RayClusterName",
			"clusterName", rayServiceInstance.Status.PendingServiceStatus.RayClusterName)

		if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
			// Set IncrementalUpgrade related Status fields for new pending RayCluster if enabled.
			if rayServiceInstance.Status.ActiveServiceStatus.RayClusterName == "" {
				// If no Active RayCluster exists - default to starting with 100% TargetCapacity.
				// This is the case when a RayCluster is first starting for a RayService, so we should
				// immediately scale it to full target capacity.
				if rayServiceInstance.Status.ActiveServiceStatus.TargetCapacity == nil {
					rayServiceInstance.Status.PendingServiceStatus.TargetCapacity = new(int32(100))
				}
			} else {
				// Pending RayCluster during an upgrade should start with 0% TargetCapacity, since
				// traffic will be gradually migrated to the new cluster.
				if rayServiceInstance.Status.PendingServiceStatus.TargetCapacity == nil {
					rayServiceInstance.Status.PendingServiceStatus.TargetCapacity = new(int32(0))
				}
			}
		}
	}

	// Calculate the number of ready serve endpoints for the active cluster.
	serveServiceNamespacedName := common.RayServiceServeServiceNamespacedName(rayServiceInstance)
	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) && activeCluster != nil {
		// The Serve service name is based on the unique RayCluster name, since we use the
		// per-cluster Serve services for traffic routing during an incremental upgrade.
		serveServiceNamespacedName.Name = utils.GenerateServeServiceName(activeCluster.Name)
	}
	numServeEndpoints, err := r.calculateNumServeEndpointsFromSlices(ctx, serveServiceNamespacedName)
	if err != nil {
		return err
	}

	// During NewClusterWithIncrementalUpgrade, the pending RayCluster is also serving.
	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) && pendingCluster != nil {
		pendingServeServiceNamespacedName := common.RayClusterServeServiceNamespacedName(pendingCluster)
		pendingEndpoints, err := r.calculateNumServeEndpointsFromSlices(ctx, pendingServeServiceNamespacedName)
		if err != nil {
			return err
		}
		numServeEndpoints += pendingEndpoints
	}

	if numServeEndpoints > math.MaxInt32 {
		return errstd.New("numServeEndpoints exceeds math.MaxInt32")
	}

	rayServiceInstance.Status.NumServeEndpoints = int32(numServeEndpoints) //nolint:gosec // This is a false positive from gosec. See https://github.com/securego/gosec/issues/1212 for more details.

	// Calculate conditions based on current state (endpoints, clusters, timeout, etc.)
	calculateConditions(ctx, r, rayServiceInstance)

	// The definition of `ServiceStatus` is equivalent to the `RayServiceReady` condition
	rayServiceInstance.Status.ServiceStatus = rayv1.NotRunning
	if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceReady)) {
		rayServiceInstance.Status.ServiceStatus = rayv1.Running
	}

	return nil
}

func calculateConditions(ctx context.Context, r *RayServiceReconciler, rayServiceInstance *rayv1.RayService) {
	if rayServiceInstance.Status.Conditions == nil {
		rayServiceInstance.Status.Conditions = []metav1.Condition{}
	}
	if len(rayServiceInstance.Status.Conditions) == 0 {
		message := "RayService is initializing"
		setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceInitializing, message)
		setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.RayServiceInitializing, message)
	}

	if rayServiceInstance.Status.NumServeEndpoints > 0 {
		setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "Number of serve endpoints is greater than 0")
	} else if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RayServiceReady)) {
		setCondition(rayServiceInstance, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.ZeroServeEndpoints, "Number of serve endpoints dropped to 0")
	} else {
		// Check if initializing timeout has been exceeded
		// This runs after endpoint check, so if endpoints appear, they take priority
		markFailedOnInitializingTimeout(ctx, r, rayServiceInstance)
	}

	activeClusterName := rayServiceInstance.Status.ActiveServiceStatus.RayClusterName
	pendingClusterName := rayServiceInstance.Status.PendingServiceStatus.RayClusterName
	if activeClusterName != "" && pendingClusterName != "" {
		setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.BothActivePendingClustersExist, "Both active and pending Ray clusters exist")
	} else if activeClusterName != "" {
		setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionFalse, rayv1.NoPendingCluster, "Active Ray cluster exists and no pending Ray cluster")
	} else {
		cond := meta.FindStatusCondition(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress))
		// Don't override the condition if RayService is initializing or has timed out
		if cond == nil || (cond.Reason != string(rayv1.RayServiceInitializing) && cond.Reason != string(rayv1.RayServiceInitializingTimeout)) {
			setCondition(rayServiceInstance, rayv1.UpgradeInProgress, metav1.ConditionUnknown, rayv1.NoActiveCluster, "No active Ray cluster exists, and the RayService is not initializing. Please open a GitHub issue in the KubeRay repository.")
		}
	}
}

func setCondition(rayServiceInstance *rayv1.RayService, conditionType rayv1.RayServiceConditionType, status metav1.ConditionStatus, reason rayv1.RayServiceConditionReason, message string) {
	condition := metav1.Condition{
		Type:               string(conditionType),
		Status:             status,
		Reason:             string(reason),
		Message:            message,
		ObservedGeneration: rayServiceInstance.Status.ObservedGeneration,
	}
	meta.SetStatusCondition(&rayServiceInstance.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *RayServiceReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayService{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		))).
		Owns(&rayv1.RayCluster{}).
		Owns(&corev1.Service{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayService")
				if request != nil {
					logger = logger.WithValues("RayService", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

func (r *RayServiceReconciler) getRayServiceInstance(ctx context.Context, request ctrl.Request) (*rayv1.RayService, error) {
	logger := ctrl.LoggerFrom(ctx)
	rayServiceInstance := &rayv1.RayService{}
	if err := r.Get(ctx, request.NamespacedName, rayServiceInstance); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Read request instance not found error!")
		} else {
			logger.Error(err, "Read request instance error!")
		}
		return nil, err
	}
	return rayServiceInstance, nil
}

func isZeroDowntimeUpgradeEnabled(ctx context.Context, upgradeStrategy *rayv1.RayServiceUpgradeStrategy) bool {
	// For LLM serving, some users might not have sufficient GPU resources to run two RayClusters simultaneously.
	// Therefore, KubeRay offers ENABLE_ZERO_DOWNTIME as a feature flag for zero-downtime upgrades.
	// There are two ways to enable zero downtime upgrade. Through ENABLE_ZERO_DOWNTIME env var or setting Spec.UpgradeStrategy.Type.
	// If no fields are set, zero downtime upgrade by default is enabled.
	// Spec.UpgradeStrategy.Type takes precedence over ENABLE_ZERO_DOWNTIME.
	logger := ctrl.LoggerFrom(ctx)
	if upgradeStrategy != nil {
		upgradeType := upgradeStrategy.Type
		if upgradeType != nil {
			if features.Enabled(features.RayServiceIncrementalUpgrade) {
				if *upgradeType != rayv1.RayServiceNewCluster && *upgradeType != rayv1.RayServiceNewClusterWithIncrementalUpgrade {
					logger.Info("Zero-downtime upgrade is disabled because UpgradeStrategy.Type is not set to %s or %s.", string(rayv1.RayServiceNewCluster), string(rayv1.RayServiceNewClusterWithIncrementalUpgrade))
					return false
				}
			} else if *upgradeType != rayv1.RayServiceNewCluster {
				logger.Info("Zero-downtime upgrade is disabled because UpgradeStrategy.Type is not set to NewCluster.")
				return false
			}
			return true
		}
	}
	zeroDowntimeEnvVar := os.Getenv(ENABLE_ZERO_DOWNTIME)
	if strings.ToLower(zeroDowntimeEnvVar) == "false" {
		logger.Info("Zero-downtime upgrade is disabled because ENABLE_ZERO_DOWNTIME is set to false.")
		return false
	}
	return true
}

// `createGateway` creates a Gateway for a RayService or updates an existing Gateway.
func (r *RayServiceReconciler) createGateway(rayServiceInstance *rayv1.RayService) (*gwv1.Gateway, error) {
	options := utils.GetRayServiceClusterUpgradeOptions(&rayServiceInstance.Spec)
	if options == nil {
		return nil, errstd.New("Missing RayService ClusterUpgradeOptions during upgrade.")
	}

	gatewayName := rayServiceInstance.Name + "-gateway"
	// Define the desired Gateway object
	rayServiceGateway := &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      gatewayName,
			Namespace: rayServiceInstance.Namespace,
		},
		Spec: gwv1.GatewaySpec{
			GatewayClassName: gwv1.ObjectName(options.GatewayClassName),
			Listeners: []gwv1.Listener{
				{
					Name:     gwv1.SectionName(utils.GatewayListenerPortName),
					Protocol: gwv1.HTTPProtocolType,
					Port:     utils.DefaultGatewayListenerPort,
				},
			},
		},
	}

	return rayServiceGateway, nil
}

// `reconcileGateway` reconciles a Gateway resource for a RayService. The possible cases are:
// (1) Create a new Gateway instance. (2) Update the Gateway instance if RayService has updated. (3) Do nothing.
func (r *RayServiceReconciler) reconcileGateway(ctx context.Context, rayServiceInstance *rayv1.RayService) error {
	logger := ctrl.LoggerFrom(ctx)
	var err error

	// For a shared Gateway, verify it exists before managing the HTTPRoute.
	if opts := utils.GetRayServiceClusterUpgradeOptions(&rayServiceInstance.Spec); opts != nil && opts.GatewayRef != nil {
		gatewayName := common.RayServiceGatewayNamespacedName(rayServiceInstance)
		if err := r.Get(ctx, gatewayName, &gwv1.Gateway{}); err != nil {
			if errors.IsNotFound(err) {
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToGetGateway), string(utils.GetAction),
					"Referenced Gateway %s/%s not found for RayService %s/%s", gatewayName.Namespace, gatewayName.Name, rayServiceInstance.Namespace, rayServiceInstance.Name)
				return nil
			}
			return err
		}
		return nil
	}

	// Construct desired Gateway object for RayService
	desiredGateway, err := r.createGateway(rayServiceInstance)
	if err != nil {
		logger.Error(err, "Failed to build Gateway object for Rayservice")
		return err
	}
	if desiredGateway == nil {
		logger.Info("Skipping Gateway reconciliation: desired Gateway is nil")
		return nil
	}

	// Check for existing RayService Gateway, create the desired Gateway if none is found
	existingGateway := &gwv1.Gateway{}
	if err := r.Get(ctx, common.RayServiceGatewayNamespacedName(rayServiceInstance), existingGateway); err != nil {
		if errors.IsNotFound(err) {
			// Set the ownership in order to do the garbage collection by k8s.
			if err := ctrl.SetControllerReference(rayServiceInstance, desiredGateway, r.Scheme); err != nil {
				return err
			}
			logger.Info("Creating a new Gateway instance", "Gateway Listeners", desiredGateway.Spec.Listeners)
			if err := r.Create(ctx, desiredGateway); err != nil {
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateGateway), string(utils.CreateAction), "Failed to create Gateway for RayService %s/%s: %v", desiredGateway.Namespace, desiredGateway.Name, err)
				return err
			}
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.CreatedGateway), string(utils.CreateAction), "Created Gateway for RayService %s/%s", desiredGateway.Namespace, desiredGateway.Name)
			return nil
		}
		return err
	}

	// If Gateway already exists, check if update is needed to reach desired state
	if !utils.IsGatewayEqual(existingGateway, desiredGateway) {
		logger.Info("Updating existing Gateway", "name", existingGateway.Name)
		existingGateway.Spec = desiredGateway.Spec
		if err := r.Update(ctx, existingGateway); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateGateway), string(utils.UpdateAction), "Failed to update the Gateway %s/%s: %v", existingGateway.Namespace, existingGateway.Name, err)
			return err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedGateway), string(utils.UpdateAction), "Updated the Gateway %s/%s", existingGateway.Namespace, existingGateway.Name)
	}

	return nil
}

// calculateTrafficRoutedPercent determines the HTTPRoute traffic split between the active and pending RayClusters.
//
// The new weights are calculated using:
// - Current TrafficRoutedPercent values
// - Time-based migration using StepSizePercent and IntervalSeconds
// - TargetCapacity constraints
//
// Returns the active cluster traffic weight, pending cluster traffic weight, and an error if any.
func (r *RayServiceReconciler) calculateTrafficRoutedPercent(ctx context.Context, rayServiceInstance *rayv1.RayService, isPendingClusterReady bool) (activeClusterWeight, pendingClusterWeight int32, err error) {
	logger := ctrl.LoggerFrom(ctx)
	activeServiceStatus := &rayServiceInstance.Status.ActiveServiceStatus
	pendingServiceStatus := &rayServiceInstance.Status.PendingServiceStatus

	// Default to 100% traffic on the active cluster.
	activeClusterWeight = ptr.Deref(activeServiceStatus.TrafficRoutedPercent, 100)
	pendingClusterWeight = ptr.Deref(pendingServiceStatus.TrafficRoutedPercent, 0)

	// Zero-downtime upgrade in progress.
	options := utils.GetRayServiceClusterUpgradeOptions(&rayServiceInstance.Spec)
	if options == nil {
		return 0, 0, errstd.New("ClusterUpgradeOptions are not set during upgrade.")
	}

	// Check that target_capacity has been updated before migrating traffic.
	pendingClusterTargetCapacity := ptr.Deref(pendingServiceStatus.TargetCapacity, 0)
	activeClusterTargetCapacity := ptr.Deref(activeServiceStatus.TargetCapacity, 100)
	isRollbackInProgress := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))

	if (pendingClusterWeight == pendingClusterTargetCapacity && !isRollbackInProgress) || (isRollbackInProgress && activeClusterWeight == activeClusterTargetCapacity) {
		// Stop traffic migration because the cluster being migrated to has reached its target capacity limit.
		return activeClusterWeight, pendingClusterWeight, nil
	}

	// If IntervalSeconds has passed since LastTrafficMigratedTime, migrate StepSizePercent traffic
	// from the active RayCluster to the pending RayCluster.
	interval := time.Duration(*options.IntervalSeconds) * time.Second

	// Determine which timestamp to use based on the direction of traffic flow.
	// We use the LastTrafficMigratedTime of the cluster to which traffic is increasing.
	var lastTrafficMigratedTime *metav1.Time
	if isRollbackInProgress {
		lastTrafficMigratedTime = activeServiceStatus.LastTrafficMigratedTime
	} else {
		lastTrafficMigratedTime = pendingServiceStatus.LastTrafficMigratedTime
	}

	if lastTrafficMigratedTime == nil || time.Since(lastTrafficMigratedTime.Time) >= interval {
		if isRollbackInProgress {
			// Gradually shift traffic from the pending to the active cluster.
			// Rollback traffic migration occurs regardless of pending cluster readiness.
			logger.Info("Rollback in progress. Shifting traffic back to active cluster.", "stepSize", *options.StepSizePercent)
			proposedActiveWeight := activeClusterWeight + *options.StepSizePercent
			activeClusterWeight = min(100, proposedActiveWeight, activeClusterTargetCapacity)
			pendingClusterWeight = 100 - activeClusterWeight
		} else if isPendingClusterReady {
			// Gradually shift traffic from the active to the pending cluster if it's ready.
			logger.Info("Upgrade in progress. Migrating traffic by StepSizePercent.", "stepSize", *options.StepSizePercent)
			proposedPendingWeight := pendingClusterWeight + *options.StepSizePercent
			pendingClusterWeight = min(100, proposedPendingWeight, pendingClusterTargetCapacity)
			activeClusterWeight = 100 - pendingClusterWeight
		} else {
			logger.Info("Upgrade in progress, but pending cluster is not ready. Pausing traffic migration.")
		}
	}

	return activeClusterWeight, pendingClusterWeight, nil
}

// createHTTPRoute creates a desired HTTPRoute object for RayService incremental upgrade.
//
// The function performs the following operations:
// 1. Retrieves Gateway instance to attach the HTTPRoute
// 2. Gets active and pending RayCluster instances and their Serve services
// 3. Calls `calculateTrafficRoutedPercent` to calculate the new traffic weights
// 4. Configures HTTPRoute with appropriate backend references and weights
//
// Returns the configured HTTPRoute object or error if any step fails.
func (r *RayServiceReconciler) createHTTPRoute(ctx context.Context, rayServiceInstance *rayv1.RayService, isPendingClusterReady bool) (*gwv1.HTTPRoute, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Retrieve Gateway instance to attach this HTTPRoute to.
	gatewayInstance := &gwv1.Gateway{}
	if err := r.Get(ctx, common.RayServiceGatewayNamespacedName(rayServiceInstance), gatewayInstance); err != nil {
		return nil, err
	}

	// Retrieve the active RayCluster
	activeRayCluster, err := r.getRayClusterByNamespacedName(ctx, common.RayServiceActiveRayClusterNamespacedName(rayServiceInstance))
	if err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to retrieve active RayCluster")
		return nil, err
	}
	if activeRayCluster == nil {
		logger.Info("Active RayCluster not found, skipping HTTPRoute creation.")
		return nil, nil
	}

	// Attempt to retrieve pending RayCluster
	pendingRayCluster, err := r.getRayClusterByNamespacedName(ctx, common.RayServicePendingRayClusterNamespacedName(rayServiceInstance))
	if err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to retrieve pending RayCluster.")
		return nil, err
	}

	activeClusterWeight, pendingClusterWeight, err := r.calculateTrafficRoutedPercent(ctx, rayServiceInstance, isPendingClusterReady)
	if err != nil {
		logger.Info("Failed to reconcile TrafficRoutedPercent for active and pending clusters.")
		return nil, err
	}

	activeClusterServeSvcName := utils.GenerateServeServiceName(activeRayCluster.Name)
	activeServePort := common.GetServePort(activeRayCluster)

	// Serve services are in the RayService's namespace, which may differ from the
	// Gateway's namespace when using a shared Gateway.
	backendRefs := []gwv1.HTTPBackendRef{
		{
			BackendRef: gwv1.BackendRef{
				BackendObjectReference: gwv1.BackendObjectReference{
					Name:      gwv1.ObjectName(activeClusterServeSvcName),
					Namespace: new(gwv1.Namespace(rayServiceInstance.Namespace)),
					Port:      new(activeServePort),
				},
				Weight: new(activeClusterWeight),
			},
		},
	}

	if pendingRayCluster != nil {
		logger.Info("Pending RayCluster exists. Including it in HTTPRoute.", "RayCluster", pendingRayCluster.Name)
		pendingClusterServeSvcName := utils.GenerateServeServiceName(pendingRayCluster.Name)
		pendingServePort := common.GetServePort(pendingRayCluster)

		backendRefs = append(backendRefs, gwv1.HTTPBackendRef{
			BackendRef: gwv1.BackendRef{
				BackendObjectReference: gwv1.BackendObjectReference{
					Name:      gwv1.ObjectName(pendingClusterServeSvcName),
					Namespace: new(gwv1.Namespace(rayServiceInstance.Namespace)),
					Port:      new(pendingServePort),
				},
				Weight: new(pendingClusterWeight),
			},
		})
	}

	httpRouteName := rayServiceInstance.Name + "-httproute"
	parentRef := gwv1.ParentReference{
		Name:      gwv1.ObjectName(gatewayInstance.Name),
		Namespace: new(gwv1.Namespace(gatewayInstance.Namespace)),
	}

	// GatewayRef pins the parentRef to a specific listener on the referenced Gateway.
	if opts := utils.GetRayServiceClusterUpgradeOptions(&rayServiceInstance.Spec); opts != nil && opts.GatewayRef != nil {
		if opts.GatewayRef.SectionName != "" {
			parentRef.SectionName = new(gwv1.SectionName(opts.GatewayRef.SectionName))
		}
		if opts.GatewayRef.Port != nil {
			parentRef.Port = new(*opts.GatewayRef.Port)
		}
	}

	desiredHTTPRoute := &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: httpRouteName, Namespace: rayServiceInstance.Namespace},
		Spec: gwv1.HTTPRouteSpec{
			CommonRouteSpec: gwv1.CommonRouteSpec{
				ParentRefs: []gwv1.ParentReference{parentRef},
			},
			Rules: []gwv1.HTTPRouteRule{
				{
					Matches: []gwv1.HTTPRouteMatch{
						{
							Path: &gwv1.HTTPPathMatch{
								Type:  ptr.To(gwv1.PathMatchPathPrefix),
								Value: new("/"),
							},
						},
					},
					BackendRefs: backendRefs,
				},
			},
		},
	}

	return desiredHTTPRoute, nil
}

// reconcileHTTPRoute reconciles a HTTPRoute resource for a RayService to route traffic during a NewClusterWithIncrementalUpgrade.
func (r *RayServiceReconciler) reconcileHTTPRoute(ctx context.Context, rayServiceInstance *rayv1.RayService, isPendingClusterReady bool) (*gwv1.HTTPRoute, error) {
	logger := ctrl.LoggerFrom(ctx)
	var err error

	desiredHTTPRoute, err := r.createHTTPRoute(ctx, rayServiceInstance, isPendingClusterReady)
	if err != nil {
		logger.Error(err, "Failed to build HTTPRoute for RayService upgrade")
		return nil, err
	}
	if desiredHTTPRoute == nil {
		logger.Info("Skipping HTTPRoute reconciliation: desired HTTPRoute is nil")
		return nil, nil
	}

	// Check for existing HTTPRoute for RayService
	existingHTTPRoute := &gwv1.HTTPRoute{}
	if err := r.Get(ctx, common.RayServiceHTTPRouteNamespacedName(rayServiceInstance), existingHTTPRoute); err != nil {
		if errors.IsNotFound(err) {
			// Set the ownership in order to do the garbage collection by k8s.
			if err := ctrl.SetControllerReference(rayServiceInstance, desiredHTTPRoute, r.Scheme); err != nil {
				return nil, err
			}
			if err = r.Create(ctx, desiredHTTPRoute); err != nil {
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateHTTPRoute), string(utils.CreateAction), "Failed to create the HTTPRoute for RayService %s/%s: %v", desiredHTTPRoute.Namespace, desiredHTTPRoute.Name, err)
				return nil, err
			}
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.CreatedHTTPRoute), string(utils.CreateAction), "Created HTTPRoute for RayService %s/%s", desiredHTTPRoute.Namespace, desiredHTTPRoute.Name)
			return desiredHTTPRoute, nil
		}
		return nil, err
	}

	// If HTTPRoute already exists, check if update is needed
	if !utils.IsHTTPRouteEqual(existingHTTPRoute, desiredHTTPRoute) {
		logger.Info("Updating existing HTTPRoute", "name", desiredHTTPRoute.Name)
		existingHTTPRoute.Spec = desiredHTTPRoute.Spec
		if err := r.Update(ctx, existingHTTPRoute); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateHTTPRoute), string(utils.UpdateAction), "Failed to update the HTTPRoute %s/%s: %v", existingHTTPRoute.Namespace, existingHTTPRoute.Name, err)
			return nil, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedHTTPRoute), string(utils.UpdateAction), "Updated the HTTPRoute %s/%s", existingHTTPRoute.Namespace, existingHTTPRoute.Name)
	}

	return existingHTTPRoute, nil
}

// `reconcileRayCluster` reconciles the active and pending Ray clusters. There are 4 possible cases:
// (1) Create a new pending cluster. (2) Update the active cluster. (3) Update the pending cluster. (4) Do nothing.
func (r *RayServiceReconciler) reconcileRayCluster(ctx context.Context, rayServiceInstance *rayv1.RayService) (*rayv1.RayCluster, *rayv1.RayCluster, error) {
	logger := ctrl.LoggerFrom(ctx)
	var err error

	// Get active cluster and pending cluster instances.
	activeRayCluster, err := r.getRayClusterByNamespacedName(ctx, common.RayServiceActiveRayClusterNamespacedName(rayServiceInstance))
	if err != nil {
		return nil, nil, err
	}

	pendingRayCluster, err := r.getRayClusterByNamespacedName(ctx, common.RayServicePendingRayClusterNamespacedName(rayServiceInstance))
	if err != nil {
		return nil, nil, err
	}

	if rayServiceInstance.Status.PendingServiceStatus.RayClusterName != "" && pendingRayCluster == nil {
		logger.Info("Creating a new pending RayCluster instance")
		pendingRayCluster, err = r.createRayClusterInstance(ctx, rayServiceInstance)
		return activeRayCluster, pendingRayCluster, err
	}

	if shouldUpdateCluster(rayServiceInstance, activeRayCluster, true) {
		// TODO(kevin85421): We should not reconstruct the cluster to update it. This will cause issues if autoscaler is enabled.
		logger.Info("Updating the active RayCluster instance", "clusterName", activeRayCluster.Name)
		goalCluster, err := constructRayClusterForRayService(rayServiceInstance, activeRayCluster.Name, r.Scheme)
		if err != nil {
			return nil, nil, err
		}
		modifyRayCluster(ctx, activeRayCluster, goalCluster)
		if err = r.Update(ctx, activeRayCluster); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateRayCluster), string(utils.UpdateAction), "Failed to update the active RayCluster %s/%s: %v", activeRayCluster.Namespace, activeRayCluster.Name, err)
			return activeRayCluster, pendingRayCluster, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedRayCluster), string(utils.UpdateAction), "Updated the active RayCluster %s/%s", activeRayCluster.Namespace, activeRayCluster.Name)
	}

	if shouldUpdateCluster(rayServiceInstance, pendingRayCluster, false) {
		// TODO(kevin85421): We should not reconstruct the cluster to update it. This will cause issues if autoscaler is enabled.
		logger.Info("Updating the pending RayCluster instance", "clusterName", pendingRayCluster.Name)
		goalCluster, err := constructRayClusterForRayService(rayServiceInstance, pendingRayCluster.Name, r.Scheme)
		if err != nil {
			return nil, nil, err
		}
		modifyRayCluster(ctx, pendingRayCluster, goalCluster)
		if err = r.Update(ctx, pendingRayCluster); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateRayCluster), string(utils.UpdateAction), "Failed to update the pending RayCluster %s/%s: %v", pendingRayCluster.Namespace, pendingRayCluster.Name, err)
			return activeRayCluster, pendingRayCluster, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedRayCluster), string(utils.UpdateAction), "Updated the pending RayCluster %s/%s", pendingRayCluster.Namespace, pendingRayCluster.Name)
	}

	return activeRayCluster, pendingRayCluster, nil
}

// cleanUpRayClusterInstance cleans up all the dangling RayCluster instances that are owned by the RayService instance.
// Returns true if there are still RayCluster instances that need to be cleaned up (either scheduled for deletion or waiting for deletion delay).
func (r *RayServiceReconciler) cleanUpRayClusterInstance(ctx context.Context, rayServiceInstance *rayv1.RayService) (bool, error) {
	logger := ctrl.LoggerFrom(ctx)
	rayClusterList := rayv1.RayClusterList{}
	rayServiceIsDeleted := !rayServiceInstance.ObjectMeta.DeletionTimestamp.IsZero()

	var err error
	if err = r.List(ctx, &rayClusterList, common.RayServiceRayClustersAssociationOptions(rayServiceInstance).ToListOptions()...); err != nil {
		return false, err
	}

	// Determine the ray cluster deletion delay seconds
	deletionDelay := RayClusterDeletionDelayDuration
	if rayServiceInstance.Spec.RayClusterDeletionDelaySeconds != nil {
		deletionDelay = time.Duration(*rayServiceInstance.Spec.RayClusterDeletionDelaySeconds) * time.Second
	}
	if rayServiceIsDeleted {
		deletionDelay = 0 * time.Second
	}

	hasRayClustersToClean := false
	// Clean up RayCluster instances. Each instance is deleted after the configured deletion delay.
	for _, rayClusterInstance := range rayClusterList.Items {
		if rayServiceIsDeleted || (rayClusterInstance.Name != rayServiceInstance.Status.ActiveServiceStatus.RayClusterName && rayClusterInstance.Name != rayServiceInstance.Status.PendingServiceStatus.RayClusterName) {
			hasRayClustersToClean = true
			cachedTimestamp, exists := r.RayClusterDeletionTimestamps.Get(rayClusterInstance.Name)
			if exists || rayServiceIsDeleted {
				reasonForDeletion := ""
				if rayServiceIsDeleted {
					reasonForDeletion = "RayService is being deleted. Deleting RayCluster"
				} else if time.Now().After(cachedTimestamp) {
					reasonForDeletion = fmt.Sprintf("Deletion timestamp %s "+
						"for RayCluster %s has passed. Deleting cluster "+
						"immediately.", cachedTimestamp, rayClusterInstance.Name)
				}
				if reasonForDeletion != "" {
					logger.Info("reconcileRayCluster", "delete Ray cluster", rayClusterInstance.Name, "reason", reasonForDeletion)
					if err := r.Delete(ctx, &rayClusterInstance, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
						r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteRayCluster), string(utils.DeleteAction), "Failed to delete the RayCluster %s/%s: %v", rayClusterInstance.Namespace, rayClusterInstance.Name, err)
						return false, err
					}
					r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.DeletedRayCluster), string(utils.DeleteAction), "Deleted the RayCluster %s/%s", rayClusterInstance.Namespace, rayClusterInstance.Name)
				}
			} else {
				deletionTimestamp := metav1.Now().Add(deletionDelay)
				r.RayClusterDeletionTimestamps.Set(rayClusterInstance.Name, deletionTimestamp)
				logger.Info(
					"Scheduled dangling RayCluster for deletion",
					"rayClusterName", rayClusterInstance.Name,
					"deletionDelay", deletionDelay.String(),
					"deletionTimestamp", deletionTimestamp,
				)
			}
		}
	}

	return hasRayClustersToClean, nil
}

func (r *RayServiceReconciler) getRayClusterByNamespacedName(ctx context.Context, clusterKey client.ObjectKey) (*rayv1.RayCluster, error) {
	if clusterKey.Name == "" {
		return nil, nil
	}

	rayCluster := &rayv1.RayCluster{}
	if err := r.Get(ctx, clusterKey, rayCluster); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return rayCluster, nil
}

// cleanUpServeConfigCache cleans up the unused serve applications config in the cached map.
func (r *RayServiceReconciler) cleanUpServeConfigCache(ctx context.Context, rayServiceInstance *rayv1.RayService) {
	logger := ctrl.LoggerFrom(ctx)
	activeRayClusterName := rayServiceInstance.Status.ActiveServiceStatus.RayClusterName
	pendingRayClusterName := rayServiceInstance.Status.PendingServiceStatus.RayClusterName

	cacheKey := rayServiceInstance.Namespace + "/" + rayServiceInstance.Name
	cacheValue, exist := r.ServeConfigs.Get(cacheKey)
	if !exist {
		return
	}
	clusterNameToServeConfig := cacheValue.(cmap.ConcurrentMap[string, string])

	for key := range clusterNameToServeConfig.Items() {
		if key == activeRayClusterName || key == pendingRayClusterName {
			continue
		}
		logger.Info("Remove stale serve application config", "remove key", key, "activeRayClusterName", activeRayClusterName, "pendingRayClusterName", pendingRayClusterName)
		clusterNameToServeConfig.Remove(key)
	}
}

func shouldUpdateCluster(rayServiceInstance *rayv1.RayService, cluster *rayv1.RayCluster, isActiveCluster bool) bool {
	// Check whether to update the RayCluster or not.
	if cluster == nil {
		return false
	}
	if isActiveCluster {
		if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress)) {
			// If the RayService is upgrading, the `RayService.Spec.RayClusterSpec` should only be compared with the
			// pending cluster. The active cluster should not be updated.
			return false
		}

		// If the KubeRay version has changed, update the RayCluster to get the cluster hash and new KubeRay version.
		version := cluster.ObjectMeta.Annotations[utils.KubeRayVersion]
		if version != utils.KUBERAY_VERSION {
			return true
		}
	}

	if isClusterSpecHashEqual(rayServiceInstance, cluster, false) {
		// The RayCluster spec matches the cluster spec in the RayService. No need to update the cluster.
		return false
	}
	// Update the RayCluster when the cluster spec in the RayService differs from the RayCluster, but only if the difference
	// is in the number of worker groups and the new worker groups are added at the end.
	return isClusterSpecHashEqual(rayServiceInstance, cluster, true)
}

func isClusterSpecHashEqual(rayServiceInstance *rayv1.RayService, cluster *rayv1.RayCluster, partial bool) bool {
	// If `partial` is true, only compare the first `len(cluster.Spec.WorkerGroupSpecs)` worker groups in the CR spec.
	clusterHash := cluster.ObjectMeta.Annotations[utils.HashWithoutReplicasAndWorkersToDeleteKey]
	goalClusterSpec := rayClusterSpecForHashing(rayServiceInstance)
	goalClusterHash := ""
	if !partial {
		goalClusterHash, _ = utils.GenerateHashWithoutReplicasAndWorkersToDelete(*goalClusterSpec)
	} else {
		// If everything is identical except for the Replicas and WorkersToDelete of
		// the existing workergroups, and one or more new workergroups are added at the end, then update the cluster.
		clusterNumWorkerGroups, err := strconv.Atoi(cluster.ObjectMeta.Annotations[utils.NumWorkerGroupsKey])
		if err != nil {
			return true
		}
		goalNumWorkerGroups := len(goalClusterSpec.WorkerGroupSpecs)
		if goalNumWorkerGroups >= clusterNumWorkerGroups {

			// Remove the new workergroup(s) from the end before calculating the hash.
			goalClusterSpec.WorkerGroupSpecs = goalClusterSpec.WorkerGroupSpecs[:clusterNumWorkerGroups]

			// Generate the hash of the old worker group specs.
			goalClusterHash, err = utils.GenerateHashWithoutReplicasAndWorkersToDelete(*goalClusterSpec)
			if err != nil {
				return true
			}
		}
	}
	return clusterHash == goalClusterHash
}

func shouldPrepareNewCluster(ctx context.Context, rayServiceInstance *rayv1.RayService, activeRayCluster, pendingRayCluster *rayv1.RayCluster, isPendingClusterServing bool) bool {
	if isPendingClusterServing {
		return false
	}

	// Do not prepare a new cluster while a rollback is actively in progress.
	if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress)) {
		return false
	}

	if activeRayCluster == nil && pendingRayCluster == nil {
		// Both active and pending clusters are nil, which means the RayService has just been created.
		// Create a new pending cluster.
		return true
	}
	cluster := pendingRayCluster
	if cluster == nil {
		cluster = activeRayCluster
	}
	if isClusterSpecHashEqual(rayServiceInstance, cluster, false) {
		// The RayCluster spec matches the cluster spec in the RayService. No need to create a new pending cluster.
		return false
	}
	if isClusterSpecHashEqual(rayServiceInstance, cluster, true) {
		// KubeRay should update the RayCluster instead of creating a new one.
		return false
	}
	return isZeroDowntimeUpgradeEnabled(ctx, rayServiceInstance.Spec.UpgradeStrategy)
}

// `modifyRayCluster` updates `currentCluster` in place based on `goalCluster`. `currentCluster` is the
// current RayCluster retrieved from the informer cache, and `goalCluster` is the target state of the
// RayCluster derived from the RayService spec.
func modifyRayCluster(ctx context.Context, currentCluster, goalCluster *rayv1.RayCluster) {
	logger := ctrl.LoggerFrom(ctx)

	if currentCluster.Name != goalCluster.Name || currentCluster.Namespace != goalCluster.Namespace {
		panic(fmt.Sprintf(
			"currentCluster and goalCluster have different names or namespaces: "+
				"%s/%s != %s/%s",
			currentCluster.Namespace,
			currentCluster.Name,
			goalCluster.Namespace,
			goalCluster.Name,
		))
	}
	logger.Info("updateRayClusterInstance", "Name", goalCluster.Name, "Namespace", goalCluster.Namespace)

	// Update the fetched RayCluster with new changes. Suspend is propagated
	// from the RayService to the RayCluster only at creation time; afterwards
	// the RayCluster's Suspend is delegated to Kueue, so we preserve the
	// existing cluster's Suspend here instead of letting the goal spec
	// overwrite it.
	existingSuspend := currentCluster.Spec.Suspend
	currentCluster.Spec = goalCluster.Spec
	currentCluster.Spec.Suspend = existingSuspend

	// Update the labels and annotations
	currentCluster.Labels = goalCluster.Labels
	currentCluster.Annotations = goalCluster.Annotations
}

func (r *RayServiceReconciler) createRayClusterInstance(ctx context.Context, rayServiceInstance *rayv1.RayService) (*rayv1.RayCluster, error) {
	logger := ctrl.LoggerFrom(ctx)
	rayClusterKey := common.RayServicePendingRayClusterNamespacedName(rayServiceInstance)
	logger.Info("createRayClusterInstance", "clusterName", rayClusterKey.Name)

	rayClusterInstance, err := constructRayClusterForRayService(rayServiceInstance, rayClusterKey.Name, r.Scheme)
	if err != nil {
		return nil, err
	}
	if err = r.Create(ctx, rayClusterInstance); err != nil {
		logger.Error(err, "Failed to create the RayCluster")
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRayCluster), string(utils.CreateAction), "Failed to create the RayCluster %s/%s: %v", rayClusterInstance.Namespace, rayClusterInstance.Name, err)
		return nil, err
	}
	logger.Info("Created RayCluster for RayService", "clusterName", rayClusterInstance.Name)
	r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.CreatedRayCluster), string(utils.CreateAction), "Created the RayCluster %s/%s", rayClusterInstance.Namespace, rayClusterInstance.Name)
	return rayClusterInstance, nil
}

// rayClusterSpecForHashing returns a copy of the RayService's RayClusterSpec
// to use for hash comparisons. Fields that should not trigger reconciliation
// of the underlying RayCluster are cleared here. Suspend is excluded because
// the RayService controller propagates Suspend to the RayCluster only at
// creation time; once the RayCluster exists, Suspend is delegated to Kueue
// (or whichever external controller owns the RayCluster's queueing), and
// later changes to RayService.Spec.RayClusterSpec.Suspend must not trigger
// an in-place update or a new cluster preparation.
func rayClusterSpecForHashing(rayService *rayv1.RayService) *rayv1.RayClusterSpec {
	spec := rayService.Spec.RayClusterSpec.DeepCopy()
	spec.Suspend = nil
	return spec
}

func constructRayClusterForRayService(rayService *rayv1.RayService, rayClusterName string, scheme *runtime.Scheme) (*rayv1.RayCluster, error) {
	var err error
	rayClusterLabel := make(map[string]string)
	maps.Copy(rayClusterLabel, rayService.Labels)
	rayClusterLabel[utils.RayOriginatedFromCRNameLabelKey] = rayService.Name
	rayClusterLabel[utils.RayOriginatedFromCRDLabelKey] = utils.RayOriginatedFromCRDLabelValue(utils.RayServiceCRD)

	rayClusterAnnotations := make(map[string]string)
	maps.Copy(rayClusterAnnotations, rayService.Annotations)
	rayClusterAnnotations[utils.HashWithoutReplicasAndWorkersToDeleteKey], err = utils.GenerateHashWithoutReplicasAndWorkersToDelete(*rayClusterSpecForHashing(rayService))
	if err != nil {
		return nil, err
	}
	rayClusterAnnotations[utils.NumWorkerGroupsKey] = strconv.Itoa(len(rayService.Spec.RayClusterSpec.WorkerGroupSpecs))

	// set the KubeRay version used to create the RayCluster
	rayClusterAnnotations[utils.KubeRayVersion] = utils.KUBERAY_VERSION

	clusterSpec := rayService.Spec.RayClusterSpec.DeepCopy()
	isPendingClusterForUpgrade := utils.IsIncrementalUpgradeEnabled(&rayService.Spec) &&
		rayService.Status.ActiveServiceStatus.RayClusterName != ""
	if isPendingClusterForUpgrade {
		// For incremental upgrade, start the pending cluster without a replicas value so
		// that it autoscales based on the value of target_capacity from MinReplicas.
		for i := range clusterSpec.WorkerGroupSpecs {
			clusterSpec.WorkerGroupSpecs[i].Replicas = nil
		}
	}

	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      rayClusterLabel,
			Annotations: rayClusterAnnotations,
			Name:        rayClusterName,
			Namespace:   rayService.Namespace,
		},
		Spec: *clusterSpec,
	}

	// Set the ownership in order to do the garbage collection by k8s.
	if err := ctrl.SetControllerReference(rayService, rayCluster, scheme); err != nil {
		return nil, err
	}

	return rayCluster, nil
}

func checkIfNeedSubmitServeApplications(cachedServeConfigV2 string, serveConfigV2 string, serveApplications map[string]rayv1.AppStatus) (bool, string) {
	// If the Serve config has not been cached, update the Serve config.
	if cachedServeConfigV2 == "" {
		return true, "Nothing has been cached for the cluster."
	}

	// Handle the case that the head Pod has crashed and GCS FT is not enabled.
	if len(serveApplications) == 0 {
		reason := "No Serve application found in the RayCluster. " +
			"A possible reason is that the head Pod crashed and GCS FT was not enabled."
		return true, reason
	}

	// If the Serve config has been cached, check if it needs to be updated.
	if cachedServeConfigV2 != serveConfigV2 {
		return true, "Current V2 Serve config doesn't match cached Serve config."
	}

	return false, "Current V2 Serve config matches cached Serve config."
}

func (r *RayServiceReconciler) updateServeDeployment(ctx context.Context, rayServiceInstance *rayv1.RayService, rayDashboardClient dashboardclient.RayDashboardClientInterface, clusterName string) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("updateServeDeployment", "V2 config", rayServiceInstance.Spec.ServeConfigV2)

	serveConfig := make(map[string]any)
	if err := yaml.Unmarshal([]byte(rayServiceInstance.Spec.ServeConfigV2), &serveConfig); err != nil {
		return err
	}

	if utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) {
		// For incremental upgrades, set target_capacity if specified to avoid
		// scaling initial Serve deployment to 100% immediately.
		var targetCapacity *int32
		activeStatus := rayServiceInstance.Status.ActiveServiceStatus
		pendingStatus := rayServiceInstance.Status.PendingServiceStatus

		if clusterName == activeStatus.RayClusterName && activeStatus.TargetCapacity != nil {
			targetCapacity = activeStatus.TargetCapacity
		} else if clusterName == pendingStatus.RayClusterName && pendingStatus.TargetCapacity != nil {
			targetCapacity = pendingStatus.TargetCapacity
		}
		if targetCapacity != nil {
			logger.Info("Setting target_capacity from status in Serve config.", "target_capacity", *targetCapacity)
			serveConfig["target_capacity"] = *targetCapacity
		}
	}

	configJson, err := json.Marshal(serveConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal converted serve config into bytes: %w", err)
	}
	logger.Info("updateServeDeployment", "MULTI_APP json config", string(configJson))
	if err := rayDashboardClient.UpdateDeployments(ctx, configJson); err != nil {
		err = fmt.Errorf(
			"fail to create / update Serve applications. If you observe this error consistently, "+
				"please check \"Issue 5: Fail to create / update Serve applications.\" in "+
				"https://docs.ray.io/en/master/cluster/kubernetes/troubleshooting/rayservice-troubleshooting.html#kuberay-raysvc-troubleshoot for more details. "+
				"err: %v", err)
		return err
	}

	r.cacheServeConfig(rayServiceInstance, clusterName)
	logger.Info("updateServeDeployment", "message", "Cached Serve config for Ray cluster with the key", "rayClusterName", clusterName)
	return nil
}

// checkIfNeedTargetCapacityUpdate returns whether the controller should adjust the target_capacity
// of the Serve config associated with a RayCluster during NewClusterWithIncrementalUpgrade.
//
// This function implements the incremental upgrade state machine as defined in the design document:
// https://github.com/ray-project/enhancements/blob/main/reps/2024-12-4-ray-service-incr-upgrade.md
//
// The upgrade process follows these phases:
// 1. Phase 1 (Steps 7-8): New cluster scales up to target capacity
//   - pendingTargetCapacity: 0% → 100%
//   - Returns true: "Pending RayCluster has not finished scaling up."
//
// 2. Phase 2 (Step 9): Traffic gradually migrates to new cluster
//   - pendingTrafficRoutedPercent: 0% → 100%
//   - Returns true: "Pending RayCluster has not finished scaling up."
//
// 3. Phase 3 (Step 10): Old cluster scales down after new cluster is ready
//   - activeTargetCapacity: 100% → 0%
//   - Returns true: "Active RayCluster TargetCapacity has not finished scaling down."
//
// 4. Phase 4 (Step 11): Upgrade completion
//   - Both clusters reach final state: active=0%, pending=100%
//   - Returns false: "All traffic has migrated to the upgraded cluster and NewClusterWithIncrementalUpgrade migration
//     is complete."
//
// The function ensures that traffic migration only proceeds when the target cluster has reached
// its capacity limit, preventing resource conflicts and ensuring upgrade stability.
func (r *RayServiceReconciler) checkIfNeedTargetCapacityUpdate(ctx context.Context, rayServiceInstance *rayv1.RayService) (bool, string) {
	activeRayServiceStatus := rayServiceInstance.Status.ActiveServiceStatus
	pendingRayServiceStatus := rayServiceInstance.Status.PendingServiceStatus

	if activeRayServiceStatus.RayClusterName == "" || pendingRayServiceStatus.RayClusterName == "" {
		return false, "Both active and pending RayCluster instances are required for NewClusterWithIncrementalUpgrade."
	}

	// Validate Gateway and HTTPRoute objects are ready
	gatewayInstance := &gwv1.Gateway{}
	if err := r.Get(ctx, common.RayServiceGatewayNamespacedName(rayServiceInstance), gatewayInstance); err != nil {
		return false, fmt.Sprintf("Failed to retrieve Gateway for RayService: %v", err)
	}
	if !utils.IsGatewayReady(gatewayInstance) {
		return false, "Gateway for RayService NewClusterWithIncrementalUpgrade is not ready."
	}

	httpRouteInstance := &gwv1.HTTPRoute{}
	if err := r.Get(ctx, common.RayServiceHTTPRouteNamespacedName(rayServiceInstance), httpRouteInstance); err != nil {
		return false, fmt.Sprintf("Failed to retrieve HTTPRoute for RayService: %v", err)
	}
	if !utils.IsHTTPRouteReady(gatewayInstance, httpRouteInstance) {
		return false, "HTTPRoute for RayService NewClusterWithIncrementalUpgrade is not ready."
	}

	// Retrieve the current observed NewClusterWithIncrementalUpgrade Status fields for each RayService.
	if activeRayServiceStatus.TargetCapacity == nil || activeRayServiceStatus.TrafficRoutedPercent == nil {
		return true, "Active RayServiceStatus missing TargetCapacity or TrafficRoutedPercent."
	}
	if pendingRayServiceStatus.TargetCapacity == nil || pendingRayServiceStatus.TrafficRoutedPercent == nil {
		return true, "Pending RayServiceStatus missing TargetCapacity or TrafficRoutedPercent."
	}
	activeTargetCapacity := int(*activeRayServiceStatus.TargetCapacity)
	pendingTargetCapacity := int(*pendingRayServiceStatus.TargetCapacity)
	pendingTrafficRoutedPercent := int(*pendingRayServiceStatus.TrafficRoutedPercent)

	// During rollback, the goal is activeTargetCapacity → 100, pendingTargetCapacity → 0.
	if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress)) {
		if activeTargetCapacity == 100 && pendingTargetCapacity == 0 {
			return false, "Rollback complete: all traffic has been restored to the active cluster."
		}
		return true, "Rollback in progress: traffic is being restored to the active cluster."
	}

	if activeTargetCapacity == 0 && pendingTargetCapacity == 100 {
		return false, "All traffic has migrated to the upgraded cluster and NewClusterWithIncrementalUpgrade is complete."
	} else if pendingTargetCapacity < 100 || pendingTrafficRoutedPercent < 100 {
		return true, "Pending RayCluster has not finished scaling up."
	}
	return true, "Active RayCluster TargetCapacity has not finished scaling down."
}

// applyServeTargetCapacity updates the target_capacity for a given RayCluster's Serve applications.
func (r *RayServiceReconciler) applyServeTargetCapacity(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster, rayDashboardClient dashboardclient.RayDashboardClientInterface, goalTargetCapacity int32) error {
	logger := ctrl.LoggerFrom(ctx).WithValues("RayCluster", rayClusterInstance.Name)

	// Retrieve cached ServeConfig from last reconciliation for cluster to update
	cachedConfig := r.getServeConfigFromCache(rayServiceInstance, rayClusterInstance.Name)
	if cachedConfig == "" {
		cachedConfig = rayServiceInstance.Spec.ServeConfigV2
	}

	serveConfig := make(map[string]any)
	if err := yaml.Unmarshal([]byte(cachedConfig), &serveConfig); err != nil {
		return err
	}

	// Check if ServeConfig requires update
	if currentTargetCapacity, ok := serveConfig["target_capacity"].(float64); ok {
		if int32(currentTargetCapacity) == goalTargetCapacity {
			logger.Info("target_capacity already updated on RayCluster", "target_capacity", currentTargetCapacity)
			// No update required, return early
			return nil
		}
	}

	serveConfig["target_capacity"] = goalTargetCapacity
	configJson, err := json.Marshal(serveConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal serve config: %w", err)
	}

	logger.Info("Applying new target_capacity to Ray cluster.", "goal", goalTargetCapacity)
	if err := rayDashboardClient.UpdateDeployments(ctx, configJson); err != nil {
		err = fmt.Errorf(
			"fail to create / update Serve applications. If you observe this error consistently, "+
				"please check \"Issue 5: Fail to create / update Serve applications.\" in "+
				"https://docs.ray.io/en/master/cluster/kubernetes/troubleshooting/rayservice-troubleshooting.html#kuberay-raysvc-troubleshoot for more details. "+
				"err: %v", err)
		return err
	}

	// Update the TargetCapacity status fields.
	switch rayClusterInstance.Name {
	case rayServiceInstance.Status.ActiveServiceStatus.RayClusterName:
		rayServiceInstance.Status.ActiveServiceStatus.TargetCapacity = new(goalTargetCapacity)
	case rayServiceInstance.Status.PendingServiceStatus.RayClusterName:
		rayServiceInstance.Status.PendingServiceStatus.TargetCapacity = new(goalTargetCapacity)
	}

	return nil
}

// reconcileServeTargetCapacity reconciles the target_capacity of the ServeConfig for a given RayCluster during
// a NewClusterWithIncrementalUpgrade while also updating the Status.TargetCapacity of the Active and Pending RayServices.
func (r *RayServiceReconciler) reconcileServeTargetCapacity(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster, rayDashboardClient dashboardclient.RayDashboardClientInterface) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("reconcileServeTargetCapacity", "RayService", rayServiceInstance.Name)

	activeRayServiceStatus := &rayServiceInstance.Status.ActiveServiceStatus
	pendingRayServiceStatus := &rayServiceInstance.Status.PendingServiceStatus

	// Set initial TargetCapacity values if unset
	if activeRayServiceStatus.TargetCapacity == nil {
		activeRayServiceStatus.TargetCapacity = new(int32(100))
	}
	if pendingRayServiceStatus.TargetCapacity == nil {
		pendingRayServiceStatus.TargetCapacity = new(int32(0))
	}

	// Retrieve the current observed Status fields for NewClusterWithIncrementalUpgrade
	activeTargetCapacity := *activeRayServiceStatus.TargetCapacity
	pendingTargetCapacity := *pendingRayServiceStatus.TargetCapacity
	pendingTrafficRoutedPercent := ptr.Deref(pendingRayServiceStatus.TrafficRoutedPercent, 0)

	// Retrieve MaxSurgePercent - the maximum amount to change TargetCapacity by
	options := utils.GetRayServiceClusterUpgradeOptions(&rayServiceInstance.Spec)
	if options == nil {
		return errstd.New("Missing RayService ClusterUpgradeOptions during upgrade")
	}
	maxSurgePercent := ptr.Deref(options.MaxSurgePercent, 100)

	if meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress)) {
		// Rollback the upgrade. The active RayCluster should be scaled back to 100% target_capacity,
		// while the pending RayCluster is scaled to 0%. This is the inverse of the regular upgrade path.
		activeTrafficRoutedPercent := ptr.Deref(activeRayServiceStatus.TrafficRoutedPercent, 0)
		if activeTargetCapacity != activeTrafficRoutedPercent {
			logger.Info("Traffic is rolling back to active cluster, deferring capacity update.", "ActiveTargetCapacity", activeTargetCapacity, "ActiveTrafficRoutedPercent", activeTrafficRoutedPercent)
			return nil
		}

		if activeTargetCapacity+pendingTargetCapacity > 100 {
			if rayClusterInstance.Name == pendingRayServiceStatus.RayClusterName {
				goalTargetCapacity := max(int32(0), pendingTargetCapacity-maxSurgePercent)
				logger.Info("Rollback: Scaling down pending cluster `target_capacity`.", "goal", goalTargetCapacity)
				return r.applyServeTargetCapacity(ctx, rayServiceInstance, rayClusterInstance, rayDashboardClient, goalTargetCapacity)
			}
		} else {
			if rayClusterInstance.Name == activeRayServiceStatus.RayClusterName {
				goalTargetCapacity := min(int32(100), activeTargetCapacity+maxSurgePercent)
				logger.Info("Rollback: Scaling up active cluster `target_capacity`.", "goal", goalTargetCapacity)
				return r.applyServeTargetCapacity(ctx, rayServiceInstance, rayClusterInstance, rayDashboardClient, goalTargetCapacity)
			}
		}
		return nil
	}

	// Defer updating the target_capacity until traffic weights are updated
	if pendingTargetCapacity != pendingTrafficRoutedPercent {
		logger.Info("Traffic is currently being migrated to pending cluster", "RayCluster", pendingRayServiceStatus.RayClusterName, "TargetCapacity", pendingTargetCapacity, "TrafficRoutedPercent", pendingTrafficRoutedPercent)
		return nil
	}

	// There are two cases:
	// 1. The total target_capacity is greater than 100. This means the pending RayCluster has
	// scaled up traffic and the active RayCluster can be scaled down by MaxSurgePercent.
	// 2. The total target_capacity is equal to 100. This means the pending RayCluster can
	// increase its target_capacity by MaxSurgePercent.
	// If the rayClusterInstance passed into this function is not the cluster to update based
	// on the above conditions, we return without doing anything.
	var goalTargetCapacity int32
	shouldUpdate := false
	switch rayClusterInstance.Name {
	case activeRayServiceStatus.RayClusterName:
		if activeTargetCapacity+pendingTargetCapacity > 100 {
			// Scale down the Active RayCluster TargetCapacity on this iteration.
			goalTargetCapacity = max(int32(0), activeTargetCapacity-maxSurgePercent)
			shouldUpdate = true
			logger.Info("Setting target_capacity for active Raycluster", "Raycluster", rayClusterInstance.Name, "target_capacity", goalTargetCapacity)
		}
	case pendingRayServiceStatus.RayClusterName:
		if activeTargetCapacity+pendingTargetCapacity <= 100 {
			// Scale up the Pending RayCluster TargetCapacity on this iteration.
			goalTargetCapacity = min(int32(100), pendingTargetCapacity+maxSurgePercent)
			shouldUpdate = true
			logger.Info("Setting target_capacity for pending Raycluster", "Raycluster", rayClusterInstance.Name, "target_capacity", goalTargetCapacity)
		}
	}

	if !shouldUpdate {
		return nil
	}

	return r.applyServeTargetCapacity(ctx, rayServiceInstance, rayClusterInstance, rayDashboardClient, goalTargetCapacity)
}

// `getAndCheckServeStatus` gets Serve applications' and deployments' statuses and check whether the
// Serve applications are ready to serve incoming traffic or not. It returns three values:
//
// (1) `isReady`: Whether the Serve applications are ready to serve incoming traffic or not.
// (2) `newApplications`: The Serve applications' statuses.
// (3) `err`: If `err` is not nil, it means that KubeRay failed to get Serve application statuses from the dashboard.
func getAndCheckServeStatus(ctx context.Context, dashboardClient dashboardclient.RayDashboardClientInterface) (bool, map[string]rayv1.AppStatus, error) {
	logger := ctrl.LoggerFrom(ctx)
	var serveAppStatuses map[string]*utiltypes.ServeApplicationStatus
	var err error
	if serveAppStatuses, err = dashboardClient.GetMultiApplicationStatus(ctx); err != nil {
		err = fmt.Errorf(
			"failed to get Serve application statuses from the dashboard. "+
				"If you observe this error consistently, please check https://docs.ray.io/en/latest/cluster/kubernetes/troubleshooting/rayservice-troubleshooting.html for more details. "+
				"err: %v", err)
		return false, nil, err
	}

	isReady := true

	newApplications := make(map[string]rayv1.AppStatus)
	for appName, app := range serveAppStatuses {
		if appName == "" {
			appName = utils.DefaultServeAppName
		}

		applicationStatus := rayv1.AppStatus{
			Message:     app.Message,
			Status:      app.Status,
			Deployments: make(map[string]rayv1.ServeDeploymentStatus),
		}

		// `isReady` is used to determine whether the Serve application is ready or not. The cluster switchover only happens when all Serve
		// applications in this RayCluster are ready so that the incoming traffic will not be dropped.
		if app.Status != rayv1.ApplicationStatusEnum.RUNNING {
			isReady = false
		}

		// Copy deployment statuses
		for deploymentName, deployment := range app.Deployments {
			deploymentStatus := rayv1.ServeDeploymentStatus{
				Status:  deployment.Status,
				Message: deployment.Message,
			}
			applicationStatus.Deployments[deploymentName] = deploymentStatus
		}
		newApplications[appName] = applicationStatus
	}

	if len(newApplications) == 0 {
		logger.Info("No Serve application found. The RayCluster is not ready to serve requests. Set 'isReady' to false")
		isReady = false
	}
	return isReady, newApplications, nil
}

func (r *RayServiceReconciler) getServeConfigFromCache(rayServiceInstance *rayv1.RayService, clusterName string) string {
	cacheKey := rayServiceInstance.Namespace + "/" + rayServiceInstance.Name
	cacheValue, exist := r.ServeConfigs.Get(cacheKey)
	if !exist {
		return ""
	}
	serveConfigs := cacheValue.(cmap.ConcurrentMap[string, string])
	serveConfig, exist := serveConfigs.Get(clusterName)
	if !exist {
		return ""
	}
	return serveConfig
}

func (r *RayServiceReconciler) cacheServeConfig(rayServiceInstance *rayv1.RayService, clusterName string) {
	serveConfig := rayServiceInstance.Spec.ServeConfigV2
	if serveConfig == "" {
		return
	}
	cacheKey := rayServiceInstance.Namespace + "/" + rayServiceInstance.Name
	cacheValue, exist := r.ServeConfigs.Get(cacheKey)
	var rayServiceServeConfigs cmap.ConcurrentMap[string, string]
	if !exist {
		rayServiceServeConfigs = cmap.New[string]()
		r.ServeConfigs.Add(cacheKey, rayServiceServeConfigs)
	} else {
		rayServiceServeConfigs = cacheValue.(cmap.ConcurrentMap[string, string])
	}
	rayServiceServeConfigs.Set(clusterName, serveConfig)
}

func (r *RayServiceReconciler) reconcileServices(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster, serviceType utils.ServiceType) (*corev1.Service, error) {
	logger := ctrl.LoggerFrom(ctx)

	var newSvc *corev1.Service
	var err error

	switch serviceType {
	case utils.HeadService:
		newSvc, err = common.BuildHeadServiceForRayService(ctx, *rayServiceInstance, *rayClusterInstance)
	case utils.ServingService:
		newSvc, err = common.BuildServeServiceForRayService(ctx, *rayServiceInstance, *rayClusterInstance)
	default:
		panic(fmt.Sprintf("unknown service type %v. This should never happen. Please open an issue in the KubeRay repository.", serviceType))
	}

	if err != nil {
		return nil, err
	}

	// Retrieve the Service from the Kubernetes cluster with the name and namespace.
	oldSvc := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{Name: newSvc.Name, Namespace: rayServiceInstance.Namespace}, oldSvc)

	if err == nil {
		// Only update the service if the RayCluster switches.
		if newSvc.Spec.Selector[utils.RayClusterLabelKey] == oldSvc.Spec.Selector[utils.RayClusterLabelKey] {
			logger.Info("Service already exists in the RayCluster, skipping Update", "rayCluster", newSvc.Spec.Selector[utils.RayClusterLabelKey], "serviceType", serviceType)
			return oldSvc, nil
		}

		// ClusterIP is immutable. Starting from Kubernetes v1.21.5, if the new service does not specify a ClusterIP,
		// Kubernetes will assign the ClusterIP of the old service to the new one. However, to maintain compatibility
		// with older versions of Kubernetes, we need to assign the ClusterIP here.
		newSvc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		oldSvc.Spec = *newSvc.Spec.DeepCopy()
		logger.Info("Update Kubernetes Service", "serviceType", serviceType)
		if updateErr := r.Update(ctx, oldSvc); updateErr != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateService), string(utils.UpdateAction), "Failed to update the service %s/%s, %v", oldSvc.Namespace, oldSvc.Name, updateErr)
			return nil, updateErr
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedService), string(utils.UpdateAction), "Updated the service %s/%s", oldSvc.Namespace, oldSvc.Name)
		// Return the updated service.
		return oldSvc, nil
	} else if errors.IsNotFound(err) {
		logger.Info("Create a Kubernetes Service", "serviceType", serviceType)
		if err := ctrl.SetControllerReference(rayServiceInstance, newSvc, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, newSvc); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateService), string(utils.CreateAction), "Failed to create the service %s/%s, %v", newSvc.Namespace, newSvc.Name, err)
			return nil, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.CreatedService), string(utils.CreateAction), "Created the service %s/%s", newSvc.Namespace, newSvc.Name)
		return newSvc, nil
	}
	return nil, err
}

// Reconciles the Serve applications on the RayCluster. Returns (isReady, serveApplicationStatus, error).
// The `isReady` flag indicates whether the RayCluster is ready to handle incoming traffic.
func (r *RayServiceReconciler) reconcileServe(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster) (bool, map[string]rayv1.AppStatus, error) {
	logger := ctrl.LoggerFrom(ctx)
	var err error
	var serveApplications map[string]rayv1.AppStatus
	// Check if head pod is running and ready. If not, requeue the resource event to avoid
	// redundant custom resource status updates.
	//
	// TODO (kevin85421): Note that the Dashboard and GCS may take a few seconds to start up
	// after the head pod is running and ready. Hence, some requests to the Dashboard (e.g. `UpdateDeployments`) may fail.
	// This is not an issue since `UpdateDeployments` is an idempotent operation.
	if features.Enabled(features.RayClusterStatusConditions) {
		if !meta.IsStatusConditionTrue(rayClusterInstance.Status.Conditions, string(rayv1.HeadPodReady)) {
			logger.Info("The head Pod is not ready, requeue the resource event to avoid redundant custom resource status updates.")
			return false, serveApplications, nil
		}
	} else {
		if isRunningAndReady, err := r.isHeadPodRunningAndReady(ctx, rayClusterInstance); err != nil || !isRunningAndReady {
			if err != nil {
				logger.Error(err, "Failed to check if head Pod is running and ready!")
			} else {
				logger.Info("Skipping the update of Serve applications because the Ray head Pod is not ready.")
			}
			return false, serveApplications, nil
		}
	}

	var clientURL string
	if clientURL, err = utils.FetchHeadServiceURL(ctx, r.Client, rayClusterInstance, utils.DashboardPortName); err != nil || clientURL == "" {
		return false, serveApplications, err
	}

	rayDashboardClient, err := r.dashboardClientFunc(rayClusterInstance, clientURL)
	if err != nil {
		return false, serveApplications, err
	}

	skipConfigUpdate := false
	isActiveCluster := rayClusterInstance.Name == rayServiceInstance.Status.ActiveServiceStatus.RayClusterName
	isIncrementalUpgradeInProgress := utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) &&
		meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.UpgradeInProgress))
	isRollbackInProgress := utils.IsIncrementalUpgradeEnabled(&rayServiceInstance.Spec) &&
		meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))

	if isActiveCluster && isIncrementalUpgradeInProgress && !isRollbackInProgress {
		// Skip updating the Serve config for the Active cluster during NewClusterWithIncrementalUpgrade. The updated
		// Serve config is applied to the pending RayService's RayCluster.
		skipConfigUpdate = true
		logger.Info("Blocking new Serve config submission for Active cluster during upgrade.", "clusterName", rayClusterInstance.Name)
	} else if !isActiveCluster && isRollbackInProgress {
		// During rollback, the pending cluster is being scaled down and phased out.
		// We skip updating its Serve config to prevent unnecessary disruptive redeployments,
		// since its only goal is to drain traffic while the active cluster takes over.
		skipConfigUpdate = true
		logger.Info("Blocking new Serve config submission for Pending cluster during rollback.", "clusterName", rayClusterInstance.Name)
	}

	cachedServeConfigV2 := r.getServeConfigFromCache(rayServiceInstance, rayClusterInstance.Name)
	isReady, serveApplications, err := getAndCheckServeStatus(ctx, rayDashboardClient)
	if err != nil {
		return false, serveApplications, err
	}
	shouldUpdate, reason := checkIfNeedSubmitServeApplications(cachedServeConfigV2, rayServiceInstance.Spec.ServeConfigV2, serveApplications)
	logger.Info("checkIfNeedSubmitServeApplications", "shouldUpdate", shouldUpdate, "reason", reason)

	if shouldUpdate && !skipConfigUpdate {
		if err = r.updateServeDeployment(ctx, rayServiceInstance, rayDashboardClient, rayClusterInstance.Name); err != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateServeApplications), string(utils.UpdateAction), "Failed to update serve applications to the RayCluster %s/%s: %v", rayClusterInstance.Namespace, rayClusterInstance.Name, err)
			return false, serveApplications, err
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedServeApplications), string(utils.UpdateAction), "Updated serve applications to the RayCluster %s/%s", rayClusterInstance.Namespace, rayClusterInstance.Name)
	}
	if isIncrementalUpgradeInProgress {
		incrementalUpgradeUpdate, reason := r.checkIfNeedTargetCapacityUpdate(ctx, rayServiceInstance)
		logger.Info("checkIfNeedTargetCapacityUpdate", "incrementalUpgradeUpdate", incrementalUpgradeUpdate, "reason", reason)
		if incrementalUpgradeUpdate {
			if err := r.reconcileServeTargetCapacity(ctx, rayServiceInstance, rayClusterInstance, rayDashboardClient); err != nil {
				r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateTargetCapacity), string(utils.UpdateAction), "Failed to update target_capacity of serve applications to the RayCluster %s/%s: %v", rayClusterInstance.Namespace, rayClusterInstance.Name, err)
				return false, serveApplications, err
			}
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedServeTargetCapacity), string(utils.UpdateAction),
				"Updated target_capacity of serve applications to to the RayCluster %s/%s", rayClusterInstance.Namespace, rayClusterInstance.Name)
		}
	}

	return isReady, serveApplications, nil
}

func (r *RayServiceReconciler) updateHeadPodServeLabel(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster, excludeHeadPodFromServeSvc bool) error {
	// `updateHeadPodServeLabel` updates the head Pod's serve label based on the health status of the proxy actor.
	// If `excludeHeadPodFromServeSvc` is true, the head Pod will not be used to serve requests, regardless of proxy actor health.
	// If `excludeHeadPodFromServeSvc` is false, the head Pod's serve label will be set based on the health check result.
	// The label is used by the Kubernetes serve service to determine whether to include the head Pod in the service endpoints.
	if rayClusterInstance == nil {
		return nil
	}

	headPod, err := common.GetRayClusterHeadPod(ctx, r, rayClusterInstance)
	if err != nil {
		return err
	}
	if headPod == nil {
		return fmt.Errorf("found 0 head. cluster name %s, namespace %v", rayClusterInstance.Name, rayClusterInstance.Namespace)
	}

	rayContainer := headPod.Spec.Containers[utils.RayContainerIndex]
	servingPort := int(utils.FindContainerPort(&rayContainer, utils.ServingPortName, utils.DefaultServingPort))

	client := r.httpProxyClientFunc(headPod.Status.PodIP, headPod.Namespace, headPod.Name, servingPort)
	if headPod.Labels == nil {
		headPod.Labels = make(map[string]string)
	}
	oldLabel := headPod.Labels[utils.RayClusterServingServiceLabelKey]
	newLabel := utils.EnableRayClusterServingServiceFalse

	// If excludeHeadPodFromServeSvc is true, head Pod will not be used to serve requests
	// no matter whether the proxy actor is healthy or not. Therefore, only send the health
	// check request if excludeHeadPodFromServeSvc is false.
	if !excludeHeadPodFromServeSvc {
		isHealthy := client.CheckProxyActorHealth(ctx) == nil
		newLabel = strconv.FormatBool(isHealthy)
	}

	if oldLabel != newLabel {
		headPod.Labels[utils.RayClusterServingServiceLabelKey] = newLabel
		if updateErr := r.Update(ctx, headPod); updateErr != nil {
			r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateHeadPodServeLabel), string(utils.UpdateAction), "Failed to update the serve label to %q for the Head Pod %s/%s: %v", newLabel, headPod.Namespace, headPod.Name, updateErr)
			return updateErr
		}
		r.Recorder.Eventf(rayServiceInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedHeadPodServeLabel), string(utils.UpdateAction), "Updated the serve label to %q for the Head Pod %s/%s", newLabel, headPod.Namespace, headPod.Name)
	}

	return nil
}

// calculateNumServeEndpointsFromSlices calculates the number of unique ready Pods
// from EndpointSlices associated with a given service.
//
// In dual-stack environments (IPv4 + IPv6), the same Pod may appear in multiple
// EndpointSlices with different IP addresses. This function deduplicates by TargetRef.UID
// to ensure each Pod is counted only once, regardless of how many IP families it serves.
//
// An endpoint is considered ready if it has the `Ready` condition set to true.
// This replaces the legacy Endpoints API approach.
func (r *RayServiceReconciler) calculateNumServeEndpointsFromSlices(ctx context.Context, serviceNamespacedName client.ObjectKey) (int, error) {
	logger := ctrl.LoggerFrom(ctx)

	// List all EndpointSlices for the service.
	// EndpointSlices are automatically created by Kubernetes and labeled with
	// kubernetes.io/service-name to associate them with their parent Service.
	endpointSliceList := &discoveryv1.EndpointSliceList{}
	listOpts := []client.ListOption{
		client.InNamespace(serviceNamespacedName.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: serviceNamespacedName.Name},
	}

	if err := r.List(ctx, endpointSliceList, listOpts...); err != nil {
		logger.Error(err, "Failed to list EndpointSlices", "serviceName", serviceNamespacedName.Name, "serviceNamespace", serviceNamespacedName.Namespace)
		return 0, err
	}

	uniqueNumReadyPods := make(map[types.UID]struct{})

	for _, endpointSlice := range endpointSliceList.Items {
		for _, endpoint := range endpointSlice.Endpoints {
			// Only count endpoints that are ready to serve traffic
			if ptr.Deref(endpoint.Conditions.Ready, false) {
				if endpoint.TargetRef != nil && endpoint.TargetRef.UID != "" {
					uniqueNumReadyPods[endpoint.TargetRef.UID] = struct{}{}
				}
			}
		}
	}

	numPods := len(uniqueNumReadyPods)

	logger.V(1).Info("Counted serve-ready pods via EndpointSlices",
		"serviceName", serviceNamespacedName.Name,
		"serviceNamespace", serviceNamespacedName.Namespace,
		"numSlices", len(endpointSliceList.Items),
		"numReadyPods", numPods,
	)

	return numPods, nil
}

// isHeadPodRunningAndReady checks if the head pod of the RayCluster is running and ready.
func (r *RayServiceReconciler) isHeadPodRunningAndReady(ctx context.Context, instance *rayv1.RayCluster) (bool, error) {
	headPod, err := common.GetRayClusterHeadPod(ctx, r, instance)
	if err != nil {
		return false, err
	}
	if headPod == nil {
		return false, fmt.Errorf("found 0 head. cluster name %s, namespace %v", instance.Name, instance.Namespace)
	}
	return utils.IsRunningAndReady(headPod), nil
}

// getInitializingTimeout parses the initializing timeout annotation from RayService.
// Returns (timeout, true) if valid. Accepts Go duration format (e.g., "5m", "1h") or integer seconds.
// The annotation is assumed to be already validated by ValidateRayServiceMetadata.
// If the annotation is absent, returns (0, false).
func getInitializingTimeout(rs *rayv1.RayService) (time.Duration, bool) {
	if rs.Annotations == nil {
		return 0, false
	}

	timeoutStr, exists := rs.Annotations[utils.RayServiceInitializingTimeoutAnnotation]
	if !exists || timeoutStr == "" {
		return 0, false
	}

	// Try parsing as Go duration first (e.g., "30m", "1h")
	// Validation already ensures this is valid, so we can ignore errors
	if timeout, err := time.ParseDuration(timeoutStr); err == nil {
		return timeout, true
	}

	// Try parsing as integer seconds
	// Validation already ensures this is valid if ParseDuration failed
	if seconds, err := strconv.Atoi(timeoutStr); err == nil {
		return time.Duration(seconds) * time.Second, true
	}

	// This should never happen since validation ensures correctness,
	// but we handle it gracefully by returning false
	return 0, false
}

// isInitializingTimeout returns true if RayServiceReady is False with Reason=InitializingTimeout.
// Once a RayService has timed out, it remains in a terminal failure state regardless of generation changes.
func isInitializingTimeout(rs *rayv1.RayService) bool {
	readyCond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
	if readyCond == nil {
		return false
	}

	return readyCond.Status == metav1.ConditionFalse &&
		readyCond.Reason == string(rayv1.RayServiceInitializingTimeout)
}

// markFailedOnInitializingTimeout checks if the RayService has been initializing for too long.
// If timeout is configured and exceeded, it marks the service as failed and triggers cleanup.
func markFailedOnInitializingTimeout(ctx context.Context, r *RayServiceReconciler, rs *rayv1.RayService) {
	logger := ctrl.LoggerFrom(ctx)

	// Skip if no timeout is configured
	timeout, ok := getInitializingTimeout(rs)
	if !ok {
		return
	}

	// Check if currently in Initializing state
	readyCond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
	if readyCond == nil {
		return
	}

	if readyCond.Status != metav1.ConditionFalse || readyCond.Reason != string(rayv1.RayServiceInitializing) {
		// Not in Initializing state
		return
	}

	// Check if timeout has been exceeded
	timeInInitializing := time.Since(readyCond.LastTransitionTime.Time)
	if timeInInitializing < timeout {
		// Still within timeout
		return
	}

	// Timeout exceeded - mark as failed
	logger.Info("RayService initializing timeout exceeded",
		"timeout", timeout,
		"timeInInitializing", timeInInitializing,
		"generation", rs.Generation)

	// Clear cluster names to trigger cleanup
	rs.Status.ActiveServiceStatus.RayClusterName = ""
	rs.Status.PendingServiceStatus.RayClusterName = ""

	// Set condition to Failed with InitializingTimeout reason
	message := fmt.Sprintf("RayService failed to become ready within the configured timeout of %s. Time spent initializing: %s",
		timeout, timeInInitializing)
	setCondition(rs, rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceInitializingTimeout, message)

	// Emit warning event
	r.Recorder.Eventf(rs, nil, corev1.EventTypeWarning, string(utils.RayServiceInitializingTimeout), string(utils.ReconcileAction),
		"RayService initializing timeout exceeded after %s (configured timeout: %s)",
		timeInInitializing, timeout)
}

// reconcilePerClusterServeService reconciles a load-balancing serve service for a given RayCluster.
func (r *RayServiceReconciler) reconcilePerClusterServeService(ctx context.Context, rayServiceInstance *rayv1.RayService, rayClusterInstance *rayv1.RayCluster) error {
	if rayClusterInstance == nil {
		return nil
	}

	logger := ctrl.LoggerFrom(ctx).WithValues("RayCluster", rayClusterInstance.Name)

	logger.Info("Building per-cluster serve service")

	// Create a serve service for the RayCluster associated with this RayService. During an incremental
	// upgrade, this will be called for both the active and pending RayCluster instances.
	desiredSvc, err := common.BuildServeService(ctx, *rayServiceInstance, *rayClusterInstance, true)
	if err != nil {
		logger.Error(err, "Failed to build per-cluster serve service spec")
		return err
	}
	if err := ctrl.SetControllerReference(rayClusterInstance, desiredSvc, r.Scheme); err != nil {
		return err
	}

	existingSvc := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{Name: desiredSvc.Name, Namespace: desiredSvc.Namespace}, existingSvc)
	if errors.IsNotFound(err) {
		logger.Info("Creating new per-cluster serve service for incremental upgrade.", "Service", desiredSvc.Name)
		return r.Create(ctx, desiredSvc)
	}

	return err
}

func shouldCompleteIncrementalRollback(
	activeStatus *rayv1.RayServiceStatus,
	pendingStatus *rayv1.RayServiceStatus,
	pendingCluster *rayv1.RayCluster,
) bool {
	// Check if the pending cluster is healthy enough to receive scale-down API calls.
	// If the pending cluster is unhealthy and the active cluster has 100% of traffic
	// routed to it, we should complete the rollback.
	pendingClusterHealthy := pendingCluster != nil &&
		meta.IsStatusConditionTrue(pendingCluster.Status.Conditions, string(rayv1.HeadPodReady))

	// A rollback is complete when the active cluster is back at 100% TargetCapacity and TrafficRoutedPercent,
	// and either:
	// - The pending cluster has been fully scaled down (TargetCapacity and TrafficRoutedPercent are both 0), or
	// - The pending cluster is unhealthy (head Pod not ready)
	return ptr.Deref(activeStatus.TargetCapacity, -1) == 100 &&
		ptr.Deref(activeStatus.TrafficRoutedPercent, -1) == 100 &&
		(!pendingClusterHealthy || (ptr.Deref(pendingStatus.TargetCapacity, -1) == 0 &&
			ptr.Deref(pendingStatus.TrafficRoutedPercent, -1) == 0))
}

// reconcileRollbackState determines whether to initiate a rollback by setting the RollbackInProgress condition.
func (r *RayServiceReconciler) reconcileRollbackState(ctx context.Context, rayServiceInstance *rayv1.RayService, activeCluster, pendingCluster *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	targetHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(*rayClusterSpecForHashing(rayServiceInstance))
	if err != nil {
		return fmt.Errorf("failed to generate hash for goal cluster spec: %w", err)
	}

	originalHash := activeCluster.Annotations[utils.HashWithoutReplicasAndWorkersToDeleteKey]
	pendingHash := pendingCluster.Annotations[utils.HashWithoutReplicasAndWorkersToDeleteKey]

	isRollbackInProgress := meta.IsStatusConditionTrue(rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))

	// Case 1: The goal spec matches the original active cluster's spec.
	// The user reverted the upgrade. We must cancel the upgrade and safely roll back to the active cluster.
	if targetHash == originalHash {
		if !isRollbackInProgress {
			logger.Info("Goal state reverted to original cluster during upgrade. Initiating safe rollback.", "targetHash", targetHash, "originalHash", originalHash)
			setCondition(rayServiceInstance, rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "Goal state reverted to original cluster, rolling back.")
		}
		return nil
	}

	// Case 2: The goal spec matches the pending cluster's spec.
	// The upgrade is on track. We should revert any accidental rollback attempt and continue.
	if targetHash == pendingHash {
		if isRollbackInProgress {
			logger.Info("Goal state matches pending cluster. Canceling rollback and resuming upgrade.")
			meta.RemoveStatusCondition(&rayServiceInstance.Status.Conditions, string(rayv1.RollbackInProgress))
		}
		return nil
	}

	// Case 3: The goal spec diverges from the pending cluster.
	// The user submitted a 3rd entirely new spec mid-upgrade (targetHash != originalHash && targetHash != pendingHash).
	// The pending cluster doesn't match the new goal either, so we must first roll back
	// to the active cluster, clean up the pending cluster, and then start a fresh upgrade.
	// We must safely route all traffic back to the original cluster before
	// allowing a new cluster to be spun up.
	if !isRollbackInProgress {
		logger.Info("Goal state has changed during upgrade. Initiating safe rollback to the original cluster.", "targetHash", targetHash, "originalHash", originalHash, "pendingHash", pendingHash)
		setCondition(rayServiceInstance, rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "Goal state changed mid-upgrade, rolling back to original cluster.")
	}

	return nil
}
