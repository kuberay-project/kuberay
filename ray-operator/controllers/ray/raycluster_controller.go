package ray

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	errstd "errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/go-logr/logr"
	routev1 "github.com/openshift/api/route/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/expectations"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

type reconcileFunc func(context.Context, *rayv1.RayCluster) error

var DefaultRequeueDuration = 2 * time.Second

// NewReconciler returns a new reconcile.Reconciler
func NewReconciler(mgr manager.Manager, options RayClusterReconcilerOptions) *RayClusterReconciler {
	return &RayClusterReconciler{
		Client:                     mgr.GetClient(),
		Scheme:                     mgr.GetScheme(),
		Recorder:                   mgr.GetEventRecorder("raycluster-controller"),
		rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(mgr.GetClient()),
		options:                    options,
	}
}

// RayClusterReconciler reconciles a RayCluster object
type RayClusterReconciler struct {
	client.Client
	Scheme                     *k8sruntime.Scheme
	Recorder                   events.EventRecorder
	rayClusterScaleExpectation expectations.RayClusterScaleExpectation
	options                    RayClusterReconcilerOptions
}

type RayClusterReconcilerOptions struct {
	RayClusterMetricsManager *metrics.RayClusterMetricsManager
	BatchSchedulerManager    *batchscheduler.SchedulerManager
	HeadSidecarContainers    []corev1.Container
	WorkerSidecarContainers  []corev1.Container
	DefaultContainerEnvs     []corev1.EnvVar
	DefaultPodAnnotations    map[string]string
	DefaultPodLabels         map[string]string
	CollectorImage           string
	IsOpenShift              bool
	UseIngressOnOpenShift    bool
	CertManagerAvailable     bool
}

// Reconcile reads that state of the cluster for a RayCluster object and makes changes based on it
// and what is in the RayCluster.Spec
// Automatically generate RBAC rules to allow the Controller to read and write workloads
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/resize,verbs=patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;create;update
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingressclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=route.openshift.io,resources=routes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensions,resources=ingresses,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=get;list;watch;create;delete;update
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=get;list;watch;create;delete

// [WARNING]: There MUST be a newline after kubebuilder markers.

// Reconcile used to bridge the desired state with the current state
func (r *RayClusterReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	var err error

	// Try to fetch the RayCluster instance
	instance := &rayv1.RayCluster{}
	if err = r.Get(ctx, request.NamespacedName, instance); err == nil {
		return r.rayClusterReconcile(ctx, instance)
	}

	// No match found
	if errors.IsNotFound(err) {
		// Clear all related expectations
		r.rayClusterScaleExpectation.Delete(request.Name, request.Namespace)
		cleanUpRayClusterMetrics(r.options.RayClusterMetricsManager, request.Name, request.Namespace)
	} else {
		logger.Error(err, "Read request instance error!")
	}
	// Error reading the object - requeue the request.
	return ctrl.Result{}, client.IgnoreNotFound(err)
}

func (r *RayClusterReconciler) deleteAllPods(ctx context.Context, filters common.AssociationOptions) (pods corev1.PodList, err error) {
	logger := ctrl.LoggerFrom(ctx)
	if err = r.List(ctx, &pods, filters.ToListOptions()...); err != nil {
		return pods, err
	}
	active := 0
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp.IsZero() {
			active++
		}
	}
	if active > 0 {
		logger.Info("Deleting all Pods with labels", "filters", filters, "Number of active Pods", active)
		return pods, r.DeleteAllOf(ctx, &corev1.Pod{}, filters.ToDeleteOptions()...)
	}
	return pods, nil
}

func (r *RayClusterReconciler) rayClusterReconcile(ctx context.Context, instance *rayv1.RayCluster) (ctrl.Result, error) {
	var reconcileErr error
	logger := ctrl.LoggerFrom(ctx)

	if manager := utils.ManagedByExternalController(instance.Spec.ManagedBy); manager != nil {
		logger.Info("Skipping RayCluster managed by a custom controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	setDefaults(instance)

	if err := utils.ValidateRayClusterMetadata(instance.ObjectMeta); err != nil {
		logger.Error(err, "The RayCluster metadata is invalid")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterMetadata), string(utils.ValidateAction),
			"The RayCluster metadata is invalid %s/%s: %v", instance.Namespace, instance.Name, err)
		return ctrl.Result{}, nil
	}

	if err := utils.ValidateRayClusterSpec(&instance.Spec, instance.Annotations); err != nil {
		logger.Error(err, "The RayCluster spec is invalid")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterSpec), string(utils.ValidateAction),
			"The RayCluster spec is invalid %s/%s: %v", instance.Namespace, instance.Name, err)
		return ctrl.Result{}, nil
	}

	// Validate the worker pod restartPolicy,
	// If it is not empty or Never, it would output the warning log and the event.
	if utils.IsAutoscalingEnabled(&instance.Spec) && utils.IsAutoscalingV1Enabled(&instance.Spec) {
		var autoscalerV1GroupNames []string
		for _, workerGroup := range instance.Spec.WorkerGroupSpecs {
			if workerGroup.Template.Spec.RestartPolicy != "" && workerGroup.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
				autoscalerV1GroupNames = append(autoscalerV1GroupNames, workerGroup.GroupName)
			}
		}
		if len(autoscalerV1GroupNames) > 0 {
			msg := fmt.Sprintf("restartPolicy for worker groups [%s] should be Never or unset when using autoscaler V1", strings.Join(autoscalerV1GroupNames, ", "))
			logger.Info(msg)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterSpec), string(utils.ValidateAction),
				"RayCluster spec warning %s/%s: %s", instance.Namespace, instance.Name, msg)
		}
	}

	// Fail fast when mTLS is requested but cert-manager is not installed.
	if utils.IsTLSEnabled(&instance.Spec) && !r.options.CertManagerAvailable {
		err := fmt.Errorf("tlsOptions requires cert-manager, but cert-manager is not installed")
		logger.Error(err, "cert-manager not available for mTLS")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterSpec), string(utils.ValidateAction),
			"The RayCluster spec is invalid %s/%s: %v", instance.Namespace, instance.Name, err)
		return ctrl.Result{}, nil
	}

	if err := utils.ValidateRayClusterUpgradeOptions(instance); err != nil {
		logger.Error(err, "The RayCluster UpgradeStrategy is invalid")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterSpec), string(utils.ValidateAction),
			"The RayCluster UpgradeStrategy is invalid %s/%s: %v", instance.Namespace, instance.Name, err)
		return ctrl.Result{}, nil
	}

	if err := utils.ValidateRayClusterStatus(instance); err != nil {
		logger.Error(err, "The RayCluster status is invalid")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.InvalidRayClusterStatus), string(utils.ValidateAction),
			"The RayCluster status is invalid %s/%s, %v", instance.Namespace, instance.Name, err)
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
	}

	// Please do NOT modify `originalRayClusterInstance` in the following code.
	originalRayClusterInstance := instance.DeepCopy()

	// With idleTerminationOptions.policy=Delete, the Ray autoscaler adds the idle termination cleanup finalizer
	// before deleting an idle RayCluster, so that the operator can record why the RayCluster was deleted.
	if instance.DeletionTimestamp != nil && !instance.DeletionTimestamp.IsZero() &&
		r.hasIdleTerminationCleanupFinalizer(instance) {
		logger.Info("RayCluster is being deleted due to idle termination",
			"namespace", instance.Namespace, "name", instance.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal,
			string(utils.DeletedIdleRayCluster), string(utils.DeleteAction),
			"RayCluster %s/%s is being deleted due to idle termination",
			instance.Namespace, instance.Name)

		// Remove finalizer to allow deletion to proceed
		controllerutil.RemoveFinalizer(instance, utils.IdleTerminationCleanupFinalizer)
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
		}
	}

	// The `enableGCSFTRedisCleanup` is a feature flag introduced in KubeRay v1.0.0. It determines whether
	// the Redis cleanup job should be activated. Users can disable the feature by setting the environment
	// variable `ENABLE_GCS_FT_REDIS_CLEANUP` to `false`, and undertake the Redis storage namespace cleanup
	// manually after the RayCluster CR deletion.
	enableGCSFTRedisCleanup := strings.ToLower(os.Getenv(utils.ENABLE_GCS_FT_REDIS_CLEANUP)) != "false"

	// A RayCluster that was created with the Redis backend (and therefore received
	// the Redis cleanup finalizer) and later switched to the embedded RocksDB
	// backend still carries that finalizer. The embedded backend has no Redis to
	// clean up and the main Redis-cleanup block below is skipped for it, so the
	// finalizer-removal path would never run — leaving the CR stuck Terminating.
	// Remove the stale finalizer here on deletion.
	if !instance.DeletionTimestamp.IsZero() &&
		utils.IsGCSFaultToleranceEmbedded(instance.Spec.GcsFaultToleranceOptions) &&
		controllerutil.ContainsFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer) {
		logger.Info(
			"Removing the stale Redis cleanup finalizer from an embedded-backend RayCluster that is being deleted.",
			"finalizer", utils.GCSFaultToleranceRedisCleanupFinalizer,
		)
		controllerutil.RemoveFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer)
		if err := r.Update(ctx, instance); err != nil {
			return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
		}
		return ctrl.Result{}, nil
	}

	if enableGCSFTRedisCleanup && utils.IsGCSFaultToleranceEnabled(&instance.Spec, instance.Annotations) &&
		!utils.IsGCSFaultToleranceEmbedded(instance.Spec.GcsFaultToleranceOptions) {
		if instance.DeletionTimestamp.IsZero() {
			if !controllerutil.ContainsFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer) {
				logger.Info(
					"GCS fault tolerance has been enabled. Implementing a finalizer to ensure that Redis is properly cleaned up once the RayCluster custom resource (CR) is deleted.",
					"finalizer", utils.GCSFaultToleranceRedisCleanupFinalizer)
				controllerutil.AddFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer)
				if err := r.Update(ctx, instance); err != nil {
					err = fmt.Errorf("failed to add the finalizer %s to the RayCluster: %w", utils.GCSFaultToleranceRedisCleanupFinalizer, err)
					return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
				}
				// Only start the RayCluster reconciliation after the finalizer is added.
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
			}
		} else {
			logger.Info(
				"The RayCluster with GCS enabled is being deleted. Start to handle the Redis cleanup finalizer.",
				"redisCleanupFinalizer", utils.GCSFaultToleranceRedisCleanupFinalizer,
				"deletionTimestamp", instance.ObjectMeta.DeletionTimestamp,
			)

			// Delete the head Pod if it exists.
			headPods, err := r.deleteAllPods(ctx, common.RayClusterHeadPodsAssociationOptions(instance))
			if err != nil {
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
			}
			// Delete all worker Pods if they exist.
			if _, err = r.deleteAllPods(ctx, common.RayClusterWorkerPodsAssociationOptions(instance)); err != nil {
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
			}
			if len(headPods.Items) > 0 {
				logger.Info(
					"Wait for the head Pod to be terminated before initiating the Redis cleanup process. "+"The storage namespace in Redis cannot be fully deleted if the GCS process on the head Pod is still writing to it.",
					"headPodName", headPods.Items[0].Name,
					"redisStorageNamespace", headPods.Items[0].Annotations[utils.RayExternalStorageNSAnnotationKey],
				)
				// Requeue after 10 seconds because it takes much longer than DefaultRequeueDuration (2 seconds) for the head Pod to be terminated.
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

			filterLabels := common.RayClusterRedisCleanupJobAssociationOptions(instance).ToListOptions()
			redisCleanupJobs := batchv1.JobList{}
			if err := r.List(ctx, &redisCleanupJobs, filterLabels...); err != nil {
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
			}

			// Check finalizer timeout for RayService FT clusters and set requeue flag
			if hasGCSFTFinalizer(instance) {
				deletionAge := time.Since(instance.DeletionTimestamp.Time)
				// This is to handle edge cases where the finalizer doesn't get removed,
				// thus preventing release of resource quotas.
				if deletionAge >= getGCSFTDeletionTimeout(instance) {
					return r.forceRemoveGCSFTFinalizer(ctx, instance)
				}
			}

			if len(redisCleanupJobs.Items) != 0 {
				// Check whether the Redis cleanup Job has been completed.
				redisCleanupJob := redisCleanupJobs.Items[0]
				logger.Info("Redis cleanup Job status", "Job name", redisCleanupJob.Name,
					"Active", redisCleanupJob.Status.Active, "Succeeded", redisCleanupJob.Status.Succeeded, "Failed", redisCleanupJob.Status.Failed)
				if condition, finished := utils.IsJobFinished(&redisCleanupJob); finished {
					controllerutil.RemoveFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer)
					if err := r.Update(ctx, instance); err != nil {
						return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
					}
					switch condition {
					case batchv1.JobComplete:
						logger.Info(
							"The Redis cleanup Job has been completed. "+
								"The storage namespace in Redis has been fully deleted.",
							"redisCleanupJobName", redisCleanupJob.Name,
							"redisStorageNamespace", redisCleanupJob.Annotations[utils.RayExternalStorageNSAnnotationKey],
						)
					case batchv1.JobFailed:
						logger.Info(
							"The Redis cleanup Job has failed, requeue the RayCluster CR after 5 minute. "+
								"You should manually delete the storage namespace in Redis and remove the RayCluster's finalizer. "+
								"Please check https://docs.ray.io/en/master/cluster/kubernetes/user-guides/kuberay-gcs-ft.html for more details.",
							"redisCleanupJobName", redisCleanupJob.Name,
							"redisStorageNamespace", redisCleanupJob.Annotations[utils.RayExternalStorageNSAnnotationKey],
						)
					}
					return ctrl.Result{}, nil
				}
				// the redisCleanupJob is still running
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
			}
			redisCleanupJob := r.buildRedisCleanupJob(ctx, *instance)
			if err := r.Create(ctx, &redisCleanupJob); err != nil {
				if errors.IsAlreadyExists(err) {
					logger.Info("Redis cleanup Job already exists. Requeue the RayCluster CR.")
					return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
				}
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRedisCleanupJob), string(utils.CreateAction),
					"Failed to create Redis cleanup Job %s/%s, %v", redisCleanupJob.Namespace, redisCleanupJob.Name, err)
				return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
			}
			logger.Info("Created Redis cleanup Job", "name", redisCleanupJob.Name)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedRedisCleanupJob), string(utils.CreateAction),
				"Created Redis cleanup Job %s/%s", redisCleanupJob.Namespace, redisCleanupJob.Name)
			return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, nil
		}
	}

	if instance.DeletionTimestamp != nil && !instance.DeletionTimestamp.IsZero() {
		logger.Info("RayCluster is being deleted, just ignore")
		return ctrl.Result{}, nil
	}

	reconcileFuncs := []reconcileFunc{
		r.reconcileAutoscalerServiceAccount,
		r.reconcileAutoscalerRole,
		r.reconcileAutoscalerRoleBinding,
		r.reconcileIngress,
		r.reconcileAuthSecret,
		r.reconcileHeadService,
		r.reconcileHeadlessService,
		r.reconcileServeService,
		r.reconcileGCSStoragePVC,
		r.reconcilePods,
	}

	for _, fn := range reconcileFuncs {
		if reconcileErr = fn(ctx, instance); reconcileErr != nil {
			funcName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
			logger.Error(reconcileErr, "Error reconcile resources", "function name", funcName)
			break
		}
	}

	// Calculate the new status for the RayCluster. Note that the function will deep copy `instance` instead of mutating it.
	newInstance, calculateErr := r.calculateStatus(ctx, instance, reconcileErr)
	var updateErr error
	var inconsistent bool
	if calculateErr != nil {
		logger.Info("Got error when calculating new status", "error", calculateErr)
	} else {
		inconsistent, updateErr = r.updateRayClusterStatus(ctx, originalRayClusterInstance, newInstance)
	}

	// Return error based on order.
	var err error
	if reconcileErr != nil {
		err = reconcileErr
	} else if calculateErr != nil {
		err = calculateErr
	} else {
		err = updateErr
	}
	// If the custom resource's status is updated, requeue the reconcile key.
	// Without this behavior, atomic operations such as the suspend operation would need to wait for `RAYCLUSTER_DEFAULT_REQUEUE_SECONDS` to delete Pods
	// after the condition rayv1.RayClusterSuspending is set to true.
	if err != nil || inconsistent {
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
	}

	// Unconditionally requeue after the number of seconds specified in the
	// environment variable RAYCLUSTER_DEFAULT_REQUEUE_SECONDS_ENV. If the
	// environment variable is not set, requeue after the default value.
	requeueAfterSeconds, err := strconv.Atoi(os.Getenv(utils.RAYCLUSTER_DEFAULT_REQUEUE_SECONDS_ENV))
	if err != nil {
		logger.Info(
			"Environment variable is not set, using default value of seconds",
			"environmentVariable", utils.RAYCLUSTER_DEFAULT_REQUEUE_SECONDS_ENV,
			"defaultValue", utils.RAYCLUSTER_DEFAULT_REQUEUE_SECONDS,
		)
		requeueAfterSeconds = utils.RAYCLUSTER_DEFAULT_REQUEUE_SECONDS
	}
	logger.Info("Unconditional requeue after", "seconds", requeueAfterSeconds)
	return ctrl.Result{RequeueAfter: time.Duration(requeueAfterSeconds) * time.Second}, nil
}

func (r *RayClusterReconciler) reconcileAuthSecret(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("Reconciling Auth")

	if instance.Spec.AuthOptions == nil || instance.Spec.AuthOptions.Mode == rayv1.AuthModeDisabled {
		return nil
	}

	// When K8s token auth is enabled, authentication is delegated to the K8s API server
	// via ServiceAccount tokens, so no Secret needs to be created.
	if utils.IsK8sAuthEnabled(instance.Spec.AuthOptions) {
		return nil
	}

	if instance.Spec.AuthOptions.SecretName != nil && *instance.Spec.AuthOptions.SecretName != "" {
		return nil
	}

	secret := &corev1.Secret{}
	secretName := utils.CheckName(instance.Name)
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: instance.Namespace}, secret)
	if err != nil {
		if errors.IsNotFound(err) {
			return r.createAuthSecret(ctx, instance, secretName)
		}
		return err
	}

	return nil
}

// createAuthSecret generates a new secret with a random token.
func (r *RayClusterReconciler) createAuthSecret(ctx context.Context, rayCluster *rayv1.RayCluster, secretName string) error {
	token, err := generateRandomToken(32)
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: rayCluster.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey: rayCluster.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(rayCluster, rayv1.SchemeGroupVersion.WithKind("RayCluster")),
			},
		},
		StringData: map[string]string{
			"auth_token": token,
		},
	}

	return r.Create(ctx, secret)
}

// generateRandomToken creates a random base64 encoded string.
func generateRandomToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bytes), nil
}

// shouldCreateOpenShiftRoute determines if an OpenShift Route should be created based on cluster type and configuration.
// Uses the stored values from reconciler options (no function calls during reconciliation).
// TODO: Remove once Gateway API support is mature and Route-based dashboard access is no longer needed.
// See: https://github.com/ray-project/kuberay/pull/4365#issuecomment-4143407845
func (r *RayClusterReconciler) shouldCreateOpenShiftRoute() bool {
	return r.options.IsOpenShift && !r.options.UseIngressOnOpenShift
}

func (r *RayClusterReconciler) reconcileIngress(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.Info("Reconciling Ingress")
	if instance.Spec.HeadGroupSpec.EnableIngress == nil || !*instance.Spec.HeadGroupSpec.EnableIngress {
		return nil
	}

	if r.shouldCreateOpenShiftRoute() {
		return r.reconcileRouteOpenShift(ctx, instance)
	}
	return r.reconcileIngressKubernetes(ctx, instance)
}

func (r *RayClusterReconciler) reconcileRouteOpenShift(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	headRoutes := routev1.RouteList{}
	filterLabels := common.RayClusterNetworkResourcesOptions(instance).ToListOptions()
	if err := r.List(ctx, &headRoutes, filterLabels...); err != nil {
		return err
	}

	if len(headRoutes.Items) == 1 {
		logger.Info("reconcileIngresses", "head service route found", headRoutes.Items[0].Name)
		return nil
	}

	if len(headRoutes.Items) == 0 {
		route, err := common.BuildRouteForHeadService(*instance)
		if err != nil {
			return err
		}

		if err := ctrl.SetControllerReference(instance, route, r.Scheme); err != nil {
			return err
		}

		if err := r.createHeadRoute(ctx, route, instance); err != nil {
			return err
		}
	}

	return nil
}

func (r *RayClusterReconciler) reconcileIngressKubernetes(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	desiredIngress, err := common.BuildIngressForHeadService(ctx, *instance)
	if err != nil {
		return err
	}

	existingIngress := &networkingv1.Ingress{}
	switch err := r.Get(ctx, client.ObjectKeyFromObject(desiredIngress), existingIngress); {
	case errors.IsNotFound(err):
		return r.createHeadIngress(ctx, desiredIngress, instance)
	case err != nil:
		return err
	}

	if !metav1.IsControlledBy(existingIngress, instance) {
		// An Ingress with our generated name already exists but is not owned
		// by this RayCluster. Surface the collision via an event instead of
		// silently swallowing the AlreadyExists error on create, and leave the
		// user's Ingress untouched.
		logger.Info("reconcileIngresses", "skipping Ingress not owned by this RayCluster", existingIngress.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateIngress), string(utils.CreateAction), "Ingress %s/%s already exists and is not owned by this RayCluster; not modifying it", existingIngress.Namespace, existingIngress.Name)
		return nil
	}

	if ingressNeedsUpdate(existingIngress, desiredIngress) {
		if err := r.Update(ctx, existingIngress); err != nil {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateIngress), string(utils.UpdateAction), "Failed updating ingress %s/%s, %v", existingIngress.Namespace, existingIngress.Name, err)
			return err
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.UpdatedIngress), string(utils.UpdateAction), "Updated ingress %s/%s", existingIngress.Namespace, existingIngress.Name)
		logger.Info("reconcileIngresses", "head service ingress updated", existingIngress.Name)
	} else {
		logger.Info("reconcileIngresses", "head service ingress found", existingIngress.Name)
	}
	return nil
}

func ingressNeedsUpdate(existingIngress, desiredIngress *networkingv1.Ingress) bool {
	updated := false

	if !maps.Equal(existingIngress.Labels, desiredIngress.Labels) {
		existingIngress.Labels = maps.Clone(desiredIngress.Labels)
		updated = true
	}

	if !maps.Equal(existingIngress.Annotations, desiredIngress.Annotations) {
		existingIngress.Annotations = maps.Clone(desiredIngress.Annotations)
		updated = true
	}

	// When the RayCluster does not request an ingress class, the DefaultIngressClass
	// admission plugin may set spec.ingressClassName on the live Ingress. We have no
	// opinion in that case, so adopt the existing value instead of clearing it; doing
	// otherwise would drop the class and thrash with the API server every reconcile.
	if desiredIngress.Spec.IngressClassName == nil {
		desiredIngress.Spec.IngressClassName = existingIngress.Spec.IngressClassName
	}

	if !reflect.DeepEqual(existingIngress.Spec, desiredIngress.Spec) {
		existingIngress.Spec = desiredIngress.Spec
		updated = true
	}

	return updated
}

// Return nil only when the head service successfully created or already exists.
func (r *RayClusterReconciler) reconcileHeadService(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	services := corev1.ServiceList{}
	filterLabels := common.RayClusterHeadServiceListOptions(instance)

	if err := r.List(ctx, &services, filterLabels...); err != nil {
		return err
	}

	// Check if there's existing head service in the cluster.
	if len(services.Items) != 0 {
		if len(services.Items) == 1 {
			logger.Info("reconcileHeadService", "1 head service found", services.Items[0].Name)
			return nil
		}
		// This should never happen. This protects against the case that users manually create service with the same label.
		if len(services.Items) > 1 {
			logger.Info("reconcileHeadService", "Duplicate head service found", services.Items)
			return fmt.Errorf("%d head service found %v", len(services.Items), services.Items)
		}
	} else {
		// Create head service if there's no existing one in the cluster.
		labels := make(map[string]string)
		if val, ok := instance.Spec.HeadGroupSpec.Template.ObjectMeta.Labels[utils.KubernetesApplicationNameLabelKey]; ok {
			labels[utils.KubernetesApplicationNameLabelKey] = val
		}
		annotations := make(map[string]string)
		// TODO (kevin85421): KubeRay has already exposed the entire head service (#1040) to users.
		// We may consider deprecating this field when we bump the CRD version.
		maps.Copy(annotations, instance.Spec.HeadServiceAnnotations)
		headSvc, err := common.BuildServiceForHeadPod(ctx, *instance, labels, annotations)
		if err != nil {
			return err
		}
		// TODO (kevin85421): Provide a detailed and actionable error message. For example, which port is missing?
		if len(headSvc.Spec.Ports) == 0 {
			logger.Info("Ray head service does not have any ports set up.", "serviceSpecification", headSvc.Spec)
			return fmt.Errorf("ray head service does not have any ports set up. Service specification: %v", headSvc.Spec)
		}

		if err := r.createService(ctx, headSvc, instance); err != nil {
			return err
		}
	}

	return nil
}

// reconcileGCSStoragePVC provisions the persistent volume backing the embedded
// RocksDB GCS store. It is a no-op unless GCS FT uses the embedded backend.
//
// Two lifecycles are supported:
//
//   - Operator-managed (default): the operator creates a PVC ({cluster}-gcs-pvc)
//     owned by the RayCluster, so it is garbage-collected together with the
//     cluster. This is the sane default for a standalone RayCluster. Because the
//     PVC is keyed by (and owned by) the RayCluster, it does NOT survive a
//     RayService zero-downtime upgrade: the new RayCluster gets a new name and a
//     fresh PVC, and the old PVC is GC'd with the old RayCluster. Set
//     Storage.DeletionPolicy: Retain to keep the PVC (and its data) after the
//     cluster is deleted so it can be recovered later via ClaimName.
//
//   - User-managed (Storage.ClaimName): the operator never creates, deletes,
//     or takes ownership of the PVC. This is the path for persisting GCS state
//     across RayService upgrades: point every RayService generation at the same
//     stable claim. During a zero-downtime upgrade the old and new head Pods run
//     concurrently, so the claim must allow concurrent attach (ReadWriteMany) and
//     single-writer semantics must be coordinated externally (RocksDB tolerates
//     only one writer at a time). ReadWriteOnce is acceptable for active-passive /
//     non-overlapping handoffs where only one Pod attaches at a time.
//
// Why not an operator-managed PVC re-parented to the RayService for automatic
// cross-upgrade persistence?
//
// Re-parenting (keying the PVC by RayService name + ownerReference to the
// RayService) would make the volume survive an upgrade, and Ray does resume the
// prior session automatically (a head that mounts an existing store adopts the
// persisted session_name), but that alone is NOT sufficient and would deadlock
// the default zero-downtime (NewCluster) upgrade:
//
//   - RocksDB is a hard single-writer, enforced by its own on-disk LOCK file (not
//     by the PVC access mode). A second DB open on the same directory fails rather
//     than queueing/handing off.
//   - In a zero-downtime upgrade the old and new RayClusters run concurrently, both
//     wanting the one store. With RWX the new head's GCS cannot open the DB while
//     the old head holds the lock (crash-loop); with RWO the new head cannot even
//     attach the volume (multi-attach). Either way the new cluster never becomes
//     healthy, so the RayService never tears down the old cluster -> the upgrade
//     hangs.
//
// Making operator-managed cross-upgrade persistence safe requires an active-passive
// handoff (stop the old head's GCS so it closes the DB, detach, then start the new
// head) -- i.e. an in-place / RecreateCluster upgrade with a brief GCS-unavailability
// window, which is a different upgrade strategy than the concurrent NewCluster path.
// That is deferred as follow-up work; today, cross-upgrade persistence is served by
// the user-managed ClaimName path above.
func (r *RayClusterReconciler) reconcileGCSStoragePVC(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	if !utils.IsGCSFaultToleranceEnabled(&instance.Spec, instance.Annotations) ||
		!utils.IsGCSFaultToleranceEmbedded(instance.Spec.GcsFaultToleranceOptions) {
		return nil
	}

	pvcName := utils.GetGCSStoragePVCName(instance)

	// BYO PVC: the user owns the lifecycle. Verify it exists and surface an event if not.
	if storage := instance.Spec.GcsFaultToleranceOptions.Storage; storage != nil && storage.ClaimName != "" {
		existing := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: instance.Namespace}, existing)
		if errors.IsNotFound(err) {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreatePVC), string(utils.ValidateAction),
				"GcsFaultToleranceOptions.Storage.ClaimName %q not found in namespace %s; the head Pod will not start until it exists", pvcName, instance.Namespace)
			return nil
		}
		return err
	}

	// Operator-managed PVC: create if absent. Once created the PVC is effectively
	// immutable (StorageClassName and AccessModes cannot change, and most
	// StorageClasses do not allow shrinking); if the desired spec later diverges
	// from the live PVC we surface a warning event rather than silently dropping
	// the change.
	existing := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: instance.Namespace}, existing)
	if err == nil {
		// An existing {cluster}-gcs-pvc is adopted as-is (drift is reported but the
		// PVC is not recreated). This is deliberate: a same-named cluster recreated
		// after a `deletionPolicy: Retain` delete reuses the retained RocksDB state,
		// which is the documented same-name recovery path (see the GcsEmbeddedStorage
		// field docs). We intentionally do NOT error on an "unowned" existing PVC to
		// force explicit opt-in, because a `Retain` cluster deliberately creates its
		// own PVC without an ownerReference: erroring on unowned PVCs would break the
		// steady-state reconcile / restart of a Retain cluster (its own PVC would be
		// rejected). To start from a fresh store instead, delete the leftover PVC.
		if drift := gcsStoragePVCDrift(common.BuildGCSStoragePVC(instance), existing); drift != "" {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreatePVC), string(utils.UpdateAction),
				"GCS storage PVC %s/%s already exists and cannot be reconfigured in place (%s); delete the PVC to recreate it with the new settings", existing.Namespace, existing.Name, drift)
		}
		// Reconcile the controller ownerReference so that changing
		// DeletionPolicy takes effect on an already-provisioned PVC.
		return r.reconcileGCSStoragePVCOwnerRef(ctx, instance, existing)
	} else if !errors.IsNotFound(err) {
		return err
	}

	pvc := common.BuildGCSStoragePVC(instance)
	// When DeletionPolicy is Retain, deliberately omit the controller
	// ownerReference so the PVC (and its data) outlives the RayCluster and can be
	// recovered via ClaimName on a future cluster.
	if !gcsStorageRetainOnDeletion(instance) {
		if err := ctrl.SetControllerReference(instance, pvc, r.Scheme); err != nil {
			return err
		}
	}

	if err := r.Create(ctx, pvc); err != nil {
		if errors.IsAlreadyExists(err) {
			// Lost a create race after the NotFound Get above: fetch the live PVC
			// and reconcile its ownerReference, consistent with the already-exists
			// path so DeletionPolicy is still honored.
			if getErr := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: instance.Namespace}, existing); getErr != nil {
				return getErr
			}
			return r.reconcileGCSStoragePVCOwnerRef(ctx, instance, existing)
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreatePVC), string(utils.CreateAction),
			"Failed to create GCS storage PVC %s/%s, %v", pvc.Namespace, pvc.Name, err)
		return err
	}
	logger.Info("Created GCS storage PVC for embedded RocksDB backend", "name", pvc.Name)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedPVC), string(utils.CreateAction),
		"Created GCS storage PVC %s/%s", pvc.Namespace, pvc.Name)
	return nil
}

// gcsStorageRetainOnDeletion reports whether the operator-managed GCS storage PVC
// should be retained (kept without a controller ownerReference) after the owning
// RayCluster is deleted, i.e. whether Storage.DeletionPolicy is Retain.
func gcsStorageRetainOnDeletion(instance *rayv1.RayCluster) bool {
	storage := instance.Spec.GcsFaultToleranceOptions.Storage
	return storage != nil && storage.DeletionPolicy != nil &&
		*storage.DeletionPolicy == rayv1.RetainGCSStorageDeletionPolicy
}

// reconcileGCSStoragePVCOwnerRef aligns the controller ownerReference on an
// already-provisioned operator-managed PVC with the desired DeletionPolicy: it
// removes the RayCluster's controller ownerReference when retention is requested
// (DeletionPolicy: Retain), and (re)adds it otherwise. This makes changing the
// policy effective on an existing PVC without recreating it.
func (r *RayClusterReconciler) reconcileGCSStoragePVCOwnerRef(ctx context.Context, instance *rayv1.RayCluster, pvc *corev1.PersistentVolumeClaim) error {
	retain := gcsStorageRetainOnDeletion(instance)
	hasOwnerRef := metav1.IsControlledBy(pvc, instance)

	switch {
	case retain && hasOwnerRef:
		if err := controllerutil.RemoveControllerReference(instance, pvc, r.Scheme); err != nil {
			return err
		}
		return r.Update(ctx, pvc)
	case !retain && !hasOwnerRef:
		if err := ctrl.SetControllerReference(instance, pvc, r.Scheme); err != nil {
			return err
		}
		return r.Update(ctx, pvc)
	default:
		return nil
	}
}

// gcsStoragePVCDrift returns a human-readable description of the fields that
// differ between the desired operator-managed GCS storage PVC and the live one,
// or "" when they are equivalent for the immutable/relevant fields. It only
// compares fields the operator manages and that Kubernetes cannot reconcile in
// place (requested size, StorageClassName, AccessModes).
func gcsStoragePVCDrift(desired, existing *corev1.PersistentVolumeClaim) string {
	var diffs []string

	wantSize := desired.Spec.Resources.Requests[corev1.ResourceStorage]
	gotSize := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	if wantSize.Cmp(gotSize) != 0 {
		diffs = append(diffs, fmt.Sprintf("requested size %s != current %s", wantSize.String(), gotSize.String()))
	}

	// Only compare StorageClassName when the desired value is explicitly set.
	// When it is omitted (nil), the caller asked for the cluster's default
	// StorageClass and Kubernetes populates the live PVC with the resolved default
	// class name, so a nil-vs-resolved-default comparison would be a false positive.
	if desired.Spec.StorageClassName != nil && !ptr.Equal(desired.Spec.StorageClassName, existing.Spec.StorageClassName) {
		diffs = append(diffs, fmt.Sprintf("storageClassName %s != current %s",
			ptr.Deref(desired.Spec.StorageClassName, "<default>"), ptr.Deref(existing.Spec.StorageClassName, "<default>")))
	}

	if !equalAccessModes(desired.Spec.AccessModes, existing.Spec.AccessModes) {
		diffs = append(diffs, fmt.Sprintf("accessModes %v != current %v", desired.Spec.AccessModes, existing.Spec.AccessModes))
	}

	return strings.Join(diffs, "; ")
}

func equalAccessModes(a, b []corev1.PersistentVolumeAccessMode) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[corev1.PersistentVolumeAccessMode]int, len(a))
	for _, m := range a {
		seen[m]++
	}
	for _, m := range b {
		seen[m]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

// Return nil only when the serve service successfully created or already exists.
func (r *RayClusterReconciler) reconcileServeService(ctx context.Context, instance *rayv1.RayCluster) error {
	// Only reconcile the K8s service for Ray Serve when the "ray.io/enable-serve-service" annotation is set to true.
	if enableServeServiceValue, exist := instance.Annotations[utils.EnableServeServiceKey]; !exist || enableServeServiceValue != utils.EnableServeServiceTrue {
		return nil
	}

	// Retrieve the Service from the Kubernetes cluster with the name and namespace.
	svc := &corev1.Service{}
	err := r.Get(ctx, common.RayClusterServeServiceNamespacedName(instance), svc)
	if err == nil {
		// service exists, do nothing
		return nil
	} else if errors.IsNotFound(err) {
		// Service does not exist, create it
		svc, err = common.BuildServeServiceForRayCluster(ctx, *instance)
		if err != nil {
			return err
		}
		// Set the ownwer reference
		if err := ctrl.SetControllerReference(instance, svc, r.Scheme); err != nil {
			return err
		}
		// create service
		return r.Create(ctx, svc)
	}
	return err
}

// Return nil only when the headless service for multi-host worker groups is successfully created or already exists.
func (r *RayClusterReconciler) reconcileHeadlessService(ctx context.Context, instance *rayv1.RayCluster) error {
	// Check if there are worker groups with NumOfHosts > 1 in the cluster
	isMultiHost := false
	for _, workerGroup := range instance.Spec.WorkerGroupSpecs {
		if workerGroup.NumOfHosts > 1 {
			isMultiHost = true
			break
		}
	}

	if isMultiHost {
		services := corev1.ServiceList{}
		options := common.RayClusterHeadlessServiceListOptions(instance)

		if err := r.List(ctx, &services, options...); err != nil {
			return err
		}
		// Check if there's an existing headless service in the cluster.
		if len(services.Items) != 0 {
			// service exists, do nothing
			return nil
		}
		// Create headless tpu worker service if there's no existing one in the cluster.
		headlessSvc := common.BuildHeadlessServiceForRayCluster(*instance)

		if err := r.createService(ctx, headlessSvc, instance); err != nil {
			return err
		}
	}

	return nil
}

func (r *RayClusterReconciler) reconcilePods(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	// Calculate RayCluster spec hash for the UpgradeStrategy
	clusterHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(instance.Spec)
	if err != nil {
		return err
	}

	// if RayCluster is suspending, delete all pods and skip reconcile
	suspendStatus := utils.FindRayClusterSuspendStatus(instance)
	statusConditionGateEnabled := features.Enabled(features.RayClusterStatusConditions)
	if suspendStatus == rayv1.RayClusterSuspending ||
		(!statusConditionGateEnabled && utils.IsRayClusterSuspendOrIdleSuspend(instance)) {
		if _, err := r.deleteAllPods(ctx, common.RayClusterAllPodsAssociationOptions(instance)); err != nil {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeletePodCollection), string(utils.DeleteAction),
				"Failed deleting Pods due to suspension for RayCluster %s/%s, %v",
				instance.Namespace, instance.Name, err)
			return errstd.Join(utils.ErrFailedDeleteAllPods, err)
		}

		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedPod), string(utils.DeleteAction),
			"Deleted Pods for RayCluster %s/%s due to suspension",
			instance.Namespace, instance.Name)
		return nil
	}

	if statusConditionGateEnabled {
		if suspendStatus == rayv1.RayClusterSuspended {
			return nil // stop reconcilePods because the cluster is suspended.
		}
		// (suspendStatus != rayv1.RayClusterSuspending) is always true here because it has been checked above.
		if utils.IsRayClusterSuspendOrIdleSuspend(instance) {
			return nil // stop reconcilePods because the cluster is going to suspend.
		}
	}

	// Check if pods need to be recreated with Recreate upgradeStrategy
	if r.shouldRecreatePodsForUpgrade(ctx, instance) {
		logger.Info("RayCluster spec changed with Recreate upgradeStrategy, deleting all pods")
		if _, err := r.deleteAllPods(ctx, common.RayClusterAllPodsAssociationOptions(instance)); err != nil {
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeletePodCollection), string(utils.DeleteAction),
				"Failed deleting Pods due to spec change with Recreate upgradeStrategy for RayCluster %s/%s, %v",
				instance.Namespace, instance.Name, err)
			return errstd.Join(utils.ErrFailedDeleteAllPods, err)
		}
		r.rayClusterScaleExpectation.Delete(instance.Name, instance.Namespace)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedPod), string(utils.DeleteAction),
			"Deleted all Pods for RayCluster %s/%s due to spec change with Recreate upgradeStrategy",
			instance.Namespace, instance.Name)
		return nil
	}

	// check if all the pods exist
	headPods := corev1.PodList{}
	if err := r.List(ctx, &headPods, common.RayClusterHeadPodsAssociationOptions(instance).ToListOptions()...); err != nil {
		return err
	}
	// check if the batch scheduler integration is enabled
	// call the scheduler plugin if so
	if r.options.BatchSchedulerManager != nil {
		if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
			if err := scheduler.DoBatchSchedulingOnSubmission(ctx, instance); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	// Reconcile head Pod
	if !r.rayClusterScaleExpectation.IsSatisfied(ctx, instance.Namespace, instance.Name, expectations.HeadGroup) {
		logger.Info("reconcilePods", "Expectation", "NotSatisfiedHeadExpectations, reconcile head later")
	} else if len(headPods.Items) == 1 {
		headPod := headPods.Items[0]
		logger.Info("reconcilePods", "Found 1 head Pod", headPod.Name, "Pod status", headPod.Status.Phase,
			"Pod status reason", headPod.Status.Reason,
			"Pod restart policy", headPod.Spec.RestartPolicy,
			"Ray container terminated status", getRayContainerStateTerminated(headPod))

		shouldDelete, reason := shouldDeletePod(headPod, rayv1.HeadNode)
		logger.Info("reconcilePods", "head Pod", headPod.Name, "shouldDelete", shouldDelete, "reason", reason)
		if shouldDelete {
			if err := r.Delete(ctx, &headPod); err != nil {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteHeadPod), string(utils.DeleteAction),
					"Failed deleting head Pod %s/%s; Pod status: %s; Pod restart policy: %s; Ray container terminated status: %v, %v",
					headPod.Namespace, headPod.Name, headPod.Status.Phase, headPod.Spec.RestartPolicy, getRayContainerStateTerminated(headPod), err)
				return errstd.Join(utils.ErrFailedDeleteHeadPod, err)
			}
			r.rayClusterScaleExpectation.ExpectScalePod(headPod.Namespace, instance.Name, expectations.HeadGroup, headPod.Name, expectations.Delete)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedHeadPod), string(utils.DeleteAction),
				"Deleted head Pod %s/%s; Pod status: %s; Pod restart policy: %s; Ray container terminated status: %v",
				headPod.Namespace, headPod.Name, headPod.Status.Phase, headPod.Spec.RestartPolicy, getRayContainerStateTerminated(headPod))
			return errstd.New(reason)
		}
	} else if len(headPods.Items) == 0 {
		if meta.IsStatusConditionTrue(instance.Status.Conditions, string(rayv1.RayClusterProvisioned)) &&
			shouldSkipHeadPodRestart(instance) {
			// Recreating the head Pod if the RayCluster created by RayJob is provisioned doesn't help RayJob.
			//
			// Case 1: GCS fault tolerance is disabled
			//
			// In this case, the worker Pods will be killed by the new head Pod when it is created, so the new Ray job will not be running in
			// a "provisioned" cluster.
			//
			// Case 2: GCS fault tolerance is enabled
			//
			// In this case, the worker Pods will not be killed by the new head Pod when it is created, but the submission ID has already been
			// used by the old Ray job, so the new Ray job will fail.
			logger.Info(
				"reconcilePods: Found 0 head Pods for the RayCluster; Skipped head recreation due to ray.io/disable-provisioned-head-restart",
				"rayCluster", instance.Name,
			)
			return nil
		}
		// Create head Pod if it does not exist.
		logger.Info("reconcilePods: Found 0 head Pods; creating a head Pod for the RayCluster.")
		if utils.IsTLSEnabled(&instance.Spec) {
			if err := r.checkMTLSSecretsReady(ctx, instance); err != nil {
				logger.Info("mTLS secrets not ready, requeuing before head pod creation", "error", err.Error())
				return fmt.Errorf("mTLS secrets not ready: %w", err)
			}
		}
		if err := r.createHeadPod(ctx, *instance, clusterHash); err != nil {
			return errstd.Join(utils.ErrFailedCreateHeadPod, err)
		}
	} else if len(headPods.Items) > 1 { // This should never happen. This protects against the case that users manually create headpod.
		headPodNames := make([]string, len(headPods.Items))
		for i, pod := range headPods.Items {
			headPodNames[i] = pod.Name
		}

		logger.Info("Multiple head pods found, it should only exist one head pod. Please delete extra head pods.",
			"found pods", headPodNames,
		)
		return fmt.Errorf("%d head pods found %v. Please delete extra head pods", len(headPods.Items), headPodNames)
	}

	// Reconcile worker pods now
	for _, worker := range instance.Spec.WorkerGroupSpecs {
		if !r.rayClusterScaleExpectation.IsSatisfied(ctx, instance.Namespace, instance.Name, worker.GroupName) {
			logger.Info("reconcilePods", "worker group", worker.GroupName, "Expectation", "NotSatisfiedGroupExpectations, reconcile the group later")
			continue
		}
		// workerReplicas will store the target number of pods for this worker group.
		numExpectedWorkerPods := int(utils.GetWorkerGroupDesiredReplicas(worker))
		logger.Info("reconcilePods", "desired workerReplicas (always adhering to minReplicas/maxReplica)", numExpectedWorkerPods, "worker group", worker.GroupName, "maxReplicas", worker.MaxReplicas, "minReplicas", worker.MinReplicas, "replicas", worker.Replicas)

		workerPods := corev1.PodList{}
		if err := r.List(ctx, &workerPods, common.RayClusterGroupPodsAssociationOptions(instance, worker.GroupName).ToListOptions()...); err != nil {
			return err
		}

		// Delete all workers if worker group is suspended and skip reconcile
		if worker.Suspend != nil && *worker.Suspend {
			if _, err := r.deleteAllPods(ctx, common.RayClusterGroupPodsAssociationOptions(instance, worker.GroupName)); err != nil {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteWorkerPodCollection), string(utils.DeleteAction),
					"Failed deleting worker Pods for suspended group %s in RayCluster %s/%s, %v", worker.GroupName, instance.Namespace, instance.Name, err)
				return errstd.Join(utils.ErrFailedDeleteWorkerPod, err)
			}
			r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedWorkerPod), string(utils.DeleteAction),
				"Deleted all pods for suspended worker group %s in RayCluster %s/%s", worker.GroupName, instance.Namespace, instance.Name)
			continue
		}

		isRayMultiHostIndexing := worker.NumOfHosts > 1 && features.Enabled(features.RayMultiHostIndexing)
		if isRayMultiHostIndexing {
			if err := r.reconcileMultiHostWorkerGroup(ctx, instance, &worker, workerPods.Items); err != nil {
				return err
			}
			// Skip to the next worker as we've already handled multi-host reconciliation.
			continue
		}

		// Delete unhealthy worker Pods.
		deletedWorkers := make(map[string]struct{})
		deleted := struct{}{}
		numDeletedUnhealthyWorkerPods := 0
		for _, workerPod := range workerPods.Items {
			shouldDelete, reason := shouldDeletePod(workerPod, rayv1.WorkerNode)
			logger.Info("reconcilePods", "worker Pod", workerPod.Name, "shouldDelete", shouldDelete, "reason", reason)
			if shouldDelete {
				numDeletedUnhealthyWorkerPods++
				deletedWorkers[workerPod.Name] = deleted
				if err := r.Delete(ctx, &workerPod); err != nil {
					r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteWorkerPod), string(utils.DeleteAction),
						"Failed deleting worker Pod %s/%s; Pod status: %s; Pod restart policy: %s; Ray container terminated status: %v, %v",
						workerPod.Namespace, workerPod.Name, workerPod.Status.Phase, workerPod.Spec.RestartPolicy, getRayContainerStateTerminated(workerPod), err)
					return errstd.Join(utils.ErrFailedDeleteWorkerPod, err)
				}
				r.rayClusterScaleExpectation.ExpectScalePod(workerPod.Namespace, instance.Name, worker.GroupName, workerPod.Name, expectations.Delete)
				r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedWorkerPod), string(utils.DeleteAction),
					"Deleted worker Pod %s/%s; Pod status: %s; Pod restart policy: %s; Ray container terminated status: %v",
					workerPod.Namespace, workerPod.Name, workerPod.Status.Phase, workerPod.Spec.RestartPolicy, getRayContainerStateTerminated(workerPod))
			}
		}

		// If we delete unhealthy Pods, we will not create new Pods in this reconciliation.
		if numDeletedUnhealthyWorkerPods > 0 {
			return fmt.Errorf("delete %d unhealthy worker Pods", numDeletedUnhealthyWorkerPods)
		}

		// Always remove the specified WorkersToDelete - regardless of the value of Replicas.
		// Essentially WorkersToDelete has to be deleted to meet the expectations of the Autoscaler.
		logger.Info("reconcilePods", "removing the pods in the scaleStrategy of", worker.GroupName)
		for _, podsToDelete := range worker.ScaleStrategy.WorkersToDelete {
			pod := corev1.Pod{}
			pod.Name = podsToDelete
			pod.Namespace = utils.GetNamespace(instance.ObjectMeta)
			logger.Info("Deleting pod", "namespace", pod.Namespace, "name", pod.Name)
			if err := r.Delete(ctx, &pod); err != nil {
				if !errors.IsNotFound(err) {
					logger.Info("reconcilePods", "Fail to delete Pod", pod.Name, "error", err)
					r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteWorkerPod), string(utils.DeleteAction), "Failed deleting pod %s/%s, %v", pod.Namespace, pod.Name, err)
					return errstd.Join(utils.ErrFailedDeleteWorkerPod, err)
				}
				logger.Info("reconcilePods", "The worker Pod has already been deleted", pod.Name)
			} else {
				r.rayClusterScaleExpectation.ExpectScalePod(pod.Namespace, instance.Name, worker.GroupName, pod.Name, expectations.Delete)
				deletedWorkers[pod.Name] = deleted
				r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedWorkerPod), string(utils.DeleteAction), "Deleted pod %s/%s", pod.Namespace, pod.Name)
			}
		}

		runningPods := corev1.PodList{}
		for _, pod := range workerPods.Items {
			if _, ok := deletedWorkers[pod.Name]; !ok {
				runningPods.Items = append(runningPods.Items, pod)
			}
		}
		// A replica can contain multiple hosts, so we need to calculate this based on the number of hosts per replica.
		// If the user doesn't install the CRD with `NumOfHosts`, the zero value of `NumOfHosts`, which is 0, will be used.
		// Hence, all workers will be deleted. Here, we set `NumOfHosts` to max(1, `NumOfHosts`) to avoid this situation.
		if worker.NumOfHosts <= 0 {
			worker.NumOfHosts = 1
		}
		diff := numExpectedWorkerPods - len(runningPods.Items)

		logger.Info("reconcilePods", "workerReplicas", numExpectedWorkerPods, "NumOfHosts", worker.NumOfHosts, "runningPods", len(runningPods.Items), "diff", diff)

		// Support replica indices for single-host, multi-slice environments.
		validReplicaIndices := make(map[int]bool)
		if features.Enabled(features.RayMultiHostIndexing) {
			for _, pod := range runningPods.Items {
				if indexStr, ok := pod.Labels[utils.RayWorkerReplicaIndexKey]; ok {
					if index, err := strconv.Atoi(indexStr); err == nil {
						validReplicaIndices[index] = true
					}
				}
			}
			logger.Info("reconcilePods: found existing replica indices", "group", worker.GroupName, "indices", validReplicaIndices)
		}
		if diff > 0 {
			// pods need to be added
			logger.Info("reconcilePods", "Number workers to add", diff, "Worker group", worker.GroupName)
			if utils.IsTLSEnabled(&instance.Spec) {
				if err := r.checkMTLSSecretsReady(ctx, instance); err != nil {
					logger.Info("mTLS secrets not ready, requeuing before worker pod creation", "error", err.Error())
					return fmt.Errorf("mTLS secrets not ready: %w", err)
				}
			}
			if features.Enabled(features.RayMultiHostIndexing) {
				newReplicaIndex := 0
				// create all workers of this group
				for i := range diff {
					// Find the next available replica index.
					for validReplicaIndices[newReplicaIndex] {
						newReplicaIndex++
					}
					validReplicaIndices[newReplicaIndex] = true
					logger.Info("reconcilePods", "creating worker for group", worker.GroupName, "index", i, "total", diff, "replicaIndex", newReplicaIndex)
					if err := r.createWorkerPodWithIndex(ctx, *instance, *worker.DeepCopy(), "", newReplicaIndex, 0); err != nil {
						return errstd.Join(utils.ErrFailedCreateWorkerPod, err)
					}
				}
			} else {
				// create all workers of this group
				for i := range diff {
					logger.Info("reconcilePods", "creating worker for group", worker.GroupName, "index", i, "total", diff)
					if err := r.createWorkerPod(ctx, *instance, *worker.DeepCopy()); err != nil {
						return errstd.Join(utils.ErrFailedCreateWorkerPod, err)
					}
				}
			}
		} else if diff == 0 {
			logger.Info("reconcilePods", "all workers already exist for group", worker.GroupName)
			continue
		} else {
			// diff < 0 indicates the need to delete some Pods to match the desired number of replicas. However,
			// randomly deleting Pods is certainly not ideal. So, if autoscaling is enabled for the cluster, we
			// will disable random Pod deletion, making Autoscaler the sole decision-maker for Pod deletions.
			enableInTreeAutoscaling := utils.IsAutoscalingEnabled(&instance.Spec)

			// TODO (kevin85421): `enableRandomPodDelete` is a feature flag for KubeRay v0.6.0. If users want to use
			// the old behavior, they can set the environment variable `ENABLE_RANDOM_POD_DELETE` to `true`. When the
			// default behavior is stable enough, we can remove this feature flag.
			enableRandomPodDelete := false
			if enableInTreeAutoscaling {
				if s := os.Getenv(utils.ENABLE_RANDOM_POD_DELETE); strings.ToLower(s) == "true" {
					enableRandomPodDelete = true
				}
			}
			// Case 1: If Autoscaler is disabled, we will always enable random Pod deletion no matter the value of the feature flag.
			// Case 2: If Autoscaler is enabled, we will respect the value of the feature flag. If the feature flag environment variable
			// is not set, we will disable random Pod deletion by default.
			if !enableInTreeAutoscaling || enableRandomPodDelete {
				// diff < 0 means that we need to delete some Pods to meet the desired number of replicas.
				randomlyRemovedWorkers := -diff
				logger.Info("reconcilePods", "Number workers to delete randomly", randomlyRemovedWorkers, "Worker group", worker.GroupName)
				for i := range randomlyRemovedWorkers {
					randomPodToDelete := runningPods.Items[i]
					logger.Info("Randomly deleting Pod", "progress", fmt.Sprintf("%d / %d", i+1, randomlyRemovedWorkers), "with name", randomPodToDelete.Name)
					if err := r.Delete(ctx, &randomPodToDelete); err != nil {
						if !errors.IsNotFound(err) {
							r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteWorkerPod), string(utils.DeleteAction), "Failed deleting Pod %s/%s, %v", randomPodToDelete.Namespace, randomPodToDelete.Name, err)
							return errstd.Join(utils.ErrFailedDeleteWorkerPod, err)
						}
						logger.Info("reconcilePods", "The worker Pod has already been deleted", randomPodToDelete.Name)
					}
					r.rayClusterScaleExpectation.ExpectScalePod(randomPodToDelete.Namespace, instance.Name, worker.GroupName, randomPodToDelete.Name, expectations.Delete)
					r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedWorkerPod), string(utils.DeleteAction), "Deleted Pod %s/%s", randomPodToDelete.Namespace, randomPodToDelete.Name)
				}
			} else {
				logger.Info("Random Pod deletion is disabled for the cluster. The only decision-maker for Pod deletions is Autoscaler.")
			}
		}
	}
	return nil
}

// deletePods is a helper function to handle the deletion of a list of Pods, setting scale expectations
// and recording events.
func (r *RayClusterReconciler) deletePods(ctx context.Context, instance *rayv1.RayCluster, podsToDelete []corev1.Pod, groupName string, reason string) error {
	for i := range podsToDelete {
		pod := podsToDelete[i]
		if err := r.Delete(ctx, &pod); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteWorkerPod), string(utils.DeleteAction),
				"Failed deleting worker Pod %s/%s for group %s; Pod status: %s; Pod restart policy: %s; Ray container terminated status: %v, %v",
				pod.Namespace, pod.Name, groupName, pod.Status.Phase, pod.Spec.RestartPolicy, getRayContainerStateTerminated(pod), err)
			return errstd.Join(utils.ErrFailedDeleteWorkerPod, err)
		}
		r.rayClusterScaleExpectation.ExpectScalePod(pod.Namespace, instance.Name, groupName, pod.Name, expectations.Delete)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.DeletedWorkerPod), string(utils.DeleteAction),
			"Deleted worker Pod %s/%s for group %s: %s", pod.Namespace, pod.Name, groupName, reason)
	}
	return nil
}

// reconcileMultiHostWorkerGroup handles reconciliation and Pod deletion for worker groups with NumOfHosts > 1 when
// the RayMultihostIndexing feature is enabled. This function is responsible for:
// 1. Deleting incomplete or unhealthy multi-host groups atomically.
// 2. Explicit deletes of entire multi-host groups for the autoscaler.
// 3. Scale up/down of multi-host groups.
func (r *RayClusterReconciler) reconcileMultiHostWorkerGroup(ctx context.Context, instance *rayv1.RayCluster, worker *rayv1.WorkerGroupSpec, workerPods []corev1.Pod) error {
	logger := ctrl.LoggerFrom(ctx)

	// 1. Group existing pods by ray.io/worker-group-replica-index.
	replicaMap := make(map[string][]corev1.Pod)
	for _, pod := range workerPods {
		if replicaName, ok := pod.Labels[utils.RayWorkerReplicaNameKey]; ok {
			replicaMap[replicaName] = append(replicaMap[replicaName], pod)
		}
	}

	// 2. Clean up incomplete replica groups with deleted Pods caused by external deletion.
	for replicaName, podList := range replicaMap {
		if len(podList) < int(worker.NumOfHosts) {
			logger.Info("Found incomplete multi-host replica group, deleting all remaining pods to maintain atomicity.", "group", worker.GroupName, "replica", replicaName, "found", len(podList), "expected", worker.NumOfHosts)
			if err := r.deletePods(ctx, instance, podList, worker.GroupName, "cleanup of incomplete multi-host group"); err != nil {
				return err
			}
			// Requeue to avoid creating new Pods on this reconciliation.
			return fmt.Errorf("cleaned up incomplete replica group %s, requeueing", replicaName)
		}
	}

	// 3. Delete unhealthy replica groups.
	deletedPods := make(map[string]struct{})
	for _, pod := range workerPods {
		if _, alreadyDeleted := deletedPods[pod.Name]; alreadyDeleted {
			continue
		}
		shouldDelete, reason := shouldDeletePod(pod, rayv1.WorkerNode)
		if shouldDelete {
			replicaName := pod.Labels[utils.RayWorkerReplicaNameKey]
			podsToDelete, ok := replicaMap[replicaName]
			if !ok {
				continue
			}
			logger.Info("Deleting unhealthy replica group.", "group", worker.GroupName, "replica", replicaName, "reason", reason)
			if err := r.deletePods(ctx, instance, podsToDelete, worker.GroupName, reason); err != nil {
				return err
			}
			// All Pods in the group have been deleted.
			for _, p := range podsToDelete {
				deletedPods[p.Name] = struct{}{}
			}
		}
	}

	// 4. Handle explicit deletions from the autoscaler.
	if len(worker.ScaleStrategy.WorkersToDelete) > 0 {
		podsToDeleteFromStrategy := make(map[string]corev1.Pod)
		for _, podName := range worker.ScaleStrategy.WorkersToDelete {
			for _, pod := range workerPods {
				if pod.Name == podName {
					replicaName := pod.Labels[utils.RayWorkerReplicaNameKey]
					for _, p := range replicaMap[replicaName] {
						podsToDeleteFromStrategy[p.Name] = p
					}
					break
				}
			}
		}

		if len(podsToDeleteFromStrategy) > 0 {
			logger.Info("removing the pods in the scaleStrategy of", "group", worker.GroupName, "podsToDelete", len(podsToDeleteFromStrategy))
			var podsToDel []corev1.Pod
			for _, p := range podsToDeleteFromStrategy {
				podsToDel = append(podsToDel, p)
			}
			if err := r.deletePods(ctx, instance, podsToDel, worker.GroupName, "autoscaler scale-down request"); err != nil {
				return err
			}
			return fmt.Errorf("deleted %d worker Pods based on ScaleStrategy, requeueing", len(podsToDel))
		}
	}

	// 5. Calculate Pod diff for scaling up or down by NumOfHosts.

	validReplicaGroups := make(map[string]struct{})
	for replicaName, podList := range replicaMap {
		isHealthyAndComplete := true
		for _, pod := range podList {
			if _, isDeleted := deletedPods[pod.Name]; isDeleted {
				isHealthyAndComplete = false
				break
			}
		}
		if isHealthyAndComplete {
			validReplicaGroups[replicaName] = struct{}{}
		}
	}
	numRunningReplicas := len(validReplicaGroups)
	numExpectedWorkerPods := int(utils.GetWorkerGroupDesiredReplicas(*worker))

	// Ensure that if numExpectedWorkerPods is not a multiple of NumOfHosts, we log an error.
	if numExpectedWorkerPods%int(worker.NumOfHosts) != 0 {
		return fmt.Errorf("desired worker pods (%d) is not a multiple of NumOfHosts (%d) for group %s",
			numExpectedWorkerPods, worker.NumOfHosts, worker.GroupName)
	}
	numExpectedReplicas := numExpectedWorkerPods / int(worker.NumOfHosts)
	replicasToCreate := numExpectedReplicas - numRunningReplicas

	// Track full replica groups to determine next replica index to assign to.
	validReplicaIndices := make(map[int]bool)
	for replicaName := range validReplicaGroups {
		if len(replicaMap[replicaName]) > 0 {
			pod := replicaMap[replicaName][0]
			if indexStr, ok := pod.Labels[utils.RayWorkerReplicaIndexKey]; ok {
				if index, err := strconv.Atoi(indexStr); err == nil {
					validReplicaIndices[index] = true
				}
			}
		}
	}
	logger.Info("Reconciling multi-host group", "group", worker.GroupName, "expectedReplicas", numExpectedReplicas, "runningReplicas", numRunningReplicas, "replicasToCreate", replicasToCreate, "inUseIndices", validReplicaIndices)
	if replicasToCreate > 0 {
		logger.Info("Scaling up multi-host group", "group", worker.GroupName, "replicasToCreate", replicasToCreate)
		if utils.IsTLSEnabled(&instance.Spec) {
			if err := r.checkMTLSSecretsReady(ctx, instance); err != nil {
				logger.Info("mTLS secrets not ready, requeuing before multi-host worker pod creation", "error", err.Error())
				return fmt.Errorf("mTLS secrets not ready: %w", err)
			}
		}
		newReplicaIndex := 0 // Find the next available index starting from 0
		for range replicasToCreate {
			for validReplicaIndices[newReplicaIndex] {
				newReplicaIndex++
			}
			validReplicaIndices[newReplicaIndex] = true
			replicaName := utils.GenerateRayWorkerReplicaGroupName(worker.GroupName)
			logger.Info("Creating new replica group", "group", worker.GroupName, "replicaName", replicaName, "replicaIndex", newReplicaIndex)
			for j := 0; j < int(worker.NumOfHosts); j++ {
				if err := r.createWorkerPodWithIndex(ctx, *instance, *worker.DeepCopy(), replicaName, newReplicaIndex, j); err != nil {
					return errstd.Join(utils.ErrFailedCreateWorkerPod, err)
				}
			}
		}
	} else if replicasToCreate < 0 {
		// Scale down NumOfHost workers per replica.
		enableInTreeAutoscaling := utils.IsAutoscalingEnabled(&instance.Spec)
		enableRandomPodDelete := false
		if enableInTreeAutoscaling {
			if s := os.Getenv(utils.ENABLE_RANDOM_POD_DELETE); strings.ToLower(s) == "true" {
				enableRandomPodDelete = true
			}
		}
		if !enableInTreeAutoscaling || enableRandomPodDelete {
			replicasToRemove := -replicasToCreate
			logger.Info("Scaling down multi-host group by randomly deleting replica groups", "group", worker.GroupName, "groupsToRemove", replicasToRemove)
			replicasDeleted := 0
			// Iterate over validReplicaGroups which contains the IDs of replica groups with NumOfHosts running Pods.
			for replicaID := range validReplicaGroups {
				if replicasDeleted >= replicasToRemove {
					break
				}
				podList := replicaMap[replicaID]
				if err := r.deletePods(ctx, instance, podList, worker.GroupName, "scaling down"); err != nil {
					return err
				}
				replicasDeleted++
			}
		} else {
			logger.Info("Random replica group deletion is disabled for the cluster. The only decision-maker for Pod deletions is the Ray Autoscaler.")
		}
	}

	return nil
}

func shouldSkipHeadPodRestart(instance *rayv1.RayCluster) bool {
	return instance.Annotations[utils.DisableProvisionedHeadRestartAnnotationKey] == "true"
}

// shouldRecreatePodsForUpgrade checks if any pods need to be recreated based on RayClusterSpec changes
func (r *RayClusterReconciler) shouldRecreatePodsForUpgrade(ctx context.Context, instance *rayv1.RayCluster) bool {
	logger := ctrl.LoggerFrom(ctx)
	if instance.Spec.UpgradeStrategy == nil || instance.Spec.UpgradeStrategy.Type == nil || *instance.Spec.UpgradeStrategy.Type != rayv1.RayClusterRecreate {
		return false
	}

	expectedClusterHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(instance.Spec)
	if err != nil {
		logger.Error(err, "Failed to generate cluster spec hash for Recreate upgradeStrategy, skipping comparison")
		return false
	}

	headPods := corev1.PodList{}
	if err := r.List(ctx, &headPods, common.RayClusterHeadPodsAssociationOptions(instance).ToListOptions()...); err != nil {
		logger.Error(err, "Failed to list head pods for upgrade check")
		return false
	}

	// If the KubeRay version has changed, skip recreation to avoid unnecessary pod recreation
	if len(headPods.Items) > 0 {
		headPod := headPods.Items[0]
		podVersion := headPod.Annotations[utils.KubeRayVersion]
		// If the KubeRay version has changed, update the head pod to get the cluster hash and new KubeRay version
		if podVersion != "" && podVersion != utils.KUBERAY_VERSION {
			logger.Info("KubeRay version has changed, skipping pod recreation", "rayCluster", instance.Name)
			headPod.Annotations[utils.UpgradeStrategyRecreateHashKey] = expectedClusterHash
			headPod.Annotations[utils.KubeRayVersion] = utils.KUBERAY_VERSION
			if err := r.Update(ctx, &headPod); err != nil {
				logger.Error(err, "Failed to update head pod annotations after KUBERAY_VERSION change", "pod", headPod.Name)
			}
			return false
		}
		actualHash := headPod.Annotations[utils.UpgradeStrategyRecreateHashKey]
		if actualHash != "" && actualHash != expectedClusterHash {
			logger.Info("RayCluster spec has changed, will recreate all pods", "rayCluster", instance.Name)
			return true
		}
	}
	return false
}

// shouldDeletePod returns whether the Pod should be deleted and the reason
//
// @param pod: The Pod to be checked.
// @param nodeType: The type of the node that the Pod belongs to (head or worker).
//
// @return: shouldDelete (bool), reason (string)
// (1) shouldDelete: Whether the Pod should be deleted.
// (2) reason: The reason why the Pod should or should not be deleted.
func shouldDeletePod(pod corev1.Pod, nodeType rayv1.RayNodeType) (bool, string) {
	// Based on the logic of the change of the status of the K8S pod, the following judgment is made.
	// https://github.com/kubernetes/kubernetes/blob/3361895612dac57044d5dacc029d2ace1865479c/pkg/kubelet/kubelet_pods.go#L1556

	// If the Pod's status is `Failed` or `Succeeded`, the Pod will not restart and we can safely delete it.
	if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		reason := fmt.Sprintf(
			"The %s Pod %s status is %s which is a terminal state. "+
				"KubeRay will delete the Pod and create new Pods in the next reconciliation if necessary.",
			nodeType, pod.Name, pod.Status.Phase)
		return true, reason
	}

	rayContainerTerminated := getRayContainerStateTerminated(pod)
	if pod.Status.Phase == corev1.PodRunning && rayContainerTerminated != nil {
		if pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
			reason := fmt.Sprintf(
				"The Pod status of the %s Pod %s is %s, and the Ray container terminated status is %v. "+
					"The container is unable to restart due to its restart policy %s, so KubeRay will delete it.",
				nodeType, pod.Name, pod.Status.Phase, rayContainerTerminated, pod.Spec.RestartPolicy)
			return true, reason
		}
		// If restart policy is set to `Always` or `OnFailure`, KubeRay will not delete the Pod.
		reason := fmt.Sprintf(
			"The Pod status of the %s Pod %s is %s, and the Ray container terminated status is %v. "+
				"However, KubeRay will not delete the Pod because its restartPolicy is set to %s and it should be able to restart automatically.",
			nodeType, pod.Name, pod.Status.Phase, rayContainerTerminated, pod.Spec.RestartPolicy)
		return false, reason
	}

	// TODO (kevin85421): Consider deleting a Pod if its Ray container restarts excessively, as this might
	// suggest an unhealthy Kubernetes node. Deleting and then recreating the Pod might allow it to be
	// scheduled on a different node.
	//
	// (1) Head Pod:
	// It's aggressive to delete a head Pod that is not in a terminated state (i.e., `Failed` or `Succeeded`).
	// We should only delete a head Pod when GCS fault tolerance is enabled, and drain the head Pod before
	// deleting it.
	//
	// (2) Worker Pod:
	// Compared to deleting a head Pod, removing a worker Pod is less aggressive and aligns more closely with
	// the behavior of the Ray Autoscaler. Nevertheless, we should still carefully drain the node before deleting
	// the worker Pod. Enabling GCS fault tolerance might not be necessary when deleting worker Pods. Note that
	// the Ray Autoscaler will not delete any worker Pods that have never been registered with the Ray cluster.
	// Therefore, we may need to address the Ray Autoscaler's blind spots.

	reason := fmt.Sprintf(
		"KubeRay does not need to delete the %s Pod %s. The Pod status is %s, and the Ray container terminated status is %v.",
		nodeType, pod.Name, pod.Status.Phase, rayContainerTerminated)
	return false, reason
}

// `ContainerStatuses` does not guarantee the order of the containers. Therefore, we need to find the Ray
// container's status by name. See the following links for more details:
// (1) https://discuss.kubernetes.io/t/pod-spec-containers-and-pod-status-containerstatuses-can-have-a-different-order-why/25273
// (2) https://github.com/kubernetes/kubernetes/blob/03762cbcb52b2a4394e4d795f9d3517a78a5e1a2/pkg/api/v1/pod/util.go#L261-L268
func getRayContainerStateTerminated(pod corev1.Pod) *corev1.ContainerStateTerminated {
	rayContainerName := pod.Spec.Containers[utils.RayContainerIndex].Name
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == rayContainerName {
			return containerStatus.State.Terminated
		}
	}
	// If the Ray container isn't found, we'll assume it hasn't terminated. This scenario
	// typically arises during testing (`raycluster_controller_test.go`) as `envtest` lacks
	// a Pod controller, preventing automatic Pod status updates.
	return nil
}

func (r *RayClusterReconciler) createHeadIngress(ctx context.Context, ingress *networkingv1.Ingress, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	if err := controllerutil.SetControllerReference(instance, ingress, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, ingress); err != nil {
		if errors.IsAlreadyExists(err) {
			logger.Info("Ingress already exists, no need to create")
			return nil
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateIngress), string(utils.CreateAction), "Failed creating ingress %s/%s, %v", ingress.Namespace, ingress.Name, err)
		return err
	}
	logger.Info("Created ingress for RayCluster", "name", ingress.Name)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedIngress), string(utils.CreateAction), "Created ingress %s/%s", ingress.Namespace, ingress.Name)
	return nil
}

func (r *RayClusterReconciler) createHeadRoute(ctx context.Context, route *routev1.Route, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	// making sure the name is valid
	route.Name = utils.CheckRouteName(ctx, route.Name, route.Namespace)

	if err := r.Create(ctx, route); err != nil {
		if errors.IsAlreadyExists(err) {
			logger.Info("Route already exists, no need to create")
			return nil
		}
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRoute), string(utils.CreateAction), "Failed creating route %s/%s, %v", route.Namespace, route.Name, err)
		return err
	}
	logger.Info("Created route for RayCluster", "name", route.Name)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedRoute), string(utils.CreateAction), "Created route %s/%s", route.Namespace, route.Name)
	return nil
}

func (r *RayClusterReconciler) createService(ctx context.Context, svc *corev1.Service, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	if err := controllerutil.SetControllerReference(instance, svc, r.Scheme); err != nil {
		return err
	}

	if err := r.Create(ctx, svc); err != nil {
		r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateService), string(utils.CreateAction), "Failed creating service %s/%s, %v", svc.Namespace, svc.Name, err)
		return err
	}
	logger.Info("Created service for RayCluster", "name", svc.Name)
	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedService), string(utils.CreateAction), "Created service %s/%s", svc.Namespace, svc.Name)
	return nil
}

func (r *RayClusterReconciler) createHeadPod(ctx context.Context, instance rayv1.RayCluster, clusterHash string) error {
	logger := ctrl.LoggerFrom(ctx)

	// build the pod then create it
	pod := r.buildHeadPod(ctx, instance)

	// Set RayClusterUpgradeStrategyHashKey and KubeRayVersion annotations
	if clusterHash != "" {
		pod.Annotations[utils.UpgradeStrategyRecreateHashKey] = clusterHash
		pod.Annotations[utils.KubeRayVersion] = utils.KUBERAY_VERSION
	}

	// check if the batch scheduler integration is enabled
	// call the scheduler plugin if so
	if r.options.BatchSchedulerManager != nil {
		if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
			scheduler.AddMetadataToChildResource(ctx, &instance, &pod, utils.RayNodeHeadGroupLabelValue)
		} else {
			return err
		}
	}

	if err := r.Create(ctx, &pod); err != nil {
		r.Recorder.Eventf(&instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateHeadPod), string(utils.CreateAction), "Failed to create head Pod %s/%s, %v", pod.Namespace, pod.Name, err)
		return err
	}
	r.rayClusterScaleExpectation.ExpectScalePod(pod.Namespace, instance.Name, expectations.HeadGroup, pod.Name, expectations.Create)
	logger.Info("Created head Pod for RayCluster", "name", pod.Name)
	r.Recorder.Eventf(&instance, nil, corev1.EventTypeNormal, string(utils.CreatedHeadPod), string(utils.CreateAction), "Created head Pod %s/%s", pod.Namespace, pod.Name)
	return nil
}

func (r *RayClusterReconciler) createWorkerPod(ctx context.Context, instance rayv1.RayCluster, worker rayv1.WorkerGroupSpec) error {
	logger := ctrl.LoggerFrom(ctx)

	// build the pod then create it
	pod := r.buildWorkerPod(ctx, instance, worker, "", 0, 0)
	if r.options.BatchSchedulerManager != nil {
		if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
			scheduler.AddMetadataToChildResource(ctx, &instance, &pod, worker.GroupName)
		} else {
			return err
		}
	}

	replica := pod
	if err := r.Create(ctx, &replica); err != nil {
		r.Recorder.Eventf(&instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateWorkerPod), string(utils.CreateAction), "Failed to create worker Pod for the cluster %s/%s, %v", instance.Namespace, instance.Name, err)
		return err
	}
	r.rayClusterScaleExpectation.ExpectScalePod(replica.Namespace, instance.Name, worker.GroupName, replica.Name, expectations.Create)
	logger.Info("Created worker Pod for RayCluster", "name", replica.Name)
	r.Recorder.Eventf(&instance, nil, corev1.EventTypeNormal, string(utils.CreatedWorkerPod), string(utils.CreateAction), "Created worker Pod %s/%s", replica.Namespace, replica.Name)
	return nil
}

func (r *RayClusterReconciler) createWorkerPodWithIndex(ctx context.Context, instance rayv1.RayCluster, worker rayv1.WorkerGroupSpec, replicaGrpName string, replicaIndex int, hostIndex int) error {
	logger := ctrl.LoggerFrom(ctx)

	// build the pod then create it
	pod := r.buildWorkerPod(ctx, instance, worker, replicaGrpName, replicaIndex, hostIndex)
	if r.options.BatchSchedulerManager != nil {
		if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
			scheduler.AddMetadataToChildResource(ctx, &instance, &pod, worker.GroupName)
		} else {
			return err
		}
	}

	replica := pod
	if err := r.Create(ctx, &replica); err != nil {
		r.Recorder.Eventf(&instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateWorkerPod), string(utils.CreateAction), "Failed to create worker Pod for the cluster %s/%s, %v", instance.Namespace, instance.Name, err)
		return err
	}
	r.rayClusterScaleExpectation.ExpectScalePod(replica.Namespace, instance.Name, worker.GroupName, replica.Name, expectations.Create)
	logger.Info("Created worker Pod for RayCluster", "name", replica.Name)
	r.Recorder.Eventf(&instance, nil, corev1.EventTypeNormal, string(utils.CreatedWorkerPod), string(utils.CreateAction), "Created worker Pod %s/%s", replica.Namespace, replica.Name)
	return nil
}

// Build head instance pod(s).
func (r *RayClusterReconciler) buildHeadPod(ctx context.Context, instance rayv1.RayCluster) corev1.Pod {
	logger := ctrl.LoggerFrom(ctx)
	podName := utils.PodName(instance.Name, rayv1.HeadNode, !utils.IsDeterministicHeadPodNameEnabled())
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, instance, instance.Namespace) // Fully Qualified Domain Name

	// The Ray head port used by workers to connect to the cluster (GCS server port for Ray >= 1.11.0, Redis port for older Ray.)
	headPort := common.GetHeadPort(instance.Spec.HeadGroupSpec.RayStartParams)
	autoscalingEnabled := utils.IsAutoscalingEnabled(&instance.Spec)
	podConf := common.DefaultHeadPodTemplate(ctx, instance, instance.Spec.HeadGroupSpec, podName, headPort)
	common.SetDefaultCollectorImage(&instance, &podConf, r.options.CollectorImage)
	if len(r.options.HeadSidecarContainers) > 0 {
		podConf.Spec.Containers = append(podConf.Spec.Containers, r.options.HeadSidecarContainers...)
	}
	if len(r.options.DefaultPodAnnotations) > 0 {
		if podConf.Annotations == nil {
			podConf.Annotations = make(map[string]string)
		}
		for k, v := range r.options.DefaultPodAnnotations {
			if _, exists := podConf.Annotations[k]; !exists {
				podConf.Annotations[k] = v
			}
		}
	}
	if len(r.options.DefaultPodLabels) > 0 {
		if podConf.Labels == nil {
			podConf.Labels = make(map[string]string)
		}
		for k, v := range r.options.DefaultPodLabels {
			if _, exists := podConf.Labels[k]; !exists {
				podConf.Labels[k] = v
			}
		}
	}
	logger.Info("head pod labels", "labels", podConf.Labels)
	creatorCRDType := getCreatorCRDType(instance)
	pod := common.BuildPod(ctx, podConf, rayv1.HeadNode, instance.Spec.HeadGroupSpec.RayStartParams, headPort, autoscalingEnabled, creatorCRDType, fqdnRayIP, r.options.DefaultContainerEnvs, instance.Spec.RayVersion)
	// Set raycluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(&instance, &pod, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference for raycluster pod")
	}

	return pod
}

func getCreatorCRDType(instance rayv1.RayCluster) utils.CRDType {
	return utils.GetCRDType(instance.Labels[utils.RayOriginatedFromCRDLabelKey])
}

// Build worker instance pods.
func (r *RayClusterReconciler) buildWorkerPod(ctx context.Context, instance rayv1.RayCluster, worker rayv1.WorkerGroupSpec, replicaGrpName string, replicaIndex int, hostIndex int) corev1.Pod {
	logger := ctrl.LoggerFrom(ctx)
	podName := utils.PodName(fmt.Sprintf("%s-%s", instance.Name, worker.GroupName), rayv1.WorkerNode, true)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, instance, instance.Namespace) // Fully Qualified Domain Name

	// The Ray head port used by workers to connect to the cluster (GCS server port for Ray >= 1.11.0, Redis port for older Ray.)
	headPort := common.GetHeadPort(instance.Spec.HeadGroupSpec.RayStartParams)
	autoscalingEnabled := utils.IsAutoscalingEnabled(&instance.Spec)
	podTemplateSpec := common.DefaultWorkerPodTemplate(ctx, instance, worker, podName, fqdnRayIP, headPort, replicaGrpName, replicaIndex, hostIndex)
	common.SetDefaultCollectorImage(&instance, &podTemplateSpec, r.options.CollectorImage)
	if len(r.options.WorkerSidecarContainers) > 0 {
		podTemplateSpec.Spec.Containers = append(podTemplateSpec.Spec.Containers, r.options.WorkerSidecarContainers...)
	}
	if len(r.options.DefaultPodAnnotations) > 0 {
		if podTemplateSpec.Annotations == nil {
			podTemplateSpec.Annotations = make(map[string]string)
		}
		for k, v := range r.options.DefaultPodAnnotations {
			if _, exists := podTemplateSpec.Annotations[k]; !exists {
				podTemplateSpec.Annotations[k] = v
			}
		}
	}
	if len(r.options.DefaultPodLabels) > 0 {
		if podTemplateSpec.Labels == nil {
			podTemplateSpec.Labels = make(map[string]string)
		}
		for k, v := range r.options.DefaultPodLabels {
			if _, exists := podTemplateSpec.Labels[k]; !exists {
				podTemplateSpec.Labels[k] = v
			}
		}
	}
	creatorCRDType := getCreatorCRDType(instance)
	pod := common.BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, headPort, autoscalingEnabled, creatorCRDType, fqdnRayIP, r.options.DefaultContainerEnvs, instance.Spec.RayVersion)
	// Set raycluster instance as the owner and controller
	if err := controllerutil.SetControllerReference(&instance, &pod, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference for raycluster pod")
	}

	return pod
}

func (r *RayClusterReconciler) buildRedisCleanupJob(ctx context.Context, instance rayv1.RayCluster) batchv1.Job {
	logger := ctrl.LoggerFrom(ctx)

	// Build the head pod
	pod := r.buildHeadPod(ctx, instance)
	pod.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.RedisCleanupNode)

	// Only keep the Ray container in the Redis cleanup Job.
	pod.Spec.Containers = []corev1.Container{pod.Spec.Containers[utils.RayContainerIndex]}

	// Remove the wait-for-tls-ip-san init container if present. The cleanup pod's IP is
	// never added to the head certificate SANs (the mTLS controller only tracks running
	// Ray pods), so the init container would block for the full timeout and then fail,
	// preventing Redis cleanup from running at all.
	pod.Spec.InitContainers = slices.DeleteFunc(pod.Spec.InitContainers, func(c corev1.Container) bool {
		return c.Name == "wait-for-tls-ip-san"
	})

	pod.Spec.Containers[utils.RayContainerIndex].Command = utils.GetContainerCommand([]string{})
	pod.Spec.Containers[utils.RayContainerIndex].Args = []string{
		"echo \"To get more information about manually deleting the storage namespace in Redis and removing the RayCluster's finalizer, please check https://docs.ray.io/en/master/cluster/kubernetes/user-guides/kuberay-gcs-ft.html for more details.\" && " +
			"python -c " +
			"\"from ray._private.gcs_utils import cleanup_redis_storage; " +
			"from urllib.parse import urlparse; " +
			"import os; " +
			"import sys; " +
			"redis_address = os.getenv('RAY_REDIS_ADDRESS', '').split(',')[0]; " +
			"redis_address = redis_address if '://' in redis_address else 'redis://' + redis_address; " +
			"parsed = urlparse(redis_address); ",
	}
	if utils.EnvVarExists(utils.REDIS_USERNAME, pod.Spec.Containers[utils.RayContainerIndex].Env) {
		pod.Spec.Containers[utils.RayContainerIndex].Args[0] += "sys.exit(1) if not cleanup_redis_storage(host=parsed.hostname, port=parsed.port, username=os.getenv('REDIS_USERNAME', parsed.username), password=os.getenv('REDIS_PASSWORD', parsed.password or ''), use_ssl=parsed.scheme=='rediss', storage_namespace=os.getenv('RAY_external_storage_namespace')) else None\""
	} else {
		pod.Spec.Containers[utils.RayContainerIndex].Args[0] += "sys.exit(1) if not cleanup_redis_storage(host=parsed.hostname, port=parsed.port, password=os.getenv('REDIS_PASSWORD', parsed.password or ''), use_ssl=parsed.scheme=='rediss', storage_namespace=os.getenv('RAY_external_storage_namespace')) else None\""
	}

	// Disable liveness and readiness probes because the Job will not launch processes like Raylet and GCS.
	pod.Spec.Containers[utils.RayContainerIndex].LivenessProbe = nil
	pod.Spec.Containers[utils.RayContainerIndex].ReadinessProbe = nil

	// Set the environment variables to ensure that the cleanup Job has at least 60s.
	pod.Spec.Containers[utils.RayContainerIndex].Env = append(pod.Spec.Containers[utils.RayContainerIndex].Env, corev1.EnvVar{
		Name:  "RAY_redis_db_connect_retries",
		Value: "120",
	})
	pod.Spec.Containers[utils.RayContainerIndex].Env = append(pod.Spec.Containers[utils.RayContainerIndex].Env, corev1.EnvVar{
		Name:  "RAY_redis_db_connect_wait_milliseconds",
		Value: "500",
	})

	// The container's resource consumption remains constant. Hard-coding the resources is acceptable.
	// Avoid using the GPU for the Redis cleanup Job.
	pod.Spec.Containers[utils.RayContainerIndex].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("200m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}

	// For Kubernetes Job, the valid values for Pod's `RestartPolicy` are `Never` and `OnFailure`.
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever

	// Trim the job name to ensure it is within the 63-character limit.
	jobName := utils.TrimJobName(fmt.Sprintf("%s-%s", instance.Name, "redis-cleanup"))

	redisCleanupJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        jobName,
			Namespace:   instance.Namespace,
			Labels:      pod.Labels,
			Annotations: pod.Annotations,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To[int32](0),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: pod.ObjectMeta,
				Spec:       pod.Spec,
			},
			// Make this job best-effort only for 5 minutes.
			ActiveDeadlineSeconds: ptr.To[int64](300),
		},
	}

	if err := controllerutil.SetControllerReference(&instance, &redisCleanupJob, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference for the Redis cleanup Job.")
	}

	return redisCleanupJob
}

// checkMTLSSecretsReady verifies that cert-manager has finished (re)issuing the TLS
// certificates at the current generation and that the resulting secrets exist and contain
// the required keys. This prevents pods from being created while an older secret is still
// mounted during a SAN reissue (e.g. after a pod IP is added). Checking ObservedGeneration
// on the condition ensures Ready=True reflects the current spec, not a previous generation.
func (r *RayClusterReconciler) checkMTLSSecretsReady(ctx context.Context, instance *rayv1.RayCluster) error {
	certNames := []string{
		utils.GetTLSCertName(instance.Name, rayv1.HeadNode),
		utils.GetTLSCertName(instance.Name, rayv1.WorkerNode),
	}
	for _, certName := range certNames {
		cert := &certmanagerv1.Certificate{}
		if err := r.Get(ctx, client.ObjectKey{Name: certName, Namespace: instance.Namespace}, cert); err != nil {
			return fmt.Errorf("certificate %s not found: %w", certName, err)
		}
		if !isCertificateReady(cert) {
			return fmt.Errorf("certificate %s is not ready at current generation %d", certName, cert.Generation)
		}
	}

	secretNames := []string{
		utils.GetTLSSecretName(instance.Name, rayv1.HeadNode),
		utils.GetTLSSecretName(instance.Name, rayv1.WorkerNode),
	}
	requiredKeys := []string{"tls.crt", "tls.key", "ca.crt"}
	for _, secretName := range secretNames {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, client.ObjectKey{Name: secretName, Namespace: instance.Namespace}, secret); err != nil {
			return fmt.Errorf("secret %s not found: %w", secretName, err)
		}
		for _, key := range requiredKeys {
			if _, ok := secret.Data[key]; !ok {
				return fmt.Errorf("secret %s missing required key %s", secretName, key)
			}
		}
	}
	return nil
}

// SetupWithManager builds the reconciler.
func (r *RayClusterReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayCluster{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
		))).
		Owns(&corev1.Pod{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.PersistentVolumeClaim{})
	if r.options.BatchSchedulerManager != nil {
		r.options.BatchSchedulerManager.ConfigureReconciler(b)
	}

	return b.
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayCluster")
				if request != nil {
					logger = logger.WithValues("RayCluster", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

func (r *RayClusterReconciler) calculateStatus(ctx context.Context, instance *rayv1.RayCluster, reconcileErr error) (*rayv1.RayCluster, error) {
	// TODO: Replace this log and use reconcileErr to set the condition field.
	logger := ctrl.LoggerFrom(ctx)
	if reconcileErr != nil {
		logger.Info("Reconciliation error", "error", reconcileErr)
	}

	// Deep copy the instance, so we don't mutate the original object.
	newInstance := instance.DeepCopy()

	statusConditionGateEnabled := features.Enabled(features.RayClusterStatusConditions)
	if statusConditionGateEnabled {
		if reconcileErr != nil {
			if reason := utils.RayClusterReplicaFailureReason(reconcileErr); reason != "" {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:    string(rayv1.RayClusterReplicaFailure),
					Status:  metav1.ConditionTrue,
					Reason:  reason,
					Message: reconcileErr.Error(),
				})
			}
		} else {
			// if reconcileErr == nil, we can safely remove the RayClusterReplicaFailure condition.
			meta.RemoveStatusCondition(&newInstance.Status.Conditions, string(rayv1.RayClusterReplicaFailure))
		}
	}

	// TODO (kevin85421): ObservedGeneration should be used to determine whether to update this CR or not.
	newInstance.Status.ObservedGeneration = newInstance.ObjectMeta.Generation

	runtimePods := corev1.PodList{}
	if err := r.List(ctx, &runtimePods, common.RayClusterAllPodsAssociationOptions(newInstance).ToListOptions()...); err != nil {
		return nil, err
	}

	newInstance.Status.ReadyWorkerReplicas = utils.CalculateReadyReplicas(runtimePods)
	newInstance.Status.AvailableWorkerReplicas = utils.CalculateAvailableReplicas(runtimePods)
	newInstance.Status.DesiredWorkerReplicas = utils.CalculateDesiredReplicas(newInstance)
	newInstance.Status.MinWorkerReplicas = utils.CalculateMinReplicas(newInstance)
	newInstance.Status.MaxWorkerReplicas = utils.CalculateMaxReplicas(newInstance)

	totalResources := utils.CalculateDesiredResources(newInstance)
	newInstance.Status.DesiredCPU = totalResources[corev1.ResourceCPU]
	newInstance.Status.DesiredMemory = totalResources[corev1.ResourceMemory]
	newInstance.Status.DesiredGPU = sumGPUs(totalResources)
	newInstance.Status.DesiredTPU = totalResources[corev1.ResourceName("google.com/tpu")]

	if reconcileErr == nil && len(runtimePods.Items) == int(newInstance.Status.DesiredWorkerReplicas)+1 { // workers + 1 head
		if utils.CheckAllPodsRunning(ctx, runtimePods) {
			newInstance.Status.State = rayv1.Ready
			newInstance.Status.Reason = ""
		}
	}

	// Check if the head node is running and ready by checking the head pod's status or if the cluster has been suspended.
	if statusConditionGateEnabled {
		headPod, err := common.GetRayClusterHeadPod(ctx, r, newInstance)
		if err != nil {
			return nil, err
		}
		// GetRayClusterHeadPod can return nil, nil when pod is not found, we handle it separately.
		if headPod == nil {
			meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
				Type:    string(rayv1.HeadPodReady),
				Status:  metav1.ConditionFalse,
				Reason:  rayv1.HeadPodNotFound,
				Message: "Head Pod not found",
			})
		} else {
			headPodReadyCondition := utils.FindHeadPodReadyCondition(headPod)
			meta.SetStatusCondition(&newInstance.Status.Conditions, headPodReadyCondition)
		}

		suspendStatus := utils.FindRayClusterSuspendStatus(newInstance)
		if !meta.IsStatusConditionTrue(newInstance.Status.Conditions, string(rayv1.RayClusterProvisioned)) && suspendStatus != rayv1.RayClusterSuspended {
			// RayClusterProvisioned indicates whether all Ray Pods are ready when the RayCluster is first created.
			// Note RayClusterProvisioned StatusCondition will not be updated after all Ray Pods are ready for the first time. Unless the cluster has been suspended.
			if utils.CheckAllPodsRunning(ctx, runtimePods) {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:    string(rayv1.RayClusterProvisioned),
					Status:  metav1.ConditionTrue,
					Reason:  rayv1.AllPodRunningAndReadyFirstTime,
					Message: "All Ray Pods are ready for the first time",
				})
			} else {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:    string(rayv1.RayClusterProvisioned),
					Status:  metav1.ConditionFalse,
					Reason:  rayv1.RayClusterPodsProvisioning,
					Message: "RayCluster Pods are being provisioned for first time",
				})
			}
		}

		switch suspendStatus {
		case rayv1.RayClusterSuspending:
			if len(runtimePods.Items) == 0 {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:    string(rayv1.RayClusterProvisioned),
					Status:  metav1.ConditionFalse,
					Reason:  rayv1.RayClusterPodsProvisioning,
					Message: "RayCluster has been suspended",
				})
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:   string(rayv1.RayClusterSuspending),
					Reason: string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionFalse,
				})
				suspendReason := rayv1.RayClusterSuspended
				if instance.Spec.IdleSuspend != nil && *instance.Spec.IdleSuspend {
					suspendReason = rayv1.RayClusterIdleSuspended
				}
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:   string(rayv1.RayClusterSuspended),
					Reason: string(suspendReason),
					Status: metav1.ConditionTrue,
				})
			}
		case rayv1.RayClusterSuspended:
			if !utils.IsRayClusterSuspendOrIdleSuspend(instance) {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:   string(rayv1.RayClusterSuspended),
					Reason: string(rayv1.RayClusterSuspended),
					Status: metav1.ConditionFalse,
				})
			}
		default:
			meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
				Type:   string(rayv1.RayClusterSuspended),
				Reason: string(rayv1.RayClusterSuspended),
				Status: metav1.ConditionFalse,
			})
			if utils.IsRayClusterSuspendOrIdleSuspend(instance) {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:   string(rayv1.RayClusterSuspending),
					Reason: string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionTrue,
				})
			} else {
				meta.SetStatusCondition(&newInstance.Status.Conditions, metav1.Condition{
					Type:   string(rayv1.RayClusterSuspending),
					Reason: string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionFalse,
				})
			}
		}
	}

	if utils.IsRayClusterSuspendOrIdleSuspend(newInstance) && len(runtimePods.Items) == 0 {
		newInstance.Status.State = rayv1.Suspended
	}

	if err := r.updateEndpoints(ctx, newInstance); err != nil {
		return nil, err
	}

	if err := r.updateHeadInfo(ctx, newInstance); err != nil {
		return nil, err
	}

	timeNow := metav1.Now()
	newInstance.Status.LastUpdateTime = &timeNow

	if instance.Status.State != newInstance.Status.State {
		if newInstance.Status.StateTransitionTimes == nil {
			newInstance.Status.StateTransitionTimes = make(map[rayv1.ClusterState]*metav1.Time)
		}
		newInstance.Status.StateTransitionTimes[newInstance.Status.State] = &timeNow
	}

	return newInstance, nil
}

func (r *RayClusterReconciler) getHeadServiceIPAndName(ctx context.Context, instance *rayv1.RayCluster) (string, string, error) {
	runtimeServices := corev1.ServiceList{}
	if err := r.List(ctx, &runtimeServices, common.RayClusterHeadServiceListOptions(instance)...); err != nil {
		return "", "", err
	}
	if len(runtimeServices.Items) < 1 {
		return "", "", fmt.Errorf("unable to find head service. cluster name %s, filter labels %v", instance.Name, common.RayClusterHeadServiceListOptions(instance))
	} else if len(runtimeServices.Items) > 1 {
		return "", "", fmt.Errorf("found multiple head services. cluster name %s, filter labels %v", instance.Name, common.RayClusterHeadServiceListOptions(instance))
	} else if runtimeServices.Items[0].Spec.ClusterIP == "" {
		return "", "", fmt.Errorf("head service IP is empty. cluster name %s, filter labels %v", instance.Name, common.RayClusterHeadServiceListOptions(instance))
	} else if runtimeServices.Items[0].Spec.ClusterIP == corev1.ClusterIPNone {
		// We return Head Pod IP if the Head service is headless.
		headPod, err := common.GetRayClusterHeadPod(ctx, r, instance)
		if err != nil {
			return "", "", err
		}
		if headPod != nil {
			return headPod.Status.PodIP, runtimeServices.Items[0].Name, nil
		}
		return "", runtimeServices.Items[0].Name, nil
	}

	return runtimeServices.Items[0].Spec.ClusterIP, runtimeServices.Items[0].Name, nil
}

func (r *RayClusterReconciler) updateEndpoints(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	// TODO: (@scarlet25151) There may be several K8s Services for a RayCluster.
	// We assume we can find the right one by filtering Services with appropriate label selectors
	// and picking the first one. We may need to select by name in the future if the Service naming is stable.
	rayHeadSvc := corev1.ServiceList{}
	filterLabels := common.RayClusterHeadServiceListOptions(instance)
	if err := r.List(ctx, &rayHeadSvc, filterLabels...); err != nil {
		return err
	}

	if len(rayHeadSvc.Items) != 0 {
		svc := rayHeadSvc.Items[0]
		if instance.Status.Endpoints == nil {
			instance.Status.Endpoints = map[string]string{}
		}
		for _, port := range svc.Spec.Ports {
			if len(port.Name) == 0 {
				logger.Info("updateStatus: Service port's name is empty. Not adding it to RayCluster status.endpoints", "port", port)
				continue
			}
			if port.NodePort != 0 {
				instance.Status.Endpoints[port.Name] = fmt.Sprintf("%d", port.NodePort)
			} else if port.TargetPort.IntVal != 0 {
				instance.Status.Endpoints[port.Name] = fmt.Sprintf("%d", port.TargetPort.IntVal)
			} else if port.TargetPort.StrVal != "" {
				instance.Status.Endpoints[port.Name] = port.TargetPort.StrVal
			} else {
				logger.Info("updateStatus: Service port's targetPort is empty. Not adding it to RayCluster status.endpoints", "port", port)
			}
		}
	} else {
		logger.Info("updateEndpoints: Unable to find a Service for this RayCluster. Not adding RayCluster status.endpoints", "serviceSelectors", filterLabels)
	}

	return nil
}

func (r *RayClusterReconciler) updateHeadInfo(ctx context.Context, instance *rayv1.RayCluster) error {
	headPod, err := common.GetRayClusterHeadPod(ctx, r, instance)
	if err != nil {
		return err
	}
	if headPod != nil {
		instance.Status.Head.PodIP = headPod.Status.PodIP
		instance.Status.Head.PodName = headPod.Name
	} else {
		instance.Status.Head.PodIP = ""
		instance.Status.Head.PodName = ""
	}

	ip, name, err := r.getHeadServiceIPAndName(ctx, instance)
	if err != nil {
		return err
	}
	instance.Status.Head.ServiceIP = ip
	instance.Status.Head.ServiceName = name

	return nil
}

func (r *RayClusterReconciler) reconcileAutoscalerServiceAccount(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	if !utils.IsAutoscalingEnabled(&instance.Spec) {
		return nil
	}

	serviceAccount := &corev1.ServiceAccount{}
	namespacedName := common.RayClusterAutoscalerServiceAccountNamespacedName(instance)

	if err := r.Get(ctx, namespacedName, serviceAccount); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// If users specify ServiceAccountName for the head Pod, they need to create a ServiceAccount themselves.
		// However, if KubeRay creates a ServiceAccount for users, the autoscaler may encounter permission issues during
		// zero-downtime rolling updates when RayService is performed. See https://github.com/ray-project/kuberay/issues/1123
		// for more details.
		if instance.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName == namespacedName.Name {
			actionableMessage := fmt.Sprintf("If users specify ServiceAccountName for the head Pod, they need to create a ServiceAccount themselves. "+
				"However, ServiceAccount %s is not found. Please create one. See the PR description of https://github.com/ray-project/kuberay/pull/1128 for more details.", namespacedName.Name)

			logger.Error(err, actionableMessage)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.AutoscalerServiceAccountNotFound), string(utils.ValidateAction), "Failed to reconcile RayCluster %s/%s. %s", instance.Namespace, instance.Name, actionableMessage)
			return err
		}

		// Create service account for autoscaler if there's no existing one in the cluster.
		serviceAccount, err := common.BuildServiceAccount(instance)
		if err != nil {
			return err
		}

		// making sure the name is valid
		serviceAccount.Name = utils.CheckName(serviceAccount.Name)

		// Set controller reference
		if err := controllerutil.SetControllerReference(instance, serviceAccount, r.Scheme); err != nil {
			return err
		}

		if err := r.Create(ctx, serviceAccount); err != nil {
			if errors.IsAlreadyExists(err) {
				logger.Info("Pod service account already exist, no need to create")
				return nil
			}
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateServiceAccount), string(utils.CreateAction), "Failed creating service account %s/%s, %v", serviceAccount.Namespace, serviceAccount.Name, err)
			return err
		}
		logger.Info("Created service account for Ray Autoscaler", "name", serviceAccount.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedServiceAccount), string(utils.CreateAction), "Created service account %s/%s", serviceAccount.Namespace, serviceAccount.Name)
		return nil
	}

	return nil
}

func (r *RayClusterReconciler) reconcileAutoscalerRole(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	if !utils.IsAutoscalingEnabled(&instance.Spec) {
		return nil
	}

	role := &rbacv1.Role{}
	namespacedName := common.RayClusterAutoscalerRoleNamespacedName(instance)
	if err := r.Get(ctx, namespacedName, role); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// Create role for autoscaler if there's no existing one in the cluster.
		role, err := common.BuildRole(instance)
		if err != nil {
			return err
		}

		// making sure the name is valid
		role.Name = utils.CheckName(role.Name)
		// Set controller reference
		if err := controllerutil.SetControllerReference(instance, role, r.Scheme); err != nil {
			return err
		}

		if err := r.Create(ctx, role); err != nil {
			if errors.IsAlreadyExists(err) {
				logger.Info("role already exist, no need to create")
				return nil
			}
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRole), string(utils.CreateAction), "Failed creating role %s/%s, %v", role.Namespace, role.Name, err)
			return err
		}
		logger.Info("Created role for Ray Autoscaler", "name", role.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedRole), string(utils.CreateAction), "Created role %s/%s", role.Namespace, role.Name)
		return nil
	}

	return nil
}

func (r *RayClusterReconciler) reconcileAutoscalerRoleBinding(ctx context.Context, instance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	if !utils.IsAutoscalingEnabled(&instance.Spec) {
		return nil
	}

	roleBinding := &rbacv1.RoleBinding{}
	namespacedName := common.RayClusterAutoscalerRoleBindingNamespacedName(instance)
	if err := r.Get(ctx, namespacedName, roleBinding); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		// Create role bindings for autoscaler if there's no existing one in the cluster.
		roleBinding, err := common.BuildRoleBinding(instance)
		if err != nil {
			return err
		}

		// making sure the name is valid
		roleBinding.Name = utils.CheckName(roleBinding.Name)
		// Set controller reference
		if err := controllerutil.SetControllerReference(instance, roleBinding, r.Scheme); err != nil {
			return err
		}

		if err := r.Create(ctx, roleBinding); err != nil {
			if errors.IsAlreadyExists(err) {
				logger.Info("role binding already exist, no need to create")
				return nil
			}
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRoleBinding), string(utils.CreateAction), "Failed creating role binding %s/%s, %v", roleBinding.Namespace, roleBinding.Name, err)
			return err
		}
		logger.Info("Created role binding for Ray Autoscaler", "name", roleBinding.Name)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.CreatedRoleBinding), string(utils.CreateAction), "Created role binding %s/%s", roleBinding.Namespace, roleBinding.Name)
		return nil
	}

	return nil
}

// updateRayClusterStatus updates the RayCluster status if it is inconsistent with the old status and returns a bool to indicate the inconsistency.
// We rely on the returning bool to requeue the reconciliation for atomic operations, such as suspending a RayCluster.
func (r *RayClusterReconciler) updateRayClusterStatus(ctx context.Context, originalRayClusterInstance, newInstance *rayv1.RayCluster) (bool, error) {
	logger := ctrl.LoggerFrom(ctx)
	inconsistent := utils.InconsistentRayClusterStatus(originalRayClusterInstance.Status, newInstance.Status)
	if !inconsistent {
		return inconsistent, nil
	}
	logger.Info("updateRayClusterStatus", "name", originalRayClusterInstance.Name, "old status", originalRayClusterInstance.Status, "new status", newInstance.Status)
	err := r.Status().Update(ctx, newInstance)
	if err != nil {
		logger.Info("Error updating status", "name", originalRayClusterInstance.Name, "error", err, "RayCluster", newInstance)
	} else {
		emitRayClusterMetrics(r.options.RayClusterMetricsManager, newInstance.Name, newInstance.Namespace, newInstance.UID, originalRayClusterInstance.Status, newInstance.Status, newInstance.CreationTimestamp.Time)
	}

	return inconsistent, err
}

func emitRayClusterMetrics(rayClusterMetricsManager *metrics.RayClusterMetricsManager, clusterName, namespace string, clusterUID types.UID, oldStatus, newStatus rayv1.RayClusterStatus, creationTimestamp time.Time) {
	if rayClusterMetricsManager == nil {
		return
	}
	emitRayClusterProvisionedDuration(rayClusterMetricsManager, clusterName, namespace, clusterUID, oldStatus, newStatus, creationTimestamp)
}

func emitRayClusterProvisionedDuration(RayClusterMetricsObserver metrics.RayClusterMetricsObserver, clusterName, namespace string, clusterUID types.UID, oldStatus, newStatus rayv1.RayClusterStatus, creationTimestamp time.Time) {
	// Emit kuberay_cluster_provisioned_duration_seconds when a RayCluster's RayClusterProvisioned status transitions from false (or unset) to true
	if !meta.IsStatusConditionTrue(oldStatus.Conditions, string(rayv1.RayClusterProvisioned)) &&
		meta.IsStatusConditionTrue(newStatus.Conditions, string(rayv1.RayClusterProvisioned)) {
		RayClusterMetricsObserver.ObserveRayClusterProvisionedDuration(clusterName, namespace, clusterUID, time.Since(creationTimestamp).Seconds())
	}
}

func cleanUpRayClusterMetrics(rayClusterMetricsManager *metrics.RayClusterMetricsManager, clusterName, namespace string) {
	if rayClusterMetricsManager == nil {
		return
	}
	rayClusterMetricsManager.DeleteRayClusterMetrics(clusterName, namespace)
}

// sumGPUs sums the GPUs in the given resource list.
func sumGPUs(resources map[corev1.ResourceName]resource.Quantity) resource.Quantity {
	totalGPUs := resource.Quantity{}

	for key, val := range resources {
		if utils.IsGPUResourceKey(string(key)) && !val.IsZero() {
			totalGPUs.Add(val)
		}
	}

	return totalGPUs
}

// setDefaults sets some default values for the RayCluster
func setDefaults(instance *rayv1.RayCluster) {
	if instance.Spec.HeadGroupSpec.RayStartParams == nil {
		instance.Spec.HeadGroupSpec.RayStartParams = map[string]string{}
	}

	for i := range instance.Spec.WorkerGroupSpecs {
		if instance.Spec.WorkerGroupSpecs[i].RayStartParams == nil {
			instance.Spec.WorkerGroupSpecs[i].RayStartParams = map[string]string{}
		}
	}
}

// hasGCSFTFinalizer reports whether the GCS fault-tolerance Redis cleanup finalizer is
// present on the cluster.
func hasGCSFTFinalizer(cluster *rayv1.RayCluster) bool {
	return controllerutil.ContainsFinalizer(cluster, utils.GCSFaultToleranceRedisCleanupFinalizer)
}

// getGCSFTDeletionTimeout returns the timeout after which the GCS FT finalizer is
// force-removed from a stuck RayCluster.
// The value is read from the RayClusterGCSFTDeletionTimeoutAnnotation annotation on the
// cluster (integer seconds). If the annotation is absent or cannot be parsed, the default
// of RAYCLUSTER_GCS_FT_DELETION_TIMEOUT_DEFAULT seconds is used.
func getGCSFTDeletionTimeout(cluster *rayv1.RayCluster) time.Duration {
	if annotationVal, exists := cluster.Annotations[utils.RayClusterGCSFTDeletionTimeoutAnnotation]; exists {
		if timeoutSeconds, err := strconv.Atoi(annotationVal); err == nil && timeoutSeconds > 0 {
			return time.Duration(timeoutSeconds) * time.Second
		}
	}
	return utils.RAYCLUSTER_GCS_FT_DELETION_TIMEOUT_DEFAULT * time.Second
}

// forceRemoveGCSFTFinalizer forcibly removes the GCS FT Redis cleanup finalizer when the
// cleanup job has been stuck past the configured timeout, unblocking cluster deletion.
func (r *RayClusterReconciler) forceRemoveGCSFTFinalizer(ctx context.Context, instance *rayv1.RayCluster) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	deletionAge := time.Since(instance.DeletionTimestamp.Time)

	logger.Info("Force-removing GCS FT finalizer — Redis cleanup job stuck past timeout",
		"cluster", instance.Name, "deletionAge", deletionAge,
		"timeout", getGCSFTDeletionTimeout(instance))

	// Remove finalizer to allow deletion to proceed
	controllerutil.RemoveFinalizer(instance, utils.GCSFaultToleranceRedisCleanupFinalizer)
	if err := r.Update(ctx, instance); err != nil {
		return ctrl.Result{RequeueAfter: DefaultRequeueDuration}, err
	}

	storageNamespace := instance.Annotations[utils.RayExternalStorageNSAnnotationKey]
	r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.ForceDeletedStuckCluster), string(utils.CleanupAction),
		"Force-removed GCS FT finalizer after %v due to Redis cleanup timeout; Redis storage namespace %q may need manual cleanup",
		deletionAge, storageNamespace)

	return ctrl.Result{}, nil // No requeue - deletion proceeds naturally
}

// hasIdleTerminationCleanupFinalizer reports whether the idle termination cleanup finalizer is present in the RayCluster
func (r *RayClusterReconciler) hasIdleTerminationCleanupFinalizer(cluster *rayv1.RayCluster) bool {
	return controllerutil.ContainsFinalizer(cluster, utils.IdleTerminationCleanupFinalizer)
}
