package ray

import (
	"context"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const (
	RayJobDefaultRequeueDuration    = 3 * time.Second
	PythonUnbufferedEnvVarName      = "PYTHONUNBUFFERED"
	DefaultSubmitterFinishedTimeout = 30 * time.Second
)

// RayJobReconciler reconciles a RayJob object
type RayJobReconciler struct {
	client.Client
	Recorder            events.EventRecorder
	options             RayJobReconcilerOptions
	Scheme              *runtime.Scheme
	dashboardClientFunc func(rayCluster *rayv1.RayCluster, url string) (dashboardclient.RayDashboardClientInterface, error)
}

type RayJobReconcilerOptions struct {
	RayJobMetricsManager  *metrics.RayJobMetricsManager
	BatchSchedulerManager *batchscheduler.SchedulerManager
}

// NewRayJobReconciler returns a new reconcile.Reconciler
func NewRayJobReconciler(ctx context.Context, mgr manager.Manager, options RayJobReconcilerOptions, provider utils.ClientProvider) *RayJobReconciler {
	dashboardClientFunc := provider.GetDashboardClient(ctx, mgr)
	return &RayJobReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		Recorder:            mgr.GetEventRecorder("rayjob-controller"),
		dashboardClientFunc: dashboardClientFunc,
		options:             options,
	}
}

// +kubebuilder:rbac:groups=ray.io,resources=rayjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ray.io,resources=rayjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ray.io,resources=rayjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services/proxy,verbs=get;update;patch;create
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;create;update
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=get;list;watch;create;delete;update
// +kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

// [WARNING]: There MUST be a newline after kubebuilder markers.
// Reconcile reads that state of a RayJob object and makes changes based on it
// and what is in the RayJob.Spec
// Automatically generate RBAC rules to allow the Controller to read and write workloads
// Reconcile used to bridge the desired state with the current state
func (r *RayJobReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Get RayJob instance
	var err error
	rayJobInstance := &rayv1.RayJob{}
	if err := r.Get(ctx, request.NamespacedName, rayJobInstance); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request. Stop reconciliation.
			logger.Info("RayJob resource not found.")
			cleanUpRayJobMetrics(r.options.RayJobMetricsManager, request.Name, request.Namespace)
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to get RayJob")
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
	}

	if manager := utils.ManagedByExternalController(rayJobInstance.Spec.ManagedBy); manager != nil {
		logger.Info("Skipping RayJob managed by a custom controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	if !rayJobInstance.ObjectMeta.DeletionTimestamp.IsZero() {
		logger.Info("RayJob is being deleted", "DeletionTimestamp", rayJobInstance.ObjectMeta.DeletionTimestamp)
		// If the JobStatus is not terminal, it is possible that the Ray job is still running. This includes
		// the case where JobStatus is JobStatusNew.
		if !rayv1.IsJobTerminal(rayJobInstance.Status.JobStatus) {
			rayClusterNamespacedName := common.RayJobRayClusterNamespacedName(rayJobInstance)
			rayClusterInstance := &rayv1.RayCluster{}
			if err := r.Get(ctx, rayClusterNamespacedName, rayClusterInstance); err != nil {
				logger.Error(err, "Failed to get RayCluster")
			}

			rayDashboardClient, err := r.dashboardClientFunc(rayClusterInstance, rayJobInstance.Status.DashboardURL)
			if err != nil {
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}
			if err := rayDashboardClient.StopJob(ctx, rayJobInstance.Status.JobId); err != nil {
				logger.Error(err, "Failed to stop job for RayJob")
			}
		}

		logger.Info("Remove the finalizer no matter StopJob() succeeds or not.", "finalizer", utils.RayJobStopJobFinalizer)
		controllerutil.RemoveFinalizer(rayJobInstance, utils.RayJobStopJobFinalizer)
		if err := r.Update(ctx, rayJobInstance); err != nil {
			logger.Error(err, "Failed to remove finalizer for RayJob")
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
	}

	// Please do NOT modify `originalRayJobInstance` in the following code.
	originalRayJobInstance := rayJobInstance.DeepCopy()

	// Perform all validations and directly fail the RayJob if any of the validation fails
	errType, err := validateRayJob(ctx, rayJobInstance)
	// Immediately update the status after validation
	if err != nil {
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(errType), string(utils.ValidateAction),
			"%s/%s: %v", rayJobInstance.Namespace, rayJobInstance.Name, err)

		rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusValidationFailed
		rayJobInstance.Status.Reason = rayv1.ValidationFailed
		rayJobInstance.Status.Message = err.Error()

		// This is one of the only 2 places where we update the RayJob status. This will directly
		// update the JobDeploymentStatus to ValidationFailed if there's validation error.
		if err = r.updateRayJobStatus(ctx, originalRayJobInstance, rayJobInstance); err != nil {
			logger.Info("Failed to update RayJob status", "error", err)
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("RayJob", "JobStatus", rayJobInstance.Status.JobStatus, "JobDeploymentStatus", rayJobInstance.Status.JobDeploymentStatus, "SubmissionMode", rayJobInstance.Spec.SubmissionMode)
	switch rayJobInstance.Status.JobDeploymentStatus {
	case rayv1.JobDeploymentStatusNew:
		if !controllerutil.ContainsFinalizer(rayJobInstance, utils.RayJobStopJobFinalizer) {
			logger.Info("Add a finalizer", "finalizer", utils.RayJobStopJobFinalizer)
			controllerutil.AddFinalizer(rayJobInstance, utils.RayJobStopJobFinalizer)
			if err := r.Update(ctx, rayJobInstance); err != nil {
				logger.Error(err, "Failed to update RayJob with finalizer")
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}
		}

		if rayJobInstance.Spec.Suspend {
			// Skip initialization to avoid generating an unwanted RayClusterName.
			rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusSuspended
			break
		}

		// Set `Status.JobDeploymentStatus` to `JobDeploymentStatusInitializing`, and initialize `Status.JobId`
		// and `Status.RayClusterName` prior to avoid duplicate job submissions and cluster creations.
		logger.Info("JobDeploymentStatusNew")
		initRayJobStatusIfNeed(ctx, rayJobInstance)
	case rayv1.JobDeploymentStatusInitializing:
		if shouldUpdate := updateStatusToSuspendingIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if shouldUpdate := checkActiveDeadlineAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if shouldUpdate := checkPreRunningDeadlineAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if r.options.BatchSchedulerManager != nil {
			if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
				if err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayJobInstance); err != nil {
					return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
				}
			} else {
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}
		}

		var rayClusterInstance *rayv1.RayCluster
		if rayClusterInstance, err = r.getOrCreateRayClusterInstance(ctx, rayJobInstance); err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}

		// Check the current status of RayCluster before submitting.
		if clientURL := rayJobInstance.Status.DashboardURL; clientURL == "" {
			if rayClusterInstance.Status.State != rayv1.Ready {
				logger.Info("Wait for the RayCluster.Status.State to be ready before submitting the job.", "RayCluster", rayClusterInstance.Name, "State", rayClusterInstance.Status.State)
				// The nonready RayCluster status should be reflected in the RayJob's status.
				// Breaking from the switch statement will drop directly to the status update code
				// and return a default requeue duration and no error.
				rayJobInstance.Status.RayClusterStatus = rayClusterInstance.Status
				break
			}

			if clientURL, err = utils.FetchHeadServiceURL(ctx, r.Client, rayClusterInstance, utils.DashboardPortName); err != nil || clientURL == "" {
				logger.Error(err, "Failed to get the dashboard URL after the RayCluster is ready!", "RayCluster", rayClusterInstance.Name)
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}
			rayJobInstance.Status.DashboardURL = clientURL
		}

		if rayJobInstance.Spec.SubmissionMode == rayv1.InteractiveMode {
			rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusWaiting
			break
		}

		if rayJobInstance.Spec.SubmissionMode == rayv1.K8sJobMode {
			if err := r.createK8sJobIfNeed(ctx, rayJobInstance, rayClusterInstance); err != nil {
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}
		}

		rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRunning
	case rayv1.JobDeploymentStatusWaiting:
		if shouldUpdate := checkPreRunningDeadlineAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		// Try to get the Ray job id from rayJob.Spec.JobId
		if rayJobInstance.Spec.JobId == "" {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
		}

		rayJobInstance.Status.JobId = rayJobInstance.Spec.JobId
		rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRunning
	case rayv1.JobDeploymentStatusRunning:
		if shouldUpdate := updateStatusToSuspendingIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if shouldUpdate := checkActiveDeadlineAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if shouldUpdate := checkTransitionGracePeriodAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		if shouldUpdate := checkJobStatusCheckTimeoutAndUpdateStatusIfNeeded(ctx, rayJobInstance); shouldUpdate {
			break
		}

		var rayClusterInstance *rayv1.RayCluster
		// TODO (kevin85421): Maybe we only need to `get` the RayCluster because the RayCluster should have been created
		// before transitioning the status from `Initializing` to `Running`.
		if rayClusterInstance, err = r.getOrCreateRayClusterInstance(ctx, rayJobInstance); err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}

		err = r.reconcileServices(ctx, rayJobInstance, rayClusterInstance)
		if err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}

		var finishedAt *time.Time
		if rayJobInstance.Spec.SubmissionMode == rayv1.K8sJobMode || rayJobInstance.Spec.SubmissionMode == rayv1.SidecarMode {
			var shouldUpdate bool
			shouldUpdate, finishedAt, err = r.checkSubmitterAndUpdateStatusIfNeeded(ctx, rayJobInstance)
			if err != nil {
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
			}

			if checkSubmitterFinishedTimeoutAndUpdateStatusIfNeeded(ctx, rayJobInstance, finishedAt) {
				break
			}

			if shouldUpdate {
				break
			}
		}

		// Check the current status of ray jobs
		rayDashboardClient, err := r.dashboardClientFunc(rayClusterInstance, rayJobInstance.Status.DashboardURL)
		if err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}

		jobInfo, err := rayDashboardClient.GetJobInfo(ctx, rayJobInstance.Status.JobId)
		if err != nil {
			// If the Ray job was not found, GetJobInfo returns a BadRequest error.
			if errors.IsBadRequest(err) {
				if rayJobInstance.Spec.SubmissionMode == rayv1.HTTPMode {
					if reconcileHTTPModeJobNotFound(ctx, rayJobInstance, rayDashboardClient) {
						break
					}
					return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
				}
				// finishedAt will only be set if submitter finished
				if finishedAt != nil {
					rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
					rayJobInstance.Status.Reason = rayv1.AppFailed
					rayJobInstance.Status.Message = "Submitter completed but Ray job not found in RayCluster."
					break
				}
				return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
			}

			logger.Error(err, "Failed to get job info", "JobId", rayJobInstance.Status.JobId)
			if recordJobStatusCheckFailure(ctx, rayJobInstance, err) {
				break
			}
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
		}

		// Reset JobStatusCheckFailureStartTime when the job status check succeeds.
		rayJobInstance.Status.JobStatusCheckFailureStartTime = nil

		// If the JobStatus is in a terminal status, such as SUCCEEDED, FAILED, or STOPPED, it is impossible for the Ray job
		// to transition to any other. Additionally, RayJob does not currently support retries. Hence, we can mark the RayJob
		// as "Complete" or "Failed" to avoid unnecessary reconciliation.
		jobDeploymentStatus := rayv1.JobDeploymentStatusRunning
		reason := rayv1.JobFailedReason("")
		isJobTerminal := rayv1.IsJobTerminal(jobInfo.JobStatus)
		// If in K8sJobMode or SidecarMode, further refine the terminal condition by checking if the submitter has finished.
		// See https://github.com/ray-project/kuberay/pull/1919 for reasons.
		if utils.HasSubmitter(rayJobInstance) {
			isJobTerminal = isJobTerminal && finishedAt != nil
		}

		if isJobTerminal {
			jobDeploymentStatus = rayv1.JobDeploymentStatusComplete
			if jobInfo.JobStatus == rayv1.JobStatusFailed {
				jobDeploymentStatus = rayv1.JobDeploymentStatusFailed
				reason = rayv1.AppFailed
			}
		}

		// Always update RayClusterStatus along with JobStatus and JobDeploymentStatus updates.
		rayJobInstance.Status.RayClusterStatus = rayClusterInstance.Status
		rayJobInstance.Status.JobStatus = jobInfo.JobStatus
		rayJobInstance.Status.JobDeploymentStatus = jobDeploymentStatus
		rayJobInstance.Status.Reason = reason
		rayJobInstance.Status.Message = jobInfo.Message
		// It is safe to convert uint64 to int64 here because Ray Core uses `int64_t` under the hood,
		// even though the type defined in `message JobTableData` (gcs.proto) is `uint64`.
		// See `gcs_job_manager.cc` and the function `current_sys_time_ms` for more details.
		if jobInfo.StartTime != 0 {
			rayJobInstance.Status.RayJobStatusInfo.StartTime = &metav1.Time{Time: time.UnixMilli(utils.SafeUint64ToInt64(jobInfo.StartTime))}
		}
		if jobInfo.EndTime != 0 {
			rayJobInstance.Status.RayJobStatusInfo.EndTime = &metav1.Time{Time: time.UnixMilli(utils.SafeUint64ToInt64(jobInfo.EndTime))}
		}
	case rayv1.JobDeploymentStatusSuspending, rayv1.JobDeploymentStatusRetrying:
		// The `suspend` operation should be atomic. In other words, if users set the `suspend` flag to true and then immediately
		// set it back to false, either all of the RayJob's associated resources should be cleaned up, or no resources should be
		// cleaned up at all. To keep the atomicity, if a RayJob is in the `Suspending` status, we should delete all of its
		// associated resources and then transition the status to `Suspended` no matter the value of the `suspend` flag.

		// TODO (kevin85421): Currently, Ray doesn't have a best practice to stop a Ray job gracefully. At this moment,
		// KubeRay doesn't stop the Ray job before suspending the RayJob. If users want to stop the Ray job by SIGTERM,
		// users need to set the Pod's preStop hook by themselves.
		isClusterDeleted, err := r.deleteClusterResources(ctx, rayJobInstance)
		if err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
		isJobDeleted, err := r.deleteSubmitterJob(ctx, rayJobInstance)
		if err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
		if !isClusterDeleted || !isJobDeleted {
			logger.Info("The release of the compute resources has not been completed yet. " +
				"Wait for the resources to be deleted before the status transitions to avoid a resource leak.")
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
		}

		if err := r.cleanupBatchSchedulerResources(ctx, rayJobInstance); err != nil {
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}

		// Reset the RayCluster and Ray job related status.
		rayJobInstance.Status.RayClusterStatus = rayv1.RayClusterStatus{}
		rayJobInstance.Status.RayClusterName = ""
		rayJobInstance.Status.DashboardURL = ""
		rayJobInstance.Status.JobId = ""
		rayJobInstance.Status.Message = ""
		rayJobInstance.Status.Reason = ""
		rayJobInstance.Status.RayJobStatusInfo = rayv1.RayJobStatusInfo{}
		rayJobInstance.Status.JobStatusCheckFailureStartTime = nil
		// Reset the JobStatus to JobStatusNew and transition the JobDeploymentStatus to `Suspended`.
		rayJobInstance.Status.JobStatus = rayv1.JobStatusNew

		if rayJobInstance.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusSuspending {
			rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusSuspended
		}
		if rayJobInstance.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusRetrying {
			rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusNew
		}
	case rayv1.JobDeploymentStatusSuspended:
		if !rayJobInstance.Spec.Suspend {
			logger.Info("The status is 'Suspended', but the suspend flag is false. Transition the status to 'New'.")
			rayJobInstance.Status.JobStatus = rayv1.JobStatusNew
			rayJobInstance.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusNew
			break
		}
		// The RayJob is already suspended, we should not requeue it.
		return ctrl.Result{}, nil
	case rayv1.JobDeploymentStatusComplete, rayv1.JobDeploymentStatusFailed:
		defer func() {
			if err := r.cleanupBatchSchedulerResources(ctx, rayJobInstance); err != nil {
				logger.Error(err, "Failed to cleanup batch scheduler resources")
			}
		}()

		// The RayJob has reached a terminal state. Handle the cleanup and deletion logic.
		// If the RayJob uses an existing RayCluster, we must not delete it.
		if len(rayJobInstance.Spec.ClusterSelector) > 0 {
			logger.Info("RayJob is using an existing RayCluster via clusterSelector; skipping resource deletion.", "RayClusterSelector", rayJobInstance.Spec.ClusterSelector)
			return ctrl.Result{}, nil
		}

		if features.Enabled(features.RayJobDeletionPolicy) && rayJobInstance.Spec.DeletionStrategy != nil {
			// The previous validation logic ensures that either DeletionRules or the legacy policies are set, but not both.
			if rayJobInstance.Spec.DeletionStrategy.DeletionRules != nil {
				return r.handleDeletionRules(ctx, rayJobInstance)
			}
			return r.handleLegacyDeletionPolicy(ctx, rayJobInstance)
		}

		if rayJobInstance.Spec.ShutdownAfterJobFinishes {
			return r.handleShutdownAfterJobFinishes(ctx, rayJobInstance)
		}

		// Default: No deletion policy is configured. The reconciliation is complete for this RayJob.
		return ctrl.Result{}, nil
	default:
		logger.Info("Unknown JobDeploymentStatus", "JobDeploymentStatus", rayJobInstance.Status.JobDeploymentStatus)
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
	}
	checkBackoffLimitAndUpdateStatusIfNeeded(ctx, rayJobInstance)

	// This is one of the only 2 places where we update the RayJob status. Please do NOT add any
	// code between `checkBackoffLimitAndUpdateStatusIfNeeded` and the following code.
	if err = r.updateRayJobStatus(ctx, originalRayJobInstance, rayJobInstance); err != nil {
		logger.Info("Failed to update RayJob status", "error", err)
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
	}
	emitRayJobMetrics(r.options.RayJobMetricsManager, rayJobInstance.Name, rayJobInstance.Namespace, rayJobInstance.UID, originalRayJobInstance.Status, rayJobInstance.Status)
	return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, nil
}

func validateRayJob(ctx context.Context, rayJobInstance *rayv1.RayJob) (utils.K8sEventType, error) {
	logger := ctrl.LoggerFrom(ctx)
	validationRules := []struct {
		validate func() error
		errType  utils.K8sEventType
	}{
		{func() error { return utils.ValidateRayJobMetadata(rayJobInstance.ObjectMeta) }, utils.InvalidRayJobMetadata},
		{func() error { return utils.ValidateRayJobSpec(rayJobInstance) }, utils.InvalidRayJobSpec},
		{func() error { return utils.ValidateRayJobStatus(rayJobInstance) }, utils.InvalidRayJobStatus},
	}

	for _, validation := range validationRules {
		if err := validation.validate(); err != nil {
			logger.Error(err, err.Error())
			return validation.errType, err
		}
	}
	return "", nil
}

func emitRayJobMetrics(rayJobMetricsManager *metrics.RayJobMetricsManager, rayJobName, rayJobNamespace string, rayJobUID types.UID, originalRayJobStatus, rayJobStatus rayv1.RayJobStatus) {
	if rayJobMetricsManager == nil {
		return
	}
	emitRayJobExecutionDuration(rayJobMetricsManager, rayJobName, rayJobNamespace, rayJobUID, originalRayJobStatus, rayJobStatus)
}

func emitRayJobExecutionDuration(rayJobMetricsObserver metrics.RayJobMetricsObserver, rayJobName, rayJobNamespace string, rayJobUID types.UID, originalRayJobStatus, rayJobStatus rayv1.RayJobStatus) {
	// Emit kuberay_job_execution_duration_seconds when a job transitions from a non-terminal state to either a terminal state or a retrying state (following a failure).
	if !rayv1.IsJobDeploymentTerminal(originalRayJobStatus.JobDeploymentStatus) && (rayv1.IsJobDeploymentTerminal(rayJobStatus.JobDeploymentStatus) || rayJobStatus.JobDeploymentStatus == rayv1.JobDeploymentStatusRetrying) {
		retryCount := 0
		if originalRayJobStatus.Failed != nil {
			retryCount += int(*originalRayJobStatus.Failed)
		}
		rayJobMetricsObserver.ObserveRayJobExecutionDuration(
			rayJobName,
			rayJobNamespace,
			rayJobUID,
			rayJobStatus.JobDeploymentStatus,
			retryCount,
			time.Since(rayJobStatus.StartTime.Time).Seconds(),
		)
	}
}

func cleanUpRayJobMetrics(rayJobMetricsManager *metrics.RayJobMetricsManager, rayJobName, rayJobNamespace string) {
	if rayJobMetricsManager == nil {
		return
	}
	rayJobMetricsManager.DeleteRayJobMetrics(rayJobName, rayJobNamespace)
}

// checkBackoffLimitAndUpdateStatusIfNeeded determines if a RayJob is eligible for retry based on the configured backoff limit,
// the job's success status, and its failure status. If eligible, sets the JobDeploymentStatus to Retrying.
func checkBackoffLimitAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) {
	logger := ctrl.LoggerFrom(ctx)

	failedCount := int32(0)
	if rayJob.Status.Failed != nil {
		failedCount = *rayJob.Status.Failed
	}

	succeededCount := int32(0)
	if rayJob.Status.Succeeded != nil {
		succeededCount = *rayJob.Status.Succeeded
	}

	if rayJob.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusFailed {
		failedCount++
	}

	if rayJob.Status.JobStatus == rayv1.JobStatusSucceeded && rayJob.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusComplete {
		succeededCount++
	}

	rayJob.Status.Failed = new(failedCount)
	rayJob.Status.Succeeded = new(succeededCount)

	if rayJob.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusFailed && rayJob.Spec.BackoffLimit != nil && *rayJob.Status.Failed < *rayJob.Spec.BackoffLimit+1 {
		if rayJob.Status.Reason == rayv1.DeadlineExceeded || rayJob.Status.Reason == rayv1.JobStatusCheckTimeoutExceeded {
			logger.Info(
				"RayJob is not eligible for retry due to failure with DeadlineExceeded or JobStatusCheckTimeoutExceeded",
				"reason", rayJob.Status.Reason,
				"backoffLimit", *rayJob.Spec.BackoffLimit,
				"succeeded", *rayJob.Status.Succeeded,
				"failed", *rayJob.Status.Failed,
			)
			return
		}
		logger.Info("RayJob is eligible for retry, setting JobDeploymentStatus to Retrying",
			"backoffLimit", *rayJob.Spec.BackoffLimit, "succeeded", *rayJob.Status.Succeeded, "failed", *rayJob.Status.Failed)
		rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusRetrying
	}
}

// createK8sJobIfNeed creates a Kubernetes Job for the RayJob if it doesn't exist.
func (r *RayJobReconciler) createK8sJobIfNeed(ctx context.Context, rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	job := &batchv1.Job{}
	namespacedName := common.RayJobK8sJobNamespacedName(rayJobInstance)
	if err := r.Client.Get(ctx, namespacedName, job); err != nil {
		if errors.IsNotFound(err) {
			submitterTemplate, err := getSubmitterTemplate(rayJobInstance, rayClusterInstance)
			if err != nil {
				return err
			}
			if r.options.BatchSchedulerManager != nil {
				if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
					scheduler.AddMetadataToChildResource(ctx, rayJobInstance, &submitterTemplate, utils.RayNodeSubmitterGroupLabelValue)
				} else {
					return err
				}
			}
			return r.createNewK8sJob(ctx, rayJobInstance, submitterTemplate)
		}
		return err
	}

	logger.Info("The submitter Kubernetes Job for RayJob already exists", "Kubernetes Job", job.Name)
	return nil
}

// getSubmitterTemplate builds the submitter pod template for the Ray job.
func getSubmitterTemplate(rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster) (corev1.PodTemplateSpec, error) {
	// Set the default value for the optional field SubmitterPodTemplate if not provided.
	submitterTemplate := common.GetSubmitterTemplate(&rayJobInstance.Spec, &rayClusterInstance.Spec)

	if err := configureSubmitterContainer(&submitterTemplate.Spec.Containers[utils.RayContainerIndex], rayJobInstance, rayClusterInstance, rayv1.K8sJobMode); err != nil {
		return corev1.PodTemplateSpec{}, err
	}

	if rayClusterInstance != nil && utils.IsK8sAuthEnabled(rayClusterInstance.Spec.AuthOptions) {
		common.AddRayTokenVolume(&submitterTemplate.Spec)
	}

	if submitterTemplate.Labels == nil {
		submitterTemplate.Labels = make(map[string]string)
	}
	submitterTemplate.Labels[utils.RayOriginatedFromCRNameLabelKey] = rayJobInstance.Name
	submitterTemplate.Labels[utils.RayOriginatedFromCRDLabelKey] = utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)
	submitterTemplate.Labels[utils.KubernetesCreatedByLabelKey] = utils.ComponentName

	return submitterTemplate, nil
}

// getSubmitterContainer builds the submitter container for the Ray job Sidecar mode.
func getSubmitterContainer(rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster) (corev1.Container, error) {
	submitterContainer := common.GetDefaultSubmitterContainer(&rayClusterInstance.Spec)

	if err := configureSubmitterContainer(&submitterContainer, rayJobInstance, rayClusterInstance, rayv1.SidecarMode); err != nil {
		return corev1.Container{}, err
	}

	// When SidecarSubmitterRestart feature gate is enabled, configure per-container restart rules.
	// This requires Kubernetes 1.35+ with ContainerRestartRules feature gate enabled.
	if features.Enabled(features.SidecarSubmitterRestart) {
		// OnFailure restarts only the submitter container (not all containers in the pod) on non-zero exit.
		// The non-zero exit can come from `ray job submit --no-wait` or `ray job logs --follow`.
		// The key case is `ray job logs --follow` exiting non-zero on a transient
		// WebSocket closure even when the Ray job is still running.
		// On restart, the submitter checks ray job status first.
		// Since the job is still running, the submitter simply reattaches to the log stream.
		// See BuildJobSubmitCommand in ray-operator/controllers/ray/common/job.go for more details.
		submitterContainer.RestartPolicy = ptr.To(corev1.ContainerRestartPolicyOnFailure)
	}

	return submitterContainer, nil
}

// pass the RayCluster instance for cluster selector case
func configureSubmitterContainer(container *corev1.Container, rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster, submissionMode rayv1.JobSubmissionMode) error {
	// If the command in the submitter container manifest isn't set, use the default command.
	jobCmd, err := common.BuildJobSubmitCommand(rayJobInstance, submissionMode)
	if err != nil {
		return err
	}

	// K8sJobMode: If the user doesn't specify the command, use the default command.
	// SidecarMode: Use the default command.
	if len(container.Command) == 0 || submissionMode == rayv1.SidecarMode {
		// Without the -e option, the Bash script will continue executing even if a command returns a non-zero exit code.
		container.Command = utils.GetContainerCommand([]string{"e"})
		container.Args = []string{strings.Join(jobCmd, " ")}
	}

	// Set PYTHONUNBUFFERED=1 for real-time logging
	container.Env = append(container.Env, corev1.EnvVar{Name: PythonUnbufferedEnvVarName, Value: "1"})

	// Users can use `RAY_DASHBOARD_ADDRESS` to specify the dashboard address and `RAY_JOB_SUBMISSION_ID` to specify the job id to avoid
	// double submission in the `ray job submit` command. For example:
	// ray job submit --address=http://$RAY_DASHBOARD_ADDRESS --submission-id=$RAY_JOB_SUBMISSION_ID ...
	container.Env = append(container.Env, corev1.EnvVar{Name: utils.RAY_DASHBOARD_ADDRESS, Value: rayJobInstance.Status.DashboardURL})
	container.Env = append(container.Env, corev1.EnvVar{Name: utils.RAY_JOB_SUBMISSION_ID, Value: rayJobInstance.Status.JobId})
	if rayClusterInstance != nil && utils.IsAuthEnabled(&rayClusterInstance.Spec) {
		common.SetContainerTokenAuthEnvVars(rayClusterInstance.Name, container, rayClusterInstance.Spec.AuthOptions)
	}

	return nil
}

// TODO: Move this function into a common package so that both RayJob and RayService can use it.
func (r *RayJobReconciler) reconcileServices(ctx context.Context, rayJobInstance *rayv1.RayJob, rayClusterInstance *rayv1.RayCluster) error {
	if len(rayJobInstance.Spec.ClusterSelector) != 0 {
		return nil
	}

	logger := ctrl.LoggerFrom(ctx)
	newSvc, err := common.BuildHeadServiceForRayJob(ctx, *rayJobInstance, *rayClusterInstance)
	if err != nil {
		return err
	}

	// Retrieve the Service from the Kubernetes cluster with the name and namespace.
	oldSvc := &corev1.Service{}
	err = r.Get(ctx, client.ObjectKey{Name: newSvc.Name, Namespace: rayJobInstance.Namespace}, oldSvc)

	if err == nil {
		// Only update the service if the RayCluster switches.
		if newSvc.Spec.Selector[utils.RayClusterLabelKey] == oldSvc.Spec.Selector[utils.RayClusterLabelKey] {
			logger.Info("Service has already exists in the RayCluster, skip Update", "rayCluster",
				newSvc.Spec.Selector[utils.RayClusterLabelKey], "serviceType", utils.HeadService)
			return nil
		}
		// ClusterIP is immutable. Starting from Kubernetes v1.21.5, if the new service does not specify a ClusterIP,
		// Kubernetes will assign the ClusterIP of the old service to the new one. However, to maintain compatibility
		// with older versions of Kubernetes, we need to assign the ClusterIP here.
		newSvc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		oldSvc.Spec = *newSvc.Spec.DeepCopy()
		logger.Info("Update Kubernetes Service", "serviceType", utils.HeadService)
		if updateErr := r.Update(ctx, oldSvc); updateErr != nil {
			r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToUpdateService), string(utils.UpdateAction), "Failed to update the service %s/%s, %v", oldSvc.Namespace, oldSvc.Name, updateErr)
			return updateErr
		}
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedService), string(utils.UpdateAction), "Updated the service %s/%s", oldSvc.Namespace, oldSvc.Name)
		// Return the updated service.
		return nil
	} else if errors.IsNotFound(err) {
		logger.Info("Create a Kubernetes Service", "serviceType", utils.HeadService)
		if err := ctrl.SetControllerReference(rayJobInstance, newSvc, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, newSvc); err != nil {
			r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateService), string(utils.CreateAction), "Failed to create the service %s/%s, %v", newSvc.Namespace, newSvc.Name, err)
			return err
		}
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.CreatedService), string(utils.CreateAction), "Created the service %s/%s", newSvc.Namespace, newSvc.Name)
		return nil
	}
	return err
}

// createNewK8sJob creates a new Kubernetes Job. It returns an error.
func (r *RayJobReconciler) createNewK8sJob(ctx context.Context, rayJobInstance *rayv1.RayJob, submitterTemplate corev1.PodTemplateSpec) error {
	logger := ctrl.LoggerFrom(ctx)
	submitterBackoffLimit := ptr.To[int32](2)
	if rayJobInstance.Spec.SubmitterConfig != nil && rayJobInstance.Spec.SubmitterConfig.BackoffLimit != nil {
		submitterBackoffLimit = rayJobInstance.Spec.SubmitterConfig.BackoffLimit
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rayJobInstance.Name,
			Namespace: rayJobInstance.Namespace,
			Labels: map[string]string{
				utils.RayOriginatedFromCRNameLabelKey: rayJobInstance.Name,
				utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
				utils.KubernetesCreatedByLabelKey:     utils.ComponentName,
			},
		},
		Spec: batchv1.JobSpec{
			// Reduce the number of retries, which defaults to 6, so the ray job submission command
			// is attempted 3 times at the maximum, but still mitigates the case of unrecoverable
			// application-level errors, where the maximum number of retries is reached, and the job
			// completion time increases with no benefits, but wasted resource cycles.
			BackoffLimit: submitterBackoffLimit,
			Template:     submitterTemplate,
		},
	}

	// Set the ownership in order to do the garbage collection by k8s.
	if err := ctrl.SetControllerReference(rayJobInstance, job, r.Scheme); err != nil {
		return err
	}

	// Create the Kubernetes Job
	if err := r.Client.Create(ctx, job); err != nil {
		logger.Error(err, "Failed to create new submitter Kubernetes Job for RayJob")
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRayJobSubmitter), string(utils.CreateAction), "Failed to create new Kubernetes Job %s/%s: %v", job.Namespace, job.Name, err)
		return err
	}
	logger.Info("Created submitter Kubernetes Job for RayJob", "Kubernetes Job", job.Name)
	r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.CreatedRayJobSubmitter), string(utils.CreateAction), "Created Kubernetes Job %s/%s", job.Namespace, job.Name)
	return nil
}

// deleteSubmitterJob deletes the submitter Job associated with the RayJob.
func (r *RayJobReconciler) deleteSubmitterJob(ctx context.Context, rayJobInstance *rayv1.RayJob) (bool, error) {
	// In HTTPMode and SidecarMode, there's no job submitter pod to delete.
	if rayJobInstance.Spec.SubmissionMode == rayv1.HTTPMode || rayJobInstance.Spec.SubmissionMode == rayv1.SidecarMode {
		return true, nil
	}

	logger := ctrl.LoggerFrom(ctx)
	var isJobDeleted bool

	// Since the name of the Kubernetes Job is the same as the RayJob, we need to delete the Kubernetes Job
	// and its Pods when suspending. A new submitter Kubernetes Job must be created to resubmit the
	// Ray job if the RayJob is resumed.
	job := &batchv1.Job{}
	namespacedName := common.RayJobK8sJobNamespacedName(rayJobInstance)
	if err := r.Client.Get(ctx, namespacedName, job); err != nil {
		if errors.IsNotFound(err) {
			isJobDeleted = true
			logger.Info("The submitter Kubernetes Job has been already deleted", "Kubernetes Job", job.Name)
		} else {
			return false, err
		}
	} else {
		if !job.DeletionTimestamp.IsZero() {
			logger.Info("The deletion of submitter Kubernetes Job for RayJob is ongoing.", "Submitter K8s Job", job.Name)
		} else {
			if err := r.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteRayJobSubmitter), string(utils.DeleteAction), "Failed to delete submitter K8s Job %s/%s: %v", job.Namespace, job.Name, err)
				return false, err
			}
			logger.Info("The associated submitter Kubernetes Job for RayJob is deleted", "Submitter K8s Job", job.Name)
			r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.DeletedRayJobSubmitter), string(utils.DeleteAction), "Deleted submitter K8s Job %s/%s", job.Namespace, job.Name)
		}
	}

	logger.Info("deleteSubmitterJob", "isJobDeleted", isJobDeleted)
	return isJobDeleted, nil
}

// deleteClusterResources deletes the RayCluster associated with the RayJob to release the compute resources.
func (r *RayJobReconciler) deleteClusterResources(ctx context.Context, rayJobInstance *rayv1.RayJob) (bool, error) {
	logger := ctrl.LoggerFrom(ctx)

	if len(rayJobInstance.Spec.ClusterSelector) > 0 {
		logger.Info("RayJob is using an existing RayCluster via clusterSelector; skipping resource deletion.", "RayClusterSelector", rayJobInstance.Spec.ClusterSelector)
		return true, nil
	}

	clusterIdentifier := common.RayJobRayClusterNamespacedName(rayJobInstance)

	var isClusterDeleted bool
	cluster := rayv1.RayCluster{}
	if err := r.Get(ctx, clusterIdentifier, &cluster); err != nil {
		if errors.IsNotFound(err) {
			// If the cluster is not found, it means the cluster has been already deleted.
			// Don't return error to make this function idempotent.
			isClusterDeleted = true
			logger.Info("The associated RayCluster for RayJob has been already deleted and it can not be found", "RayCluster", clusterIdentifier)
		} else {
			return false, err
		}
	} else {
		if !cluster.DeletionTimestamp.IsZero() {
			logger.Info("The deletion of the associated RayCluster for RayJob is ongoing.", "RayCluster", cluster.Name)
		} else {
			if err := r.Delete(ctx, &cluster); err != nil {
				r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToDeleteRayCluster), string(utils.DeleteAction), "Failed to delete cluster %s/%s: %v", cluster.Namespace, cluster.Name, err)
				return false, err
			}
			logger.Info("The associated RayCluster for RayJob is deleted", "RayCluster", clusterIdentifier)
			r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.DeletedRayCluster), string(utils.DeleteAction), "Deleted cluster %s/%s", cluster.Namespace, cluster.Name)
		}
	}

	logger.Info("deleteClusterResources", "isClusterDeleted", isClusterDeleted)
	return isClusterDeleted, nil
}

func (r *RayJobReconciler) suspendWorkerGroups(ctx context.Context, rayJobInstance *rayv1.RayJob) error {
	logger := ctrl.LoggerFrom(ctx)
	clusterIdentifier := common.RayJobRayClusterNamespacedName(rayJobInstance)

	cluster := rayv1.RayCluster{}
	if err := r.Get(ctx, clusterIdentifier, &cluster); err != nil {
		return err
	}

	for i := range cluster.Spec.WorkerGroupSpecs {
		cluster.Spec.WorkerGroupSpecs[i].Suspend = new(true)
	}

	if err := r.Update(ctx, &cluster); err != nil {
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning,
			string(utils.FailedToUpdateRayCluster), string(utils.UpdateAction),
			"Failed to suspend worker groups in cluster %s/%s: %v",
			cluster.Namespace, cluster.Name, err)
		return err
	}

	logger.Info("All worker groups for RayCluster have had `suspend` set to true", "RayCluster", clusterIdentifier)
	r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.UpdatedRayCluster), string(utils.UpdateAction), "Set the `suspend` field to true for all worker groups in cluster %s/%s", cluster.Namespace, cluster.Name)

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RayJobReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayJob{}).
		Owns(&rayv1.RayCluster{}).
		Owns(&corev1.Service{}).
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayJob")
				if request != nil {
					logger = logger.WithValues("RayJob", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

// This function is the sole place where `JobDeploymentStatusInitializing` is defined. It initializes `Status.JobId` and `Status.RayClusterName`
// prior to job submissions and RayCluster creations. This is used to avoid duplicate job submissions and cluster creations. In addition, this
// function also sets `Status.StartTime` to support `ActiveDeadlineSeconds`.
// This function will set or generate JobId if SubmissionMode is not InteractiveMode.
func initRayJobStatusIfNeed(ctx context.Context, rayJob *rayv1.RayJob) {
	logger := ctrl.LoggerFrom(ctx)
	shouldUpdateStatus := rayJob.Status.JobId == "" || rayJob.Status.RayClusterName == "" || rayJob.Status.JobStatus == ""
	// Please don't update `shouldUpdateStatus` below.
	logger.Info("initRayJobStatusIfNeed", "shouldUpdateStatus", shouldUpdateStatus, "jobId", rayJob.Status.JobId, "rayClusterName", rayJob.Status.RayClusterName, "jobStatus", rayJob.Status.JobStatus)
	if !shouldUpdateStatus {
		return
	}

	if rayJob.Spec.SubmissionMode != rayv1.InteractiveMode && rayJob.Status.JobId == "" {
		if rayJob.Spec.JobId != "" {
			rayJob.Status.JobId = rayJob.Spec.JobId
		} else {
			rayJob.Status.JobId = utils.GenerateRayJobId(rayJob.Name)
		}
	}

	if rayJob.Status.RayClusterName == "" {
		if clusterName := rayJob.Spec.ClusterSelector[utils.RayJobClusterSelectorKey]; clusterName != "" {
			rayJob.Status.RayClusterName = clusterName
		} else {
			rayJob.Status.RayClusterName = utils.GenerateRayClusterName(rayJob.Name)
		}
	}

	if rayJob.Status.JobStatus == "" {
		rayJob.Status.JobStatus = rayv1.JobStatusNew
	}
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusInitializing
	rayJob.Status.StartTime = &metav1.Time{Time: time.Now()}
}

func (r *RayJobReconciler) updateRayJobStatus(ctx context.Context, oldRayJob *rayv1.RayJob, newRayJob *rayv1.RayJob) error {
	logger := ctrl.LoggerFrom(ctx)
	oldRayJobStatus := oldRayJob.Status
	newRayJobStatus := newRayJob.Status
	logger.Info("updateRayJobStatus", "oldRayJobStatus", oldRayJobStatus, "newRayJobStatus", newRayJobStatus)

	rayClusterStatusChanged := utils.InconsistentRayClusterStatus(oldRayJobStatus.RayClusterStatus, newRayJobStatus.RayClusterStatus)
	jobStatusCheckFailureStartTimeChanged := jobStatusCheckFailureStartTimeChanged(oldRayJobStatus.JobStatusCheckFailureStartTime, newRayJobStatus.JobStatusCheckFailureStartTime)

	// If a status field is crucial for the RayJob state machine, it MUST be
	// updated with a distinct JobStatus or JobDeploymentStatus value.
	if oldRayJobStatus.JobStatus != newRayJobStatus.JobStatus ||
		oldRayJobStatus.JobDeploymentStatus != newRayJobStatus.JobDeploymentStatus ||
		rayClusterStatusChanged ||
		jobStatusCheckFailureStartTimeChanged {
		if rayv1.IsJobDeploymentTerminal(newRayJobStatus.JobDeploymentStatus) {
			newRayJob.Status.EndTime = &metav1.Time{Time: time.Now()}
		}

		logger.Info("updateRayJobStatus", "old JobStatus", oldRayJobStatus.JobStatus, "new JobStatus", newRayJobStatus.JobStatus,
			"old JobDeploymentStatus", oldRayJobStatus.JobDeploymentStatus, "new JobDeploymentStatus", newRayJobStatus.JobDeploymentStatus)
		if err := r.Status().Update(ctx, newRayJob); err != nil {
			return err
		}
	}
	return nil
}

func (r *RayJobReconciler) getOrCreateRayClusterInstance(ctx context.Context, rayJobInstance *rayv1.RayJob) (*rayv1.RayCluster, error) {
	logger := ctrl.LoggerFrom(ctx)
	rayClusterNamespacedName := common.RayJobRayClusterNamespacedName(rayJobInstance)
	logger.Info("try to find existing RayCluster instance", "name", rayClusterNamespacedName.Name)

	rayClusterInstance := &rayv1.RayCluster{}
	if err := r.Get(ctx, rayClusterNamespacedName, rayClusterInstance); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("RayCluster not found", "RayCluster", rayClusterNamespacedName)
			if len(rayJobInstance.Spec.ClusterSelector) != 0 {
				err := fmt.Errorf("clusterSelector mode is enabled, but RayCluster %s/%s is not found: %w", rayClusterNamespacedName.Namespace, rayClusterNamespacedName.Name, err)
				r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.RayClusterNotFound), string(utils.ValidateAction), "RayCluster %s/%s set in the clusterSelector is not found. It must be created manually", rayClusterNamespacedName.Namespace, rayClusterNamespacedName.Name)
				return nil, err
			}

			logger.Info("RayCluster not found, creating RayCluster!", "RayCluster", rayClusterNamespacedName)
			rayClusterInstance, err = r.constructRayClusterForRayJob(rayJobInstance, rayClusterNamespacedName.Name)
			if err != nil {
				return nil, err
			}

			if r.options.BatchSchedulerManager != nil && rayJobInstance.Spec.SubmissionMode == rayv1.K8sJobMode {
				if scheduler, err := r.options.BatchSchedulerManager.GetScheduler(); err == nil {
					// Group name is only used for individual pods to specify their task group ("headgroup", "worker-group-1", etc.).
					// RayCluster contains multiple groups, so we pass an empty string.
					scheduler.AddMetadataToChildResource(ctx, rayJobInstance, rayClusterInstance, "")
				} else {
					return nil, err
				}
			}
			if err := r.Create(ctx, rayClusterInstance); err != nil {
				r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCreateRayCluster), string(utils.CreateAction), "Failed to create RayCluster %s/%s: %v", rayClusterInstance.Namespace, rayClusterInstance.Name, err)
				return nil, err
			}
			r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.CreatedRayCluster), string(utils.CreateAction), "Created RayCluster %s/%s", rayClusterInstance.Namespace, rayClusterInstance.Name)
		} else {
			return nil, err
		}
	}
	logger.Info("Found the associated RayCluster for RayJob", "RayCluster", rayClusterNamespacedName)

	// Verify that RayJob is not in cluster selector mode first to avoid nil pointer dereference error during spec comparison.
	// This is checked by ensuring len(rayJobInstance.Spec.ClusterSelector) equals 0.
	if len(rayJobInstance.Spec.ClusterSelector) == 0 && !utils.CompareJsonStruct(rayClusterInstance.Spec, *rayJobInstance.Spec.RayClusterSpec) {
		logger.Info("Disregard changes in RayClusterSpec of RayJob")
	}

	return rayClusterInstance, nil
}

func (r *RayJobReconciler) constructRayClusterForRayJob(rayJobInstance *rayv1.RayJob, rayClusterName string) (*rayv1.RayCluster, error) {
	labels := make(map[string]string, len(rayJobInstance.Labels))
	maps.Copy(labels, rayJobInstance.Labels)
	labels[utils.RayOriginatedFromCRNameLabelKey] = rayJobInstance.Name
	labels[utils.RayOriginatedFromCRDLabelKey] = utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)
	labels[utils.RayJobSubmissionModeLabelKey] = string(rayJobInstance.Spec.SubmissionMode)

	annotations := make(map[string]string, len(rayJobInstance.Annotations))
	maps.Copy(annotations, rayJobInstance.Annotations)
	if rayJobInstance.Spec.SubmissionMode == rayv1.SidecarMode {
		annotations[utils.DisableProvisionedHeadRestartAnnotationKey] = "true"
	}

	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annotations,
			Name:        rayClusterName,
			Namespace:   rayJobInstance.Namespace,
		},
		Spec: *rayJobInstance.Spec.RayClusterSpec.DeepCopy(),
	}

	// Set the ownership in order to do the garbage collection by k8s.
	if err := ctrl.SetControllerReference(rayJobInstance, rayCluster, r.Scheme); err != nil {
		return nil, err
	}

	// Inject a submitter container into the head Pod in SidecarMode.
	if rayJobInstance.Spec.SubmissionMode == rayv1.SidecarMode {
		sidecar, err := getSubmitterContainer(rayJobInstance, rayCluster)
		if err != nil {
			return nil, err
		}
		rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers = append(
			rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers, sidecar)
		// In K8sJobMode, the submitter Job relies on the K8s Job backoffLimit API to restart if it fails.
		// This mainly handles WebSocket connection failures caused by transient network issues.
		// In SidecarMode, the pod-level RestartPolicy is set to Never.
		// The submitter container may override this with per-container restart rules when the SidecarSubmitterRestart feature gate is enabled.
		rayCluster.Spec.HeadGroupSpec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}

	return rayCluster, nil
}

func updateStatusToSuspendingIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) bool {
	logger := ctrl.LoggerFrom(ctx)
	if !rayJob.Spec.Suspend {
		return false
	}
	// In KubeRay, only `Running` and `Initializing` are allowed to transition to `Suspending`.
	validTransitions := map[rayv1.JobDeploymentStatus]struct{}{
		rayv1.JobDeploymentStatusRunning:      {},
		rayv1.JobDeploymentStatusInitializing: {},
	}
	if _, ok := validTransitions[rayJob.Status.JobDeploymentStatus]; !ok {
		logger.Info("The current status is not allowed to transition to `Suspending`", "JobDeploymentStatus", rayJob.Status.JobDeploymentStatus)
		return false
	}
	logger.Info("Try to transition the status to `Suspending`", "oldStatus", rayJob.Status.JobDeploymentStatus)
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusSuspending
	return true
}

func (r *RayJobReconciler) checkSubmitterAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) (shouldUpdate bool, finishedAt *time.Time, err error) {
	logger := ctrl.LoggerFrom(ctx)
	shouldUpdate = false
	finishedAt = nil
	var submitterContainerStatus *corev1.ContainerStatus
	var condition *batchv1.JobCondition

	switch rayJob.Spec.SubmissionMode {
	case rayv1.SidecarMode:
		var rayClusterInstance *rayv1.RayCluster
		var headPod *corev1.Pod

		rayClusterInstance, err = r.getOrCreateRayClusterInstance(ctx, rayJob)
		if err != nil {
			logger.Error(err, "Failed to get RayCluster instance for checking sidecar container status")
			return
		}

		headPod, err = common.GetRayClusterHeadPod(ctx, r.Client, rayClusterInstance)
		if err != nil {
			logger.Error(err, "Failed to get Ray head pod for checking sidecar container status")
			return
		}

		if headPod == nil {
			// If head pod is deleted, mark the RayJob as failed
			shouldUpdate = true
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
			rayJob.Status.Reason = rayv1.AppFailed
			rayJob.Status.Message = "Ray head pod not found."
			return
		}

		// Only check exit code when the feature gate is disabled.
		// When SidecarSubmitterRestart is enabled, the container restarts on non-zero exit,
		// so a terminated container is transient — not a permanent failure.
		if !features.Enabled(features.SidecarSubmitterRestart) {
			shouldUpdate, submitterContainerStatus = checkSidecarContainerStatus(headPod)
			if shouldUpdate {
				logger.Info("The submitter sidecar container has failed. Attempting to transition the status to `Failed`.",
					"Submitter sidecar container", submitterContainerStatus.Name, "Reason", submitterContainerStatus.State.Terminated.Reason, "Message", submitterContainerStatus.State.Terminated.Message)
				rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
				// The submitter sidecar container needs to wait for the user code to finish and retrieve its logs.
				// Therefore, a failed Submitter sidecar container indicates that the submission itself has failed or the user code has thrown an error.
				// If the failure is due to user code, the JobStatus and Job message will be updated accordingly from the previous reconciliation.
				if rayJob.Status.JobStatus == rayv1.JobStatusFailed {
					rayJob.Status.Reason = rayv1.AppFailed
				} else {
					rayJob.Status.Reason = rayv1.SubmissionFailed
					rayJob.Status.Message = fmt.Sprintf("Ray head pod container %s terminated with exit code %d: %s",
						submitterContainerStatus.Name, submitterContainerStatus.State.Terminated.ExitCode, submitterContainerStatus.State.Terminated.Reason)
				}
			}
		} else {
			submitterBackoffLimit := int32(2)
			if rayJob.Spec.SubmitterConfig != nil && rayJob.Spec.SubmitterConfig.BackoffLimit != nil {
				submitterBackoffLimit = *rayJob.Spec.SubmitterConfig.BackoffLimit
			}
			shouldUpdate, submitterContainerStatus = checkIsRestartCountExceeded(headPod, submitterBackoffLimit)
			if shouldUpdate {
				logger.Info("The submitter sidecar container has exceeded the max restart count. Attempting to transition the status to `Failed`.",
					"Submitter sidecar container", submitterContainerStatus.Name,
					"RestartCount", submitterContainerStatus.RestartCount)
				rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
				if rayJob.Status.JobStatus == rayv1.JobStatusFailed {
					rayJob.Status.Reason = rayv1.AppFailed
				} else {
					rayJob.Status.Reason = rayv1.SubmissionFailed
					rayJob.Status.Message = fmt.Sprintf("Ray head pod submitter container %s terminated after exceeding the maximum restart count",
						submitterContainerStatus.Name)
				}
			}
		}

		finishedAt = getSubmitterContainerFinishedTime(headPod)
		return
	case rayv1.K8sJobMode:
		job := &batchv1.Job{}
		// If the submitting Kubernetes Job reaches the backoff limit, transition the status to `Complete` or `Failed`.
		// This is because, beyond this point, it becomes impossible for the submitter to submit any further Ray jobs.
		// For light-weight mode, we don't transition the status to `Complete` or `Failed` based on the number of failed
		// requests. Instead, users can use the `ActiveDeadlineSeconds` to ensure that the RayJob in the light-weight
		// mode is not stuck in the `Running` status indefinitely.
		namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
		if err = r.Client.Get(ctx, namespacedName, job); err != nil {
			logger.Error(err, "Failed to get the submitter Kubernetes Job for RayJob", "NamespacedName", namespacedName)
			return
		}

		shouldUpdate, condition = checkK8sJobStatus(job)
		if shouldUpdate {
			logger.Info("The submitter Kubernetes Job has failed. Attempting to transition the status to `Failed`.",
				"Submitter K8s Job", job.Name, "Reason", condition.Reason, "Message", condition.Message)
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
			// The submitter Job needs to wait for the user code to finish and retrieve its logs.
			// Therefore, a failed Submitter Job indicates that the submission itself has failed or the user code has thrown an error.
			// If the failure is due to user code, the JobStatus and Job message will be updated accordingly from the previous reconciliation.
			if rayJob.Status.JobStatus == rayv1.JobStatusFailed {
				rayJob.Status.Reason = rayv1.AppFailed
			} else {
				rayJob.Status.Reason = rayv1.SubmissionFailed
				rayJob.Status.Message = fmt.Sprintf("Job submission has failed. Reason: %s. Message: %s", condition.Reason, condition.Message)
			}
		}

		var jobFinishedCondition *batchv1.JobCondition
		// Find the terminal condition to get its LastTransitionTime
		jobFinishedCondition = getJobFinishedCondition(job)
		if jobFinishedCondition != nil && !jobFinishedCondition.LastTransitionTime.IsZero() {
			finishedAt = &jobFinishedCondition.LastTransitionTime.Time
		}

		return
	default:
		// This means that the KubeRay logic is wrong, and it's better to panic as a system error than to allow the operator to
		// continue reconciling in a wrong state as an application error.
		// https://github.com/ray-project/kuberay/pull/3971#discussion_r2292808734
		panic("You can only call checkSubmitterAndUpdateStatusIfNeeded when using K8sJobMode and SidecarMode.")
	}
}

func checkK8sJobStatus(job *batchv1.Job) (bool, *batchv1.JobCondition) {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true, &condition
		}
	}
	return false, nil
}

func getJobFinishedCondition(job *batchv1.Job) *batchv1.JobCondition {
	for _, condition := range job.Status.Conditions {
		if (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) && condition.Status == corev1.ConditionTrue {
			return &condition
		}
	}
	return nil
}

func checkSidecarContainerStatus(headPod *corev1.Pod) (bool, *corev1.ContainerStatus) {
	for _, containerStatus := range headPod.Status.ContainerStatuses {
		if containerStatus.Name == utils.SubmitterContainerName {
			// Check for terminated containers with error exit codes
			// Based on the document, "ray job submit" will exit with 0 if the job succeeded, or exit with 1 if it failed.
			// https://docs.ray.io/en/latest/cluster/running-applications/job-submission/cli.html#ray-job-submit
			if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
				return true, &containerStatus
			}
			break
		}
	}
	return false, nil
}

func checkIsRestartCountExceeded(headPod *corev1.Pod, backoffLimit int32) (bool, *corev1.ContainerStatus) {
	for _, containerStatus := range headPod.Status.ContainerStatuses {
		if containerStatus.Name == utils.SubmitterContainerName {
			// Only check when the container has been terminated at least once.
			// When the submitter container fails in a CrashLoopBackOff fashion, LastTerminationState.Terminated is populated
			if containerStatus.LastTerminationState.Terminated != nil {
				// If the container exited successfully, we do not fail the job.
				if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode == 0 {
					break
				}
				return containerStatus.RestartCount >= backoffLimit, &containerStatus
			}
			break
		}
	}
	return false, nil
}

func checkActiveDeadlineAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) bool {
	logger := ctrl.LoggerFrom(ctx)
	if rayJob.Spec.ActiveDeadlineSeconds == nil || time.Now().Before(rayJob.Status.StartTime.Add(time.Duration(*rayJob.Spec.ActiveDeadlineSeconds)*time.Second)) {
		return false
	}

	logger.Info("The RayJob has passed the activeDeadlineSeconds. Transition the status to `Failed`.", "StartTime", rayJob.Status.StartTime, "ActiveDeadlineSeconds", *rayJob.Spec.ActiveDeadlineSeconds)
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
	rayJob.Status.Reason = rayv1.DeadlineExceeded
	rayJob.Status.Message = fmt.Sprintf("The RayJob has passed the activeDeadlineSeconds. StartTime: %v. ActiveDeadlineSeconds: %d", rayJob.Status.StartTime, *rayJob.Spec.ActiveDeadlineSeconds)
	return true
}

// checkPreRunningDeadlineAndUpdateStatusIfNeeded transitions the RayJob to Failed if it has not
// reached the Running state within preRunningDeadlineSeconds seconds of StartTime.
func checkPreRunningDeadlineAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) bool {
	logger := ctrl.LoggerFrom(ctx)

	if rayJob.Spec.PreRunningDeadlineSeconds == nil || time.Now().Before(rayJob.Status.StartTime.Add(time.Duration(*rayJob.Spec.PreRunningDeadlineSeconds)*time.Second)) {
		return false
	}

	logger.Info("The RayJob has passed the preRunningDeadlineSeconds. Transition the status to `Failed`.", "StartTime", rayJob.Status.StartTime, "PreRunningDeadlineSeconds", *rayJob.Spec.PreRunningDeadlineSeconds)
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
	rayJob.Status.Reason = rayv1.PreRunningDeadlineExceeded
	rayJob.Status.Message = fmt.Sprintf("The RayJob has passed the preRunningDeadlineSeconds. StartTime: %v. PreRunningDeadlineSeconds: %d", rayJob.Status.StartTime, *rayJob.Spec.PreRunningDeadlineSeconds)
	return true
}

func checkSubmitterFinishedTimeoutAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob, finishedAt *time.Time) bool {
	logger := ctrl.LoggerFrom(ctx)

	// Check if timeout is configured and submitter has finished
	if finishedAt == nil {
		return false
	}

	// Check if timeout has been exceeded
	if time.Now().Before(finishedAt.Add(DefaultSubmitterFinishedTimeout)) {
		return false
	}

	logger.Info("The RayJob has passed the submitterFinishedTimeoutSeconds. Transition the status to terminal.",
		"SubmitterFinishedTime", finishedAt,
		"SubmitterFinishedTimeoutSeconds", DefaultSubmitterFinishedTimeout.String())

	rayJob.Status.JobStatus = rayv1.JobStatusFailed
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
	rayJob.Status.Reason = rayv1.JobDeploymentStatusTransitionGracePeriodExceeded
	rayJob.Status.Message = fmt.Sprintf("The RayJob submitter finished at %v but the ray job did not reach terminal state within %v",
		finishedAt.Format(time.DateTime), DefaultSubmitterFinishedTimeout)
	return true
}

func getJobStatusCheckTimeoutSeconds() int {
	timeoutSeconds, err := strconv.Atoi(os.Getenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS))
	if err != nil || timeoutSeconds <= 0 {
		return utils.DEFAULT_RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS
	}
	return timeoutSeconds
}

// Clears JobStatusCheckFailureStartTime. Returns whether status should be persisted.
func clearJobStatusCheckFailureTimer(rayJob *rayv1.RayJob) bool {
	if rayJob.Status.JobStatusCheckFailureStartTime == nil {
		return false
	}
	rayJob.Status.JobStatusCheckFailureStartTime = nil
	return true
}

// reconcileHTTPModeJobNotFound handles GetJobInfo 404 in HTTPMode. If the job has not been
// observed on the cluster yet, this performs the initial submission. If the job was already
// observed, the 404 means the cluster lost the job state (e.g. the head Pod was recreated),
// so the RayJob transitions to Failed and retries are governed by spec.backoffLimit, consistent
// with the other submission modes.
// Returns whether status should be persisted.
func reconcileHTTPModeJobNotFound(
	ctx context.Context,
	rayJob *rayv1.RayJob,
	rayDashboardClient dashboardclient.RayDashboardClientInterface,
) bool {
	logger := ctrl.LoggerFrom(ctx)

	if rayJob.Status.JobStatus != rayv1.JobStatusNew {
		logger.Info("The Ray job was previously submitted but no longer exists in the RayCluster. Marking the RayJob as Failed.",
			"JobId", rayJob.Status.JobId, "JobStatus", rayJob.Status.JobStatus)
		rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
		rayJob.Status.Reason = rayv1.AppFailed
		rayJob.Status.Message = "The Ray job was previously submitted but no longer exists in the RayCluster, possibly because the head Pod was recreated."
		return true
	}

	logger.Info("The Ray job was not found. Submit a Ray job via an HTTP request.", "JobId", rayJob.Status.JobId)

	if _, submitErr := rayDashboardClient.SubmitJob(ctx, rayJob); submitErr != nil {
		logger.Error(submitErr, "Failed to submit the Ray job", "JobId", rayJob.Status.JobId)
		return recordJobStatusCheckFailure(ctx, rayJob, submitErr)
	}

	return clearJobStatusCheckFailureTimer(rayJob)
}

// Records the first job status check failure timestamp.
// Returns whether the timestamp was set and the status should be persisted.
func recordJobStatusCheckFailure(ctx context.Context, rayJob *rayv1.RayJob, err error) bool {
	if rayJob.Status.JobStatusCheckFailureStartTime != nil {
		return false
	}

	logger := ctrl.LoggerFrom(ctx)
	rayJob.Status.JobStatusCheckFailureStartTime = &metav1.Time{Time: time.Now()}
	logger.Error(err, "Job status check began failing.", "JobId", rayJob.Status.JobId)
	return true
}

func checkJobStatusCheckTimeoutAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) bool {
	logger := ctrl.LoggerFrom(ctx)
	if rayJob.Status.JobStatusCheckFailureStartTime == nil {
		return false
	}

	timeout := time.Duration(getJobStatusCheckTimeoutSeconds()) * time.Second
	startTime := rayJob.Status.JobStatusCheckFailureStartTime.Time
	if time.Now().Before(startTime.Add(timeout)) {
		return false
	}

	logger.Info("Job status check timeout exceeded.", "JobId", rayJob.Status.JobId, "JobStatusCheckFailureStartTime", startTime, "timeoutSeconds", timeout.Seconds())
	rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
	rayJob.Status.Reason = rayv1.JobStatusCheckTimeoutExceeded
	rayJob.Status.Message = fmt.Sprintf("Job status checks have been failing since %v; exceeded timeout of %ds",
		startTime.Format(time.DateTime), int(timeout.Seconds()))
	return true
}

func jobStatusCheckFailureStartTimeChanged(oldTime, newTime *metav1.Time) bool {
	if oldTime == nil && newTime == nil {
		return false
	}
	if oldTime == nil || newTime == nil {
		return true
	}
	return !oldTime.Time.Equal(newTime.Time)
}

func checkTransitionGracePeriodAndUpdateStatusIfNeeded(ctx context.Context, rayJob *rayv1.RayJob) bool {
	logger := ctrl.LoggerFrom(ctx)
	if rayv1.IsJobTerminal(rayJob.Status.JobStatus) && rayJob.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusRunning {
		rayJobDeploymentGracePeriodTime, err := strconv.Atoi(os.Getenv(utils.RAYJOB_DEPLOYMENT_STATUS_TRANSITION_GRACE_PERIOD_SECONDS))
		if err != nil {
			rayJobDeploymentGracePeriodTime = utils.DEFAULT_RAYJOB_DEPLOYMENT_STATUS_TRANSITION_GRACE_PERIOD_SECONDS
		}

		// EndTime isn't nil when JobStatus is in a terminal state under normal conditions.
		// This check is a nil pointer dereference safeguard when older RayJob CRDs without the
		// RayJobStatusInfo field are used with newer versions of KubeRay operator.
		if rayJob.Status.RayJobStatusInfo.EndTime == nil {
			return false
		}

		if time.Now().Before(rayJob.Status.RayJobStatusInfo.EndTime.Add(time.Duration(rayJobDeploymentGracePeriodTime) * time.Second)) {
			return false
		}
		logger.Info("JobDeploymentStatus does not transition to Complete or Failed within the grace period after JobStatus reaches a terminal state.", "EndTime", rayJob.Status.RayJobStatusInfo.EndTime, "rayJobDeploymentGracePeriodTime", rayJobDeploymentGracePeriodTime)
		rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusComplete
		if rayJob.Status.JobStatus == rayv1.JobStatusFailed {
			rayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
		}
		rayJob.Status.Reason = rayv1.JobDeploymentStatusTransitionGracePeriodExceeded
		rayJob.Status.Message = "JobDeploymentStatus does not transition to Complete or Failed within the grace period after JobStatus reaches a terminal state."
		return true
	}
	return false
}

func getSubmitterContainerFinishedTime(pod *corev1.Pod) *time.Time {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == utils.SubmitterContainerName {
			if containerStatus.State.Terminated != nil {
				return &containerStatus.State.Terminated.FinishedAt.Time
			}
			break
		}
	}
	return nil
}

// handleDeletionRules processes the DeletionRules with a impact-aware strategy.
// It categorizes rules into "overdue" and "pending". If overdue rules exist,
// it executes the most destructive one and then requeues for the next pending rule.
// If no rules are overdue, it simply requeues for the
// next pending rule. This function performs at most one deletion action per reconciliation.
func (r *RayJobReconciler) handleDeletionRules(ctx context.Context, rayJob *rayv1.RayJob) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithValues("deletionMechanism", "DeletionRules")
	nowTime := time.Now()

	var overdueRules []rayv1.DeletionRule
	var nextRequeueTime *time.Time

	// Categorize all applicable and incomplete rules into "overdue" or "pending".
	for _, rule := range rayJob.Spec.DeletionStrategy.DeletionRules {
		// Skip rules that don't match the current job status or job deployment status.
		if !isDeletionRuleMatched(rule, rayJob) {
			continue
		}

		deletionTime := rayJob.Status.EndTime.Add(time.Duration(rule.Condition.TTLSeconds) * time.Second)
		// Track the earliest requeue time to re-check later.
		if nowTime.Before(deletionTime) {
			if nextRequeueTime == nil || deletionTime.Before(*nextRequeueTime) {
				nextRequeueTime = &deletionTime
			}
			continue
		}

		// Need to check if the deletion action has already been completed to ensure idempotency.
		isCompleted, err := r.isDeletionActionCompleted(ctx, rayJob, rule.Policy)
		if err != nil {
			logger.Error(err, "Failed to check if deletion action is completed", "rule", rule)
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
		if isCompleted {
			logger.Info("Skipping completed deletion rule", "rule", rule)
			continue
		}

		overdueRules = append(overdueRules, rule)
	}

	// Handle overdue rules if any exist.
	if len(overdueRules) > 0 {
		ruleToExecute := selectMostImpactfulRule(overdueRules)
		logger.Info("Executing the most impactful overdue deletion rule", "rule", ruleToExecute, "overdueRulesCount", len(overdueRules))
		if _, err := r.executeDeletionPolicy(ctx, rayJob, ruleToExecute.Policy); err != nil {
			// If execution fails, return immediately for a retry.
			return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
		}
	}

	if nextRequeueTime != nil {
		requeueAfter := requeueDelayFor(*nextRequeueTime)
		logger.Info("Requeuing for the next scheduled rule", "requeueAfter", requeueAfter)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	logger.Info("All applicable deletion rules have been processed.")
	return ctrl.Result{}, nil
}

// handleLegacyDeletionPolicy handles the deprecated onSuccess and onFailure policies.
func (r *RayJobReconciler) handleLegacyDeletionPolicy(ctx context.Context, rayJob *rayv1.RayJob) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithValues("deletionMechanism", "LegacyOnSuccessFailure")

	var policy rayv1.DeletionPolicyType
	switch rayJob.Status.JobStatus {
	case rayv1.JobStatusSucceeded:
		policy = *rayJob.Spec.DeletionStrategy.OnSuccess.Policy
	case rayv1.JobStatusFailed:
		policy = *rayJob.Spec.DeletionStrategy.OnFailure.Policy
	default:
		logger.Info("JobStatus is not valid for deletion, no policy applied", "jobStatus", rayJob.Status.JobStatus)
		return ctrl.Result{}, nil
	}

	// If the policy is DeleteNone, we are done.
	if policy == rayv1.DeleteNone {
		logger.Info("Deletion policy is DeleteNone; no action taken.")
		return ctrl.Result{}, nil
	}

	// These legacy policies use the top-level TTLSecondsAfterFinished.
	nowTime := time.Now()
	ttlSeconds := rayJob.Spec.TTLSecondsAfterFinished
	shutdownTime := rayJob.Status.EndTime.Add(time.Duration(ttlSeconds) * time.Second)
	logger.Info("Evaluating legacy deletion policy (onSuccess/onFailure)",
		"JobDeploymentStatus", rayJob.Status.JobDeploymentStatus,
		"policy", policy,
		"JobStatus", rayJob.Status.JobStatus,
		"ttlSecondsAfterFinished", ttlSeconds,
		"Status.endTime", rayJob.Status.EndTime,
		"Now", nowTime,
		"ShutdownTime", shutdownTime)

	if shutdownTime.After(nowTime) {
		requeueAfter := requeueDelayFor(shutdownTime)
		logger.Info("TTL has not been met for legacy policy. Requeuing.", "shutdownTime", shutdownTime, "requeueAfter", requeueAfter)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	logger.Info("Executing legacy deletion policy.", "policy", policy)
	return r.executeDeletionPolicy(ctx, rayJob, policy)
}

// handleShutdownAfterJobFinishes handles the oldest deletion mechanism, the ShutdownAfterJobFinishes boolean flag.
func (r *RayJobReconciler) handleShutdownAfterJobFinishes(ctx context.Context, rayJob *rayv1.RayJob) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx).WithValues("deletionMechanism", "ShutdownAfterJobFinishes")

	nowTime := time.Now()
	ttlSeconds := rayJob.Spec.TTLSecondsAfterFinished
	shutdownTime := rayJob.Status.EndTime.Add(time.Duration(ttlSeconds) * time.Second)
	logger.Info("Evaluating job deletion policy based on ShutdownAfterJobFinishes",
		"JobDeploymentStatus", rayJob.Status.JobDeploymentStatus,
		"ShutdownAfterJobFinishes", rayJob.Spec.ShutdownAfterJobFinishes,
		"ClusterSelector", rayJob.Spec.ClusterSelector,
		"ttlSecondsAfterFinished", ttlSeconds,
		"Status.endTime", rayJob.Status.EndTime,
		"Now", nowTime,
		"ShutdownTime", shutdownTime)

	if shutdownTime.After(nowTime) {
		requeueAfter := requeueDelayFor(shutdownTime)
		logger.Info("TTL has not been met for ShutdownAfterJobFinishes. Requeuing.", "shutdownTime", shutdownTime, "requeueAfter", requeueAfter)
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	var err error
	if s := os.Getenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES); strings.ToLower(s) == "true" {
		err = r.Client.Delete(ctx, rayJob)
		if err == nil {
			logger.Info("RayJob is deleted", "RayJob", rayJob.Name)
		}
	} else {
		// We only need to delete the RayCluster. We don't need to delete the submitter Kubernetes Job so that users can still access
		// the driver logs. In addition, a completed Kubernetes Job does not actually use any compute resources.
		_, err = r.deleteClusterResources(ctx, rayJob)
		if err == nil {
			logger.Info("RayCluster is deleted", "RayCluster", rayJob.Status.RayClusterName)
		}
	}

	if err != nil {
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
	}

	return ctrl.Result{}, nil
}

// executeDeletionPolicy performs the actual resource deletion based on the policy type.
// This function centralizes the deletion logic to avoid code duplication.
func (r *RayJobReconciler) executeDeletionPolicy(ctx context.Context, rayJob *rayv1.RayJob, policy rayv1.DeletionPolicyType) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)
	var err error

	switch policy {
	case rayv1.DeleteCluster:
		logger.Info("Executing deletion policy: DeleteCluster", "RayCluster", rayJob.Status.RayClusterName)
		_, err = r.deleteClusterResources(ctx, rayJob)
	case rayv1.DeleteWorkers:
		logger.Info("Executing deletion policy: DeleteWorkers", "RayCluster", rayJob.Status.RayClusterName)
		err = r.suspendWorkerGroups(ctx, rayJob)
	case rayv1.DeleteSelf:
		logger.Info("Executing deletion policy: DeleteSelf", "RayJob", rayJob.Name)
		err = r.Client.Delete(ctx, rayJob)
	case rayv1.DeleteNone:
		// This should be handled by the callers, but we include it for safety.
		logger.Info("Executing deletion policy: DeleteNone. No action taken.")
	default:
		// This case should not be reached if validation is working correctly.
		logger.Error(fmt.Errorf("unknown deletion policy: %s", policy), "Unknown deletion policy encountered")
	}

	if err != nil {
		return ctrl.Result{RequeueAfter: RayJobDefaultRequeueDuration}, err
	}
	return ctrl.Result{}, nil
}

// isDeletionRuleMatched checks if the deletion rule matches the current job status or job deployment status.
func isDeletionRuleMatched(rule rayv1.DeletionRule, rayJob *rayv1.RayJob) bool {
	// It's guaranteed that exactly one of JobStatus and JobDeploymentStatus is specified.
	if rule.Condition.JobStatus != nil {
		return *rule.Condition.JobStatus == rayJob.Status.JobStatus
	}
	return *rule.Condition.JobDeploymentStatus == rayJob.Status.JobDeploymentStatus
}

// isDeletionActionCompleted checks if the state corresponding to a deletion policy is already achieved.
// This is crucial for making the reconciliation loop idempotent by checking the actual cluster state.
func (r *RayJobReconciler) isDeletionActionCompleted(ctx context.Context, rayJob *rayv1.RayJob, policy rayv1.DeletionPolicyType) (bool, error) {
	clusterIdentifier := common.RayJobRayClusterNamespacedName(rayJob)
	cluster := &rayv1.RayCluster{}

	switch policy {
	case rayv1.DeleteWorkers:
		if err := r.Get(ctx, clusterIdentifier, cluster); err != nil {
			if errors.IsNotFound(err) {
				// If the cluster is gone, the workers are definitely gone.
				return true, nil
			}
			// For any other error, we can't be sure of the state, so report the error.
			return false, err
		}

		if !cluster.DeletionTimestamp.IsZero() {
			// If the cluster is being deleted, we consider the action complete.
			return true, nil
		}

		// If the cluster exists, check if all worker groups are suspended.
		for _, wg := range cluster.Spec.WorkerGroupSpecs {
			if wg.Suspend == nil || !*wg.Suspend {
				// Found an active worker group, so the action is not complete.
				return false, nil
			}
		}

		return true, nil

	case rayv1.DeleteCluster:
		if err := r.Get(ctx, clusterIdentifier, cluster); err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			// For any other error, we can't be sure of the state, so report the error.
			return false, err
		}

		if !cluster.DeletionTimestamp.IsZero() {
			// If the cluster is being deleted, we consider the action complete.
			return true, nil
		}

		return false, nil

	case rayv1.DeleteSelf:
		// This action is terminal. If this function is running, the RayJob still exists,
		// so the action cannot be considered complete.
		return false, nil

	case rayv1.DeleteNone:
		// "DeleteNone" is a no-op and is always considered complete.
		return true, nil
	}

	return false, fmt.Errorf("unknown deletion policy for completion check: %s", policy)
}

// cleanupBatchSchedulerResources cleans up batch scheduler resources (e.g., zeroes out the Volcano
// PodGroup) for the given RayJob and returns an error if the cleanup failed. Callers that must
// guarantee the resources are released (e.g., suspend) should requeue on error.
func (r *RayJobReconciler) cleanupBatchSchedulerResources(ctx context.Context, rayJobInstance *rayv1.RayJob) error {
	if r.options.BatchSchedulerManager == nil {
		return nil
	}

	scheduler, err := r.options.BatchSchedulerManager.GetScheduler()
	if err != nil {
		return fmt.Errorf("failed to get batch scheduler: %w", err)
	}

	didUpdate, err := scheduler.CleanupOnCompletion(ctx, rayJobInstance)
	if err != nil {
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeWarning, string(utils.FailedToCleanupBatchScheduler), string(utils.CleanupAction),
			"Failed to cleanup batch scheduler resources for RayJob %s/%s: %v", rayJobInstance.Namespace, rayJobInstance.Name, err)
		return fmt.Errorf("failed to cleanup batch scheduler resources: %w", err)
	}
	if didUpdate {
		r.Recorder.Eventf(rayJobInstance, nil, corev1.EventTypeNormal, string(utils.BatchSchedulerCleanedUp), string(utils.CleanupAction),
			"Cleaned up batch scheduler resources for RayJob %s/%s", rayJobInstance.Namespace, rayJobInstance.Name)
	}
	return nil
}

// selectMostImpactfulRule finds the rule with the most destructive policy from a given list.
func selectMostImpactfulRule(rules []rayv1.DeletionRule) rayv1.DeletionRule {
	order := map[rayv1.DeletionPolicyType]int{
		rayv1.DeleteSelf:    4,
		rayv1.DeleteCluster: 3,
		rayv1.DeleteWorkers: 2,
		rayv1.DeleteNone:    1,
	}

	mostImpactfulRule := rules[0]
	for _, rule := range rules[1:] {
		if order[rule.Policy] > order[mostImpactfulRule.Policy] {
			mostImpactfulRule = rule
		}
	}
	return mostImpactfulRule
}

// requeueDelayFor computes the duration for the next requeue, ensuring a minimum buffer.
func requeueDelayFor(t time.Time) time.Duration {
	return time.Until(t) + 2*time.Second
}
