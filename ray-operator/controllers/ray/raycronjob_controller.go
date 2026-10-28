package ray

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	RayCronJobDefaultRequeueDuration = 3 * time.Second
)

// RayCronJobReconciler reconciles a RayCronJob object
type RayCronJobReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	clock    clock.Clock
}

// NewRayCronJobReconciler returns a new RayCronJobReconciler
func NewRayCronJobReconciler(mgr ctrl.Manager) *RayCronJobReconciler {
	return &RayCronJobReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorder("raycronjob-controller"),
		clock:    clock.RealClock{},
	}
}

//+kubebuilder:rbac:groups=ray.io,resources=raycronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ray.io,resources=raycronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ray.io,resources=raycronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=ray.io,resources=rayjobs,verbs=create
//+kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// [WARNING]: There MUST be a newline after kubebuilder markers.
// Reconcile reads that state of a RayCronJob object and makes changes based on it
// and what is in the RayCronJob.Spec
// Automatically generate RBAC rules to allow the Controller to read and write workloads
// Reconcile used to bridge the desired state with the current state
func (r *RayCronJobReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Get RayCronJob instance
	rayCronJobInstance := &rayv1.RayCronJob{}
	if err := r.Get(ctx, request.NamespacedName, rayCronJobInstance); err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request. Stop reconciliation.
			logger.Info("RayCronJob resource not found.")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Failed to get RayCronJob")
		return ctrl.Result{RequeueAfter: RayCronJobDefaultRequeueDuration}, err
	}

	// Please do NOT modify `originalRayCronJobInstance` in the following code.
	originalRayCronJobInstance := rayCronJobInstance.DeepCopy()

	// validate RayCronJob
	if err := utils.ValidateRayCronJobSpec(rayCronJobInstance); err != nil {
		r.Recorder.Eventf(rayCronJobInstance, nil, corev1.EventTypeWarning, string(utils.InvalidRayCronJobSpec), string(utils.ValidateAction),
			"%s/%s: %v", rayCronJobInstance.Namespace, rayCronJobInstance.Name, err)
		return ctrl.Result{}, nil
	}

	// check if the Suspend is set
	if rayCronJobInstance.Spec.Suspend != nil && *rayCronJobInstance.Spec.Suspend {
		logger.V(1).Info("RayCronJob suspended, no new RayJobs will be created.")
		r.Recorder.Eventf(rayCronJobInstance, nil, corev1.EventTypeNormal, string(utils.SuspendedRayCronJob), string(utils.SuspendAction),
			"RayCronJob suspended, no new RayJobs will be created")
		return ctrl.Result{}, nil
	}

	// Parse the schedule after validation
	schedule, err := cron.ParseStandard(formatSchedule(rayCronJobInstance))
	if err != nil {
		// This should not happen as validation already checked the schedule
		logger.Error(err, "Failed to parse validated cron schedule")
		return ctrl.Result{RequeueAfter: RayCronJobDefaultRequeueDuration}, err
	}

	now := r.clock.Now()

	// use CreationTimestamp when job not yet scheduled
	earlistTime := rayCronJobInstance.CreationTimestamp.Time
	if rayCronJobInstance.Status.LastScheduleTime != nil {
		earlistTime = rayCronJobInstance.Status.LastScheduleTime.Time
	}

	scheduleTime := schedule.Next(earlistTime)
	// if scheduleTime is after now, requeue it with their time difference
	if scheduleTime.After(now) {
		return ctrl.Result{RequeueAfter: scheduleTime.Sub(now)}, nil
	}

	// create ray job
	rayJob, err := r.constructRayJob(rayCronJobInstance, scheduleTime)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, rayJob); err != nil {
		if !errors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create RayJob from RayCronJob")
			return ctrl.Result{}, err
		}
		// The deterministic name means a RayJob for this tick already exists, so a previous
		// reconcile already scheduled it. Treat as success and still advance LastScheduleTime.
		logger.Info("RayJob for this tick already exists, skipping creation", "rayJobName", rayJob.Name, "namespace", rayJob.Namespace)
	} else {
		logger.Info("Successfully created RayJob", "rayJobName", rayJob.Name, "namespace", rayJob.Namespace)
	}
	rayCronJobInstance.Status.LastScheduleTime = &metav1.Time{Time: scheduleTime}

	// Set next schedule time
	nextScheduleTime := schedule.Next(scheduleTime)
	requeueAt := nextScheduleTime.Sub(now)
	logger.Info("Schedule timing", "now", now, "nextScheduledTime", nextScheduleTime, "requeueAfter", requeueAt)

	// This is the only place where we update the RayCronJob status. This will only update the RayCronJob status if LastScheduleTime has changed.
	if err = r.updateRayCronJobStatus(ctx, originalRayCronJobInstance, rayCronJobInstance); err != nil {
		logger.Info("Failed to update RayCronJob status", "error", err)
		return ctrl.Result{RequeueAfter: RayCronJobDefaultRequeueDuration}, err
	}

	return ctrl.Result{RequeueAfter: requeueAt}, nil
}

func (r *RayCronJobReconciler) updateRayCronJobStatus(ctx context.Context, oldRayCronJob *rayv1.RayCronJob, newRayCronJob *rayv1.RayCronJob) error {
	logger := ctrl.LoggerFrom(ctx)
	oldRayCronJobStatus := oldRayCronJob.Status
	newRayCronJobStatus := newRayCronJob.Status
	if oldRayCronJobStatus.LastScheduleTime != newRayCronJobStatus.LastScheduleTime {

		logger.Info("updateRayCronJobStatus", "old RayCronJobStatus", oldRayCronJobStatus, "new RayCronJobStatus", newRayCronJobStatus)
		if err := r.Status().Update(ctx, newRayCronJob); err != nil {
			return err
		}
	}
	return nil
}

func getTimeHashInMinutes(scheduledTime time.Time) int64 {
	return scheduledTime.Unix() / 60
}

// getRayJobName builds a deterministic child name so a repeated Create for the same tick
// collides with AlreadyExists instead of creating a duplicate (matches batch/v1 CronJob).
func getRayJobName(cronJobName string, scheduledTime time.Time) string {
	return fmt.Sprintf("%s-%d", cronJobName, getTimeHashInMinutes(scheduledTime))
}

func (r *RayCronJobReconciler) constructRayJob(cronJob *rayv1.RayCronJob, expectedTimestamp time.Time) (*rayv1.RayJob, error) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getRayJobName(cronJob.Name, expectedTimestamp),
			Namespace: cronJob.Namespace,
			Labels: map[string]string{
				utils.RayCronJobNameLabelKey: cronJob.Name,
			},
			Annotations: map[string]string{
				utils.RayCronJobTimestampAnnotationKey: expectedTimestamp.UTC().Format(time.RFC3339),
			},
		},
		Spec: *cronJob.Spec.JobTemplate.DeepCopy(),
	}

	// Set the ownership in order to do the garbage collection by k8s.
	if err := ctrl.SetControllerReference(cronJob, rayJob, r.Scheme); err != nil {
		return nil, err
	}

	return rayJob, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RayCronJobReconciler) SetupWithManager(mgr ctrl.Manager, reconcileConcurrency int) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rayv1.RayCronJob{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: reconcileConcurrency,
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayCronJob")
				if request != nil {
					logger = logger.WithValues("RayCronJob", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

// formatSchedule formats the schedule string based on schedule and time zone in RayCronJob Spec
func formatSchedule(cronJob *rayv1.RayCronJob) string {
	if cronJob.Spec.TimeZone != nil {
		return fmt.Sprintf("TZ=%s %s", *cronJob.Spec.TimeZone, cronJob.Spec.Schedule)
	}

	return cronJob.Spec.Schedule
}
