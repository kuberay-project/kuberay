package metrics

import (
	"context"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

//go:generate mockgen -destination=mocks/ray_job_metrics_mock.go -package=mocks github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics RayJobMetricsObserver
type RayJobMetricsObserver interface {
	ObserveRayJobExecutionDuration(name, namespace string, uid types.UID, jobDeploymentStatus rayv1.JobDeploymentStatus, retryCount int, duration float64)
}

// RayJobMetricsManager implements the prometheus.Collector and RayJobMetricsObserver interface to collect ray job metrics.
type RayJobMetricsManager struct {
	rayJobExecutionDurationSeconds *prometheus.GaugeVec
	rayJobInfo                     *prometheus.Desc
	rayJobDeploymentStatus         *prometheus.Desc
	client                         client.Client
	log                            logr.Logger
}

// NewRayJobMetricsManager creates a new RayJobMetricsManager instance.
func NewRayJobMetricsManager(ctx context.Context, client client.Client) *RayJobMetricsManager {
	collector := &RayJobMetricsManager{
		rayJobExecutionDurationSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kuberay_job_execution_duration_seconds",
				Help: "Duration from when the RayJob CR’s JobDeploymentStatus transitions from Initializing to either the Retrying state or a terminal state, such as Complete or Failed. The Retrying state indicates that the CR previously failed and that spec.backoffLimit is enabled.",
			},
			[]string{"name", "namespace", "uid", "job_deployment_status", "retry_count"},
		),
		// rayJobInfo is a gauge metric that indicates the metadata information about RayJob custom resources.
		rayJobInfo: prometheus.NewDesc(
			"kuberay_job_info",
			"Metadata information about RayJob custom resources",
			[]string{"name", "namespace", "uid"},
			nil,
		),
		// rayJobDeploymentStatus is a gauge metric that indicates the current deployment status of the RayJob custom resources.
		rayJobDeploymentStatus: prometheus.NewDesc(
			"kuberay_job_deployment_status",
			"The RayJob's current deployment status",
			[]string{"name", "namespace", "uid", "deployment_status"},
			nil,
		),
		client: client,
		log:    ctrl.LoggerFrom(ctx),
	}
	return collector
}

// Describe implements prometheus.Collector interface Describe method.
func (r *RayJobMetricsManager) Describe(ch chan<- *prometheus.Desc) {
	r.rayJobExecutionDurationSeconds.Describe(ch)
	ch <- r.rayJobInfo
	ch <- r.rayJobDeploymentStatus
}

// Collect implements prometheus.Collector interface Collect method.
func (r *RayJobMetricsManager) Collect(ch chan<- prometheus.Metric) {
	r.rayJobExecutionDurationSeconds.Collect(ch)

	var rayJobList rayv1.RayJobList
	err := r.client.List(context.Background(), &rayJobList)
	if err != nil {
		r.log.Error(err, "Failed to list RayJob resources")
		return
	}

	for _, rayJob := range rayJobList.Items {
		r.collectRayJobInfo(&rayJob, ch)
		r.collectRayJobDeploymentStatus(&rayJob, ch)
	}
}

func (r *RayJobMetricsManager) ObserveRayJobExecutionDuration(name, namespace string, uid types.UID, jobDeploymentStatus rayv1.JobDeploymentStatus, retryCount int, duration float64) {
	r.rayJobExecutionDurationSeconds.WithLabelValues(name, namespace, string(uid), string(jobDeploymentStatus), strconv.Itoa(retryCount)).Set(duration)
}

// DeleteRayJobMetrics removes metrics that belongs to the specified RayJob.
func (r *RayJobMetricsManager) DeleteRayJobMetrics(name, namespace string) {
	numCleanedUpMetrics := r.rayJobExecutionDurationSeconds.DeletePartialMatch(prometheus.Labels{"name": name, "namespace": namespace})
	r.log.Info("Cleaned up expired rayJob metric", "name", name, "namespace", namespace, "numCleanedUpMetrics", numCleanedUpMetrics)
}

func (r *RayJobMetricsManager) collectRayJobInfo(rayJob *rayv1.RayJob, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayJobInfo,
		prometheus.GaugeValue,
		1,
		rayJob.Name,
		rayJob.Namespace,
		string(rayJob.UID),
	)
}

func (r *RayJobMetricsManager) collectRayJobDeploymentStatus(rayJob *rayv1.RayJob, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayJobDeploymentStatus,
		prometheus.GaugeValue,
		1,
		rayJob.Name,
		rayJob.Namespace,
		string(rayJob.UID),
		string(rayJob.Status.JobDeploymentStatus),
	)
}
