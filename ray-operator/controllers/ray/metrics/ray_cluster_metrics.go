package metrics

import (
	"context"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

//go:generate mockgen -destination=mocks/ray_cluster_metrics_mock.go -package=mocks github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics RayClusterMetricsObserver
type RayClusterMetricsObserver interface {
	ObserveRayClusterProvisionedDuration(name, namespace string, uid types.UID, duration float64)
}

// RayClusterMetricsManager implements the prometheus.Collector and RayClusterMetricsObserver interface to collect ray cluster metrics.
type RayClusterMetricsManager struct {
	rayClusterProvisionedDurationSeconds *prometheus.GaugeVec
	rayClusterInfo                       *prometheus.Desc
	rayClusterConditionProvisioned       *prometheus.Desc
	client                               client.Client
	log                                  logr.Logger
}

// NewRayClusterMetricsManager creates a new RayClusterManager instance.
func NewRayClusterMetricsManager(ctx context.Context, client client.Client) *RayClusterMetricsManager {
	manager := &RayClusterMetricsManager{
		rayClusterProvisionedDurationSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "kuberay_cluster_provisioned_duration_seconds",
				Help: "The time, in seconds, when a RayCluster's `RayClusterProvisioned` status transitions from false (or unset) to true",
			},
			[]string{"name", "namespace", "uid"},
		),
		// rayClusterInfo is a gauge metric that indicates the metadata information about RayCluster custom resources.
		// The `owner_kind` label indicates the CRD type that originated the RayCluster.
		// Possible values for `owner_kind`:
		// - None: Created by a RayCluster CRD
		// - RayJob: Created by a RayJob CRD
		// - RayService: Created by a RayService CRD
		rayClusterInfo: prometheus.NewDesc(
			"kuberay_cluster_info",
			"Metadata information about RayCluster custom resources",
			[]string{"name", "namespace", "uid", "owner_kind"},
			nil,
		),
		rayClusterConditionProvisioned: prometheus.NewDesc(
			"kuberay_cluster_condition_provisioned",
			"Indicates whether the RayCluster is provisioned",
			[]string{"name", "namespace", "uid", "condition"},
			nil,
		),
		client: client,
		log:    ctrl.LoggerFrom(ctx),
	}
	return manager
}

// Describe implements prometheus.Collector interface Describe method.
func (r *RayClusterMetricsManager) Describe(ch chan<- *prometheus.Desc) {
	r.rayClusterProvisionedDurationSeconds.Describe(ch)
	ch <- r.rayClusterInfo
	ch <- r.rayClusterConditionProvisioned
}

// Collect implements prometheus.Collector interface Collect method.
func (r *RayClusterMetricsManager) Collect(ch chan<- prometheus.Metric) {
	r.rayClusterProvisionedDurationSeconds.Collect(ch)

	var rayClusterList rayv1.RayClusterList
	err := r.client.List(context.Background(), &rayClusterList)
	if err != nil {
		r.log.Error(err, "Failed to list RayClusters")
		return
	}

	for _, rayCluster := range rayClusterList.Items {
		r.collectRayClusterInfo(&rayCluster, ch)
		r.collectRayClusterConditionProvisioned(&rayCluster, ch)
	}
}

func (r *RayClusterMetricsManager) ObserveRayClusterProvisionedDuration(name, namespace string, uid types.UID, duration float64) {
	r.rayClusterProvisionedDurationSeconds.WithLabelValues(name, namespace, string(uid)).Set(duration)
}

// DeleteRayClusterMetrics removes metrics that belongs to the specified RayCluster.
func (r *RayClusterMetricsManager) DeleteRayClusterMetrics(name, namespace string) {
	numCleanedUpMetrics := r.rayClusterProvisionedDurationSeconds.DeletePartialMatch(prometheus.Labels{"name": name, "namespace": namespace})
	r.log.Info("Cleaned up expired RayCluster metric", "name", name, "namespace", namespace, "numCleanedUpMetrics", numCleanedUpMetrics)
}

func (r *RayClusterMetricsManager) collectRayClusterInfo(cluster *rayv1.RayCluster, ch chan<- prometheus.Metric) {
	ownerKind := "None"
	if v, ok := cluster.Labels[utils.RayOriginatedFromCRDLabelKey]; ok {
		ownerKind = v
	}

	ch <- prometheus.MustNewConstMetric(
		r.rayClusterInfo,
		prometheus.GaugeValue,
		1,
		cluster.Name,
		cluster.Namespace,
		string(cluster.UID),
		ownerKind,
	)
}

func (r *RayClusterMetricsManager) collectRayClusterConditionProvisioned(cluster *rayv1.RayCluster, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		r.rayClusterConditionProvisioned,
		prometheus.GaugeValue,
		1,
		cluster.Name,
		cluster.Namespace,
		string(cluster.UID),
		strconv.FormatBool(meta.IsStatusConditionTrue(cluster.Status.Conditions, string(rayv1.RayClusterProvisioned))),
	)
}
