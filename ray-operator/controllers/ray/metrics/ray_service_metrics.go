package metrics

import (
	"context"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// RayServiceMetricsManager implements the prometheus.Collector and RayServiceMetricsObserver interface to collect ray service metrics.
type RayServiceMetricsManager struct {
	rayServiceInfo                       *prometheus.Desc
	rayServiceConditionReady             *prometheus.Desc
	rayServiceConditionUpgradeInProgress *prometheus.Desc
	client                               client.Client
	log                                  logr.Logger
}

// NewRayServiceMetricsManager creates a new RayServiceMetricsManager instance.
func NewRayServiceMetricsManager(ctx context.Context, client client.Client) *RayServiceMetricsManager {
	collector := &RayServiceMetricsManager{
		rayServiceInfo: prometheus.NewDesc(
			"kuberay_service_info",
			"Metadata information about RayService custom resources",
			[]string{"name", "namespace", "uid"},
			nil,
		),
		rayServiceConditionReady: prometheus.NewDesc(
			"kuberay_service_condition_ready",
			"Describes whether the RayService is ready. Ready means users can send requests to the underlying cluster and the number of serve endpoints is greater than 0.",
			[]string{"name", "namespace", "uid", "condition"},
			nil,
		),
		rayServiceConditionUpgradeInProgress: prometheus.NewDesc(
			"kuberay_service_condition_upgrade_in_progress",
			"Describes whether the RayService is performing a zero-downtime upgrade.",
			[]string{"name", "namespace", "uid", "condition"},
			nil,
		),
		client: client,
		log:    ctrl.LoggerFrom(ctx),
	}
	return collector
}

// Describe implements prometheus.Collector interface Describe method.
func (c *RayServiceMetricsManager) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.rayServiceInfo
	ch <- c.rayServiceConditionReady
	ch <- c.rayServiceConditionUpgradeInProgress
}

// Collect implements prometheus.Collector interface Collect method.
func (c *RayServiceMetricsManager) Collect(ch chan<- prometheus.Metric) {
	var rayServiceList rayv1.RayServiceList
	if err := c.client.List(context.Background(), &rayServiceList); err != nil {
		c.log.Error(err, "Failed to list RayServices")
		return
	}
	for _, rayService := range rayServiceList.Items {
		c.collectRayServiceInfo(&rayService, ch)
		c.collectRayServiceConditionMetrics(&rayService, ch)
	}
}

func (c *RayServiceMetricsManager) collectRayServiceInfo(service *rayv1.RayService, ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		c.rayServiceInfo,
		prometheus.GaugeValue,
		1,
		service.Name,
		service.Namespace,
		string(service.UID),
	)
}

func (c *RayServiceMetricsManager) collectRayServiceConditionMetrics(service *rayv1.RayService, ch chan<- prometheus.Metric) {
	ready := meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.RayServiceReady))
	ch <- prometheus.MustNewConstMetric(
		c.rayServiceConditionReady,
		prometheus.GaugeValue,
		1,
		service.Name,
		service.Namespace,
		string(service.UID),
		strconv.FormatBool(ready),
	)
	upgradeInProgress := meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.UpgradeInProgress))
	ch <- prometheus.MustNewConstMetric(
		c.rayServiceConditionUpgradeInProgress,
		prometheus.GaugeValue,
		1,
		service.Name,
		service.Namespace,
		string(service.UID),
		strconv.FormatBool(upgradeInProgress),
	)
}
