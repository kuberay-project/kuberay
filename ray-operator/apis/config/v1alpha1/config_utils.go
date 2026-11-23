package v1alpha1

import (
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/labels"

	kaischeduler "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kai-scheduler"
	schedulerplugins "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/scheduler-plugins"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/volcano"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/yunikorn"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func ValidateBatchSchedulerConfig(logger logr.Logger, config Configuration) error {
	// The KubernetesWAS feature gate selects the Kubernetes WAS scheduler and is mutually
	// exclusive with --batch-scheduler and --enable-batch-scheduler.
	if features.Enabled(features.KubernetesWAS) {
		if config.EnableBatchScheduler || len(config.BatchScheduler) > 0 {
			return fmt.Errorf("the KubernetesWAS feature gate cannot be combined with --batch-scheduler or --enable-batch-scheduler")
		}
		return nil
	}

	if config.EnableBatchScheduler && len(config.BatchScheduler) > 0 {
		return fmt.Errorf("both feature flags enable-batch-scheduler (deprecated) and batch-scheduler are set. Please use batch-scheduler only")
	}

	if config.EnableBatchScheduler {
		logger.Info("Feature flag enable-batch-scheduler is deprecated and will not be supported soon. " +
			"Use batch-scheduler instead. ")
		return nil
	}

	if len(config.BatchScheduler) > 0 {
		// if a customized scheduler is configured, check it is supported
		if config.BatchScheduler == volcano.GetPluginName() || config.BatchScheduler == yunikorn.GetPluginName() || config.BatchScheduler == schedulerplugins.GetPluginName() || config.BatchScheduler == kaischeduler.GetPluginName() {
			logger.Info("Feature flag batch-scheduler is enabled",
				"scheduler name", config.BatchScheduler)
		} else {
			return fmt.Errorf("scheduler is not supported, name=%s", config.BatchScheduler)
		}
	}

	return nil
}

// ParseWatchNamespaceSelector validates WatchNamespaceSelector and returns the parsed selector.
// It returns a nil selector if WatchNamespaceSelector is empty, which means all namespaces are watched.
func ParseWatchNamespaceSelector(config Configuration) (labels.Selector, error) {
	if config.WatchNamespaceSelector == "" {
		return nil, nil
	}
	if config.WatchNamespace != "" {
		return nil, fmt.Errorf("watchNamespaceSelector and watchNamespace are mutually exclusive, but both are set")
	}
	selector, err := labels.Parse(config.WatchNamespaceSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid watchNamespaceSelector %q: %w", config.WatchNamespaceSelector, err)
	}
	return selector, nil
}
