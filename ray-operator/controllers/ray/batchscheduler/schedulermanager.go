package batchscheduler

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	kaischeduler "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kai-scheduler"
	kuberneteswas "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kubernetes-was"
	_ "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kubernetes-was/v1alpha3" // register the v1alpha3 WAS provider
	schedulerplugins "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/scheduler-plugins"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/volcano"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/yunikorn"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

type SchedulerManager struct {
	config     *rest.Config
	factory    schedulerinterface.BatchSchedulerFactory
	scheduler  schedulerinterface.BatchScheduler
	rayConfigs configapi.Configuration
	sync.Mutex
}

// NewSchedulerManager maintains a specific scheduler plugin based on config
func NewSchedulerManager(ctx context.Context, rayConfigs configapi.Configuration, config *rest.Config, cli client.Client) (*SchedulerManager, error) {
	// init the scheduler factory from config
	factory, err := getSchedulerFactory(rayConfigs)
	if err != nil {
		return nil, err
	}

	scheduler, err := factory.New(ctx, config, cli)
	if err != nil {
		return nil, err
	}

	manager := SchedulerManager{
		rayConfigs: rayConfigs,
		config:     config,
		factory:    factory,
		scheduler:  scheduler,
	}

	return &manager, nil
}

func getSchedulerFactory(rayConfigs configapi.Configuration) (schedulerinterface.BatchSchedulerFactory, error) {
	// The KubernetesWAS feature gate selects the Kubernetes WAS scheduler; ValidateBatchSchedulerConfig
	// enforces that it is not combined with --batch-scheduler or --enable-batch-scheduler.
	if features.Enabled(features.KubernetesWAS) {
		return &kuberneteswas.SchedulerFactory{}, nil
	}

	var factory schedulerinterface.BatchSchedulerFactory

	// when a batch scheduler name is provided
	// only support a white list of names, empty value is the default value
	// it throws error if an unknown name is provided
	if len(rayConfigs.BatchScheduler) > 0 {
		switch rayConfigs.BatchScheduler {
		case volcano.GetPluginName():
			factory = &volcano.VolcanoBatchSchedulerFactory{}
		case yunikorn.GetPluginName():
			factory = &yunikorn.YuniKornSchedulerFactory{}
		case kaischeduler.GetPluginName():
			factory = &kaischeduler.KaiSchedulerFactory{}
		case schedulerplugins.GetPluginName():
			factory = &schedulerplugins.KubeSchedulerFactory{}
		default:
			return nil, fmt.Errorf("the scheduler is not supported, name=%s", rayConfigs.BatchScheduler)
		}
	} else {
		// empty is the default value, when not set
		// use DefaultBatchSchedulerFactory, it's a no-opt factory
		factory = &schedulerinterface.DefaultBatchSchedulerFactory{}
	}

	// legacy option, if this is enabled, register volcano
	// this is for backward compatibility
	if rayConfigs.EnableBatchScheduler {
		factory = &volcano.VolcanoBatchSchedulerFactory{}
	}

	return factory, nil
}

func (batch *SchedulerManager) GetScheduler() (schedulerinterface.BatchScheduler, error) {
	return batch.scheduler, nil
}

func (batch *SchedulerManager) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	batch.factory.ConfigureReconciler(b)
	return b
}

func (batch *SchedulerManager) AddToScheme(scheme *runtime.Scheme) {
	batch.factory.AddToScheme(scheme)
}
