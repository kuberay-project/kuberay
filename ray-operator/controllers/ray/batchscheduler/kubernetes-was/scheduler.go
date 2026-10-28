// Package kuberneteswas selects a versioned Kubernetes workload-aware scheduling
// (WAS) provider and exposes it as a KubeRay batch scheduler. Each supported
// scheduling.k8s.io API version has its own provider in a versioned subpackage
// (e.g. v1alpha3); this package picks the one served by the cluster.
package kuberneteswas

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
)

const pluginName = "kubernetes-was-v1alpha3"

// GetPluginName returns the batch scheduler plugin name for Kubernetes WAS.
func GetPluginName() string { return pluginName }

// Provider is a versioned implementation of Kubernetes workload-aware scheduling.
// Each provider targets one scheduling.k8s.io API version and is registered via
// RegisterProvider (typically from the versioned subpackage's init function).
type Provider interface {
	// GroupVersion is the scheduling.k8s.io API version this provider targets.
	GroupVersion() schema.GroupVersion

	// Available returns nil when this provider's API version is served by the
	// cluster reachable through config, and an error otherwise.
	Available(config *rest.Config) error

	// AddToScheme registers this provider's API types with the given scheme.
	AddToScheme(scheme *runtime.Scheme)

	// ConfigureReconciler adds watches for this provider's owned resource types.
	ConfigureReconciler(b *builder.Builder) *builder.Builder

	// NewScheduler builds the batch scheduler backed by this provider.
	NewScheduler(cli client.Client) schedulerinterface.BatchScheduler
}

var registeredProviders []Provider

// RegisterProvider adds a versioned WAS provider to the selection registry. It is
// intended to be called only from a provider subpackage's init function; the
// registry is not safe for concurrent mutation.
func RegisterProvider(p Provider) {
	registeredProviders = append(registeredProviders, p)
}

// SchedulerFactory selects a registered Provider based on the served API version
// and builds the batch scheduler for it. New must be called before AddToScheme or
// ConfigureReconciler, which delegate to the provider chosen by New.
type SchedulerFactory struct {
	provider Provider
}

func (f *SchedulerFactory) New(_ context.Context, config *rest.Config, cli client.Client) (schedulerinterface.BatchScheduler, error) {
	provider, err := selectProvider(config)
	if err != nil {
		return nil, err
	}
	f.provider = provider
	return provider.NewScheduler(cli), nil
}

func (f *SchedulerFactory) AddToScheme(scheme *runtime.Scheme) {
	if f.provider != nil {
		f.provider.AddToScheme(scheme)
	}
}

func (f *SchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	if f.provider != nil {
		return f.provider.ConfigureReconciler(b)
	}
	return b
}
