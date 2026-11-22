package kuberneteswas

import (
	"errors"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/rest"
)

// selectProvider resolves which versioned provider to use.
//
// Registered providers are ranked using Kubernetes API version ordering, then
// the highest-ranked version served by the cluster is selected.
//
// TODO: honor a deploy-time preferred API version.
func selectProvider(config *rest.Config) (Provider, error) {
	if len(registeredProviders) == 0 {
		return nil, fmt.Errorf("no %s providers registered", pluginName)
	}

	providers := slices.Clone(registeredProviders)
	slices.SortStableFunc(providers, func(left, right Provider) int {
		return version.CompareKubeAwareVersionStrings(right.GroupVersion().Version, left.GroupVersion().Version)
	})

	// A nil config (e.g. in unit tests) skips discovery and uses the preferred provider.
	if config == nil {
		return providers[0], nil
	}

	var unavailable []error
	for _, provider := range providers {
		if err := provider.Available(config); err != nil {
			unavailable = append(unavailable, err)
			continue
		}
		return provider, nil
	}
	return nil, fmt.Errorf("no served scheduling.k8s.io API version available for %s: %w", pluginName, errors.Join(unavailable...))
}
