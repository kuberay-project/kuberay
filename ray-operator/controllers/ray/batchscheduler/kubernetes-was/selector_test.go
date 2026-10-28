package kuberneteswas

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
)

type fakeProvider struct {
	gv        schema.GroupVersion
	available error
}

var (
	errNotServed     = errors.New("not served")
	v1alpha2Provider = &fakeProvider{gv: schema.GroupVersion{Group: "scheduling.k8s.io", Version: "v1alpha2"}}
	v1alpha3Provider = &fakeProvider{gv: schema.GroupVersion{Group: "scheduling.k8s.io", Version: "v1alpha3"}}
	v1beta1Provider  = &fakeProvider{gv: schema.GroupVersion{Group: "scheduling.k8s.io", Version: "v1beta1"}}
	v1Provider       = &fakeProvider{gv: schema.GroupVersion{Group: "scheduling.k8s.io", Version: "v1"}}
)

func (f *fakeProvider) GroupVersion() schema.GroupVersion                            { return f.gv }
func (f *fakeProvider) Available(*rest.Config) error                                 { return f.available }
func (f *fakeProvider) AddToScheme(*runtime.Scheme)                                  {}
func (f *fakeProvider) ConfigureReconciler(b *builder.Builder) *builder.Builder      { return b }
func (f *fakeProvider) NewScheduler(client.Client) schedulerinterface.BatchScheduler { return nil }

// withProviders swaps the package registry for the duration of a test.
func withProviders(t *testing.T, providers ...Provider) {
	t.Helper()
	original := registeredProviders
	registeredProviders = providers
	t.Cleanup(func() { registeredProviders = original })
}

func setProviderUnavailable(t *testing.T, provider *fakeProvider) {
	t.Helper()
	original := provider.available
	provider.available = errNotServed
	t.Cleanup(func() { provider.available = original })
}

func TestSelectProviderNoneRegistered(t *testing.T) {
	withProviders(t)

	_, err := selectProvider(&rest.Config{})
	require.Error(t, err)
}

func TestSelectProviderNilConfigReturnsPreferredVersion(t *testing.T) {
	withProviders(t, v1alpha2Provider, v1alpha3Provider)

	got, err := selectProvider(nil)
	require.NoError(t, err)
	require.Same(t, v1alpha3Provider, got)
}

func TestSelectProviderUsesKubeAwareVersionOrder(t *testing.T) {
	withProviders(t, v1alpha2Provider, v1Provider, v1alpha3Provider, v1beta1Provider)

	for _, expected := range []*fakeProvider{v1Provider, v1beta1Provider, v1alpha3Provider, v1alpha2Provider} {
		got, err := selectProvider(&rest.Config{})
		require.NoError(t, err)
		require.Same(t, expected, got)
		setProviderUnavailable(t, expected)
	}
}

func TestSelectProviderPreservesRegistrationOrderForEqualVersions(t *testing.T) {
	second := *v1alpha2Provider
	withProviders(t, v1alpha2Provider, &second)

	got, err := selectProvider(&rest.Config{})
	require.NoError(t, err)
	require.Same(t, v1alpha2Provider, got)
}

func TestSelectProviderAllUnavailable(t *testing.T) {
	withProviders(t, v1alpha2Provider)
	setProviderUnavailable(t, v1alpha2Provider)

	_, err := selectProvider(&rest.Config{})
	require.Error(t, err)
}
