package testing

import (
	"k8s.io/apimachinery/pkg/runtime"

	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

// NewRayClientset creates a fake Ray clientset with FieldSelector support.
// This is a convenience wrapper that creates a clientset and adds the
// FieldSelector reactor, so tests don't need to set it up manually.
func NewRayClientset(objects ...runtime.Object) *rayClientFake.Clientset {
	client := rayClientFake.NewClientset(objects...)
	AddRayClusterListFieldSelectorReactor(client)
	return client
}
