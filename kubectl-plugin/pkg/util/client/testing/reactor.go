package testing

import (
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	kubetesting "k8s.io/client-go/testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

// AddRayClusterFieldSelectorReactor adds a reactor to the fake Ray client that
// simulates server-side FieldSelector filtering for RayCluster List operations.
// This allows tests to verify FieldSelector behavior without manual name checks.
func AddRayClusterListFieldSelectorReactor(client *rayClientFake.Clientset) {
	client.PrependReactor("list", "rayclusters", func(action kubetesting.Action) (bool, runtime.Object, error) {
		listAction, ok := action.(kubetesting.ListAction)
		if !ok {
			return false, nil, nil
		}

		fieldSelector := listAction.GetListRestrictions().Fields
		if fieldSelector == nil || fieldSelector.Empty() {
			return false, nil, nil
		}

		// Get all clusters first using the tracker
		namespace := listAction.GetNamespace()
		allClusters, err := client.Tracker().List(
			rayv1.SchemeGroupVersion.WithResource("rayclusters"),
			rayv1.SchemeGroupVersion.WithKind("RayCluster"),
			namespace,
		)
		if err != nil {
			return true, nil, err
		}

		clusterList, ok := allClusters.(*rayv1.RayClusterList)
		if !ok {
			return false, nil, nil
		}

		// Filter based on field selector
		filtered := &rayv1.RayClusterList{}
		for _, cluster := range clusterList.Items {
			if fieldSelector.Matches(clusterFieldSet(&cluster)) {
				filtered.Items = append(filtered.Items, cluster)
			}
		}

		return true, filtered, nil
	})
}

// clusterFieldSet returns a fields.Set for a RayCluster that can be matched against a FieldSelector.
func clusterFieldSet(cluster *rayv1.RayCluster) fields.Set {
	return fields.Set{
		"metadata.name":      cluster.Name,
		"metadata.namespace": cluster.Namespace,
	}
}
