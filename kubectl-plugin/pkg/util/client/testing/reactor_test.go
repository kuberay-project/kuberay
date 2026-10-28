package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayClientFake "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

func TestAddRayClusterListFieldSelectorReactor(t *testing.T) {
	cluster1 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-1",
			Namespace: "default",
		},
	}
	cluster2 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-2",
			Namespace: "default",
		},
	}
	cluster3 := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-3",
			Namespace: "other",
		},
	}

	tests := []struct {
		name          string
		namespace     string
		fieldSelector string
		expectedNames []string
		expectedCount int
	}{
		{
			name:          "no field selector returns all clusters",
			namespace:     "default",
			fieldSelector: "",
			expectedCount: 2,
			expectedNames: []string{"cluster-1", "cluster-2"},
		},
		{
			name:          "field selector filters by name",
			namespace:     "default",
			fieldSelector: "metadata.name=cluster-1",
			expectedCount: 1,
			expectedNames: []string{"cluster-1"},
		},
		{
			name:          "field selector with non-existent name returns empty",
			namespace:     "default",
			fieldSelector: "metadata.name=non-existent",
			expectedCount: 0,
			expectedNames: []string{},
		},
		{
			name:          "namespace filtering returns only clusters in specified namespace",
			namespace:     "other",
			fieldSelector: "",
			expectedCount: 1,
			expectedNames: []string{"cluster-3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rayClient := rayClientFake.NewClientset(cluster1, cluster2, cluster3)
			AddRayClusterListFieldSelectorReactor(rayClient)

			listOpts := metav1.ListOptions{}
			if tc.fieldSelector != "" {
				listOpts.FieldSelector = tc.fieldSelector
			}

			result, err := rayClient.RayV1().RayClusters(tc.namespace).List(context.Background(), listOpts)
			require.NoError(t, err)
			assert.Len(t, result.Items, tc.expectedCount)

			names := make([]string, len(result.Items))
			for i, cluster := range result.Items {
				names[i] = cluster.Name
			}
			assert.ElementsMatch(t, tc.expectedNames, names)
		})
	}
}
