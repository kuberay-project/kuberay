package managercache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

func TestK8sControllerRuntimeCacheSelectors_StripsNodeManagedFieldsAndAnnotations(t *testing.T) {
	selectors, err := K8sControllerRuntimeCacheSelectors()
	require.NoError(t, err)

	var nodeByObject cache.ByObject
	var found bool
	for obj, byObj := range selectors {
		if _, ok := obj.(*corev1.Node); ok {
			nodeByObject = byObj
			found = true
			break
		}
	}
	require.True(t, found, "expected ByObject entry for *corev1.Node")
	require.NotNil(t, nodeByObject.Transform)

	nodeMeta := &metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-1",
			Labels:      map[string]string{"topology.kubernetes.io/zone": "us-central1-a"},
			Annotations: map[string]string{"csi.volume.kubernetes.io/nodeid": `{"pd.csi.storage.gke.io":"node-1"}`},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate},
			},
		},
	}
	transformed, err := nodeByObject.Transform(nodeMeta)
	require.NoError(t, err)
	transformedMeta, ok := transformed.(*metav1.PartialObjectMetadata)
	require.True(t, ok)
	assert.Nil(t, transformedMeta.ManagedFields)
	assert.Nil(t, transformedMeta.Annotations)
	assert.Equal(t, "us-central1-a", transformedMeta.Labels["topology.kubernetes.io/zone"])
}

func TestEventForwarderCacheByObject(t *testing.T) {
	expectedNamespaces := map[string]cache.Config{
		metav1.NamespaceDefault: {},
		metav1.NamespaceSystem:  {},
	}

	t.Run("empty types allows all types server-side", func(t *testing.T) {
		byObject := EventForwarderCacheByObject(nil)
		require.NotNil(t, byObject.Field)
		assert.True(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Warning"}))
		assert.True(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Normal"}))
		assert.False(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Pod", "type": "Warning"}))
		assert.Equal(t, expectedNamespaces, byObject.Namespaces)
	})

	t.Run("single type adds type to server-side field selector", func(t *testing.T) {
		byObject := EventForwarderCacheByObject([]string{"Warning"})
		require.NotNil(t, byObject.Field)
		assert.True(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Warning"}))
		assert.False(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Normal"}))
		assert.False(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Pod", "type": "Warning"}))
		assert.Equal(t, expectedNamespaces, byObject.Namespaces)
	})

	t.Run("multiple types leaves type filtering to client-side", func(t *testing.T) {
		byObject := EventForwarderCacheByObject([]string{"Warning", "Normal"})
		require.NotNil(t, byObject.Field)
		assert.True(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Warning"}))
		assert.True(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Node", "type": "Normal"}))
		assert.False(t, byObject.Field.Matches(fields.Set{"involvedObject.kind": "Pod", "type": "Warning"}))
		assert.Equal(t, expectedNamespaces, byObject.Namespaces)
	})
}
