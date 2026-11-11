package managercache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

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
