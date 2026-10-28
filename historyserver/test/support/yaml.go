package support

import (
	"os"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	ServiceAccountManifestPath = "../../config/service_account.yaml"
)

func DeserializeRBACFromYAML(t Test, filename string) (*corev1.ServiceAccount, *rbacv1.ClusterRole, *rbacv1.ClusterRoleBinding) {
	t.T().Helper()

	file, err := os.Open(filename)
	require.NoError(t.T(), err, "Failed to open file %s", filename)
	defer file.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)

	ServiceAccount := &corev1.ServiceAccount{}
	err = decoder.Decode(ServiceAccount)
	require.NoError(t.T(), err, "Failed to decode ServiceAccount from %s", filename)

	ClusterRole := &rbacv1.ClusterRole{}
	err = decoder.Decode(ClusterRole)
	require.NoError(t.T(), err, "Failed to decode ClusterRole from %s", filename)

	ClusterRoleBinding := &rbacv1.ClusterRoleBinding{}
	err = decoder.Decode(ClusterRoleBinding)
	require.NoError(t.T(), err, "Failed to decode ClusterRoleBinding from %s", filename)

	return ServiceAccount, ClusterRole, ClusterRoleBinding
}
