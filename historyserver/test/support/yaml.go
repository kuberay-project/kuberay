package support

import (
	"os"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
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

type kubernetesTokenAuthResources struct {
	rayCluster               *rayv1.RayCluster
	serviceAccount           *corev1.ServiceAccount
	authenticatorClusterRole *rbacv1.ClusterRole
	writerClusterRole        *rbacv1.ClusterRole
	clusterRoleBinding       *rbacv1.ClusterRoleBinding
	roleBinding              *rbacv1.RoleBinding
}

func deserializeKubernetesTokenAuthYAML(t Test, filename string) *kubernetesTokenAuthResources {
	t.T().Helper()

	file, err := os.Open(filename)
	require.NoError(t.T(), err, "Failed to open file %s", filename)
	defer file.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	resources := &kubernetesTokenAuthResources{
		rayCluster:               &rayv1.RayCluster{},
		serviceAccount:           &corev1.ServiceAccount{},
		authenticatorClusterRole: &rbacv1.ClusterRole{},
		writerClusterRole:        &rbacv1.ClusterRole{},
		clusterRoleBinding:       &rbacv1.ClusterRoleBinding{},
		roleBinding:              &rbacv1.RoleBinding{},
	}

	err = decoder.Decode(resources.rayCluster)
	require.NoError(t.T(), err, "Failed to decode RayCluster from %s", filename)
	err = decoder.Decode(resources.serviceAccount)
	require.NoError(t.T(), err, "Failed to decode ServiceAccount from %s", filename)
	err = decoder.Decode(resources.authenticatorClusterRole)
	require.NoError(t.T(), err, "Failed to decode authenticator ClusterRole from %s", filename)
	err = decoder.Decode(resources.writerClusterRole)
	require.NoError(t.T(), err, "Failed to decode writer ClusterRole from %s", filename)
	err = decoder.Decode(resources.clusterRoleBinding)
	require.NoError(t.T(), err, "Failed to decode ClusterRoleBinding from %s", filename)
	err = decoder.Decode(resources.roleBinding)
	require.NoError(t.T(), err, "Failed to decode RoleBinding from %s", filename)

	return resources
}
