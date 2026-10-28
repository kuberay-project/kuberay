package session

import (
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
)

func TestCompleteFoo(t *testing.T) {
	kubeConfigWithCurrentContext, err := createTempKubeConfigFile(t, "old-context")
	require.NoError(t, err)

	testStreams, _, _, _ := genericiooptions.NewTestIOStreams()

	tests := []struct {
		name                 string
		namespace            string
		context              string
		expectedResourceType util.ResourceType
		expectedNamespace    string
		expectedName         string
		expectedContext      string
		args                 []string
		hasErr               bool
	}{
		{
			name:                 "valid raycluster without namespace",
			namespace:            "",
			args:                 []string{"raycluster/test-raycluster"},
			expectedResourceType: util.RayCluster,
			expectedNamespace:    "default",
			expectedName:         "test-raycluster",
			expectedContext:      "old-context",
			hasErr:               false,
		},
		{
			name:                 "valid RayCluster with namespace",
			namespace:            "test-namespace",
			args:                 []string{"raycluster/test-raycluster"},
			expectedResourceType: util.RayCluster,
			expectedNamespace:    "test-namespace",
			expectedName:         "test-raycluster",
			expectedContext:      "old-context",
			hasErr:               false,
		},
		{
			name:                 "valid RayCluster with context",
			namespace:            "test-namespace",
			context:              "new-context",
			args:                 []string{"raycluster/test-raycluster"},
			expectedResourceType: util.RayCluster,
			expectedNamespace:    "test-namespace",
			expectedName:         "test-raycluster",
			expectedContext:      "new-context",
			hasErr:               false,
		},
		{
			name:                 "valid RayJob without namespace",
			namespace:            "",
			args:                 []string{"rayjob/test-rayjob"},
			expectedResourceType: util.RayJob,
			expectedNamespace:    "default",
			expectedName:         "test-rayjob",
			expectedContext:      "old-context",
			hasErr:               false,
		},
		{
			name:                 "valid RayService without namespace",
			namespace:            "",
			args:                 []string{"rayservice/test-rayservice"},
			expectedResourceType: util.RayService,
			expectedNamespace:    "default",
			expectedName:         "test-rayservice",
			expectedContext:      "old-context",
			hasErr:               false,
		},
		{
			name:                 "no slash default to raycluster",
			namespace:            "",
			args:                 []string{"test-resource"},
			expectedResourceType: util.RayCluster,
			expectedNamespace:    "default",
			expectedName:         "test-resource",
			expectedContext:      "old-context",
			hasErr:               false,
		},
		{
			name:   "invalid args (no resource type)",
			args:   []string{"/test-resource"},
			hasErr: true,
		},
		{
			name:   "invalid args (no resource name)",
			args:   []string{"raycluster/"},
			hasErr: true,
		},
		{
			name:   "invalid args (invalid resource type)",
			args:   []string{"invalid-type/test-resource"},
			hasErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := &genericclioptions.ConfigFlags{KubeConfig: &kubeConfigWithCurrentContext}
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			if tc.context != "" {
				configFlags.Context = &tc.context
			}
			cmdFactory := cmdutil.NewFactory(configFlags)
			fakeSessionOptions := NewSessionOptions(cmdFactory, testStreams)
			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())

			err = fakeSessionOptions.Complete(cmd, tc.args)

			if tc.hasErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedNamespace, fakeSessionOptions.namespace)
				assert.Equal(t, tc.expectedContext, fakeSessionOptions.currentContext)
				assert.Equal(t, tc.expectedResourceType, fakeSessionOptions.ResourceType)
				assert.Equal(t, tc.expectedName, fakeSessionOptions.ResourceName)
			}
		})
	}
}

// createTempKubeConfigFile creates a temporary kubeconfig file with the given current context.
func createTempKubeConfigFile(t *testing.T, currentContext string) (string, error) {
	tmpDir := t.TempDir()

	// Set up fake config for kubeconfig
	config := &api.Config{
		Clusters: map[string]*api.Cluster{
			"test-cluster": {
				Server:                "https://fake-kubernetes-cluster.example.com",
				InsecureSkipTLSVerify: true, // For testing purposes
			},
		},
		Contexts: map[string]*api.Context{
			currentContext: {
				Cluster:  "test-cluster",
				AuthInfo: "my-fake-user",
			},
		},
		CurrentContext: currentContext,
		AuthInfos: map[string]*api.AuthInfo{
			"my-fake-user": {
				Token: "", // Empty for testing without authentication
			},
		},
	}

	fakeFile := filepath.Join(tmpDir, ".kubeconfig")

	return fakeFile, clientcmd.WriteToFile(*config, fakeFile)
}
