package create

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/generation"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func createTempKubeConfigFile(t *testing.T, currentNamespace string) (string, error) {
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
			"test-context": {
				Cluster:   "test-cluster",
				AuthInfo:  "my-fake-user",
				Namespace: currentNamespace,
			},
		},
		CurrentContext: "test-context",
		AuthInfos: map[string]*api.AuthInfo{
			"my-fake-user": {
				Token: "", // Empty for testing without authentication
			},
		},
	}

	fakeFile := filepath.Join(tmpDir, ".kubeconfig")

	return fakeFile, clientcmd.WriteToFile(*config, fakeFile)
}

func TestRayCreateClusterComplete(t *testing.T) {
	kubeConfigWithCurrentContext, err := createTempKubeConfigFile(t, "test-namespace")
	require.NoError(t, err)
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	tests := map[string]struct {
		image             string
		namespace         string
		rayVersion        string
		expectedError     string
		expectedImage     string
		expectedNamespace string
		args              []string
	}{
		"should error when there are no args": {
			args:              []string{},
			expectedError:     "See 'cluster -h' for help and examples",
			expectedNamespace: "test-namespace",
		},
		"should error when too many args": {
			args:              []string{"testRayClusterName", "extra-arg"},
			expectedError:     "See 'cluster -h' for help and examples",
			expectedNamespace: "test-namespace",
		},
		"should succeed with default image when no image is specified": {
			args:              []string{"testRayClusterName"},
			rayVersion:        util.RayVersion,
			expectedImage:     defaultImageWithTag,
			expectedNamespace: "test-namespace",
		},
		"should succeed with provided image when provided": {
			args:              []string{"testRayClusterName"},
			image:             "DEADBEEF",
			expectedImage:     "DEADBEEF",
			expectedNamespace: "test-namespace",
		},
		"should set the image to the same version as the ray version when the image is the default and the ray version is not the default": {
			args:              []string{"testRayClusterName"},
			image:             defaultImageWithTag,
			namespace:         "foo",
			rayVersion:        "2.52.0",
			expectedImage:     fmt.Sprintf("%s:2.52.0", defaultImage),
			expectedNamespace: "foo",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			configFlags := &genericclioptions.ConfigFlags{KubeConfig: &kubeConfigWithCurrentContext}
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}

			cmdFactory := cmdutil.NewFactory(configFlags)
			fakeCreateClusterOptions := NewCreateClusterOptions(cmdFactory, testStreams)
			cmd := &cobra.Command{Use: "cluster"}
			configFlags.AddFlags(cmd.Flags())
			fakeCreateClusterOptions.rayVersion = tc.rayVersion

			if tc.image != "" {
				fakeCreateClusterOptions.image = tc.image
			}

			err := fakeCreateClusterOptions.Complete(cmd, tc.args)

			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedImage, fakeCreateClusterOptions.image)
				require.Equal(t, tc.expectedNamespace, fakeCreateClusterOptions.namespace)
			}
		})
	}
}

func TestRayCreateClusterValidate(t *testing.T) {
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := map[string]struct {
		opts               *CreateClusterOptions
		configFileContents string
		expectError        string
	}{
		"should error when a resource quantity is invalid": {
			opts: &CreateClusterOptions{
				cmdFactory: cmdFactory,
				headCPU:    "1",
				headMemory: "softmax",
			},
			expectError: "head-memory is not a valid resource quantity: quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
		},
		"should error when an invalid cluster config file is used": {
			opts: &CreateClusterOptions{
				cmdFactory: cmdFactory,
			},
			configFileContents: `foo: bar`,
			expectError:        "field foo not found",
		},
		"should not error when a valid config file is used": {
			opts: &CreateClusterOptions{
				cmdFactory: cmdFactory,
			},
			configFileContents: `image: foo`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if tc.configFileContents != "" {
				tmpFile, err := os.CreateTemp("", "config.yaml")
				require.NoError(t, err)
				_, err = tmpFile.WriteString(tc.configFileContents)
				require.NoError(t, err)
				tc.opts.configFile = tmpFile.Name()
				defer os.Remove(tc.opts.configFile)
			}

			err := tc.opts.Validate(&cobra.Command{})

			if tc.expectError != "" {
				require.Contains(t, err.Error(), tc.expectError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSwitchesIncompatibleWithConfigFilePresent(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := map[string]struct {
		expectError string
		args        []string
	}{
		"should not error when no incompatible flags are used": {
			args: []string{
				"sample-cluster",
				"--file", "config.yaml",
				"--dry-run",
				"--wait",
				"--timeout", "10s",
			},
		},
		"should error when incompatible flags are used": {
			args: []string{
				"sample-cluster",
				"--ray-version", "2.52.0",
				"--image", "rayproject/ray:2.52.0",
				"--head-cpu", "1",
				"--head-memory", "5Gi",
				"--head-gpu", "1",
				"--head-ephemeral-storage", "10Gi",
				"--head-ray-start-params", "metrics-export-port=8080,num-cpus=2",
				"--worker-replicas", "3",
				"--worker-cpu", "1",
				"--worker-memory", "5Gi",
				"--worker-gpu", "1",
				"--worker-ephemeral-storage", "10Gi",
				"--worker-ray-start-params", "metrics-export-port=8081,num-cpus=2",
				"--labels", "app=ray,env=dev",
				"--annotations", "ttl-hours=24,owner=chthulu",
				"--dry-run",
				"--wait",
				"--timeout", "10s",
			},
			expectError: "the following flags are incompatible with --file: [annotations head-cpu head-ephemeral-storage head-gpu head-memory head-ray-start-params image labels ray-version worker-cpu worker-ephemeral-storage worker-gpu worker-memory worker-ray-start-params worker-replicas]",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := NewCreateClusterCommand(cmdFactory, testStreams)
			cmd.SetArgs(tc.args)
			// Parse the flags before checking for incompatible flags
			require.NoError(t, cmd.Flags().Parse(tc.args), "failed to parse flags")
			err := flagsIncompatibleWithConfigFilePresent(cmd)
			if tc.expectError != "" {
				require.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRayClusterCreateClusterRun(t *testing.T) {
	namespace := "namespace-1"
	clusterName := "cluster-1"
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	options := CreateClusterOptions{
		cmdFactory:   cmdFactory,
		ioStreams:    &testStreams,
		clusterName:  clusterName,
		labels:       map[string]string{"app": "ray", "env": "dev"},
		annotations:  map[string]string{"ttl-hours": "24", "owner": "chthulu"},
		headCPU:      "1",
		headMemory:   "1Gi",
		headGPU:      "0",
		workerCPU:    "1",
		workerMemory: "1Gi",
		workerGPU:    "1",
		workerTPU:    "0",
		autoscaler:   generation.AutoscalerV2,
	}

	t.Run("should error when the Ray cluster already exists", func(t *testing.T) {
		rayClusters := []runtime.Object{
			&rayv1.RayCluster{
				ObjectMeta: v1.ObjectMeta{
					Namespace: namespace,
					Name:      clusterName,
				},
				Spec: rayv1.RayClusterSpec{},
			},
		}

		rayClient := clienttesting.NewRayClientset(rayClusters...)
		k8sClients := client.NewClientForTesting(kubefake.NewClientset(), rayClient)

		err := options.Run(context.Background(), k8sClients)
		require.Error(t, err)
	})
}

// A config file must produce the same RayCluster as the equivalent command-line flags. Anything the
// file leaves out gets the documented default rather than the zero value.
// See https://github.com/ray-project/kuberay/issues/5165.
func TestRayCreateClusterConfigFileMatchesFlags(t *testing.T) {
	kubeConfig, err := createTempKubeConfigFile(t, "")
	require.NoError(t, err)
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(&genericclioptions.ConfigFlags{KubeConfig: &kubeConfig})

	configFile := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("ray-version: 2.56.0\nworker-groups:\n  - cpu: 3\n"), 0o600))

	rayClusterConfig, err := generation.ParseConfigFile(configFile)
	require.NoError(t, err)

	k8sClients := client.NewClientForTesting(kubefake.NewClientset(), clienttesting.NewRayClientset())

	fromFile := NewCreateClusterOptions(cmdFactory, testStreams)
	fromFile.clusterName = "cluster-1"
	fromFile.dryRun = true
	fromFile.rayClusterConfig = rayClusterConfig
	require.NoError(t, fromFile.Run(context.Background(), k8sClients))

	// The same values passed as flags, with every other flag left at its cobra default
	fromFlags := NewCreateClusterOptions(cmdFactory, testStreams)
	fromFlags.clusterName = "cluster-1"
	fromFlags.dryRun = true
	fromFlags.namespace = "default"
	fromFlags.rayVersion = "2.56.0"
	fromFlags.image = "rayproject/ray:2.56.0"
	fromFlags.headCPU = util.DefaultHeadCPU
	fromFlags.headMemory = util.DefaultHeadMemory
	fromFlags.headGPU = util.DefaultHeadGPU
	fromFlags.headEphemeralStorage = util.DefaultHeadEphemeralStorage
	fromFlags.headRayStartParams = make(map[string]string)
	fromFlags.headNodeSelectors = make(map[string]string)
	fromFlags.workerReplicas = util.DefaultWorkerReplicas
	fromFlags.numOfHosts = util.DefaultNumOfHosts
	fromFlags.workerCPU = "3"
	fromFlags.workerMemory = util.DefaultWorkerMemory
	fromFlags.workerGPU = util.DefaultWorkerGPU
	fromFlags.workerTPU = util.DefaultWorkerTPU
	fromFlags.workerEphemeralStorage = util.DefaultWorkerEphemeralStorage
	fromFlags.workerRayStartParams = make(map[string]string)
	fromFlags.workerNodeSelectors = make(map[string]string)
	require.NoError(t, fromFlags.Run(context.Background(), k8sClients))

	require.Equal(t,
		fromFlags.rayClusterConfig.GenerateRayClusterApplyConfig().Spec,
		fromFile.rayClusterConfig.GenerateRayClusterApplyConfig().Spec,
	)
}

func TestRayCreateClusterWarnsOnZeroWorkerReplicas(t *testing.T) {
	kubeConfig, err := createTempKubeConfigFile(t, "")
	require.NoError(t, err)
	cmdFactory := cmdutil.NewFactory(&genericclioptions.ConfigFlags{KubeConfig: &kubeConfig})

	tests := map[string]struct {
		config           string
		expectedWarnings []string
	}{
		"should warn when a worker group is explicitly set to 0 replicas": {
			config:           "worker-groups:\n  - cpu: 3\n    replicas: 0\n",
			expectedWarnings: []string{`Warning: worker group "default-group" has 0 replicas and will start with no worker pods.`},
		},
		"should warn only about the empty group when another group has replicas": {
			config: "worker-groups:\n  - name: cpu-workers\n    replicas: 3\n  - name: gpu-workers\n    replicas: 0\n",
			expectedWarnings: []string{
				`Warning: worker group "gpu-workers" has 0 replicas and will start with no worker pods.`,
			},
		},
		"should not warn when the replicas are defaulted": {
			config: "worker-groups:\n  - cpu: 3\n",
		},
		"should not warn when the autoscaler can scale the group up": {
			config: "autoscaler:\n  version: v2\nworker-groups:\n  - cpu: 3\n    replicas: 0\n",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "cluster.yaml")
			require.NoError(t, os.WriteFile(configFile, []byte(tc.config), 0o600))

			rayClusterConfig, err := generation.ParseConfigFile(configFile)
			require.NoError(t, err)

			testStreams, _, _, errOut := genericclioptions.NewTestIOStreams()
			options := NewCreateClusterOptions(cmdFactory, testStreams)
			options.clusterName = "cluster-1"
			options.dryRun = true
			options.rayClusterConfig = rayClusterConfig

			k8sClients := client.NewClientForTesting(kubefake.NewClientset(), clienttesting.NewRayClientset())

			// 0 replicas is a valid configuration, so the command still succeeds
			require.NoError(t, options.Run(context.Background(), k8sClients))

			if len(tc.expectedWarnings) == 0 {
				require.Empty(t, errOut.String())
				return
			}
			for _, warning := range tc.expectedWarnings {
				require.Contains(t, errOut.String(), warning)
			}
			// The warning is about the group, not the cluster: a group with replicas cannot be
			// warned about, and the cluster as a whole may still get worker pods.
			require.Len(t, strings.Split(strings.TrimSpace(errOut.String()), "\n"), len(tc.expectedWarnings))
		})
	}
}

func TestNewCreateClusterCommand(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "config.yaml")
	file, err := os.Create(filePath)
	require.NoError(t, err)
	defer file.Close()

	tests := map[string]struct {
		expectError string
		args        []string
	}{
		"should succeed when all flags are provided": {
			args: []string{
				"sample-cluster",
				"--ray-version", "2.52.0",
				"--image", "rayproject/ray:2.52.0",
				"--head-cpu", "1",
				"--head-memory", "5Gi",
				"--head-gpu", "1",
				"--head-ephemeral-storage", "10Gi",
				"--head-ray-start-params", "metrics-export-port=8080,num-cpus=2",
				"--head-node-selectors", "app=ray,env=dev",
				"--worker-replicas", "3",
				"--num-of-hosts", "2",
				"--worker-cpu", "1",
				"--worker-memory", "5Gi",
				"--worker-gpu", "1",
				"--worker-ephemeral-storage", "10Gi",
				"--worker-ray-start-params", "metrics-export-port=8081,num-cpus=2",
				"--worker-node-selectors", fmt.Sprintf("app=ray,env=dev,%s=tpu-v5,%s=2x4", util.NodeSelectorGKETPUAccelerator, util.NodeSelectorGKETPUTopology),
				"--labels", "app=ray,env=dev",
				"--annotations", "ttl-hours=24,owner=chthulu",
				"--autoscaler", "v2",
				"--dry-run",
				"--wait",
				"--timeout", "10s",
			},
		},
		"should succeed when --file is provided": {
			args: []string{
				"sample-cluster",
				"--file", filePath,
				"--dry-run",
			},
		},
		"should error when --file is provided with incompatible flags": {
			args: []string{
				"sample-cluster",
				"--file", "config.yaml",
				"--ray-version", "2.52.0",
				"--dry-run",
			},
			expectError: "the following flags are incompatible with --file: [ray-version]",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := NewCreateClusterCommand(cmdutil.NewFactory(genericclioptions.NewConfigFlags(true)), testStreams)
			cmd.Flags().StringP("namespace", "n", "", "")
			cmd.SetArgs(tc.args)

			if tc.expectError != "" {
				require.EqualError(t, cmd.Execute(), tc.expectError)
			} else {
				require.NoError(t, cmd.Execute())
			}
		})
	}
}

func TestResolveNamespace(t *testing.T) {
	kubeConfigWithCurrentContext, err := createTempKubeConfigFile(t, "")
	require.NoError(t, err)
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	tests := map[string]struct {
		cliNamespace      string  // namespace from the CLI flag
		configNamespace   *string // namespace from the config file
		expectedNamespace string  // expected namespace to be used
		expectedError     string
	}{
		"should use 'default' namespace when no namespace is provided": {
			cliNamespace:      "",
			configNamespace:   nil,
			expectedNamespace: "default",
		},
		"should use the config namespace when no CLI namespace is provided": {
			cliNamespace:      "",
			configNamespace:   new("config-namespace"),
			expectedNamespace: "config-namespace",
		},
		"should use the CLI namespace when no config namespace is provided": {
			cliNamespace:      "cli-namespace",
			configNamespace:   nil,
			expectedNamespace: "cli-namespace",
		},
		"should error when the config namespace doesn't match the CLI namespace": {
			cliNamespace:    "cli-namespace",
			configNamespace: new("config-namespace"),
			expectedError:   "the namespace in the config file \"config-namespace\" does not match the namespace \"cli-namespace\". You must pass --namespace=config-namespace to perform this operation",
		},
		"should use the specified namespace when it's provided in both the CLI and the config file and is the same": {
			cliNamespace:      "my-namespace",
			configNamespace:   new("my-namespace"),
			expectedNamespace: "my-namespace",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			configFlags := &genericclioptions.ConfigFlags{KubeConfig: &kubeConfigWithCurrentContext}
			if tc.cliNamespace != "" {
				configFlags.Namespace = &tc.cliNamespace
			}

			cmdFactory := cmdutil.NewFactory(configFlags)
			options := NewCreateClusterOptions(cmdFactory, testStreams)
			options.namespace = tc.cliNamespace
			options.rayClusterConfig = &generation.RayClusterConfig{
				Namespace: tc.configNamespace,
			}

			namespace, err := options.resolveNamespace()

			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedNamespace, namespace)
			}
		})
	}
}

func TestResolveClusterName(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	tests := map[string]struct {
		cliClusterName      string  // cluster name from the CLI flag
		configClusterName   *string // cluster name from the config file
		expectedClusterName string  // expected cluster name to be used
		expectedError       string
	}{
		"should error when no cluster name is provided": {
			cliClusterName:    "",
			configClusterName: nil,
			expectedError:     "the cluster name is required",
		},
		"should use the config cluster name when no CLI cluster name is provided": {
			cliClusterName:      "",
			configClusterName:   new("config-cluster-name"),
			expectedClusterName: "config-cluster-name",
		},
		"should use the CLI cluster name when no config cluster name is provided": {
			cliClusterName:      "cli-cluster-name",
			configClusterName:   nil,
			expectedClusterName: "cli-cluster-name",
		},
		"should error when the config cluster name doesn't match the CLI cluster name": {
			cliClusterName:    "cli-cluster-name",
			configClusterName: new("config-cluster-name"),
			expectedError:     "the cluster name in the config file \"config-cluster-name\" does not match the cluster name \"cli-cluster-name\". You must use the same name to perform this operation",
		},
		"should use the specified cluster name when it's provided in both the CLI and the config file and is the same": {
			cliClusterName:      "my-cluster-name",
			configClusterName:   new("my-cluster-name"),
			expectedClusterName: "my-cluster-name",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			options := NewCreateClusterOptions(cmdFactory, testStreams)
			options.clusterName = tc.cliClusterName
			options.rayClusterConfig = &generation.RayClusterConfig{
				Name: tc.configClusterName,
			}

			name, err := options.resolveClusterName()

			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedClusterName, name)
			}
		})
	}
}
