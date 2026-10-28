package kubectlraydelete

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
)

func TestComplete(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()

	tests := []struct {
		name                 string
		namespace            string
		expectedResources    map[util.ResourceType][]string
		expectedResourceType util.ResourceType
		expectedNamespace    string
		expectedName         string
		args                 []string
		hasErr               bool
	}{
		{
			name:              "valid raycluster without explicit resource and without namespace",
			namespace:         "",
			expectedResources: map[util.ResourceType][]string{util.RayCluster: {"test-raycluster"}},
			expectedNamespace: "default",
			args:              []string{"test-raycluster"},
			hasErr:            false,
		},
		{
			name:              "valid raycluster with explicit resource and with namespace",
			namespace:         "test-namespace",
			expectedResources: map[util.ResourceType][]string{util.RayCluster: {"test-raycluster"}},
			expectedNamespace: "test-namespace",
			args:              []string{"raycluster/test-raycluster"},
			hasErr:            false,
		},
		{
			name:              "valid raycluster without explicit resource and with namespace",
			namespace:         "test-namespace",
			expectedResources: map[util.ResourceType][]string{util.RayCluster: {"test-raycluster"}},
			expectedNamespace: "test-namespace",
			args:              []string{"test-raycluster"},
			hasErr:            false,
		},
		{
			name:                 "valid RayJob with namespace",
			namespace:            "test-namespace",
			expectedResources:    map[util.ResourceType][]string{util.RayJob: {"test-rayjob"}},
			expectedResourceType: util.RayJob,
			expectedNamespace:    "test-namespace",
			expectedName:         "test-rayjob",
			args:                 []string{"rayjob/test-rayjob"},
			hasErr:               false,
		},
		{
			name:              "valid rayservice with namespace",
			namespace:         "test-namespace",
			expectedResources: map[util.ResourceType][]string{util.RayService: {"test-rayservice"}},
			expectedNamespace: "test-namespace",
			args:              []string{"rayservice/test-rayservice"},
			hasErr:            false,
		},
		{
			name:      "invalid service type",
			namespace: "test-namespace",
			args:      []string{"rayserve/test-rayserve"},
			hasErr:    true,
		},
		{
			name:              "valid raycluster with namespace but weird ray type casing",
			namespace:         "test-namespace",
			expectedResources: map[util.ResourceType][]string{util.RayCluster: {"test-raycluster"}},
			expectedNamespace: "test-namespace",
			args:              []string{"rayCluStER/test-raycluster"},
			hasErr:            false,
		},
		{
			name: "valid args with multiple resources",
			args: []string{"raycluster/test", "rayjob/my-job", "rayservice/barista", "other-cluster"},
			expectedResources: map[util.ResourceType][]string{
				util.RayCluster: {"test", "other-cluster"},
				util.RayJob:     {"my-job"},
				util.RayService: {"barista"},
			},
			expectedNamespace: "default",
			hasErr:            false,
		},
		{
			name:   "invalid args, non valid resource type",
			args:   []string{"raycluster/test-raycluster", "test/test"},
			hasErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFlags := genericclioptions.NewConfigFlags(true)
			if tc.namespace != "" {
				configFlags.Namespace = &tc.namespace
			}
			cmdFactory := cmdutil.NewFactory(configFlags)

			fakeDeleteOptions := NewDeleteOptions(cmdFactory, testStreams)
			cmd := &cobra.Command{}
			flags := cmd.Flags()
			configFlags.AddFlags(flags)

			err := fakeDeleteOptions.Complete(cmd, tc.args)

			if tc.hasErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResources, fakeDeleteOptions.resources)
				assert.Equal(t, tc.expectedNamespace, fakeDeleteOptions.namespace)
			}
		})
	}
}
