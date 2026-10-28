package version

import (
	"bytes"
	"context"
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	clientfake "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/fake"
)

func createBuildInfo(revision, time string) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{
			Path:    "github.com/ray-project/kuberay/kubectl-plugin",
			Version: "(devel)",
		},
		Settings: []debug.BuildSetting{
			{
				Key:   "vcs.revision",
				Value: revision,
			},
			{
				Key:   "vcs.time",
				Value: time,
			},
		},
	}
}

// Tests the Run() step of the command and checks the output.
func TestRayVersionRun(t *testing.T) {
	fakeVersionOptions := &VersionOptions{
		cmdFactory: cmdutil.NewFactory(genericclioptions.NewConfigFlags(true)),
	}

	tests := []struct {
		name                           string
		kuberayImageVersion            string
		getKubeRayOperatorVersionError error
		pluginCommit                   string
		pluginBuildTime                string
		expected                       string
		buildInfoErr                   bool
	}{
		{
			name:                           "Test when we can successfully get the KubeRay operator image version",
			kuberayImageVersion:            "v0.0.1",
			getKubeRayOperatorVersionError: nil,
			expected: fmt.Sprintf("kubectl ray plugin version: %s\n"+
				"KubeRay operator version: %s\n", Version, "v0.0.1"),
		},
		{
			name:                           "Test when we cannot get the KubeRay operator image version",
			kuberayImageVersion:            "",
			getKubeRayOperatorVersionError: fmt.Errorf("something went wrong"),
			expected: fmt.Sprintf("kubectl ray plugin version: %s\n"+
				"warning: KubeRay operator installation cannot be found: something went wrong. "+
				"Did you install it with the name \"kuberay-operator\"?\n", Version),
		},
		{
			name:                "Test when build info is included",
			kuberayImageVersion: "v0.0.1",
			pluginCommit:        "abcdefg",
			pluginBuildTime:     "2023-10-27T12:00:00Z",
			buildInfoErr:        false,
			expected: fmt.Sprintf("kubectl ray plugin version: development (%s, built %s)\n"+
				"KubeRay operator version: %s\n", "abcdefg", "2023-10-27T12:00:00Z", "v0.0.1"),
		},
		{
			name:                "Test when build info fails",
			kuberayImageVersion: "v0.0.1",
			buildInfoErr:        true,
			expected: fmt.Sprintf("kubectl ray plugin version: %s\n"+
				"KubeRay operator version: %s\n", Version, "v0.0.1"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testVersion := Version                   // Store the global Version value
			defer func() { Version = testVersion }() // Restore after the test

			client := clientfake.NewFakeClient().WithKubeRayImageVersion(tc.kuberayImageVersion).WithKubeRayOperatorVersionError(tc.getKubeRayOperatorVersionError)

			fakeBuildInfo := func() (*debug.BuildInfo, bool) {
				return createBuildInfo(tc.pluginCommit, tc.pluginBuildTime), !tc.buildInfoErr
			}

			var buf bytes.Buffer
			err := fakeVersionOptions.Run(context.Background(), client, fakeBuildInfo, &buf)
			require.NoError(t, err)

			assert.Equal(t, tc.expected, buf.String())
		})
	}
}
