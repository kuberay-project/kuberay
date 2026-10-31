package historyserver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

func TestNewServerHandlerLogLineLimit(t *testing.T) {
	tests := []struct {
		name    string
		limit   int
		want    int
		wantErr bool
	}{
		{name: "unset uses default", want: 1024 * 1024},
		{name: "explicit limit", limit: 4 * 1024 * 1024, want: 4 * 1024 * 1024},
		{name: "small positive limit", limit: 32, want: 32},
		{name: "negative is invalid", limit: -1, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewServerHandler(
				&types.RayHistoryServerConfig{MaxLogLineBytes: test.limit},
				"", nil, &ClientManager{}, nil, false, false, false,
			)
			if test.wantErr {
				require.ErrorContains(t, err, "max log line bytes must be positive")
				require.Nil(t, handler)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, handler.maxLogLineBytes)
		})
	}
}

func TestNewServerHandlerAppliesLogLineLimit(t *testing.T) {
	line := strings.Repeat("x", 3*1024*1024)
	handler, err := NewServerHandler(
		&types.RayHistoryServerConfig{MaxLogLineBytes: 4 * 1024 * 1024},
		"", &taskLogStorageReader{content: line + "\n"}, &ClientManager{}, nil, false, false, false,
	)
	require.NoError(t, err)
	got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
		NodeID: "abcd", Filename: "driver.log", Lines: 1000,
	})
	require.NoError(t, err)
	require.Equal(t, line, string(got))
}
