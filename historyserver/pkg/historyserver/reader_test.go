package historyserver

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetNodeLogFileDefaultLineLimit(t *testing.T) {
	longLine := strings.Repeat("x", 70*1024)
	tests := map[string]struct {
		content string
		lines   int
		want    string
		wantErr bool
	}{
		"line over Scanner default": {
			content: "first line\n" + longLine + "\nlast line\n",
			want:    "first line\n" + longLine + "\nlast line",
		},
		"tail wraps and retains long line": {
			content: "first\nsecond\n" + longLine + "\nlast\n",
			lines:   2,
			want:    longLine + "\nlast",
		},
		"long line outside requested tail": {
			content: longLine + "\nfirst\nlast\n",
			lines:   2,
			want:    "first\nlast",
		},
		"unterminated long line": {
			content: longLine,
			want:    longLine,
		},
		"decimal megabyte fits": {
			content: strings.Repeat("x", 1000000) + "\n",
			want:    strings.Repeat("x", 1000000),
		},
		"multi-megabyte line exceeds default": {
			content: strings.Repeat("x", 3*1024*1024) + "\n",
			wantErr: true,
		},
		"over-limit line outside requested tail": {
			content: strings.Repeat("x", 3*1024*1024) + "\nlast line\n",
			lines:   1,
			wantErr: true,
		},
		"exact MiB content with LF exceeds default": {
			content: strings.Repeat("x", 1024*1024) + "\n",
			wantErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := &ServerHandler{reader: &taskLogStorageReader{content: test.content}}
			got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
				NodeID:   "abcd",
				Filename: "driver.log",
				Lines:    test.lines,
			})
			if test.wantErr {
				require.ErrorIs(t, err, bufio.ErrTooLong)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, string(got))
		})
	}
}

func TestGetNodeLogFileConfiguredLineLimit(t *testing.T) {
	tests := []struct {
		name    string
		limit   int
		size    int
		ending  string
		lines   int
		wantErr bool
	}{
		{name: "increased limit", limit: 4 * 1024 * 1024, size: 3 * 1024 * 1024, ending: "\n"},
		{name: "decreased limit", limit: 64 * 1024, size: 70 * 1024, ending: "\n", wantErr: true},
		{name: "small limit is not bypassed by initial buffer", limit: 32, size: 32, ending: "\n", wantErr: true},
		{name: "small limit boundary", limit: 32, size: 31, ending: "\n"},
		{name: "LF boundary", size: 1024*1024 - 1, ending: "\n"},
		{name: "CRLF boundary", size: 1024*1024 - 2, ending: "\r\n"},
		{name: "CRLF exceeds limit", size: 1024*1024 - 1, ending: "\r\n", wantErr: true},
		{name: "unterminated boundary", size: 1024*1024 - 1},
		{name: "unterminated exceeds limit", size: 1024 * 1024, wantErr: true},
		{name: "all lines bypass limit", limit: 32, size: 70 * 1024, ending: "\n", lines: -1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			line := strings.Repeat("x", test.size)
			content := line + test.ending
			handler := &ServerHandler{
				reader:          &taskLogStorageReader{content: content},
				maxLogLineBytes: test.limit,
			}
			got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
				NodeID:   "abcd",
				Filename: "driver.log",
				Lines:    test.lines,
			})
			if test.wantErr {
				require.ErrorIs(t, err, bufio.ErrTooLong)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if test.lines < 0 {
				require.Equal(t, content, string(got))
			} else {
				require.Equal(t, line, string(got))
			}
		})
	}
}
