package historyserver

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestGetNodeLogFileTail(t *testing.T) {
	longLine := strings.Repeat("x", 70*1024)
	tests := map[string]struct {
		content    string
		lines      int
		want       string
		filterAnsi bool
	}{
		"empty file": {},
		"request is capped at maximum lines": {
			content: strings.Repeat("x\n", MAX_LOG_LIMIT+1),
			lines:   MAX_LOG_LIMIT + 1,
			want:    strings.Repeat("x\n", MAX_LOG_LIMIT-1) + "x",
		},
		"ANSI filtering preserves long line": {
			content:    "first\n\x1b[31m" + longLine + "\x1b[0m\nlast\n",
			lines:      2,
			want:       longLine + "\nlast",
			filterAnsi: true,
		},
		"empty lines count toward tail": {
			content: "first\n\nlast\n\n",
			lines:   3,
			want:    "\nlast\n",
		},
		"mixed line endings and trailing CR": {
			content: "first\r\n\r\nlast\r",
			want:    "first\n\nlast",
		},
		"whitespace is preserved": {
			content: "  first  \n\tlast\t\n",
			want:    "  first  \n\tlast\t",
		},
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
		"multi-megabyte line": {
			content: strings.Repeat("x", 3*1024*1024) + "\n",
			want:    strings.Repeat("x", 3*1024*1024),
		},
		"multi-megabyte line outside requested tail": {
			content: strings.Repeat("x", 3*1024*1024) + "\nlast line\n",
			lines:   1,
			want:    "last line",
		},
		"exact MiB content with LF": {
			content: strings.Repeat("x", 1024*1024) + "\n",
			want:    strings.Repeat("x", 1024*1024),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			handler := &ServerHandler{reader: &taskLogStorageReader{content: test.content}}
			got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
				NodeID:         "abcd",
				Filename:       "driver.log",
				Lines:          test.lines,
				FilterAnsiCode: test.filterAnsi,
			})
			require.NoError(t, err)
			require.Equal(t, test.want, string(got))
		})
	}
}

func TestGetNodeLogFileTailOrdering(t *testing.T) {
	// Vary both line lengths and empty lines across several ring-buffer wraps.
	for lineCount := range 20 {
		lines := make([]string, lineCount)
		for i := range lines {
			lines[i] = strings.Repeat("x", i%5)
		}
		content := strings.Join(lines, "\r\n")
		if lineCount > 0 {
			content += "\r\n"
		}
		handler := &ServerHandler{reader: &taskLogStorageReader{content: content}}
		for limit := 1; limit <= 6; limit++ {
			got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
				NodeID: "abcd", Filename: "driver.log", Lines: limit,
			})
			require.NoError(t, err)
			want := strings.Join(lines[max(0, lineCount-limit):], "\n")
			require.Equalf(t, want, string(got), "lineCount=%d, limit=%d", lineCount, limit)
		}
	}
}

func BenchmarkGetNodeLogFileTail(b *testing.B) {
	content := strings.Repeat(strings.Repeat("x", 128)+"\n", 2000)
	handler := &ServerHandler{reader: &taskLogStorageReader{content: content}}
	options := GetLogFileOptions{NodeID: "abcd", Filename: "driver.log", Lines: 1000}
	b.ReportAllocs()
	b.SetBytes(int64(len(content)))
	b.ResetTimer()
	for b.Loop() {
		result, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", options)
		if err != nil || len(result) != 1000*129-1 {
			b.Fatalf("read failed: %v", err)
		}
	}
}

type logReadErrorStorageReader struct {
	taskLogStorageReader
	source io.Reader
}

func (r *logReadErrorStorageReader) GetContent(_, _ string) io.Reader {
	return r.source
}

func TestGetNodeLogFileReadError(t *testing.T) {
	readErr := errors.New("read failed")
	for name, content := range map[string]string{
		"empty":       "",
		"partial":     "partial",
		"after lines": "first\n" + strings.Repeat("x", 70*1024),
	} {
		t.Run(name, func(t *testing.T) {
			for _, lines := range []int{1, -1} {
				source := io.MultiReader(strings.NewReader(content), iotest.ErrReader(readErr))
				handler := &ServerHandler{reader: &logReadErrorStorageReader{source: source}}
				got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
					NodeID: "abcd", Filename: "driver.log", Lines: lines,
				})
				require.ErrorIs(t, err, readErr)
				require.Nil(t, got)
			}
		})
	}
}

func TestGetNodeLogFileLongLineEndings(t *testing.T) {
	line := strings.Repeat("x", 3*1024*1024)
	for name, ending := range map[string]string{"LF": "\n", "CRLF": "\r\n", "EOF": ""} {
		t.Run(name, func(t *testing.T) {
			content := line + ending
			handler := &ServerHandler{reader: &taskLogStorageReader{content: content}}
			for _, lines := range []int{1, -1} {
				got, err := handler._getNodeLogFile("raycluster_default_session", "cluster-prefix", "session", GetLogFileOptions{
					NodeID: "abcd", Filename: "driver.log", Lines: lines,
				})
				require.NoError(t, err)
				want := line
				if lines < 0 {
					want = content
				}
				require.Equal(t, want, string(got))
			}
		})
	}
}
