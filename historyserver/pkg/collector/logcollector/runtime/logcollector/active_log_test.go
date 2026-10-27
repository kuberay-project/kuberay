package logcollector

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"
)

func TestUploadNewBytes(t *testing.T) {
	const (
		name   = "job-driver-x.log"
		object = testLogPrefix + name
	)
	key := activeLogKey{sessionID: testSessionID, nodeID: testNodeID, relPath: name}
	errStorage := errors.New("storage down")

	// write is one change to the log file, followed by one scan. An empty write
	// is a scan with nothing new.
	type write struct {
		// content is appended to the file.
		content string
		// truncate replaces the file with content instead of appending.
		truncate bool
		// writeErr is injected into the storage writer for the scan that follows.
		writeErr error
	}
	tests := []struct {
		name   string
		writes []write
		// want is the map of expected objects in storage after the last write.
		want map[string]string
	}{
		{
			name:   "one chunk per write, a scan with nothing new uploads nothing",
			writes: []write{{content: "line 1\n"}, {content: "line 2\nline 3\n"}, {}},
			want: map[string]string{
				chunkObjectName(object, 0): "line 1\n",
				chunkObjectName(object, 7): "line 2\nline 3\n",
			},
		},
		{
			name:   "empty file uploads one empty chunk and is not re-uploaded",
			writes: []write{{content: ""}, {}},
			want:   map[string]string{chunkObjectName(object, 0): ""},
		},
		{
			name:   "shrunk file restarts from offset zero",
			writes: []write{{content: "a long first generation\n"}, {content: "short\n", truncate: true}},
			want:   map[string]string{chunkObjectName(object, 0): "short\n"},
		},
		{
			name:   "failed write keeps the offset so following contents are appended correctly",
			writes: []write{{content: "first\n", writeErr: errStorage}, {content: "second\n"}},
			want:   map[string]string{chunkObjectName(object, 0): "first\nsecond\n"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writer := NewMockStorageWriter()
			r := &RayLogHandler{Writer: writer}
			logPath := filepath.Join(t.TempDir(), name)

			for i, w := range tc.writes {
				flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
				if w.truncate {
					flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
				}
				// Prepare the test log file
				if err := writeTo(logPath, flag, w.content); err != nil {
					t.Fatalf("write %d: prepare file: %v", i, err)
				}
				writer.setWriteErr(w.writeErr)

				err := r.uploadNewBytes(logPath, object, key)
				if (err != nil) != (w.writeErr != nil) {
					t.Fatalf("write %d: uploadNewBytes() error = %v, want error: %v", i, err, w.writeErr != nil)
				}
			}

			if got := writer.written(); !maps.Equal(got, tc.want) {
				t.Fatalf("objects in storage = %v, want %v", got, tc.want)
			}
		})
	}
}

func writeTo(path string, flag int, content string) error {
	f, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}
