package localtest

import (
	"io"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// MockReader is a mock implementation of the StorageReader interface
type MockReader struct {
	data     map[string]map[string]string
	clusters []utils.ClusterInfo
}

// NewMockReader creates a new mock reader
func NewMockReader() *MockReader {
	clusters := []utils.ClusterInfo{
		{
			Name:            "cluster-1",
			SessionName:     "session_2023-01-01_00-00-00_000000",
			CreateTime:      "2023-01-01T00:00:00Z",
			CreateTimeStamp: 1672531200000,
		},
		{
			Name:            "cluster-2",
			SessionName:     "session_2023-01-02_00-00-00_000000",
			CreateTime:      "2023-01-02T00:00:00Z",
			CreateTimeStamp: 1672617600000,
		},
	}

	data := map[string]map[string]string{
		"cluster-1": {
			"log.txt":       "This is log content for cluster-1\nMultiple lines\nof log content",
			"metadata.json": "{\n  \"name\": \"cluster-1\",\n  \"sessionName\": \"session_2023-01-01_00-00-00_000000\",\n  \"createTime\": \"2023-01-01T00:00:00Z\"\n}",
		},
		"cluster-2": {
			"log.txt":       "This is log content for cluster-2\nMultiple lines\nof log content",
			"metadata.json": "{\n  \"name\": \"cluster-2\",\n  \"sessionName\": \"session_2023-01-02_00-00-00_000000\",\n  \"createTime\": \"2023-01-02T00:00:00Z\"\n}",
		},
	}

	return &MockReader{
		clusters: clusters,
		data:     data,
	}
}

// List returns all available files from backend
func (r *MockReader) List() []utils.ClusterInfo {
	return r.clusters
}

// GetContent returns content for a specific file
func (r *MockReader) GetContent(prefix string, fileName string) io.Reader {
	if clusterData, ok := r.data[prefix]; ok {
		if content, ok := clusterData[fileName]; ok {
			return strings.NewReader(content)
		}
	}
	return strings.NewReader("")
}

func (r *MockReader) ListFiles(prefix string, _ string) []string {
	if clusterData, ok := r.data[prefix]; ok {
		files := make([]string, 0, len(clusterData))
		for fileName := range clusterData {
			files = append(files, fileName)
		}
		return files
	}
	return []string{}
}

// NewReader creates a new StorageReader
func NewReader(_ *types.RayHistoryServerConfig, _ map[string]any) (storage.StorageReader, error) {
	return NewMockReader(), nil
}
