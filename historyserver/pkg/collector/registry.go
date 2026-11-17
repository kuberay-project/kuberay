package collector

import (
	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/aliyunoss/ray"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/azureblob"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/gcs"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/localtest"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/s3"
)

type WriterRegistry map[string]func(globalData *types.RayCollectorConfig, data map[string]any) (storage.StorageWriter, error)

func GetWriterRegistry() WriterRegistry {
	return writerRegistry
}

var writerRegistry = WriterRegistry{
	"aliyunoss": ray.NewWriter,
	"azureblob": azureblob.NewWriter,
	"s3":        s3.NewWriter,
	"gcs":       gcs.NewWriter,
}

type ReaderRegistry map[string]func(globalData *types.RayHistoryServerConfig, data map[string]any) (storage.StorageReader, error)

func GetReaderRegistry() ReaderRegistry {
	return readerRegistry
}

var readerRegistry = ReaderRegistry{
	"aliyunoss": ray.NewReader,
	"azureblob": azureblob.NewReader,
	"localtest": localtest.NewReader,
	"s3":        s3.NewReader,
	"gcs":       gcs.NewReader,
}
