package gcs

import (
	"os"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

const (
	GCSBucketEnvVar = "GCS_BUCKET"
)

type config struct {
	Bucket string
	types.RayCollectorConfig
}

func (c *config) completeCollectorConfig(rcc *types.RayCollectorConfig, jd map[string]any) {
	c.RayCollectorConfig = *rcc
	if len(jd) != 0 {
		if b, ok := jd["gcsBucket"]; ok {
			if bucket, ok := b.(string); ok {
				c.Bucket = bucket
			}
		}
	}
	if c.Bucket == "" {
		c.Bucket = os.Getenv(GCSBucketEnvVar)
	}
}

func (c *config) completeHistoryServerConfig(rcc *types.RayHistoryServerConfig, jd map[string]any) {
	c.RayCollectorConfig = types.RayCollectorConfig{
		RootDir: rcc.RootDir,
	}
	if len(jd) != 0 {
		if b, ok := jd["gcsBucket"]; ok {
			if bucket, ok := b.(string); ok {
				c.Bucket = bucket
			}
		}
	}
	if c.Bucket == "" {
		c.Bucket = os.Getenv(GCSBucketEnvVar)
	}
}
