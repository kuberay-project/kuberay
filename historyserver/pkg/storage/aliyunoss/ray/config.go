package ray

import (
	"os"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

type config struct {
	OSSRegion   string
	OSSEndpoint string
	OSSBucket   string
	types.RayCollectorConfig
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]any) {
	c.RayCollectorConfig = *rcc
	if len(jd) == 0 {
		c.OSSRegion = os.Getenv("OSS_REGION")
		c.OSSBucket = os.Getenv("OSS_BUCKET")
		c.OSSEndpoint = os.Getenv("OSS_ENDPOINT")
	} else {
		c.OSSRegion = jd["ossRegion"].(string)
		c.OSSBucket = jd["ossBucket"].(string)
		c.OSSEndpoint = jd["ossEndpoint"].(string)
	}
}

func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]any) {
	c.RayCollectorConfig = types.RayCollectorConfig{
		RootDir: rcc.RootDir,
	}
	if len(jd) == 0 {
		c.OSSRegion = os.Getenv("OSS_REGION")
		c.OSSBucket = os.Getenv("OSS_BUCKET")
		c.OSSEndpoint = os.Getenv("OSS_ENDPOINT")
	} else {
		c.OSSRegion = jd["ossRegion"].(string)
		c.OSSBucket = jd["ossBucket"].(string)
		c.OSSEndpoint = jd["ossEndpoint"].(string)
	}
}
