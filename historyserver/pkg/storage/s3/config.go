package s3

import (
	"os"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

const DefaultS3Bucket = "ray-historyserver"

type config struct {
	S3ForcePathStyle *bool
	DisableSSL       *bool
	S3Endpoint       string
	S3Bucket         string
	S3Region         string
	AccessKeyID      string
	SecretAccessKey  string
	SessionToken     string
	types.RayCollectorConfig
}

func getS3BucketWithDefault() string {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		return DefaultS3Bucket
	}
	return bucket
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]any) {
	c.RayCollectorConfig = *rcc
	c.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	c.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	c.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
	c.S3Bucket = getS3BucketWithDefault()
	if len(jd) == 0 {
		c.S3Endpoint = os.Getenv("S3_ENDPOINT")
		c.S3Region = os.Getenv("S3_REGION")
		if os.Getenv("S3FORCE_PATH_STYLE") != "" {
			c.S3ForcePathStyle = new(os.Getenv("S3FORCE_PATH_STYLE") == "true")
		}
		if os.Getenv("S3DISABLE_SSL") != "" {
			c.DisableSSL = new(os.Getenv("S3DISABLE_SSL") == "true")
		}
	} else {
		if bucket, ok := jd["s3Bucket"]; ok {
			c.S3Bucket = bucket.(string)
		}
		if endpoint, ok := jd["s3Endpoint"]; ok {
			c.S3Endpoint = endpoint.(string)
		}
		if region, ok := jd["s3Region"]; ok {
			c.S3Region = region.(string)
		}
		if forcePathStyle, ok := jd["s3ForcePathStyle"]; ok {
			c.S3ForcePathStyle = new(forcePathStyle.(string) == "true")
		}
		if s3disableSSL, ok := jd["s3DisableSSL"]; ok {
			c.DisableSSL = new(s3disableSSL.(string) == "true")
		}
	}
}

func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]any) {
	c.RayCollectorConfig = types.RayCollectorConfig{
		RootDir: rcc.RootDir,
	}
	c.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	c.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	c.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
	c.S3Bucket = getS3BucketWithDefault()
	if len(jd) == 0 {
		c.S3Endpoint = os.Getenv("S3_ENDPOINT")
		c.S3Region = os.Getenv("S3_REGION")
		if os.Getenv("S3FORCE_PATH_STYLE") != "" {
			c.S3ForcePathStyle = new(os.Getenv("S3FORCE_PATH_STYLE") == "true")
		}
		if os.Getenv("S3DISABLE_SSL") != "" {
			c.DisableSSL = new(os.Getenv("S3DISABLE_SSL") == "true")
		}
	} else {
		if bucket, ok := jd["s3Bucket"]; ok {
			c.S3Bucket = bucket.(string)
		}
		if endpoint, ok := jd["s3Endpoint"]; ok {
			c.S3Endpoint = endpoint.(string)
		}
		if region, ok := jd["s3Region"]; ok {
			c.S3Region = region.(string)
		}
		if forcePathStyle, ok := jd["s3ForcePathStyle"]; ok {
			c.S3ForcePathStyle = new(forcePathStyle.(string) == "true")
		}
		if s3disableSSL, ok := jd["s3DisableSSL"]; ok {
			c.DisableSSL = new(s3disableSSL.(string) == "true")
		}
	}
}
