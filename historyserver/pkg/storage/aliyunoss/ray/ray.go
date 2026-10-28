package ray

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/aliyunoss/rrsa"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const timeout = 2 * time.Minute

type RayLogsHandler struct {
	OssClient           *oss.Client
	OssBucket           string
	LogFiles            chan string
	HttpClient          *http.Client
	SessionDir          string
	OssRootDir          string
	LogDir              string
	RayClusterName      string
	RayClusterNamespace string
	RayNodeName         string
	LogBatching         int
	PushInterval        time.Duration
}

func (r *RayLogsHandler) CreateDirectory(d string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	objectDir := fmt.Sprintf("%s/", path.Clean(d))

	isExist, err := r.OssClient.IsObjectExist(ctx, r.OssBucket, objectDir)
	if err != nil {
		logrus.Errorf("Failed to check if dirObject %s exists: %v", objectDir, err)
		return err
	}
	if !isExist {
		logrus.Infof("Begin to create oss dir %s ...", objectDir)
		_, err = r.OssClient.PutObject(ctx, &oss.PutObjectRequest{
			Bucket: new(r.OssBucket),
			Key:    new(objectDir),
			Body:   bytes.NewReader([]byte("")),
		})
		if err != nil {
			logrus.Errorf("Failed to create directory '%s': %v", objectDir, err)
			return err
		}
		logrus.Infof("Create oss dir %s success", objectDir)
	}
	return nil
}

func (r *RayLogsHandler) Append(file string, reader io.Reader, appendPosition int64) (nextPod int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := r.OssClient.AppendObject(ctx, &oss.AppendObjectRequest{
		Bucket:   new(r.OssBucket),
		Key:      new(file),
		Position: &appendPosition,
		Body:     reader,
	})
	if err != nil {
		return 0, err
	}

	return result.NextPosition, nil
}

func (r *RayLogsHandler) WriteFile(file string, reader io.ReadSeeker) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.OssClient.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: new(r.OssBucket),
		Key:    new(file),
		Body:   reader,
	})
	return err
}

func (r *RayLogsHandler) _listFiles(prefix string, delimiter string, onlyBase bool) []string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	files := []string{}

	p := r.OssClient.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket:    new(r.OssBucket),
		Prefix:    new(prefix + "/"),
		Delimiter: new(delimiter),
		MaxKeys:   100,
	})

	for p.HasNext() {
		page, err := p.NextPage(ctx)
		if err != nil {
			logrus.Errorf("Failed to list objects from %s: %v", prefix+"/", err)
			return []string{}
		}
		logrus.Infof("[ListFiles]Returned objects in %v. length of Contents: %v, length of CommonPrefixes: %v", prefix+"/", len(page.Contents),
			len(page.CommonPrefixes))
		for _, objects := range page.Contents {
			objName := *objects.Key
			if onlyBase {
				objName = path.Base(*objects.Key)
			}
			files = append(files, objName)
		}
		for _, object := range page.CommonPrefixes {
			objName := *object.Prefix
			if onlyBase {
				objName = path.Base(*object.Prefix)
			}
			files = append(files, objName+"/")
		}
	}
	return files
}

func (r *RayLogsHandler) ListFiles(prefix string, dir string) []string {
	fullPrefix := path.Join(r.OssRootDir, prefix, dir)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
	}()
	// Initial continuation token
	clusters := make(utils.ClusterInfoList, 0, 10)
	logrus.Debugf("Prepare to get list clusters info ...")
	nodes := r._listFiles(fullPrefix, "/", true)
	sort.Sort(clusters)
	return nodes
}

func (r *RayLogsHandler) List() (res []utils.ClusterInfo) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic:", r)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	clusters := make(utils.ClusterInfoList, 0, 10)
	logrus.Debugf("Prepare to get list clusters info ...")

	prefix := clustermetadata.Prefix(r.OssRootDir)

	getClusters := func() {
		p := r.OssClient.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
			Bucket:    new(r.OssBucket),
			Prefix:    new(prefix),
			Delimiter: new(""),
			MaxKeys:   100,
		})

		for p.HasNext() {
			page, err := p.NextPage(ctx)
			if err != nil {
				logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
				return
			}
			logrus.Infof("[List]Returned objects in %v. length of Contents: %v, length of CommonPrefixes: %v", prefix, len(page.Contents),
				len(page.CommonPrefixes))
			for _, objects := range page.Contents {
				c, err := clustermetadata.DecodePath(*objects.Key, r.OssRootDir)
				if err != nil {
					logrus.Errorf("Failed to parse meta file path: %s, error: %v", *objects.Key, err)
					continue
				}
				clusters = append(clusters, c)
			}
		}
	}
	getClusters()
	sort.Sort(clusters)
	return clusters
}

func (r *RayLogsHandler) GetContent(prefix string, fileName string) io.Reader {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fullPath := path.Join(r.OssRootDir, prefix, fileName)
	logrus.Infof("Prepare to get object %s info ...", fullPath)
	result, err := r.OssClient.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: new(r.OssBucket),
		Key:    new(fullPath),
	})
	if err != nil {
		logrus.Errorf("Failed to get object %s: %v", fullPath, err)
		dirPath := path.Dir(fullPath)
		allFiles := r._listFiles(dirPath, "", false)

		// Use a fresh context for the retry. The _listFiles above has its own
		// timeout budget and may have consumed most of the parent ctx.
		retryCtx, retryCancel := context.WithTimeout(context.Background(), timeout)
		defer retryCancel()

		found := false
		for _, f := range allFiles {
			if path.Base(f) == path.Base(fullPath) {
				logrus.Infof("Get object %s info success", f)
				result, err = r.OssClient.GetObject(retryCtx, &oss.GetObjectRequest{
					Bucket: new(r.OssBucket),
					Key:    new(f),
				})
				if err != nil {
					logrus.Errorf("Failed to get object %s: %v", f, err)
					return nil
				}
				found = true
				break
			}
		}
		if !found {
			logrus.Errorf("Failed to get object by list all files %s", fullPath)
			return nil
		}
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		logrus.Errorf("Failed to read all data from object %s : %v", fullPath, err)
		return nil
	}
	return bytes.NewReader(data)
}

func NewReader(c *types.RayHistoryServerConfig, jd map[string]any) (storage.StorageReader, error) {
	config := &config{}
	config.completeHSConfig(c, jd)

	return New(config)
}

func NewWriter(c *types.RayCollectorConfig, jd map[string]any) (storage.StorageWriter, error) {
	config := &config{}
	config.complete(c, jd)

	return New(config)
}

func New(c *config) (*RayLogsHandler, error) {
	// Create OSS client using Alibaba Cloud OSS SDK v2.
	// Ref: https://github.com/aliyun/alibabacloud-oss-go-sdk-v2.

	logrus.Infof("Begin to create oss client ...")
	provider, err := rrsa.NewCredentialsProvider()
	if err != nil {
		logrus.Fatalf("Failed to create credentials provider: %v", err)
	}

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(provider).
		WithRegion(c.OSSRegion).
		WithEndpoint(c.OSSEndpoint)
	client := oss.NewClient(cfg)

	logrus.Infof("Begin to use oss bucket %s ...", c.OSSBucket)
	sessionDir := strings.TrimSpace(c.SessionDir)
	sessionDir = filepath.Clean(sessionDir)

	logdir := strings.TrimSpace(path.Join(sessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME))
	logdir = filepath.Clean(logdir)
	logrus.Infof("Clean logdir is %s", logdir)

	return &RayLogsHandler{
		OssClient:           client,
		OssBucket:           c.OSSBucket,
		SessionDir:          sessionDir,
		OssRootDir:          c.RootDir,
		LogDir:              logdir,
		LogFiles:            make(chan string, 100),
		RayClusterName:      c.RayClusterName,
		RayClusterNamespace: c.RayClusterNamespace,
		RayNodeName:         c.RayNodeName,
		HttpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,              // Max idle connections
				MaxIdleConnsPerHost: 20,               // Max idle connections per host
				IdleConnTimeout:     90 * time.Second, // Idle connection timeout
			},
		},
		LogBatching:  c.LogBatching,
		PushInterval: c.PushInterval,
	}, nil
}
