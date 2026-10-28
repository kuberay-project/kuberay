// Package s3 is
/*
Copyright 2024 by the kuberay authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const timeout = 2 * time.Minute

type RayLogsHandler struct {
	S3Client            *s3.S3
	LogFiles            chan string
	HttpClient          *http.Client
	S3Bucket            string
	SessionDir          string
	S3RootDir           string
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

	_, err := r.S3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: new(r.S3Bucket),
		Key:    new(objectDir),
	})
	if err != nil {
		// Directory doesn't exist, create it
		logrus.Infof("Begin to create s3 dir %s ...", objectDir)
		_, err = r.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
			Bucket: new(r.S3Bucket),
			Key:    new(objectDir),
			Body:   bytes.NewReader([]byte("")),
		})
		if err != nil {
			logrus.Errorf("Failed to create directory '%s': %v", objectDir, err)
			return err
		}
		logrus.Infof("Create s3 dir %s success", objectDir)
	}
	return nil
}

func (r *RayLogsHandler) WriteFile(file string, reader io.ReadSeeker) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err := r.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: new(r.S3Bucket),
		Key:    new(file),
		Body:   reader,
	})
	return err
}

func (r *RayLogsHandler) _listFiles(prefix string, delimiter string, onlyBase bool) []string {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	files := []string{}

	listInput := &s3.ListObjectsV2Input{
		Bucket:    new(r.S3Bucket),
		Prefix:    new(prefix + "/"),
		MaxKeys:   aws.Int64(100),
		Delimiter: new(delimiter),
	}

	err := r.S3Client.ListObjectsV2PagesWithContext(ctx, listInput,
		func(page *s3.ListObjectsV2Output, _ bool) bool {
			logrus.Infof("[ListFiles]Returned objects in %v. length of page.Contents: %v, length of page.CommonPrefixes: %v",
				prefix+"/", len(page.Contents), len(page.CommonPrefixes))

			for _, object := range page.Contents {
				objName := *object.Key
				if onlyBase {
					objName = path.Base(*object.Key)
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
			return true
		})
	if err != nil {
		logrus.Errorf("Failed to list objects from %s: %v", prefix+"/", err)
		return []string{}
	}

	return files
}

func (r *RayLogsHandler) ListFiles(prefix string, dir string) []string {
	fullPrefix := path.Join(r.S3RootDir, prefix, dir)

	defer func() {
		if rec := recover(); rec != nil {
			fmt.Println("Recovered from panic:", rec)
		}
	}()

	logrus.Debugf("Prepare to get list clusters info ...")
	nodes := r._listFiles(fullPrefix, "/", true)
	// Note: clusters is not defined in this scope, removed sorting
	return nodes
}

func (r *RayLogsHandler) List() (res []utils.ClusterInfo) {
	defer func() {
		if rec := recover(); rec != nil {
			fmt.Println("Recovered from panic:", rec)
		}
	}()

	clusters := make(utils.ClusterInfoList, 0, 10)
	logrus.Debugf("Prepare to get list clusters info ...")

	prefix := clustermetadata.Prefix(r.S3RootDir)

	getClusters := func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		listInput := &s3.ListObjectsV2Input{
			Bucket:    new(r.S3Bucket),
			Prefix:    new(prefix),
			MaxKeys:   aws.Int64(100),
			Delimiter: new(""),
		}

		err := r.S3Client.ListObjectsV2PagesWithContext(ctx, listInput,
			func(page *s3.ListObjectsV2Output, _ bool) bool {
				logrus.Infof("[List]Returned objects in %v. length of page.Contents: %v, length of page.CommonPrefixes: %v",
					prefix, len(page.Contents), len(page.CommonPrefixes))

				for _, object := range page.Contents {
					c, err := clustermetadata.DecodePath(*object.Key, r.S3RootDir)
					if err != nil {
						logrus.Errorf("Failed to parse meta file path: %s, error: %v", *object.Key, err)
						continue
					}
					logrus.Infof("Parsed cluster %s for session %s to list", c.Name, c.SessionName)
					clusters = append(clusters, c)
				}
				return true
			})
		if err != nil {
			logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
			return
		}
	}

	getClusters()
	sort.Sort(clusters)
	return clusters
}

func (r *RayLogsHandler) GetContent(prefix string, fileName string) io.Reader {
	fullPath := path.Join(r.S3RootDir, prefix, fileName)
	logrus.Infof("Prepare to get object %s info ...", fullPath)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result, err := r.S3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: new(r.S3Bucket),
		Key:    new(fullPath),
	})
	if err != nil {
		// Close the first result's Body if it exists to prevent connection leak
		if result != nil && result.Body != nil {
			result.Body.Close()
		}
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
				result, err = r.S3Client.GetObjectWithContext(retryCtx, &s3.GetObjectInput{
					Bucket: new(r.S3Bucket),
					Key:    new(f),
				})
				if err != nil {
					if result != nil && result.Body != nil {
						result.Body.Close()
					}
					logrus.Errorf("Failed to get object %s: %v", f, err)
					return nil
				}
				found = true
				break
			}
		}
		if !found {
			logrus.Errorf("Failed to get object by list all files %s", fileName)
			return nil
		}
	}

	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		logrus.Errorf("Failed to read all data from object %s : %v", fileName, err)
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

// TODO: refactor this
func createBucketIfNotExists(s3Client *s3.S3, bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Check if bucket exists
	_, err := s3Client.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: new(bucketName),
	})
	if err != nil {
		// Check if the error is because bucket doesn't exist
		var aerr awserr.Error
		if errors.As(err, &aerr) {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket, "NotFound", "404":
				// Bucket doesn't exist, create it
				logrus.Infof("Bucket %s does not exist, creating...", bucketName)
				_, createErr := s3Client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
					Bucket: new(bucketName),
				})
				if createErr != nil {
					// Check if bucket already exists (race condition)
					var aerr2 awserr.Error
					if errors.As(createErr, &aerr2) {
						if aerr2.Code() == s3.ErrCodeBucketAlreadyExists ||
							aerr2.Code() == s3.ErrCodeBucketAlreadyOwnedByYou ||
							aerr2.Code() == "BucketAlreadyOwnedByYou" {
							logrus.Infof("Bucket %s already exists", bucketName)
							return nil
						}
					}
					logrus.Errorf("Failed to create bucket %s: %v", bucketName, createErr)
					return fmt.Errorf("failed to create bucket %s: %w", bucketName, createErr)
				}
				logrus.Infof("Successfully created bucket %s", bucketName)
				return nil
			default:
				// For other AWS errors, try to create anyway
				logrus.Warnf("HeadBucket error for %s: %v, attempting to create bucket", bucketName, err)
			}
		}

		// Try to create the bucket anyway (might be a permission issue for HeadBucket)
		logrus.Infof("Attempting to create bucket %s...", bucketName)
		_, createErr := s3Client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
			Bucket: new(bucketName),
		})
		if createErr != nil {
			var aerr awserr.Error
			if errors.As(createErr, &aerr) {
				if aerr.Code() == s3.ErrCodeBucketAlreadyExists ||
					aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou ||
					aerr.Code() == "BucketAlreadyOwnedByYou" {
					logrus.Infof("Bucket %s already exists", bucketName)
					return nil
				}
			}
			logrus.Errorf("Failed to create bucket %s: %v", bucketName, createErr)
			return fmt.Errorf("failed to create bucket %s: %w", bucketName, createErr)
		}
		logrus.Infof("Successfully created bucket %s", bucketName)
		return nil
	}

	logrus.Infof("Bucket %s already exists", bucketName)
	return nil
}

func New(c *config) (*RayLogsHandler, error) {
	logrus.Infof("Begin to create s3 client ...")

	// Only use static credentials when explicitly provided; otherwise let the
	// SDK fall back to the default credential chain (IRSA, instance role, etc.).
	var creds *credentials.Credentials
	if c.AccessKeyID != "" {
		creds = credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, c.SessionToken)
	}

	// Create AWS session
	sess, err := session.NewSession(&aws.Config{
		Credentials:      creds,
		Endpoint:         new(c.S3Endpoint),
		Region:           new(c.S3Region),
		DisableSSL:       c.DisableSSL,
		S3ForcePathStyle: c.S3ForcePathStyle,
	})
	if err != nil {
		logrus.Fatalf("Create aws session error %v", err)
	}

	s3Client := s3.New(sess)

	// Ensure bucket exists, create if not
	logrus.Infof("Checking if bucket %s exists...", c.S3Bucket)
	if err := createBucketIfNotExists(s3Client, c.S3Bucket); err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	sessionDir := strings.TrimSpace(c.SessionDir)
	sessionDir = filepath.Clean(sessionDir)

	logdir := strings.TrimSpace(path.Join(sessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME))
	logdir = filepath.Clean(logdir)
	logrus.Infof("Clean logdir is %s", logdir)

	return &RayLogsHandler{
		S3Client:            s3Client,
		S3Bucket:            c.S3Bucket,
		SessionDir:          sessionDir,
		S3RootDir:           c.RootDir,
		LogDir:              logdir,
		LogFiles:            make(chan string, 100),
		RayClusterName:      c.RayClusterName,
		RayClusterNamespace: c.RayClusterNamespace,
		RayNodeName:         c.RayNodeName,
		HttpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		LogBatching:  c.LogBatching,
		PushInterval: c.PushInterval,
	}, nil
}
