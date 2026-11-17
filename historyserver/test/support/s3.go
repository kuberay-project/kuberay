package support

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	// RustFS configuration
	RustFSNamespace    = "rustfs-dev"
	RustFSManifestPath = "../../config/rustfs.yaml"
	S3BucketName       = "ray-historyserver"

	// Container in the storage pod (config/rustfs.yaml) that tests exec the AWS CLI in.
	S3ClientContainerName = "aws-cli"
)

// S3TestClient verifies bucket contents by executing AWS CLI commands in the S3 client container.
type S3TestClient struct {
	test Test
}

func NewS3TestClient(test Test) *S3TestClient {
	return &S3TestClient{test: test}
}

// rustfsPod returns the running RustFS pod.
func (c *S3TestClient) rustfsPod() (*corev1.Pod, error) {
	pods, err := c.test.Client().Core().CoreV1().Pods(RustFSNamespace).List(
		c.test.Ctx(), metav1.ListOptions{LabelSelector: "app=rustfs"},
	)
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		// A terminating pod still reports phase Running, but exec into it fails.
		if pods.Items[i].Status.Phase == corev1.PodRunning && pods.Items[i].DeletionTimestamp == nil {
			return &pods.Items[i], nil
		}
	}
	return nil, fmt.Errorf("no running RustFS pod found in namespace %s", RustFSNamespace)
}

// execAWS runs an AWS CLI command in the S3 client container and returns its stdout.
func (c *S3TestClient) execAWS(args ...string) (string, error) {
	pod, err := c.rustfsPod()
	if err != nil {
		return "", err
	}
	cmd := append([]string{"aws"}, args...)

	req := c.test.Client().Core().CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command:   cmd,
			Container: S3ClientContainerName,
			Stdout:    true,
			Stderr:    true,
		}, clientgoscheme.ParameterCodec)

	cfg := c.test.Client().Config()
	executor, err := remotecommand.NewSPDYExecutor(&cfg, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to create executor for %q: %w", strings.Join(cmd, " "), err)
	}

	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(c.test.Ctx(), remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		return "", fmt.Errorf("%q failed: %w (stderr: %s)", strings.Join(cmd, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

// StatObject returns nil if the object exists. An empty key checks the bucket itself.
func (c *S3TestClient) StatObject(bucket, key string) error {
	if key == "" {
		_, err := c.execAWS("s3api", "head-bucket", "--bucket", bucket)
		return err
	}
	_, err := c.execAWS("s3api", "head-object", "--bucket", bucket, "--key", key)
	return err
}

// ReadObject returns the object's content.
func (c *S3TestClient) ReadObject(bucket, key string) ([]byte, error) {
	out, err := c.execAWS("s3", "cp", fmt.Sprintf("s3://%s/%s", bucket, key), "-")
	if err != nil {
		return nil, err
	}
	return []byte(out), nil
}

// ListObjectKeys returns the full keys of all objects under bucket/prefix, recursively.
func (c *S3TestClient) ListObjectKeys(bucket, prefix string) ([]string, error) {
	// Trailing slash lists the prefix's contents rather than every key starting with it.
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	// The AWS CLI follows the ListObjectsV2 pagination and applies --query to all pages.
	out, err := c.execAWS("s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix,
		"--query", "Contents[].Key", "--output", "json")
	if err != nil {
		return nil, err
	}
	var allKeys []string // The output is null when nothing matches.
	if err := json.Unmarshal([]byte(out), &allKeys); err != nil {
		return nil, fmt.Errorf("failed to parse list-objects-v2 output %q: %w", out, err)
	}
	var keys []string
	for _, key := range allKeys {
		// Skip zero-byte directory markers such as "logs/".
		if !strings.HasSuffix(key, "/") {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// DeleteBucket removes the bucket and everything in it. A missing bucket is not an error.
func (c *S3TestClient) DeleteBucket(bucket string) error {
	_, err := c.execAWS("s3", "rb", "s3://"+bucket, "--force")
	if err != nil && strings.Contains(err.Error(), "NoSuchBucket") {
		return nil
	}
	return err
}

// ApplyRustFS deploys RustFS once per test namespace, making sure it's idempotent.
func ApplyRustFS(test Test, g *WithT) {
	KubectlApplyYAML(test, RustFSManifestPath, RustFSNamespace)

	// Wait for RustFS pods ready.
	g.Eventually(func(gg Gomega) {
		pods, err := test.Client().Core().CoreV1().Pods(RustFSNamespace).List(
			test.Ctx(), metav1.ListOptions{
				LabelSelector: "app=rustfs",
			},
		)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(pods.Items).NotTo(BeEmpty())
		gg.Expect(AllPodsRunningAndReady(pods.Items)).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())
}

// EnsureS3Client deploys RustFS and returns a client once the S3 API responds.
func EnsureS3Client(t *testing.T) *S3TestClient {
	test := With(t)
	g := NewWithT(t)
	ApplyRustFS(test, g)

	s3Client := NewS3TestClient(test)
	g.Eventually(func() error {
		_, err := s3Client.execAWS("s3api", "list-buckets") // Dummy operation to ensure accessibility
		return err
	}, TestTimeoutMedium).Should(Succeed(), "RustFS API endpoint should be ready")

	return s3Client
}

// DeleteS3Bucket deletes the S3 bucket and everything in it. Cleanup failures
// are logged rather than failing the test.
func DeleteS3Bucket(test Test, _ *WithT, s3Client *S3TestClient) {
	LogWithTimestamp(test.T(), "Deleting S3 bucket %s", S3BucketName)
	if err := s3Client.DeleteBucket(S3BucketName); err != nil {
		test.T().Logf("Failed to delete bucket %s: %v", S3BucketName, err)
	}
}
