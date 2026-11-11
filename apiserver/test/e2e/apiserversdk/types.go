package apiserversdk

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	petnames "github.com/dustinkirkland/golang-petname"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	kuberayHTTP "github.com/ray-project/kuberay/apiserver/pkg/http"
	util "github.com/ray-project/kuberay/apiserversdk/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1client "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
)

// GenericEnd2EndTest struct allows for reuse in setting up and running tests
type GenericEnd2EndTest[I proto.Message] struct {
	Input         I
	ExpectedError error
	Name          string
}

// End2EndTestingContext provides a common set of values and methods that
// can be used in executing the tests
type End2EndTestingContext struct {
	ctx                    context.Context
	apiServerHttpClient    *http.Client
	kuberayAPIServerClient *kuberayHTTP.KuberayAPIServerClient
	rayClient              rayv1client.RayV1Interface
	k8client               *kubernetes.Clientset
	apiServerBaseURL       string
	rayImage               string
	namespaceName          string
	clusterName            string
	currentName            string
}

// contextOption is a functional option that allows for building out an instance
// of *End2EndTestingContext
type contextOption func(t *testing.T, tCtx *End2EndTestingContext) error

// NewEnd2EndTestingContext constructs a *End2EndTestingContext
func NewEnd2EndTestingContext(t *testing.T) (*End2EndTestingContext, error) {
	petnames.NonDeterministicMode()
	// ordering is important as there dependencies between field values
	return newEnd2EndTestingContext(t,
		withRayImage(),
		withBaseURL(),
		withRayHttpClient(),
		withK8sClient(),
		withContext(),
		withNamespace(),
		withAPIServerClient(),
	)
}

func newEnd2EndTestingContext(t *testing.T, options ...contextOption) (*End2EndTestingContext, error) {
	testingContext := &End2EndTestingContext{
		namespaceName: fmt.Sprintf("%s-%d", petnames.Generate(2, "-"), time.Now().UnixNano()),
		clusterName:   petnames.Name(),
	}
	for _, o := range options {
		err := o(t, testingContext)
		if err != nil {
			return nil, err
		}
	}
	return testingContext, nil
}

func withRayHttpClient() contextOption {
	return func(t *testing.T, testingContext *End2EndTestingContext) error {
		kubernetesConfig, err := config.GetConfig()
		require.NoError(t, err)

		rt, err := newProxyRoundTripper(kubernetesConfig)
		require.NoError(t, err)
		httpClient := &http.Client{Transport: rt}

		testingContext.rayClient, err = rayv1client.NewForConfigAndClient(kubernetesConfig, httpClient)
		if err != nil {
			return err
		}
		return nil
	}
}

func withContext() contextOption {
	return func(_ *testing.T, testingContext *End2EndTestingContext) error {
		testingContext.ctx = context.Background()
		return nil
	}
}

func withBaseURL() contextOption {
	return func(t *testing.T, testingContext *End2EndTestingContext) error {
		// Use ProxyRoundTripper with Kubernetes API server URL. The
		// ProxyRoundTripper will route to the kuberay-apiserver service
		kubernetesConfig, err := config.GetConfig()
		require.NoError(t, err)
		testingContext.apiServerBaseURL = kubernetesConfig.Host
		return nil
	}
}

func withRayImage() contextOption {
	return func(_ *testing.T, testingContext *End2EndTestingContext) error {
		rayImage := os.Getenv("E2E_API_SERVER_RAY_IMAGE")
		if strings.TrimSpace(rayImage) == "" {
			rayImage = RayImage + "-py310"
		}
		testingContext.rayImage = rayImage
		return nil
	}
}

func withK8sClient() contextOption {
	return func(t *testing.T, testingContext *End2EndTestingContext) error {
		cfg, err := config.GetConfig()
		require.NoError(t, err, "No error expected when getting k8s client configuration")
		clientSet, err := kubernetes.NewForConfig(cfg)
		require.NoError(t, err, "No error expected when creating k8s client")
		testingContext.k8client = clientSet
		return nil
	}
}

func withNamespace() contextOption {
	return func(t *testing.T, tCtx *End2EndTestingContext) error {
		require.NotNil(t, tCtx.k8client, "A k8s client must be created prior to creating a namespace")
		require.NotNil(t, tCtx.ctx, "A context must exist prior to creating a namespace")
		require.NotEmpty(t, tCtx.namespaceName, "Namespace name must be set prior to creating a namespace")
		nsName := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: tCtx.namespaceName,
			},
		}

		_, err := tCtx.k8client.CoreV1().Namespaces().Create(tCtx.ctx, nsName, metav1.CreateOptions{})
		require.NoErrorf(t, err, "Expected to create a namespace '%s", nsName.ObjectMeta.Name)

		// register an automatic deletion of the namespace at test's end
		t.Cleanup(func() {
			err := tCtx.k8client.CoreV1().Namespaces().Delete(tCtx.ctx, tCtx.namespaceName, metav1.DeleteOptions{})
			require.NoErrorf(t, err, "No error expected when deleting namespace '%s'", tCtx.namespaceName)
		})
		return nil
	}
}

func (e2etc *End2EndTestingContext) GetCtx() context.Context {
	return e2etc.ctx
}

func (e2etc *End2EndTestingContext) GetRayHttpClient() rayv1client.RayV1Interface {
	return e2etc.rayClient
}

func (e2etc *End2EndTestingContext) GetRayClusterByName(clusterName string) (*rayv1.RayCluster, error) {
	return e2etc.rayClient.RayClusters(e2etc.namespaceName).Get(e2etc.ctx, clusterName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetRayClusterName() string {
	return e2etc.clusterName
}

func (e2etc *End2EndTestingContext) GetNamespaceName() string {
	return e2etc.namespaceName
}

func (e2etc *End2EndTestingContext) GetRayImage() string {
	return e2etc.rayImage
}

func withAPIServerClient() contextOption {
	return func(t *testing.T, testingContext *End2EndTestingContext) error {
		kubernetesConfig, err := config.GetConfig()
		require.NoError(t, err)

		rt, err := newProxyRoundTripper(kubernetesConfig)
		require.NoError(t, err)
		httpClient := &http.Client{Transport: rt, Timeout: time.Duration(10) * time.Second}

		testingContext.apiServerHttpClient = httpClient

		retryCfg := util.RetryConfig{
			MaxRetry:       util.HTTPClientDefaultMaxRetry,
			BackoffFactor:  util.HTTPClientDefaultBackoffFactor,
			InitBackoff:    util.HTTPClientDefaultInitBackoff,
			MaxBackoff:     util.HTTPClientDefaultMaxBackoff,
			OverallTimeout: util.HTTPClientDefaultOverallTimeout,
		}

		testingContext.kuberayAPIServerClient = kuberayHTTP.NewKuberayAPIServerClient(testingContext.apiServerBaseURL, testingContext.apiServerHttpClient, retryCfg)

		return nil
	}
}

func (e2etc *End2EndTestingContext) GetRayAPIServerClient() *kuberayHTTP.KuberayAPIServerClient {
	return e2etc.kuberayAPIServerClient
}

func (e2etc *End2EndTestingContext) GetK8sClient() *kubernetes.Clientset {
	return e2etc.k8client
}

func (e2etc *End2EndTestingContext) GetNextName() string {
	e2etc.currentName = fmt.Sprintf("%s-%s", petnames.Name(), rand.String(5))
	return e2etc.currentName
}

func (e2etc *End2EndTestingContext) DeleteComputeTemplate(t *testing.T, computeTemplateName string) {
	deleteComputeTemplateRequest := &api.DeleteClusterRequest{
		Name:      computeTemplateName,
		Namespace: e2etc.namespaceName,
	}
	_, err := e2etc.kuberayAPIServerClient.DeleteComputeTemplate((*api.DeleteComputeTemplateRequest)(deleteComputeTemplateRequest))
	require.NoErrorf(t, err, "No error expected while deleting a compute template (%s, %s)", computeTemplateName, e2etc.namespaceName)
}

func (e2etc *End2EndTestingContext) DeleteRayCluster(t *testing.T, clusterName string) {
	err := e2etc.rayClient.RayClusters(e2etc.namespaceName).Delete(e2etc.ctx, clusterName, metav1.DeleteOptions{})
	require.NoError(t, err, "No error expected when deleting ray cluster")
}

func (e2etc *End2EndTestingContext) DeleteRayJobByName(t *testing.T, jobName string) {
	err := e2etc.rayClient.RayJobs(e2etc.namespaceName).Delete(e2etc.ctx, jobName, metav1.DeleteOptions{})
	require.NoError(t, err, "No error expected when deleting ray job")
}

func (e2etc *End2EndTestingContext) DeleteRayService(t *testing.T, serviceName string) {
	err := e2etc.rayClient.RayServices(e2etc.namespaceName).Delete(e2etc.ctx, serviceName, metav1.DeleteOptions{})
	require.NoError(t, err, "No error expected when deleting ray service")
}

func (e2etc *End2EndTestingContext) GetRayJobByName(jobName string) (*rayv1.RayJob, error) {
	return e2etc.rayClient.RayJobs(e2etc.namespaceName).Get(e2etc.ctx, jobName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetRayServiceByName(serviceName string) (*rayv1.RayService, error) {
	return e2etc.rayClient.RayServices(e2etc.namespaceName).Get(e2etc.ctx, serviceName, metav1.GetOptions{})
}

// SendYAMLRequest sends a YAML request to the apiserver proxy with the specified method, path, and YAML content
func (e2etc *End2EndTestingContext) SendYAMLRequest(method, path, yamlContent string) ([]byte, error) {
	url := e2etc.apiServerBaseURL + path
	req, err := http.NewRequestWithContext(e2etc.ctx, method, url, bytes.NewBufferString(yamlContent))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/yaml")

	bodyBytes, _, err := e2etc.kuberayAPIServerClient.ExecuteHttpRequest(req, url)

	return bodyBytes, err
}
