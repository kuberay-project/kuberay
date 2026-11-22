package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	petnames "github.com/dustinkirkland/golang-petname"
	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8sApiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	kuberayHTTP "github.com/ray-project/kuberay/apiserver/pkg/http"
	"github.com/ray-project/kuberay/apiserversdk/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
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
	rayClient              rayv1.RayV1Interface
	k8client               *kubernetes.Clientset
	apiServerBaseURL       string
	rayImage               string
	rayVersion             string
	namespaceName          string
	computeTemplateName    string
	clusterName            string
	configMapName          string
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
		withRayVersion(),
		withBaseURL(),
		withHttpClient(),
		withContext(),
		withK8sClient(),
		withRayClient(),
		withNamespace(),
	)
}

func newEnd2EndTestingContext(t *testing.T, options ...contextOption) (*End2EndTestingContext, error) {
	testingContext := &End2EndTestingContext{
		namespaceName:       fmt.Sprintf("%s-%d", petnames.Generate(2, "-"), time.Now().UnixNano()),
		computeTemplateName: petnames.Name(),
		clusterName:         petnames.Name(),
		configMapName:       petnames.Generate(2, "-"),
		currentName:         petnames.Name(),
	}
	for _, o := range options {
		err := o(t, testingContext)
		if err != nil {
			return nil, err
		}
	}
	return testingContext, nil
}

func withHttpClient() contextOption {
	return func(_ *testing.T, testingContext *End2EndTestingContext) error {
		testingContext.apiServerHttpClient = &http.Client{Timeout: time.Duration(10) * time.Second}

		retryCfg := util.RetryConfig{
			MaxRetry:       util.HTTPClientDefaultMaxRetry,
			BackoffFactor:  util.HTTPClientDefaultBackoffFactor,
			InitBackoff:    util.HTTPClientDefaultInitBackoff,
			MaxBackoff:     util.HTTPClientDefaultMaxBackoff,
			OverallTimeout: util.HTTPClientDefaultOverallTimeout,
		}

		testingContext.kuberayAPIServerClient = kuberayHTTP.NewKuberayAPIServerClient(testingContext.apiServerBaseURL, testingContext.apiServerHttpClient, retryCfg)

		remoteExecClient, err := newRemoteExecuteClient()
		if err != nil {
			return err
		}

		testingContext.kuberayAPIServerClient.SetExecuteHttpRequest(remoteExecClient.executeRequest)

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
	return func(_ *testing.T, testingContext *End2EndTestingContext) error {
		baseURL := os.Getenv("E2E_API_SERVER_URL")
		if strings.TrimSpace(baseURL) == "" {
			baseURL = "http://localhost:8888"
		}
		testingContext.apiServerBaseURL = baseURL
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

func withRayVersion() contextOption {
	return func(_ *testing.T, testingContext *End2EndTestingContext) error {
		rayVersion := os.Getenv("E2E_API_SERVER_RAY_VERSION")
		if strings.TrimSpace(rayVersion) == "" {
			rayVersion = RayVersion
		}
		testingContext.rayVersion = rayVersion
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

func withRayClient() contextOption {
	return func(t *testing.T, tCtx *End2EndTestingContext) error {
		cfg, err := config.GetConfig()
		require.NoError(t, err, "No error expected when getting k8s client configuration")
		tCtx.rayClient, err = rayv1.NewForConfig(cfg)
		require.NoError(t, err, "No error expected when getting a ray")
		return nil
	}
}

func (e2etc *End2EndTestingContext) GetRayServiceByName(serviceName string) (*rayv1api.RayService, error) {
	return e2etc.rayClient.RayServices(e2etc.namespaceName).Get(e2etc.ctx, serviceName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetRayClusterByName(clusterName string) (*rayv1api.RayCluster, error) {
	return e2etc.rayClient.RayClusters(e2etc.namespaceName).Get(e2etc.ctx, clusterName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetBatchV1JobByName(jobName string) (*batchv1.Job, error) {
	return e2etc.k8client.BatchV1().Jobs(e2etc.namespaceName).Get(e2etc.ctx, jobName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetRayClusterName() string {
	return e2etc.clusterName
}

func (e2etc *End2EndTestingContext) GetRayJobByName(rayJobName string) (*rayv1api.RayJob, error) {
	return e2etc.rayClient.RayJobs(e2etc.namespaceName).Get(e2etc.ctx, rayJobName, metav1.GetOptions{})
}

func (e2etc *End2EndTestingContext) GetConfigMapName() string {
	return e2etc.configMapName
}

func (e2etc *End2EndTestingContext) GetNamespaceName() string {
	return e2etc.namespaceName
}

func (e2etc *End2EndTestingContext) GetComputeTemplateName() string {
	return e2etc.computeTemplateName
}

func (e2etc *End2EndTestingContext) GetRayImage() string {
	return e2etc.rayImage
}

func (e2etc *End2EndTestingContext) GetRayVersion() string {
	return e2etc.rayVersion
}

func (e2etc *End2EndTestingContext) GetRayAPIServerClient() *kuberayHTTP.KuberayAPIServerClient {
	return e2etc.kuberayAPIServerClient
}

func (e2etc *End2EndTestingContext) GetNextName() string {
	e2etc.currentName = fmt.Sprintf("%s-%s", petnames.Name(), rand.String(5))
	return e2etc.currentName
}

func (e2etc *End2EndTestingContext) GetCurrentName() string {
	return e2etc.currentName
}

func (e2etc *End2EndTestingContext) CreateComputeTemplate(t *testing.T) {
	computeTemplateRequest := &api.CreateComputeTemplateRequest{
		ComputeTemplate: &api.ComputeTemplate{
			Name:      e2etc.computeTemplateName,
			Namespace: e2etc.namespaceName,
			Cpu:       ComputeTemplateCPUForE2E,
			Memory:    CompTemplateMemGiBForE2E,
		},
		Namespace: e2etc.namespaceName,
	}

	_, _, err := e2etc.kuberayAPIServerClient.CreateComputeTemplate(computeTemplateRequest)
	require.NoErrorf(t, err, "No error expected while creating a compute template (%s, %s)", e2etc.namespaceName, e2etc.computeTemplateName)
}

func (e2etc *End2EndTestingContext) DeleteComputeTemplate(t *testing.T) {
	deleteComputeTemplateRequest := &api.DeleteClusterRequest{
		Name:      e2etc.computeTemplateName,
		Namespace: e2etc.namespaceName,
	}
	_, err := e2etc.kuberayAPIServerClient.DeleteComputeTemplate((*api.DeleteComputeTemplateRequest)(deleteComputeTemplateRequest))
	require.NoErrorf(t, err, "No error expected while deleting a compute template (%s, %s)", e2etc.computeTemplateName, e2etc.namespaceName)
}

func (e2etc *End2EndTestingContext) CreateRayClusterWithConfigMaps(t *testing.T, configMapValues map[string]string, expectedConditions []rayv1api.RayClusterConditionType, name ...string) (*api.Cluster, string) {
	configMapName := e2etc.CreateConfigMap(t, configMapValues, name...)
	t.Cleanup(func() {
		e2etc.DeleteConfigMap(t, configMapName)
	})
	items := make(map[string]string)
	for k := range configMapValues {
		items[k] = k
	}
	clusterName := e2etc.clusterName
	if len(name) > 0 {
		clusterName = name[0]
	}
	actualCluster, _, err := e2etc.kuberayAPIServerClient.CreateCluster(&api.CreateClusterRequest{
		Cluster: &api.Cluster{
			Name:        clusterName,
			Namespace:   e2etc.namespaceName,
			User:        "3cpo",
			Environment: api.Cluster_DEV,
			ClusterSpec: &api.ClusterSpec{
				HeadGroupSpec: &api.HeadGroupSpec{
					ComputeTemplate: e2etc.computeTemplateName,
					Image:           e2etc.rayImage,
					ServiceType:     "NodePort",
					EnableIngress:   false,
					RayStartParams: map[string]string{
						"dashboard-host":      "0.0.0.0",
						"metrics-export-port": "8080",
					},
					Volumes: []*api.Volume{
						{
							MountPath:  "/home/ray/samples",
							VolumeType: api.Volume_CONFIGMAP,
							Name:       "code-sample",
							Source:     configMapName,
							Items:      items,
						},
					},
				},
				WorkerGroupSpec: []*api.WorkerGroupSpec{
					{
						GroupName:       "small-wg",
						ComputeTemplate: e2etc.computeTemplateName,
						Image:           e2etc.rayImage,
						Replicas:        1,
						MinReplicas:     1,
						MaxReplicas:     5,
						RayStartParams: map[string]string{
							"dashboard-host":      "0.0.0.0",
							"metrics-export-port": "8080",
						},
						Volumes: []*api.Volume{
							{
								MountPath:  "/home/ray/samples",
								VolumeType: api.Volume_CONFIGMAP,
								Name:       "code-sample",
								Source:     configMapName,
								Items:      items,
							},
						},
					},
				},
			},
		},
		Namespace: e2etc.namespaceName,
	})
	require.NoErrorf(t, err, "No error expected while creating cluster (%s/%s)", e2etc.namespaceName, clusterName)

	waitForClusterConditions(t, e2etc, actualCluster.Name, expectedConditions)
	return actualCluster, configMapName
}

func (e2etc *End2EndTestingContext) DeleteRayCluster(t *testing.T, clusterName string) {
	_, err := e2etc.kuberayAPIServerClient.DeleteCluster(&api.DeleteClusterRequest{
		Name:      clusterName,
		Namespace: e2etc.namespaceName,
	})
	require.NoErrorf(t, err, "No error expected when deleting ray cluster: '%s', err %v", clusterName, err)

	// wait for the cluster to be deleted for 3 minutes
	// if is not in that state, return an error
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		rayCluster, err := e2etc.GetRayClusterByName(clusterName)
		if err != nil && k8sApiErrors.IsNotFound(err) {
			return true
		}
		t.Logf("Found cluster state of '%s' for ray cluster '%s'", rayCluster.Status.State, clusterName)
		return false
	}, TestTimeoutMedium).Should(gomega.BeTrue())
}

func (e2etc *End2EndTestingContext) DeleteRayService(t *testing.T, serviceName string) {
	_, err := e2etc.kuberayAPIServerClient.DeleteRayService(&api.DeleteRayServiceRequest{
		Name:      serviceName,
		Namespace: e2etc.namespaceName,
	})

	require.NoErrorf(t, err, "No error expected when deleting ray service: '%s', err %v", serviceName, err)

	// wait for the cluster to be deleted for 3 minutes
	// if is not in that state, return an error
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		rayService, err := e2etc.GetRayServiceByName(serviceName)
		if err != nil && k8sApiErrors.IsNotFound(err) {
			return true
		}
		t.Logf("Found service state of '%s' for ray service '%s'", rayService.Status.ServiceStatus, serviceName)
		return false
	}, TestTimeoutMedium).Should(gomega.BeTrue())
}

func (e2etc *End2EndTestingContext) DeleteRayJobByName(t *testing.T, rayJobName string) {
	_, err := e2etc.kuberayAPIServerClient.DeleteRayJob(&api.DeleteRayJobRequest{
		Name:      rayJobName,
		Namespace: e2etc.namespaceName,
	})

	require.NoErrorf(t, err, "No error expected when deleting ray job: '%s', err %v", rayJobName, err)

	// wait for the cluster to be deleted for 3 minutes
	// if is not in that state, return an error
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		rayJob, err := e2etc.GetRayJobByName(rayJobName)
		if err != nil && k8sApiErrors.IsNotFound(err) {
			return true
		}
		t.Logf("Found job state of '%s' for ray job '%s'", rayJob.Status.JobStatus, rayJobName)
		return false
	}, TestTimeoutMedium).Should(gomega.BeTrue())
	require.NoErrorf(t, err, "No error expected when waiting to delete ray job: '%s', err %v", rayJobName, err)
}

func (e2etc *End2EndTestingContext) CreateConfigMap(t *testing.T, values map[string]string, name ...string) string {
	configMapName := e2etc.configMapName
	if len(name) > 0 {
		configMapName = name[0]
	}
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{Kind: "ConfigMap", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: e2etc.namespaceName},
		Immutable:  new(bool),
		Data:       values,
	}
	_, err := e2etc.k8client.CoreV1().ConfigMaps(e2etc.namespaceName).Create(e2etc.ctx, cm, metav1.CreateOptions{})
	require.NoErrorf(t, err, "No error expected when creating config map '%s' in namespace '%s'", configMapName, e2etc.namespaceName)
	return configMapName
}

func (e2etc *End2EndTestingContext) DeleteConfigMap(t *testing.T, configMapName string) {
	err := e2etc.k8client.CoreV1().ConfigMaps(e2etc.namespaceName).Delete(e2etc.ctx, configMapName, metav1.DeleteOptions{})
	if err != nil {
		assert.Truef(t, k8sApiErrors.IsNotFound(err), "Only IsNotFoundException allowed, received %v", err)
	}
}
