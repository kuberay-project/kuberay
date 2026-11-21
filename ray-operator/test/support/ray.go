package support

import (
	"context"
	"errors"
	"strings"
	"time"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
)

func RayCronJob(t Test, namespace, name string) func() (*rayv1.RayCronJob, error) {
	return func() (*rayv1.RayCronJob, error) {
		return GetRayCronJob(t, namespace, name)
	}
}

func GetRayCronJob(t Test, namespace, name string) (*rayv1.RayCronJob, error) {
	return t.Client().Ray().RayV1().RayCronJobs(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func RayJob(t Test, namespace, name string) func() (*rayv1.RayJob, error) {
	return func() (*rayv1.RayJob, error) {
		return GetRayJob(t, namespace, name)
	}
}

func GetRayJob(t Test, namespace, name string) (*rayv1.RayJob, error) {
	return t.Client().Ray().RayV1().RayJobs(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func RayJobStatus(job *rayv1.RayJob) rayv1.JobStatus {
	return job.Status.JobStatus
}

func RayJobDeploymentStatus(job *rayv1.RayJob) rayv1.JobDeploymentStatus {
	return job.Status.JobDeploymentStatus
}

func RayJobManagedBy(job *rayv1.RayJob) *string {
	return job.Spec.ManagedBy
}

func RayJobReason(job *rayv1.RayJob) rayv1.JobFailedReason {
	return job.Status.Reason
}

func RayJobFailed(job *rayv1.RayJob) int32 {
	if job.Status.Failed == nil {
		return 0
	}
	return *job.Status.Failed
}

func RayJobSucceeded(job *rayv1.RayJob) int32 {
	if job.Status.Succeeded == nil {
		return 0
	}
	return *job.Status.Succeeded
}

func RayJobClusterName(job *rayv1.RayJob) string {
	return job.Status.RayClusterName
}

// ApplyRayJobAndWaitForTerminal applies the given RayJob configuration and waits for the
// resulting RayJob to reach a terminal job status (Succeeded, Failed, or Stopped).
// Returns the RayJob returned by the apply call.
//
// Tests that need a stricter completion state (e.g. Succeeded only) should chain an
// additional assertion on the returned RayJob.
func ApplyRayJobAndWaitForTerminal(t Test, rayJobAC *rayv1ac.RayJobApplyConfiguration) *rayv1.RayJob {
	t.T().Helper()
	g := NewWithT(t.T())

	rayJob, err := t.Client().Ray().RayV1().RayJobs(*rayJobAC.Namespace).Apply(t.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(t.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	LogWithTimestamp(t.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(t, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

	return rayJob
}

func RayCluster(t Test, namespace, name string) func() (*rayv1.RayCluster, error) {
	return func() (*rayv1.RayCluster, error) {
		return GetRayCluster(t, namespace, name)
	}
}

func GetRayCluster(t Test, namespace, name string) (*rayv1.RayCluster, error) {
	return t.Client().Ray().RayV1().RayClusters(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func RayClusterState(cluster *rayv1.RayCluster) rayv1.ClusterState {
	return cluster.Status.State
}

func StatusCondition(condType rayv1.RayClusterConditionType) func(*rayv1.RayCluster) metav1.Condition {
	return func(cluster *rayv1.RayCluster) metav1.Condition {
		if cluster != nil {
			for _, cond := range cluster.Status.Conditions {
				if cond.Type == string(condType) {
					return cond
				}
			}
		}
		return metav1.Condition{}
	}
}

type ConditionMatcher struct {
	expected metav1.Condition
}

func (c *ConditionMatcher) Match(actual any) (success bool, err error) {
	if actual == nil {
		return false, errors.New("<actual> should be a metav1.Condition but it is nil")
	}
	a, ok := actual.(metav1.Condition)
	if !ok {
		return false, errors.New("<actual> should be a metav1.Condition")
	}
	messageMatch := true
	if c.expected.Message != "" {
		messageMatch = strings.Contains(a.Message, c.expected.Message)
	}
	return a.Reason == c.expected.Reason && a.Status == c.expected.Status && messageMatch, nil
}

func (c *ConditionMatcher) FailureMessage(actual any) (message string) {
	a := actual.(metav1.Condition)
	return format.Message(a, "to equal", c.expected)
}

func (c *ConditionMatcher) NegatedFailureMessage(actual any) (message string) {
	a := actual.(metav1.Condition)
	return format.Message(a, "not to equal", c.expected)
}

func MatchCondition(status metav1.ConditionStatus, reason string) types.GomegaMatcher {
	return &ConditionMatcher{expected: metav1.Condition{Status: status, Reason: reason}}
}

func MatchConditionContainsMessage(status metav1.ConditionStatus, reason string, message string) types.GomegaMatcher {
	return &ConditionMatcher{expected: metav1.Condition{Status: status, Reason: reason, Message: message}}
}

func RayClusterDesiredWorkerReplicas(cluster *rayv1.RayCluster) int32 {
	return cluster.Status.DesiredWorkerReplicas
}

func HeadPod(t Test, rayCluster *rayv1.RayCluster) func() (*corev1.Pod, error) {
	return func() (*corev1.Pod, error) {
		return GetHeadPod(t, rayCluster)
	}
}

func GetHeadPod(t Test, rayCluster *rayv1.RayCluster) (*corev1.Pod, error) {
	pods, err := t.Client().Core().CoreV1().Pods(rayCluster.Namespace).List(
		t.Ctx(),
		common.RayClusterHeadPodsAssociationOptions(rayCluster).ToMetaV1ListOptions(),
	)
	if err != nil {
		return nil, err
	}
	if len(pods.Items) != 1 {
		return nil, errors.New("number of head pods is not 1")
	}
	return &pods.Items[0], nil
}

func WorkerPods(t Test, rayCluster *rayv1.RayCluster) func() ([]corev1.Pod, error) {
	return func() ([]corev1.Pod, error) {
		return GetWorkerPods(t, rayCluster)
	}
}

func GetWorkerPods(t Test, rayCluster *rayv1.RayCluster) ([]corev1.Pod, error) {
	pods, err := t.Client().Core().CoreV1().Pods(rayCluster.Namespace).List(
		t.Ctx(),
		common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToMetaV1ListOptions(),
	)
	if pods == nil {
		return nil, err
	}
	return pods.Items, err
}

// RateLimitedReplicas verifies that the initial scale-up batch size does not exceed limit.
// We pass an action closure (such as triggering autoscaling via detached actors) to execute
// concurrently while a background goroutine monitors the RayCluster.
func RateLimitedReplicas(t Test, rayCluster *rayv1.RayCluster, limit int32, action func()) bool {
	// The number of new launches must be less than or equal to the size of the RayCluster.
	// The minimum number of pending launches is 5 regardless of RayCluster size and upscaling_speed.
	initialReplicasChan := make(chan int32, 1)
	ctx, cancel := context.WithCancel(t.Ctx())
	defer cancel()

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cluster, err := GetRayCluster(t, rayCluster.Namespace, rayCluster.Name)
				if err == nil {
					replicas := GetRayClusterWorkerGroupReplicaSum(cluster)
					if replicas > 0 {
						select {
						case initialReplicasChan <- replicas:
						default:
						}
						return
					}
				}
			}
		}
	}()

	if action != nil {
		action()
	}

	select {
	case initialScale := <-initialReplicasChan:
		return initialScale <= limit
	case <-time.After(TestTimeoutMedium):
		return false
	}
}

func GetAllPods(t Test, rayCluster *rayv1.RayCluster) ([]corev1.Pod, error) {
	pods, err := t.Client().Core().CoreV1().Pods(rayCluster.Namespace).List(
		t.Ctx(),
		common.RayClusterAllPodsAssociationOptions(rayCluster).ToMetaV1ListOptions(),
	)
	if pods == nil {
		return nil, err
	}
	return pods.Items, err
}

func GetGroupPods(t Test, rayCluster *rayv1.RayCluster, group string) ([]corev1.Pod, error) {
	t.T().Helper()
	pods, err := t.Client().Core().CoreV1().Pods(rayCluster.Namespace).List(
		t.Ctx(),
		common.RayClusterGroupPodsAssociationOptions(rayCluster, group).ToMetaV1ListOptions(),
	)
	return pods.Items, err
}

func GroupPods(t Test, rayCluster *rayv1.RayCluster, group string) func() ([]corev1.Pod, error) {
	return func() ([]corev1.Pod, error) {
		return GetGroupPods(t, rayCluster, group)
	}
}

func RayClusterManagedBy(rayCluster *rayv1.RayCluster) *string {
	return rayCluster.Spec.ManagedBy
}

func RayServiceManagedBy(rayService *rayv1.RayService) *string {
	return rayService.Spec.ManagedBy
}

func GetRayService(t Test, namespace, name string) (*rayv1.RayService, error) {
	return t.Client().Ray().RayV1().RayServices(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func RayService(t Test, namespace, name string) func() (*rayv1.RayService, error) {
	return func() (*rayv1.RayService, error) {
		return GetRayService(t, namespace, name)
	}
}

func RayServiceStatus(service *rayv1.RayService) rayv1.ServiceStatus {
	return service.Status.ServiceStatus
}

func IsRayServiceReady(service *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.RayServiceReady))
}

func IsRayServiceUpgrading(service *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.UpgradeInProgress))
}

func IsRayServiceRollingBack(service *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.RollbackInProgress))
}

func IsRayServiceSuspending(service *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.RayServiceSuspending))
}

func IsRayServiceSuspended(service *rayv1.RayService) bool {
	return meta.IsStatusConditionTrue(service.Status.Conditions, string(rayv1.RayServiceSuspended))
}

func RayServicesNumEndPoints(service *rayv1.RayService) int32 {
	return service.Status.NumServeEndpoints
}

func GetRayClusterWorkerGroupReplicaSum(cluster *rayv1.RayCluster) int32 {
	var replicas int32
	for _, workerGroup := range cluster.Spec.WorkerGroupSpecs {
		replicas += *workerGroup.Replicas
	}
	return replicas
}

func GetHTTPRoute(t Test, namespace, name string) (*gwv1.HTTPRoute, error) {
	return t.Client().Gateway().GatewayV1().HTTPRoutes(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func HTTPRoute(t Test, namespace, name string) func() (*gwv1.HTTPRoute, error) {
	return func() (*gwv1.HTTPRoute, error) {
		return GetHTTPRoute(t, namespace, name)
	}
}

func GetGateway(t Test, namespace, name string) (*gwv1.Gateway, error) {
	return t.Client().Gateway().GatewayV1().Gateways(namespace).Get(t.Ctx(), name, metav1.GetOptions{})
}

func Gateway(t Test, namespace, name string) func() (*gwv1.Gateway, error) {
	return func() (*gwv1.Gateway, error) {
		return GetGateway(t, namespace, name)
	}
}

// VerifyContainerAuthTokenEnvVars verifies that the specified container has the correct auth token environment variables.
// This is a common helper function used across all auth-related E2E tests.
func VerifyContainerAuthTokenEnvVars(t Test, rayCluster *rayv1.RayCluster, container *corev1.Container) {
	t.T().Helper()
	g := NewWithT(t.T())

	// Verify RAY_AUTH_MODE environment variable
	var rayAuthModeEnvVar *corev1.EnvVar
	for _, envVar := range container.Env {
		if envVar.Name == utils.RAY_AUTH_MODE_ENV_VAR {
			rayAuthModeEnvVar = &envVar
			break
		}
	}
	g.Expect(rayAuthModeEnvVar).NotTo(BeNil(),
		"RAY_AUTH_MODE environment variable should be set in container %s", container.Name)
	g.Expect(rayAuthModeEnvVar.Value).To(Equal(string(rayv1.AuthModeToken)),
		"RAY_AUTH_MODE should be %s in container %s", rayv1.AuthModeToken, container.Name)

	// Verify RAY_AUTH_TOKEN environment variable
	var rayAuthTokenEnvVar *corev1.EnvVar
	for _, envVar := range container.Env {
		if envVar.Name == utils.RAY_AUTH_TOKEN_ENV_VAR {
			rayAuthTokenEnvVar = &envVar
			break
		}
	}
	g.Expect(rayAuthTokenEnvVar).NotTo(BeNil(),
		"RAY_AUTH_TOKEN environment variable should be set for AuthModeToken in container %s", container.Name)
	g.Expect(rayAuthTokenEnvVar.ValueFrom).NotTo(BeNil(),
		"RAY_AUTH_TOKEN should be populated from a secret in container %s", container.Name)
	g.Expect(rayAuthTokenEnvVar.ValueFrom.SecretKeyRef).NotTo(BeNil(),
		"RAY_AUTH_TOKEN should be populated from a secret key ref in container %s", container.Name)
	g.Expect(rayAuthTokenEnvVar.ValueFrom.SecretKeyRef.Name).To(ContainSubstring(rayCluster.Name),
		"Secret name should contain RayCluster name in container %s", container.Name)
	g.Expect(rayAuthTokenEnvVar.ValueFrom.SecretKeyRef.Key).To(Equal(utils.RAY_AUTH_TOKEN_SECRET_KEY),
		"Secret key should be %s in container %s", utils.RAY_AUTH_TOKEN_SECRET_KEY, container.Name)
}
