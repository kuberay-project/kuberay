package support

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	HistoryServerManifestPath = "../../config/historyserver.yaml"
	HistoryServerPort         = 30080

	// EnableLiveClustersArg turns on access to live RayClusters
	EnableLiveClustersArg = "--enable-live-clusters=true"

	// Session name constants
	LiveSessionName = "live"

	RayGrafanaIframeHost               = "http://127.0.0.1:3000"
	HistoryServerGrafanaHealthResponse = `{
  "result": true,
  "msg": "Grafana running",
  "data": {
    "grafanaHost": "%s",
    "grafanaOrgId": "1",
    "sessionName": "%s",
    "dashboardUids": {
      "default": "rayDefaultDashboard",
      "serve": "rayServeDashboard",
      "serveDeployment": "rayServeDeploymentDashboard",
      "serveLlm": "rayServeLlmDashboard",
      "serveLlmSglang": "rayServeLlmSglangDashboard",
      "data": "rayDataDashboard",
      "dataLlm": "rayDataLlmDashboard",
      "train": "rayTrainDashboard"
    },
    "dashboardDatasource": "Prometheus",
    "grafanaClusterFilter": null
  }
}`
)

// HistoryServerEndpoints defines endpoints that should be proxied to Ray Dashboard
// Ref: https://github.com/ray-project/kuberay/blob/8fc4e2a0e644db392534927b7c03d15e3ab7bdbc/historyserver/pkg/historyserver/router.go#L66-L128
//
// This generic smoke-test table only includes endpoints that need no dynamic IDs or
// workload-specific fixtures. Parameterized endpoints (nodes, jobs, logs, and actors)
// and endpoints with dedicated coverage (cluster status, jobs, Serve, Ray Data, and
// placement groups) are intentionally excluded.
var HistoryServerEndpoints = []string{
	"/nodes?view=summary",
	"/api/v0/tasks",
	"/api/v0/tasks/summarize",
	"/logical/actors",
	"/events",
}

// HistoryServerEndpointPrometheusHealth and HistoryServerEndpointGrafanaHealth are standalone constants
// because it requires some additional dependencies.
const (
	HistoryServerEndpointPrometheusHealth = "/api/prometheus_health"
	HistoryServerEndpointGrafanaHealth    = "/api/grafana_health"
)

// ApplyHistoryServer deploys the HistoryServer and RBAC resources.
// If manifestPath is empty, the default HistoryServerManifestPath is used.
func ApplyHistoryServer(test Test, g *WithT, namespace *corev1.Namespace, manifestPath string, extraArgs ...string) {
	if manifestPath == "" {
		manifestPath = HistoryServerManifestPath
	}

	sa, clusterRole, clusterRoleBinding := DeserializeRBACFromYAML(test, ServiceAccountManifestPath)
	sa.Namespace = namespace.Name
	clusterRoleBinding.Name = fmt.Sprintf("historyserver-%s", namespace.Name)
	clusterRoleBinding.Subjects[0].Namespace = namespace.Name

	// Create RBAC resources
	_, err := test.Client().Core().CoreV1().ServiceAccounts(namespace.Name).Create(test.Ctx(), sa, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	// ClusterRole is shared across tests, ignore if already exists.
	_, err = test.Client().Core().RbacV1().ClusterRoles().Create(test.Ctx(), clusterRole, metav1.CreateOptions{})
	if err != nil && !k8serrors.IsAlreadyExists(err) {
		g.Expect(err).NotTo(HaveOccurred())
	}

	_, err = test.Client().Core().RbacV1().ClusterRoleBindings().Create(test.Ctx(), clusterRoleBinding, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	// ClusterRoleBinding is cluster-scoped and won't be deleted when the namespace is cleaned up.
	// Register cleanup to prevent accumulation across test runs.
	test.T().Cleanup(func() {
		_ = test.Client().Core().RbacV1().ClusterRoleBindings().Delete(
			context.Background(), clusterRoleBinding.Name, metav1.DeleteOptions{})
	})

	KubectlApplyYAML(test, manifestPath, namespace.Name)

	appendHistoryServerArgs(test, g, namespace, extraArgs)

	LogWithTimestamp(test.T(), "Waiting for HistoryServer to be ready")
	g.Eventually(func(gg Gomega) {
		pods, err := test.Client().Core().CoreV1().Pods(namespace.Name).List(
			test.Ctx(), metav1.ListOptions{
				LabelSelector: "app=historyserver",
			},
		)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(pods.Items).NotTo(BeEmpty())
		// Also require the args, otherwise the pod from before appendHistoryServerArgs can report
		// ready and let the test run against a History Server without the requested flags.
		for _, pod := range pods.Items {
			gg.Expect(podHasArgs(pod, extraArgs)).To(BeTrue(),
				"pod %s does not carry the requested args %v", pod.Name, extraArgs)
		}
		gg.Expect(AllPodsRunningAndReady(pods.Items)).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())
	LogWithTimestamp(test.T(), "HistoryServer is ready")
}

// appendHistoryServerArgs appends args to the History Server.
func appendHistoryServerArgs(test Test, g *WithT, namespace *corev1.Namespace, args []string) {
	if len(args) == 0 {
		return
	}

	ops := make([]map[string]any, 0, len(args))
	for _, arg := range args {
		ops = append(ops, map[string]any{
			"op":    "add",
			"path":  "/spec/template/spec/containers/0/args/-",
			"value": arg,
		})
	}
	patch, err := json.Marshal(ops)
	g.Expect(err).NotTo(HaveOccurred())

	_, err = test.Client().Core().AppsV1().Deployments(namespace.Name).Patch(
		test.Ctx(), "historyserver-demo", types.JSONPatchType, patch, metav1.PatchOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Appended args %v to the HistoryServer Deployment", args)
}

// podHasArgs reports whether every arg appears on the pod's first container.
func podHasArgs(pod corev1.Pod, args []string) bool {
	if len(pod.Spec.Containers) == 0 {
		return false
	}
	present := make(map[string]bool, len(pod.Spec.Containers[0].Args))
	for _, arg := range pod.Spec.Containers[0].Args {
		present[arg] = true
	}
	for _, arg := range args {
		if !present[arg] {
			return false
		}
	}
	return true
}

// GetHistoryServerURL waits for the history server to be ready and returns its
// base URL, which routes through the API server's service proxy. Callers must
// request it with an API-server-authenticated client.
func GetHistoryServerURL(test Test, g *WithT, namespace *corev1.Namespace) string {
	cfg := test.Client().Config()
	historyServerURL := fmt.Sprintf("%s/api/v1/namespaces/%s/services/historyserver:%d/proxy",
		cfg.Host, namespace.Name, HistoryServerPort)

	client := CreateHTTPClientWithCookieJar(test, g)
	g.Eventually(func() error {
		resp, err := client.Get(historyServerURL + "/readz")
		if err != nil {
			return err
		}
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("health check failed with status: %d", resp.StatusCode)
		}
		return nil
	}, TestTimeoutMedium).Should(Succeed(), "HistoryServer should be ready")
	LogWithTimestamp(test.T(), "HistoryServer reachable through the API server proxy at %s", historyServerURL)

	return historyServerURL
}

// PrepareTestEnv prepares test environment for each test case, including applying a Ray cluster,
// checking the collector sidecar container exists in the head pod and an empty S3 bucket exists.
func PrepareTestEnv(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) *rayv1.RayCluster {
	// Deploy a Ray cluster with the collector.
	rayCluster := ApplyRayClusterWithCollectorWithEnvs(test, g, namespace, map[string]string{})

	// Check the collector sidecar exists in the head pod.
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.Containers).To(ContainElement(
		WithTransform(func(c corev1.Container) string { return c.Name }, Equal("collector")),
	))

	// Check an empty S3 bucket is automatically created.
	g.Expect(s3Client.StatObject(S3BucketName, "")).To(Succeed())

	return rayCluster
}

// PrepareTestEnvWithPrometheusAndGrafana prepares test environment with Prometheus and Grafana for each test case, including applying a Ray cluster,
// checking the collector sidecar container exists in the head pod and an empty S3 bucket exists.
func PrepareTestEnvWithPrometheusAndGrafana(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) *rayv1.RayCluster {
	InstallGrafanaAndPrometheus(test, g)

	additionalEnvs := map[string]string{
		"RAY_GRAFANA_IFRAME_HOST": RayGrafanaIframeHost,
		"RAY_GRAFANA_HOST":        "http://prometheus-grafana.prometheus-system.svc:80",
		"RAY_PROMETHEUS_HOST":     "http://prometheus-kube-prometheus-prometheus.prometheus-system.svc:9090",
	}

	// Deploy a Ray cluster with the collector.
	rayCluster := ApplyRayClusterWithCollectorWithEnvs(test, g, namespace, additionalEnvs)

	// Check the collector sidecar exists in the head pod.
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.Containers).To(ContainElement(
		WithTransform(func(c corev1.Container) string { return c.Name }, Equal("collector")),
	))

	// Check an empty S3 bucket is automatically created.
	g.Expect(s3Client.StatObject(S3BucketName, "")).To(Succeed())

	return rayCluster
}

// GetOneOfNodeID retrieves a node ID from the /nodes endpoint.
// If headNode is true, it iterates over all nodes and returns the one with isHeadNode == true.
func GetOneOfNodeID(g *WithT, client *http.Client, historyServerURL string, headNode bool) string {
	resp, err := client.Get(historyServerURL + "/nodes?view=summary")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var result map[string]any
	err = json.Unmarshal(body, &result)
	g.Expect(err).NotTo(HaveOccurred())

	data := result["data"].(map[string]any)
	summary := data["summary"].([]any)
	g.Expect(summary).ToNot(BeEmpty())

	// Both live and dead clusters return a flat array of node objects.
	if !headNode {
		raylet := getRayletFromNode(g, summary[0])
		return raylet["nodeId"].(string)
	}

	for _, node := range summary {
		raylet := getRayletFromNode(g, node)
		if isHead, ok := raylet["isHeadNode"].(bool); ok && isHead {
			return raylet["nodeId"].(string)
		}
	}

	g.Expect(false).To(BeTrue(), "Expected to find a head node in /nodes summary")
	return ""
}

// getRayletFromNode extracts the raylet object from a node summary.
func getRayletFromNode(g *WithT, node any) map[string]any {
	nodeInfo, ok := node.(map[string]any)
	g.Expect(ok).To(BeTrue(), "node should be an object")

	raylet, ok := nodeInfo["raylet"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "node should contain raylet object")

	return raylet
}

// GetOneOfActorID retrieves an actor ID from the /logical/actors endpoint.
// The history server returns actors from the in-memory ClusterActorMap, which is populated
// by the Event Handler processing events from S3.
func GetOneOfActorID(g *WithT, client *http.Client, historyServerURL string) string {
	resp, err := client.Get(historyServerURL + EndpointLogicalActors)
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var result map[string]any
	err = json.Unmarshal(body, &result)
	g.Expect(err).NotTo(HaveOccurred())

	// Response format: {"result": true, "msg": "...", "data": {"actors": {actor_id: {...}, ...}}}
	data, ok := result["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "response should have 'data' field")

	actors, ok := data["actors"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "data should have 'actors' field")
	g.Expect(actors).ToNot(BeEmpty(), "should have at least one actor")

	// Get the first actor ID from the map
	for actorID := range actors {
		return actorID
	}

	// This should never happen due to the length check above
	return ""
}

// VerifyLogFileEndpointReturnsContent verifies that the log file endpoint returns content.
func VerifyLogFileEndpointReturnsContent(test Test, g *WithT, client *http.Client, historyServerURL, nodeID string) {
	filename := "raylet.out"

	g.Eventually(func(gg Gomega) {
		logFileURL := fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=100", historyServerURL, EndpointLogsFile, nodeID, filename)
		resp, err := client.Get(logFileURL)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(body).ToNot(BeEmpty())
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Log file endpoint returned content successfully")
}

// VerifyLogFileEndpointRejectsPathTraversal verifies that the log file endpoint rejects path traversal attempts.
func VerifyLogFileEndpointRejectsPathTraversal(test Test, g *WithT, client *http.Client, historyServerURL, nodeID string) {
	maliciousPaths := []string{"../etc/passwd", "..", "/etc/passwd", "../../secret"}

	for _, malicious := range maliciousPaths {
		g.Eventually(func(gg Gomega) {
			url := fmt.Sprintf("%s%s?node_id=%s&filename=%s", historyServerURL, EndpointLogsFile, nodeID, malicious)
			resp, err := client.Get(url)
			gg.Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
		}, TestTimeoutShort).Should(Succeed())
	}

	LogWithTimestamp(test.T(), "Log file endpoint correctly rejected path traversal attempts")
}

// DeleteRayClusterAndWait deletes a RayCluster and waits for it to be fully deleted.
func DeleteRayClusterAndWait(test Test, g *WithT, namespace string, clusterName string) {
	err := test.Client().Ray().RayV1().RayClusters(namespace).Delete(test.Ctx(), clusterName, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayCluster %s/%s", namespace, clusterName)

	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace, clusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	LogWithTimestamp(test.T(), "RayCluster %s/%s fully deleted", namespace, clusterName)
}

// GetOneOfJobID retrieves a job_id from the /api/jobs endpoint.
func GetOneOfJobID(g *WithT, client *http.Client, historyServerURL string) string {
	resp, err := client.Get(historyServerURL + "/api/jobs/")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
	body, _ := io.ReadAll(resp.Body)
	var jobs []map[string]any
	err = json.Unmarshal(body, &jobs)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobs).ToNot(BeEmpty(), "expected at least one job from /api/jobs/")
	for _, j := range jobs {
		if jid, ok := j["job_id"].(string); ok && jid != "" {
			return jid
		}
	}
	g.Expect(false).To(BeTrue(), "no job with non-empty job_id in /api/jobs/ response")
	return ""
}
