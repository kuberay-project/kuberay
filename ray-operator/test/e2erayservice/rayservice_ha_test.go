package e2erayservice

import (
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestStaticRayService(t *testing.T) {
	rayserviceYamlFile := "testdata/rayservice.static.yaml"
	locustYamlFile := "testdata/locust-cluster.const-rate.yaml"

	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Create a ConfigMap with Locust runner script
	configMapAC := newConfigMap(namespace.Name, Files(test, "locust_runner.py"))
	configMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", configMap.Namespace, configMap.Name)

	// Create the RayService for testing
	rayServiceName := "test-rayservice"
	applyRayServiceYAMLAndWaitReady(g, test, rayserviceYamlFile, namespace.Name, rayServiceName)

	// Create Locust RayCluster
	KubectlApplyYAML(test, locustYamlFile, namespace.Name)
	locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created Locust RayCluster %s/%s successfully", locustCluster.Namespace, locustCluster.Name)

	g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Expect(GetRayCluster(test, locustCluster.Namespace, locustCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

	headPod, err := GetHeadPod(test, locustCluster)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

	// Install Locust in the head Pod
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

	// Run Locust test
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
		"python", "/locust-runner/locust_runner.py", "-f", "/locustfile/locustfile.py", "--host", "http://test-rayservice-serve-svc:8000",
	})
}

func TestAutoscalingRayService(t *testing.T) {
	rayserviceYamlFile := "testdata/rayservice.autoscaling.yaml"
	locustYamlFile := "testdata/locust-cluster.burst.yaml"
	numberOfPodsWhenSteady := 1

	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Create a ConfigMap with Locust runner script
	configMapAC := newConfigMap(namespace.Name, Files(test, "locust_runner.py"))
	configMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", configMap.Namespace, configMap.Name)

	// Create the RayService for testing
	rayServiceName := "test-rayservice"
	applyRayServiceYAMLAndWaitReady(g, test, rayserviceYamlFile, namespace.Name, rayServiceName)

	// Get the underlying RayCluster of the RayService
	rayService, err := GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayServiceUnderlyingRayCluster, err := GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())

	// Check the number of worker pods is correct when RayService is steady
	// TODO (rueian): with the current Ray version (2.43.0), autoscaler can have races with the scheduler and that causes overprovisioning.
	// So, we use TestTimeoutLong for here to wait for the autoscaler to do a scale down in the case of overprovisioning.
	// We may revisit the timeout again if the issue has been solved. See: https://github.com/ray-project/kuberay/issues/2981#issuecomment-2686172278
	g.Eventually(WorkerPods(test, rayServiceUnderlyingRayCluster), TestTimeoutLong).Should(HaveLen(numberOfPodsWhenSteady),
		"The WorkerGroupSpec.Replicas is %d", *rayServiceUnderlyingRayCluster.Spec.WorkerGroupSpecs[0].Replicas)

	// Create Locust RayCluster
	KubectlApplyYAML(test, locustYamlFile, namespace.Name)
	locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created Locust RayCluster %s/%s successfully", locustCluster.Namespace, locustCluster.Name)

	g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Expect(GetRayCluster(test, locustCluster.Namespace, locustCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

	headPod, err := GetHeadPod(test, locustCluster)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

	// Install Locust in the head Pod
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

	// Run Locust test
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
		"python", "/locust-runner/locust_runner.py", "-f", "/locustfile/locustfile.py", "--host", "http://test-rayservice-serve-svc:8000",
	})

	// Check the number of worker pods is more when RayService right after the burst
	pods, err := GetWorkerPods(test, rayServiceUnderlyingRayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(len(pods)).Should(BeNumerically(">", numberOfPodsWhenSteady))

	// Check the number of worker pods is correct when RayService is steady
	g.Eventually(WorkerPods(test, rayServiceUnderlyingRayCluster), TestTimeoutLong).Should(HaveLen(numberOfPodsWhenSteady))
}

func TestRayServiceZeroDowntimeUpgrade(t *testing.T) {
	rayserviceYamlFile := "testdata/rayservice.static.yaml"
	locustYamlFile := "testdata/locust-cluster.const-rate.yaml"

	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Create a ConfigMap with Locust runner script
	configMapAC := newConfigMap(namespace.Name, Files(test, "locust_runner.py"))
	configMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", configMap.Namespace, configMap.Name)

	// Create the RayService for testing
	rayServiceName := "test-rayservice"
	applyRayServiceYAMLAndWaitReady(g, test, rayserviceYamlFile, namespace.Name, rayServiceName)

	// Create Locust RayCluster
	KubectlApplyYAML(test, locustYamlFile, namespace.Name)
	locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created Locust RayCluster %s/%s successfully", locustCluster.Namespace, locustCluster.Name)

	g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Expect(GetRayCluster(test, locustCluster.Namespace, locustCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

	headPod, err := GetHeadPod(test, locustCluster)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

	// Install Locust in the head Pod
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

	// Start a goroutine to perform zero-downtime upgrade
	var wg sync.WaitGroup

	wg.Go(func() {
		LogWithTimestamp(test.T(), "Waiting several seconds before updating RayService")
		time.Sleep(30 * time.Second)

		LogWithTimestamp(test.T(), "Updating RayService")
		rayService, err := GetRayService(test, namespace.Name, rayServiceName)
		g.Expect(err).NotTo(HaveOccurred())
		rayClusterName := rayService.Status.ActiveServiceStatus.RayClusterName

		newRayService := rayService.DeepCopy()
		newRayService.Spec.RayClusterSpec.RayVersion = ""
		newRayService, err = test.Client().Ray().RayV1().RayServices(newRayService.Namespace).Update(test.Ctx(), newRayService, metav1.UpdateOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		waitingForRayClusterSwitch(g, test, newRayService, rayClusterName)
	})

	// Run Locust test
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
		"python", "/locust-runner/locust_runner.py", "-f", "/locustfile/locustfile.py", "--host", "http://test-rayservice-serve-svc:8000",
	})

	wg.Wait()
}

func TestRayServiceGCSFaultTolerance(t *testing.T) {
	rayserviceYamlFile := "testdata/ray-service.ft.yaml"
	locustYamlFile := "testdata/locust-cluster.const-rate.yaml"

	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Create a ConfigMap with Locust runner script
	configMapAC := newConfigMap(namespace.Name, Files(test, "locust_runner.py"))
	configMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", configMap.Namespace, configMap.Name)

	// Create the RayService for testing
	rayServiceName := "test-rayservice"
	applyRayServiceYAMLAndWaitReady(g, test, rayserviceYamlFile, namespace.Name, rayServiceName)

	g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutShort).
		Should(WithTransform(RayServicesNumEndPoints, Equal(int32(1))))

	// Get the underlying RayCluster of the RayService
	rayService, err := GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	rayServiceUnderlyingRayCluster, err := GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())

	// Create Locust RayCluster
	KubectlApplyYAML(test, locustYamlFile, namespace.Name)
	locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created Locust RayCluster %s/%s successfully", locustCluster.Namespace, locustCluster.Name)

	g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Expect(GetRayCluster(test, locustCluster.Namespace, locustCluster.Name)).To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

	locustHeadPod, err := GetHeadPod(test, locustCluster)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Found head pod %s/%s", locustHeadPod.Namespace, locustHeadPod.Name)

	// Install Locust in the Locust head Pod
	ExecPodCmd(test, locustHeadPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

	// Get current head pod
	oldHeadPod, err := GetHeadPod(test, rayServiceUnderlyingRayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	// Store the name of the head Pod in a variable
	oldHeadPodName := oldHeadPod.Name
	// Kill gcs server
	ExecPodCmd(test, oldHeadPod, common.RayHeadContainer, []string{"pkill", "gcs_server"})
	// wait for head pod not to be ready
	g.Eventually(HeadPod(test, rayServiceUnderlyingRayCluster), TestTimeoutShort).Should(WithTransform(IsPodRunningAndReady, BeFalse()))

	startTime := time.Now()
	// Run Locust test
	ExecPodCmd(test, locustHeadPod, common.RayHeadContainer, []string{
		"python", "/locust-runner/locust_runner.py", "-f", "/locustfile/locustfile.py", "--host", "http://test-rayservice-serve-svc:8000",
	})
	// Because this test shares the Locust RayCluster YAML file with other tests,
	// we need to ensure the YAML file is not accidentally updated.
	g.Expect(time.Since(startTime)).To(BeNumerically(">", 2*time.Minute))

	newHeadPod, err := GetHeadPod(test, rayServiceUnderlyingRayCluster)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(newHeadPod.Name).To(Equal(oldHeadPodName))
	g.Expect(newHeadPod.Status.ContainerStatuses[0].RestartCount).To(Equal(int32(1)))
	// Verify that all pods are running
	g.Expect(GetHeadPod(test, rayServiceUnderlyingRayCluster)).Should(WithTransform(IsPodRunningAndReady, BeTrue()))
	g.Expect(GetWorkerPods(test, rayServiceUnderlyingRayCluster)).Should(WithTransform(AllPodsRunningAndReady, BeTrue()))
}

func TestRayServiceRayClusterDeletionDelaySeconds(t *testing.T) {
	rayserviceYamlFile := "testdata/rayservice.deletiondelay.yaml"

	test := With(t)
	g := NewWithT(t)
	namespace := test.NewTestNamespace()

	// Apply the RayService YAML with deletion delay set to 10 seconds
	rayServiceName := "test-rayservice-deletion-delay"
	applyRayServiceYAMLAndWaitReady(g, test, rayserviceYamlFile, namespace.Name, rayServiceName)

	// Save the current RayCluster name
	rayService, err := GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	oldClusterName := rayService.Status.ActiveServiceStatus.RayClusterName

	// Try updating and see if the new cluster created
	LogWithTimestamp(test.T(), "Updating RayService")
	newRayService := rayService.DeepCopy()
	newRayService.Spec.RayClusterSpec.RayVersion = ""
	newRayService, err = test.Client().Ray().RayV1().RayServices(newRayService.Namespace).Update(test.Ctx(), newRayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	waitingForRayClusterSwitchWithDeletionDelay(g, test, newRayService, oldClusterName, 10*time.Second)
}
