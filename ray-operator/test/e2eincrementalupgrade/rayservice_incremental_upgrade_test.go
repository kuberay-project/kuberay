package e2eincrementalupgrade

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// TestRayServiceIncrementalUpgrade is the functional test for the basic incremental upgrade flow of the RayService,
// focusing on the behaviors of the RayService during the upgrade process.
//
// It iterates over incrementalUpgradeCombinations to cover diverse upgrade scenarios:
//   - BlueGreen
//   - AggressiveGradual
//   - ConservativeGradual
//
// The test case follows these steps:
// 1. Enable the RayServiceIncrementalUpgrade feature gate
// 2. Create a RayService with IncrementalUpgrade enabled
// 3. Wait for the RayService to be ready
// 4. Wait for the Gateway object to be ready (Accepted and Programmed conditions are True)
// 5. Wait for the corresponding HTTPRoute object to be ready (Accepted and ResolvedRefs conditions are True)
// 6. Create a Curl pod to test traffic routing through Gateway to RayService and wait for it to be ready
// 7. Validate RayService is serving traffic by sending requests through the Gateway in-cluster DNS name
// 8. Modify the RayCluster spec to trigger upgrade and change the RayService serve config to update the serve deployment
//   - NOTE: Incremental upgrade is triggered by RayCluster spec changes, not serve config changes
//
// 9. Wait for the RayService to be upgrading
// 10. Validate the active and pending cluster serve services have been created
// 11. Validate the pending cluster head pod is ready
// 12. Validate the HTTPRoute backends have two backends (active and pending cluster serve services)
// 13. Perform behavioral checks, covering:
//   - Current state value matches the expected value or the RayService finished upgrading
//   - Both old and new versions served traffic during the upgrade and no requests are dropped
//   - Traffic is migrated with respect to the interval seconds
//   - Active TargetCapacity is monotonically decreasing, and Pending TargetCapacity is monotonically increasing
//   - Active TrafficRoutedPercent is monotonically decreasing, and Pending TrafficRoutedPercent is monotonically increasing
//
// 14. Wait for incremental upgrade to complete
// 15. Validate the RayService uses updated ServeConfig after upgrade completes
func TestRayServiceIncrementalUpgrade(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	for _, params := range incrementalUpgradeCombinations {
		t.Run(params.Name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)

			namespace := test.NewTestNamespace()
			rayServiceName := "incremental-rayservice"

			stepSize, interval, maxSurge := params.ptrs()
			serveConfigV2 := defaultIncrementalUpgradeServeConfigV2

			// Create RayService with IncrementalUpgrade enabled and wait for key components to be ready
			rayService, httpRoute, gatewayHost := bootstrapIncrementalRayService(test, g, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2)

			// Create curl pod to test traffic routing through Gateway to RayService
			curlPod, err := CreateCurlPod(g, test, CurlPodName, CurlContainerName, namespace.Name)
			g.Expect(err).NotTo(HaveOccurred())

			LogWithTimestamp(test.T(), "Verifying RayService is serving traffic")
			stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
			g.Expect(stdout.String()).To(Equal("6"))
			stdout, _ = CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/calc", `["MUL", 3]`)
			g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

			LogWithTimestamp(test.T(), "Triggering incremental upgrade by modifying RayCluster spec and updating serve deployment by changing RayService serve config")
			g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2,
				withWorkerCPURequest("500m"), withUpgradedServeConfig())).To(Succeed(), "Failed to update RayService to trigger upgrade")
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be true", rayService.Namespace, rayService.Name)
			g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

			LogWithTimestamp(test.T(), "Verifying active and pending cluster serve services and HTTPRoute backends")
			upgradingRaySvc, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			activeClusterName := upgradingRaySvc.Status.ActiveServiceStatus.RayClusterName
			g.Expect(activeClusterName).NotTo(BeEmpty(), "The active cluster should be set when a RayService is ready.")
			pendingClusterName := upgradingRaySvc.Status.PendingServiceStatus.RayClusterName
			g.Expect(pendingClusterName).NotTo(BeEmpty(), "The controller should have created a pending cluster.")

			// Validate serve service for the active cluster exists.
			activeServeSvcName := utils.GenerateServeServiceName(activeClusterName)
			_, err = test.Client().Core().CoreV1().Services(namespace.Name).Get(test.Ctx(), activeServeSvcName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred(), "The serve service for the active cluster should be created.")

			// Validate serve service for the pending cluster has been created for the upgrade.
			pendingServeSvcName := utils.GenerateServeServiceName(pendingClusterName)
			g.Eventually(func(gg Gomega) {
				_, err = test.Client().Core().CoreV1().Services(namespace.Name).Get(test.Ctx(), pendingServeSvcName, metav1.GetOptions{})
				gg.Expect(err).NotTo(HaveOccurred(), "The serve service for the pending cluster should be created.")
			}, TestTimeoutShort).Should(Succeed())

			LogWithTimestamp(test.T(), "Waiting for pending RayCluster %s to have a ready head pod", pendingClusterName)
			g.Eventually(RayCluster(test, namespace.Name, pendingClusterName), TestTimeoutMedium).
				Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionTrue, rayv1.HeadPodRunningAndReady)))

			// Wait for the HTTPRoute to reflect the two backends.
			LogWithTimestamp(test.T(), "Waiting for HTTPRoute to have two backends")
			g.Eventually(func(gg Gomega) {
				route, err := GetHTTPRoute(test, namespace.Name, httpRoute.Name)
				gg.Expect(err).NotTo(HaveOccurred())
				gg.Expect(route.Spec.Rules).To(HaveLen(1))
				gg.Expect(route.Spec.Rules[0].BackendRefs).To(HaveLen(2))
				gg.Expect(string(route.Spec.Rules[0].BackendRefs[1].Name)).To(Equal(pendingServeSvcName))
			}, TestTimeoutShort).Should(Succeed())

			// Validate expected behavior during an IncrementalUpgrade. The following checks ensure
			// that no requests are dropped throughout the upgrade process.
			LogWithTimestamp(test.T(), "Performing behavioral checks, validating stepwise traffic and capacity migration")

			oldVersionServed := false
			newVersionServed := false

			intervalSeconds := *interval
			var lastMigratedTime *metav1.Time

			prevActiveCapacity := int32(100)
			prevPendingCapacity := int32(0)
			prevActiveTraffic := int32(100)
			prevPendingTraffic := int32(0)

			upgradeSteps := generateUpgradeSteps(*stepSize, *maxSurge)
			for _, step := range upgradeSteps {
				// Behavior 1: Current state value matches the expected value or the RayService finished upgrading
				LogWithTimestamp(test.T(), "%s", step.name)
				g.Eventually(func(gg Gomega) {
					svc, err := GetRayService(test, namespace.Name, rayServiceName)
					gg.Expect(err).NotTo(HaveOccurred())
					gg.Expect(step.getValue(svc) == step.expectedValue || !IsRayServiceUpgrading(svc)).
						To(BeTrue())
				}, TestTimeoutMedium).Should(Succeed())

				// Behavior 2: Both old and new versions served traffic during the upgrade and no requests are dropped
				stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
				response := stdout.String()
				g.Expect(response).To(Or(Equal("6"), Equal("8")), "Response should be from the old or new app version during the upgrade")
				if response == "6" {
					oldVersionServed = true
				}
				if response == "8" {
					newVersionServed = true
				}

				svc, err := GetRayService(test, namespace.Name, rayServiceName)
				g.Expect(err).NotTo(HaveOccurred())
				if !IsRayServiceUpgrading(svc) {
					break
				}

				// Behavior 3: Traffic is migrated with respect to the interval seconds
				if strings.Contains(step.name, "pending traffic to shift") {
					currentMigratedTime := GetLastTrafficMigratedTime(svc)
					g.Expect(currentMigratedTime).NotTo(BeNil())

					// Verify IntervalSeconds have passed since last TrafficRoutedPercent update.
					if lastMigratedTime != nil {
						duration := currentMigratedTime.Sub(lastMigratedTime.Time)
						intervalDuration := time.Duration(intervalSeconds) * time.Second
						g.Expect(duration).To(BeNumerically(">=", intervalDuration),
							"Time between traffic steps should be >= IntervalSeconds")
					}
					lastMigratedTime = currentMigratedTime
				}

				// Behavior 4: Active TargetCapacity is monotonically decreasing, and Pending TargetCapacity is monotonically increasing
				currentActiveCapacity := GetActiveCapacity(svc)
				currentPendingCapacity := GetPendingCapacity(svc)
				g.Expect(currentActiveCapacity).To(BeNumerically("<=", prevActiveCapacity),
					"TargetCapacity in active cluster must be monotonically decreasing: prev %d, curr %d", prevActiveCapacity, currentActiveCapacity)
				g.Expect(currentPendingCapacity).To(BeNumerically(">=", prevPendingCapacity),
					"TargetCapacity in pending cluster must be monotonically increasing: prev %d, curr %d", prevPendingCapacity, currentPendingCapacity)
				prevActiveCapacity = currentActiveCapacity
				prevPendingCapacity = currentPendingCapacity

				// Behavior 5: Active TrafficRoutedPercent is monotonically decreasing, and Pending TrafficRoutedPercent is monotonically increasing
				currentActiveTraffic := GetActiveTraffic(svc)
				currentPendingTraffic := GetPendingTraffic(svc)
				g.Expect(currentActiveTraffic).To(BeNumerically("<=", prevActiveTraffic),
					"TrafficRoutedPercent in active cluster must be monotonically decreasing: prev %d, curr %d", prevActiveTraffic, currentActiveTraffic)
				g.Expect(currentPendingTraffic).To(BeNumerically(">=", prevPendingTraffic),
					"TrafficRoutedPercent in pending cluster must be monotonically increasing: prev %d, curr %d", prevPendingTraffic, currentPendingTraffic)
				prevActiveTraffic = currentActiveTraffic
				prevPendingTraffic = currentPendingTraffic
			}

			LogWithTimestamp(test.T(), "Verifying both old and new versions served traffic during the upgrade")
			g.Expect(oldVersionServed).To(BeTrue(), "The old version of the service should have served traffic during the upgrade.")
			g.Expect(newVersionServed).To(BeTrue(), "The new version of the service should have served traffic during the upgrade.")

			// Check that RayService completed upgrade
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be false", rayService.Namespace, rayService.Name)
			g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeFalse()))

			LogWithTimestamp(test.T(), "Verifying RayService uses updated ServeConfig after upgrade completes")
			stdout, _ = CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
			g.Expect(stdout.String()).To(Equal("8"))
		})
	}
}

// TestRayServiceIncrementalUpgradeWithLocust tests the incremental upgrade flow of the RayService
// by running a Locust load test throughout the entire upgrade lifecycle:
//   - Initial serving on the old cluster
//   - During the upgrade phase
//   - Final serving on the new cluster
//
// It iterates over incrementalUpgradeCombinations to cover diverse upgrade scenarios under load:
//   - BlueGreen
//   - AggressiveGradual
//   - ConservativeGradual
//
// The ultimate goal is to ensure that there are no request drops during the upgrade.
//
// The test case follows these steps:
// 1. Enable the RayServiceIncrementalUpgrade feature gate
// 2. Create a RayService with IncrementalUpgrade enabled
// 3. Wait for the RayService to be ready
// 4. Wait for the Gateway object to be ready (Accepted and Programmed conditions are True)
// 5. Wait for the corresponding HTTPRoute object to be ready (Accepted and ResolvedRefs conditions are True)
// 6. Create a ConfigMap with Locust runner script
// 7. Deploy a head-only Locust RayCluster and install Locust in the head Pod
// 8. Start Locust in a background goroutine targeting the Gateway in-cluster DNS name
// 9. Wait for Locust to ramp up and enter the steady state
// 10. Modify the RayCluster spec to trigger upgrade and change the RayService serve config to update the serve deployment
//   - NOTE: Incremental upgrade is triggered by RayCluster spec changes, not serve config changes
//
// 11. Wait for incremental upgrade to complete
// 12. Ensure all remaining traffic is routed to the new cluster after upgrade completes
//
// NOTE: locust_runner.py validates whether the Locust load test completed with zero failures.
// So we don't need to validate this here.
func TestRayServiceIncrementalUpgradeWithLocust(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	for _, params := range incrementalUpgradeCombinations {
		t.Run(params.Name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)

			namespace := test.NewTestNamespace()
			rayServiceName := "incremental-rayservice"
			stepSize, interval, maxSurge := params.ptrs()
			serveConfigV2 := highRPSServeConfigV2

			// Phase 1: Create RayService with incremental upgrade and wait for key components to be ready
			_, _, gatewayHost := bootstrapIncrementalRayService(test, g, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2)

			// Phase 2: Deploy Locust RayCluster and install Locust
			// TODO(jwj): Extract a helper for cross-module reusability (rayservice_ha_test.go) if needed.
			locustYamlFile := "testdata/locust-cluster.incremental-upgrade.yaml"

			configMapAC := newLocustRunnerConfigMapAC(namespace.Name, Files(test, "locust_runner.py"))
			configMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", configMap.Namespace, configMap.Name)

			KubectlApplyYAML(test, locustYamlFile, namespace.Name)
			locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(test.T(), "Created Locust RayCluster %s/%s successfully", locustCluster.Namespace, locustCluster.Name)

			g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
				Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, locustCluster.Namespace, locustCluster.Name)).
				To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

			locustHeadPod, err := GetHeadPod(test, locustCluster)
			g.Expect(err).NotTo(HaveOccurred())
			LogWithTimestamp(test.T(), "Found Locust head pod %s/%s", locustHeadPod.Namespace, locustHeadPod.Name)

			ExecPodCmd(test, locustHeadPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

			// Phase 3: Start Locust in a background goroutine targeting the Gateway in-cluster DNS name
			locustHost := fmt.Sprintf("http://%s", gatewayHost)

			eg, _ := errgroup.WithContext(test.Ctx())
			eg.Go(func() error {
				LogWithTimestamp(test.T(), "Starting Locust load test against %s", locustHost)
				_, _, err := ExecPodCmdWithError(test, locustHeadPod, common.RayHeadContainer, []string{
					"python", "/locust-runner/locust_runner.py",
					"-f", "/locustfile/locustfile.py",
					"--host", locustHost,
				})
				return err
			})
			defer func() {
				LogWithTimestamp(test.T(), "Stopping Locust load test")
				_, _, _ = ExecPodCmdWithError(
					test,
					locustHeadPod,
					common.RayHeadContainer,
					[]string{"pkill", "-SIGINT", "-f", "locust --headless"},
				)
				LogWithTimestamp(test.T(), "Waiting for Locust load test goroutine to finish")
				if err := eg.Wait(); err != nil && !test.T().Failed() {
					test.T().Errorf("Locust load test failed: %v", err)
				}
			}()

			// Allow Locust to ramp up and send traffic to the old cluster before triggering upgrade.
			err = warmupLocust(test, locustHeadPod, locustWarmupRPSThreshold, locustWarmupStableWindowSeconds, locustWarmupTimeout)
			g.Expect(err).NotTo(HaveOccurred())

			// Phase 4: Trigger incremental upgrade
			LogWithTimestamp(test.T(), "Triggering incremental upgrade by updating RayCluster spec")
			g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2,
				withWorkerCPURequest("500m"), withUpgradedServeConfig())).To(Succeed(), "Failed to update RayService to trigger upgrade")
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be true", namespace.Name, rayServiceName)
			g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

			// Phase 5: Wait for upgrade to complete and validate remaining traffic is routed to the new cluster
			// Since no additional validation is needed during the upgrade, we use a longer timeout.
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be false", namespace.Name, rayServiceName)
			g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutMedium).Should(WithTransform(IsRayServiceUpgrading, BeFalse()))

			LogWithTimestamp(test.T(), "Validating remaining traffic is routed to the new cluster after upgrade completes")
			curlPod, err := CreateCurlPod(g, test, CurlPodName, CurlContainerName, namespace.Name)
			g.Expect(err).NotTo(HaveOccurred())

			stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodGet, "/test", "")
			var resp struct {
				Status string `json:"status"`
			}
			err = json.Unmarshal(stdout.Bytes(), &resp)
			g.Expect(err).NotTo(HaveOccurred(), "GET /test response should be valid JSON, got: %s", stdout.String())
			g.Expect(resp.Status).To(Equal("ok"), "GET /test status field should be ok, got: %s", resp.Status)
		})
	}
}

func TestRayServiceIncrementalUpgradeRollback(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	type RollbackTrigger string
	const (
		TriggerBeforeTraffic RollbackTrigger = "BeforeTraffic"
		TriggerEarly         RollbackTrigger = "Early"
		TriggerMiddle        RollbackTrigger = "Middle"
		TriggerLate          RollbackTrigger = "Late"
		TriggerLateWithCrash RollbackTrigger = "LateWithCrash"
	)

	type RollbackSequence string
	const (
		SeqABA  RollbackSequence = "A->B->A"
		SeqABAB RollbackSequence = "A->B->A->B"
		SeqABAC RollbackSequence = "A->B->A->C"
	)

	type rollbackTestCase struct {
		Name         string
		Sequence     RollbackSequence
		Strategy     incrementalUpgradeParams
		TriggerStage RollbackTrigger
	}

	rollbackMatrix := []rollbackTestCase{
		{Name: "BasicRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "CancelRollback", Sequence: SeqABAB, Strategy: incrementalUpgradeParams{Name: "Conservative", MaxSurge: 25, StepSize: 5, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "ThirdSpec", Sequence: SeqABAC, Strategy: incrementalUpgradeParams{Name: "Conservative", MaxSurge: 25, StepSize: 5, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "UnhealthyPendingCluster", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 10}, TriggerStage: TriggerLateWithCrash},
		{Name: "EarlyRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 2}, TriggerStage: TriggerEarly},
		{Name: "LateRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 10}, TriggerStage: TriggerLate},
		{Name: "FastRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "BlueGreen", MaxSurge: 100, StepSize: 100, Interval: 1}, TriggerStage: TriggerBeforeTraffic},
	}

	for _, tc := range rollbackMatrix {
		t.Run(tc.Name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)

			namespace := test.NewTestNamespace()
			rayServiceName := "rollback-rayservice"

			stepSize, interval, maxSurge := tc.Strategy.ptrs()
			serveConfigV2 := defaultIncrementalUpgradeServeConfigV2

			_, httpRoute, gatewayHost := bootstrapIncrementalRayService(test, g, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2)

			// Copy original spec and active cluster name to use to trigger a rollback later.
			rayService, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			originalSpec := rayService.Spec.DeepCopy()
			originalActiveClusterName := rayService.Status.ActiveServiceStatus.RayClusterName
			g.Expect(originalActiveClusterName).NotTo(BeEmpty())

			// Create curl pod to test traffic routing through Gateway to RayService
			curlPod, err := CreateCurlPod(g, test, CurlPodName, CurlContainerName, namespace.Name)
			g.Expect(err).NotTo(HaveOccurred())

			LogWithTimestamp(test.T(), "Verifying RayService is serving traffic")
			stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
			g.Expect(stdout.String()).To(Equal("6"))

			// Trigger an incremental upgrade through a change to the RayCluster spec.
			LogWithTimestamp(test.T(), "Triggering an upgrade for RayService %s/%s", rayService.Namespace, rayService.Name)
			err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest("500m"), withUpgradedServeConfig())
			g.Expect(err).NotTo(HaveOccurred())

			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be true", rayService.Namespace, rayService.Name)
			g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

			LogWithTimestamp(test.T(), "Waiting for trigger condition: %s", tc.TriggerStage)
			var pendingClusterName string
			g.Eventually(func(g Gomega) {
				svc, err := GetRayService(test, namespace.Name, rayServiceName)
				g.Expect(err).NotTo(HaveOccurred())

				pendingClusterName = svc.Status.PendingServiceStatus.RayClusterName
				g.Expect(pendingClusterName).NotTo(BeEmpty())

				if tc.TriggerStage == TriggerBeforeTraffic {
					pending := svc.Status.PendingServiceStatus.TrafficRoutedPercent
					if pending != nil {
						g.Expect(*pending).Should(Equal(int32(0)))
					}
					return
				}

				pending := svc.Status.PendingServiceStatus.TrafficRoutedPercent
				g.Expect(pending).NotTo(BeNil())

				switch tc.TriggerStage {
				case TriggerEarly:
					g.Expect(*pending).Should(BeNumerically(">", 0))
					g.Expect(*pending).Should(BeNumerically("<=", 30))
				case TriggerMiddle:
					g.Expect(*pending).Should(BeNumerically(">=", 30))
					g.Expect(*pending).Should(BeNumerically("<=", 70))
				case TriggerLate, TriggerLateWithCrash:
					g.Expect(*pending).Should(BeNumerically(">=", 70))
					g.Expect(*pending).Should(BeNumerically("<", 100))
				}
			}, TestTimeoutMedium).Should(Succeed())

			// Try to curl to make sure everything is working while upgrading (old and/or new versions)
			stdout, _ = CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
			response := stdout.String()
			g.Expect(response).To(Or(Equal("6"), Equal("8")), "Response should be from the old or new app version during the upgrade")

			if tc.TriggerStage == TriggerLateWithCrash {
				LogWithTimestamp(test.T(), "Simulating crash of pending cluster head pod")
				pendingCluster, err := GetRayCluster(test, namespace.Name, pendingClusterName)
				g.Expect(err).NotTo(HaveOccurred())
				pendingHeadPod, err := GetHeadPod(test, pendingCluster)
				g.Expect(err).NotTo(HaveOccurred())
				err = test.Client().Core().CoreV1().Pods(namespace.Name).Delete(test.Ctx(), pendingHeadPod.Name, metav1.DeleteOptions{})
				g.Expect(err).NotTo(HaveOccurred())
			}

			// Trigger a rollback by updating the spec back to the original version.
			LogWithTimestamp(test.T(), "Triggering a rollback for RayService %s/%s", rayService.Namespace, rayService.Name)
			rs, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			activeBeforeRollback := int32(0)
			if rs.Status.ActiveServiceStatus.TrafficRoutedPercent != nil {
				activeBeforeRollback = *rs.Status.ActiveServiceStatus.TrafficRoutedPercent
			}
			err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest(defaultWorkerCPURequest))
			g.Expect(err).NotTo(HaveOccurred())

			if tc.TriggerStage != TriggerBeforeTraffic {
				g.Eventually(func(g Gomega) {
					svc, err := GetRayService(test, rayService.Namespace, rayService.Name)
					g.Expect(err).NotTo(HaveOccurred())
					isRollingBack := IsRayServiceRollingBack(svc)
					hasConverged := !IsRayServiceUpgrading(svc) && svc.Status.PendingServiceStatus.RayClusterName == ""
					g.Expect(isRollingBack || hasConverged).To(BeTrue(), "RayService should be rolling back or already completed rollback")
				}, TestTimeoutShort).Should(Succeed())
			}

			if tc.Sequence == SeqABAB || tc.Sequence == SeqABAC {
				g.Eventually(func(g Gomega) {
					svc, err := GetRayService(test, namespace.Name, rayServiceName)
					g.Expect(err).NotTo(HaveOccurred())
					active := svc.Status.ActiveServiceStatus.TrafficRoutedPercent
					g.Expect(active).NotTo(BeNil())
					g.Expect(*active).Should(BeNumerically(">", activeBeforeRollback))
				}, TestTimeoutShort).Should(Succeed())

				cpuRequest := "500m" // Spec B
				switch tc.Sequence {
				case SeqABAB:
					LogWithTimestamp(test.T(), "Canceling rollback for RayService %s/%s (Spec B)", namespace.Name, rayServiceName)
				case SeqABAC:
					LogWithTimestamp(test.T(), "Third spec for RayService %s/%s (Spec C)", namespace.Name, rayServiceName)
					cpuRequest = "600m" // Spec C
				}
				err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest(cpuRequest), withUpgradedServeConfig())
				g.Expect(err).NotTo(HaveOccurred())
			}

			// Verify that the rollback/upgrade completes and the pending cluster is cleaned up.
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to converge", namespace.Name, rayServiceName)
			g.Eventually(func(g Gomega) {
				svc, err := GetRayService(test, namespace.Name, rayServiceName)
				g.Expect(err).NotTo(HaveOccurred())
				// Rollback/Upgrade is done when both conditions are false and pending status is empty.
				g.Expect(IsRayServiceRollingBack(svc)).To(BeFalse())
				g.Expect(IsRayServiceUpgrading(svc)).To(BeFalse())
				g.Expect(svc.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())

				active := svc.Status.ActiveServiceStatus.TrafficRoutedPercent
				g.Expect(active).NotTo(BeNil())
				g.Expect(*active).Should(Equal(int32(100)))
			}, TestTimeoutLong).Should(Succeed())

			// Check that the pending RayCluster resource is deleted (if it was a rollback to A).
			// If it's ABAB or ABAC, the initial pending cluster might be reused or deleted, but eventually pending status is empty.
			if tc.Sequence == SeqABA {
				LogWithTimestamp(test.T(), "Verifying the pending RayCluster resource was deleted")
				g.Eventually(func() error {
					_, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), pendingClusterName, metav1.GetOptions{})
					return err
				}, TestTimeoutMedium).Should(WithTransform(errors.IsNotFound, BeTrue()))
			}

			// The HTTPRoute should now only have one backend after it completes.
			g.Eventually(HTTPRoute(test, namespace.Name, httpRoute.Name), TestTimeoutShort).
				Should(WithTransform(func(route *gwv1.HTTPRoute) int {
					if route == nil || len(route.Spec.Rules) == 0 {
						return 0
					}
					return len(route.Spec.Rules[0].BackendRefs)
				}, Equal(1)))

			// Check resources on the final active cluster based on the sequence
			svc, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			finalClusterName := svc.Status.ActiveServiceStatus.RayClusterName
			finalCluster, err := GetRayCluster(test, namespace.Name, finalClusterName)
			g.Expect(err).NotTo(HaveOccurred())
			finalCPUReq := finalCluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]

			switch tc.Sequence {
			case SeqABA:
				g.Expect(finalClusterName).To(Equal(originalActiveClusterName), "Original active RayCluster should be preserved after rollback")
				expectedCPU := originalSpec.RayClusterSpec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
				g.Expect(finalCPUReq.Cmp(expectedCPU)).To(Equal(0), "CPU request should match original")
			case SeqABAB:
				expectedCPU := resource.MustParse("500m")
				g.Expect(finalCPUReq.Cmp(expectedCPU)).To(Equal(0), "CPU request should match 500m")
			case SeqABAC:
				expectedCPU := resource.MustParse("600m")
				g.Expect(finalCPUReq.Cmp(expectedCPU)).To(Equal(0), "CPU request should match 600m")
			}

			// Verify the curl request returns the expected value based on the final serve config
			stdout, _ = CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
			if tc.Sequence == SeqABA {
				g.Expect(stdout.String()).To(Equal("6"))
			} else {
				g.Expect(stdout.String()).To(Equal("8"))
			}
		})
	}
}

func TestRayServiceIncrementalUpgradeRollbackMatrixWithLocust(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	type RollbackTrigger string
	const (
		TriggerBeforeTraffic RollbackTrigger = "BeforeTraffic"
		TriggerEarly         RollbackTrigger = "Early"
		TriggerMiddle        RollbackTrigger = "Middle"
		TriggerLate          RollbackTrigger = "Late"
	)

	type RollbackSequence string
	const (
		SeqABA  RollbackSequence = "A->B->A"
		SeqABAB RollbackSequence = "A->B->A->B"
		SeqABAC RollbackSequence = "A->B->A->C"
	)

	type rollbackTestCase struct {
		Name         string
		Sequence     RollbackSequence
		Strategy     incrementalUpgradeParams
		TriggerStage RollbackTrigger
	}

	rollbackMatrix := []rollbackTestCase{
		{Name: "BasicRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "CancelRollback", Sequence: SeqABAB, Strategy: incrementalUpgradeParams{Name: "Conservative", MaxSurge: 25, StepSize: 5, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "ThirdSpec", Sequence: SeqABAC, Strategy: incrementalUpgradeParams{Name: "Conservative", MaxSurge: 25, StepSize: 5, Interval: 2}, TriggerStage: TriggerMiddle},
		{Name: "EarlyRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 2}, TriggerStage: TriggerEarly},
		{Name: "LateRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "Standard", MaxSurge: 50, StepSize: 25, Interval: 10}, TriggerStage: TriggerLate},
		{Name: "FastRollback", Sequence: SeqABA, Strategy: incrementalUpgradeParams{Name: "BlueGreen", MaxSurge: 100, StepSize: 100, Interval: 1}, TriggerStage: TriggerBeforeTraffic},
	}

	for _, tc := range rollbackMatrix {
		t.Run(tc.Name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)

			namespace := test.NewTestNamespace()
			rayServiceName := "rollback-rayservice"
			stepSize, interval, maxSurge := tc.Strategy.ptrs()
			serveConfigV2 := highRPSServeConfigV2

			// Phase 1: Create RayService with incremental upgrade and wait for it to be ready
			_, _, gatewayIP := bootstrapIncrementalRayService(test, g, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2)

			// Save original spec (Spec A) and original active cluster name
			rayService, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			originalSpec := rayService.Spec.DeepCopy()
			originalActiveClusterName := rayService.Status.ActiveServiceStatus.RayClusterName
			g.Expect(originalActiveClusterName).NotTo(BeEmpty())

			// Phase 2: Deploy Locust RayCluster and install Locust
			locustYamlFile := "testdata/locust-cluster.incremental-upgrade.yaml"
			configMapAC := newLocustRunnerConfigMapAC(namespace.Name, Files(test, "locust_runner.py"))
			_, err = test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), configMapAC, TestApplyOptions)
			g.Expect(err).NotTo(HaveOccurred())

			KubectlApplyYAML(test, locustYamlFile, namespace.Name)
			locustCluster, err := GetRayCluster(test, namespace.Name, "locust-cluster")
			g.Expect(err).NotTo(HaveOccurred())

			g.Eventually(RayCluster(test, locustCluster.Namespace, locustCluster.Name), TestTimeoutMedium).
				Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

			locustHeadPod, err := GetHeadPod(test, locustCluster)
			g.Expect(err).NotTo(HaveOccurred())

			ExecPodCmd(test, locustHeadPod, common.RayHeadContainer, []string{"pip", "install", "locust==2.32.10"})

			// Phase 3: Start Locust in a background goroutine targeting Gateway IP
			locustHost := fmt.Sprintf("http://%s", gatewayIP)
			eg, _ := errgroup.WithContext(test.Ctx())
			eg.Go(func() error {
				LogWithTimestamp(test.T(), "Starting Locust load test against %s", locustHost)
				_, _, err := ExecPodCmdWithError(test, locustHeadPod, common.RayHeadContainer, []string{
					"python", "/locust-runner/locust_runner.py",
					"-f", "/locustfile/locustfile.py",
					"--host", locustHost,
				})
				return err
			})
			defer func() {
				LogWithTimestamp(test.T(), "Stopping Locust load test")
				_, _, _ = ExecPodCmdWithError(
					test,
					locustHeadPod,
					common.RayHeadContainer,
					[]string{"pkill", "-SIGINT", "-f", "locust --headless"},
				)
				LogWithTimestamp(test.T(), "Waiting for Locust load test goroutine to finish")
				if err := eg.Wait(); err != nil && !test.T().Failed() {
					test.T().Errorf("Locust load test failed: %v", err)
				}
			}()

			err = warmupLocust(test, locustHeadPod, locustWarmupRPSThreshold, locustWarmupStableWindowSeconds, locustWarmupTimeout)
			g.Expect(err).NotTo(HaveOccurred())

			// Phase 4: Trigger incremental upgrade (A -> B)
			LogWithTimestamp(test.T(), "Triggering an upgrade for RayService %s/%s (Spec B)", rayService.Namespace, rayService.Name)
			err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest("500m"), withUpgradedServeConfig())
			g.Expect(err).NotTo(HaveOccurred())

			g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

			// Wait for the trigger condition
			LogWithTimestamp(test.T(), "Waiting for trigger condition: %s", tc.TriggerStage)
			var pendingClusterName string
			g.Eventually(func(g Gomega) {
				svc, err := GetRayService(test, namespace.Name, rayServiceName)
				g.Expect(err).NotTo(HaveOccurred())

				pendingClusterName = svc.Status.PendingServiceStatus.RayClusterName
				g.Expect(pendingClusterName).NotTo(BeEmpty())

				if tc.TriggerStage == TriggerBeforeTraffic {
					// For BlueGreen (StepSize=100), we trigger rollback while the pending cluster is being created,
					// before any traffic is shifted.
					pending := svc.Status.PendingServiceStatus.TrafficRoutedPercent
					if pending != nil {
						g.Expect(*pending).Should(Equal(int32(0)))
					}
					return
				}

				pending := svc.Status.PendingServiceStatus.TrafficRoutedPercent
				g.Expect(pending).NotTo(BeNil())

				switch tc.TriggerStage {
				case TriggerEarly:
					g.Expect(*pending).Should(BeNumerically(">", 0))
					g.Expect(*pending).Should(BeNumerically("<=", 30))
				case TriggerMiddle:
					g.Expect(*pending).Should(BeNumerically(">=", 30))
					g.Expect(*pending).Should(BeNumerically("<=", 70))
				case TriggerLate:
					g.Expect(*pending).Should(BeNumerically(">=", 70))
					g.Expect(*pending).Should(BeNumerically("<", 100))
				}
			}, TestTimeoutMedium).Should(Succeed())

			// Phase 5: Trigger rollback (B -> A)
			LogWithTimestamp(test.T(), "Triggering a rollback for RayService %s/%s (Spec A)", rayService.Namespace, rayService.Name)
			rs, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			activeBeforeRollback := int32(0)
			if rs.Status.ActiveServiceStatus.TrafficRoutedPercent != nil {
				activeBeforeRollback = *rs.Status.ActiveServiceStatus.TrafficRoutedPercent
			}
			err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest(defaultWorkerCPURequest))
			g.Expect(err).NotTo(HaveOccurred())

			if tc.TriggerStage != TriggerBeforeTraffic {
				g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceRollingBack, BeTrue()))
			}

			// Handle sequence branches for more complex rollback scenarios
			// For scenarios involving a Cancel Rollback (SeqABAB) or a Third Spec (SeqABAC),
			// we wait until the rollback has started progressing (indicated by active traffic increasing)
			// and then submit another spec update.
			// - SeqABAB: We re-apply Spec B. Because the pending cluster already matches Spec B,
			//   KubeRay should cancel the rollback and resume migrating traffic to Spec B.
			// - SeqABAC: We apply a brand new Spec C. KubeRay should abandon the rollback to Spec A,
			//   clean up Spec B, and start provisioning Spec C as the new target.
			if tc.Sequence == SeqABAB || tc.Sequence == SeqABAC {
				g.Eventually(func(g Gomega) {
					svc, err := GetRayService(test, namespace.Name, rayServiceName)
					g.Expect(err).NotTo(HaveOccurred())
					active := svc.Status.ActiveServiceStatus.TrafficRoutedPercent
					g.Expect(active).NotTo(BeNil())
					g.Expect(*active).Should(BeNumerically(">", activeBeforeRollback))
				}, TestTimeoutShort).Should(Succeed())

				cpuRequest := "500m" // Spec B
				switch tc.Sequence {
				case SeqABAB:
					LogWithTimestamp(test.T(), "Canceling rollback for RayService %s/%s (Spec B)", namespace.Name, rayServiceName)
				case SeqABAC:
					LogWithTimestamp(test.T(), "Third spec for RayService %s/%s (Spec C)", namespace.Name, rayServiceName)
					cpuRequest = "600m" // Spec C
				}
				err = triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2, withWorkerCPURequest(cpuRequest), withUpgradedServeConfig())
				g.Expect(err).NotTo(HaveOccurred())
			}

			// Phase 6: Ensure the upgrade/rollback operation is fully complete:
			// 1. The UpgradeInProgress condition is cleared (False).
			// 2. The RollbackInProgress condition is cleared (False).
			// 3. The pending cluster has been deleted, meaning its name field is empty.
			// 4. The active cluster serves 100% of the traffic.
			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to converge", namespace.Name, rayServiceName)
			g.Eventually(func(g Gomega) {
				svc, err := GetRayService(test, namespace.Name, rayServiceName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsRayServiceUpgrading(svc)).To(BeFalse())
				g.Expect(IsRayServiceRollingBack(svc)).To(BeFalse())
				g.Expect(svc.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())

				active := svc.Status.ActiveServiceStatus.TrafficRoutedPercent
				g.Expect(active).NotTo(BeNil())
				g.Expect(*active).Should(Equal(int32(100)))
			}, TestTimeoutLong).Should(Succeed())

			// Check resources on the final active cluster based on the sequence
			svc, err := GetRayService(test, namespace.Name, rayServiceName)
			g.Expect(err).NotTo(HaveOccurred())
			finalClusterName := svc.Status.ActiveServiceStatus.RayClusterName
			finalCluster, err := GetRayCluster(test, namespace.Name, finalClusterName)
			g.Expect(err).NotTo(HaveOccurred())
			finalCPUReq := finalCluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]

			switch tc.Sequence {
			case SeqABA:
				g.Expect(finalClusterName).To(Equal(originalActiveClusterName), "Original active RayCluster should be preserved after rollback")
				g.Expect(finalCPUReq).To(Equal(originalSpec.RayClusterSpec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]))
			case SeqABAB:
				g.Expect(finalCPUReq).To(Equal(resource.MustParse("500m")))
			case SeqABAC:
				g.Expect(finalCPUReq).To(Equal(resource.MustParse("600m")))
			}
		})
	}
}

// TestRayServiceIncrementalUpgradeWithGatewayRef validates attaching an incremental
// upgrade to a pre-existing (shared) Gateway via clusterUpgradeOptions.gatewayRef
// instead of letting KubeRay create a per-RayService Gateway. It checks:
//  1. KubeRay attaches the HTTPRoute to the pre-existing Gateway (its ParentRef
//     targets the shared Gateway) and does NOT create a per-RayService Gateway.
//  2. Traffic resolves through the shared Gateway to the active cluster, and keeps
//     resolving to active/pending clusters as it shifts during an upgrade.
//  3. Suspending the RayService cleans up the HTTPRoute and active/pending Services
//     but leaves the shared Gateway intact (KubeRay does not own it).
func TestRayServiceIncrementalUpgradeWithGatewayRef(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "gatewayref-incremental-rayservice"
	sharedGatewayName := "shared-gateway"

	stepSize := new(int32(25))
	interval := new(int32(20))
	maxSurge := new(int32(30))
	serveConfigV2 := defaultIncrementalUpgradeServeConfigV2

	// Pre-create the shared Gateway the RayService will attach to.
	sharedGateway := createSharedGateway(test, g, namespace.Name, sharedGatewayName)
	gatewayHost := GetGatewayHost(sharedGateway)
	g.Expect(gatewayHost).NotTo(BeEmpty())

	// Deploy the RayService with gatewayRef -> the shared Gateway.
	LogWithTimestamp(test.T(), "Creating RayService %s/%s with gatewayRef -> %s", namespace.Name, rayServiceName, sharedGatewayName)
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2,
		withGatewayRef(sharedGatewayName, namespace.Name))).To(Succeed())
	g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	// Behavior 1: KubeRay must NOT create a per-RayService Gateway.
	perServiceGatewayName := fmt.Sprintf("%s-gateway", rayServiceName)
	g.Consistently(func(gg Gomega) {
		_, err := GetGateway(test, namespace.Name, perServiceGatewayName)
		gg.Expect(errors.IsNotFound(err)).To(BeTrue(), "KubeRay must not create a per-RayService Gateway when gatewayRef is set; got err=%v", err)
	}, TestTimeoutShort).Should(Succeed())

	// The HTTPRoute's ParentRef must target the pre-existing shared Gateway.
	httpRouteName := fmt.Sprintf("%s-httproute", rayServiceName)
	var httpRoute *gwv1.HTTPRoute
	g.Eventually(func(gg Gomega) {
		var err error
		httpRoute, err = GetHTTPRoute(test, namespace.Name, httpRouteName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(httpRoute.Spec.ParentRefs).To(HaveLen(1))
		gg.Expect(string(httpRoute.Spec.ParentRefs[0].Name)).To(Equal(sharedGatewayName))
		gg.Expect(utils.IsHTTPRouteReady(sharedGateway, httpRoute)).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())

	// Behavior 2: Traffic resolves through the shared Gateway to the active cluster.
	curlPod, err := CreateCurlPod(g, test, CurlPodName, CurlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Verifying traffic resolves through the shared Gateway")
	g.Eventually(func(gg Gomega) {
		stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
		gg.Expect(stdout.String()).To(Equal("6"))
	}, TestTimeoutMedium).Should(Succeed())

	// Trigger an incremental upgrade; traffic must keep resolving (old or new) via
	// the shared Gateway while it shifts to the pending cluster.
	LogWithTimestamp(test.T(), "Triggering incremental upgrade; both active and pending clusters route through the shared Gateway")
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2,
		withGatewayRef(sharedGatewayName, namespace.Name), withWorkerCPURequest("500m"), withUpgradedServeConfig())).To(Succeed())
	g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

	// HTTPRoute gains the pending backend, still on the same shared Gateway.
	g.Eventually(func(gg Gomega) {
		route, err := GetHTTPRoute(test, namespace.Name, httpRouteName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(route.Spec.ParentRefs).To(HaveLen(1))
		gg.Expect(string(route.Spec.ParentRefs[0].Name)).To(Equal(sharedGatewayName))
		gg.Expect(route.Spec.Rules).To(HaveLen(1))
		gg.Expect(route.Spec.Rules[0].BackendRefs).To(HaveLen(2))
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying traffic keeps resolving (old or new) through the shared Gateway during the shift")
	g.Eventually(func(gg Gomega) {
		stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
		gg.Expect(stdout.String()).To(Or(Equal("6"), Equal("8")))
	}, TestTimeoutMedium).Should(Succeed())

	// Behavior 3: Suspend must clean up HTTPRoute + Services but leave the shared Gateway.
	LogWithTimestamp(test.T(), "Suspending the RayService")
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, serveConfigV2,
		withGatewayRef(sharedGatewayName, namespace.Name), withWorkerCPURequest("500m"), withUpgradedServeConfig(), withSuspend(true))).To(Succeed())
	g.Eventually(RayService(test, namespace.Name, rayServiceName), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceSuspended, BeTrue()))

	LogWithTimestamp(test.T(), "HTTPRoute and Services must be deleted; the shared Gateway must remain")
	g.Eventually(func(gg Gomega) {
		_, err := GetHTTPRoute(test, namespace.Name, httpRouteName)
		gg.Expect(errors.IsNotFound(err)).To(BeTrue(), "HTTPRoute %s should be deleted on suspend; got err=%v", httpRouteName, err)

		svcList, err := test.Client().Core().CoreV1().Services(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(svcList.Items).To(BeEmpty(), "All Services owned by the RayService should be deleted on suspend")
	}, TestTimeoutMedium).Should(Succeed())

	// The shared Gateway is not owned by KubeRay and must survive the suspend.
	g.Consistently(func(gg Gomega) {
		_, err := GetGateway(test, namespace.Name, sharedGatewayName)
		gg.Expect(err).NotTo(HaveOccurred(), "shared Gateway %s must NOT be deleted on suspend", sharedGatewayName)
	}, TestTimeoutShort).Should(Succeed())
}
