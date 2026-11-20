package ray

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/lru"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func setupDeployFailedRetryTest(t *testing.T, backoff *flowcontrol.Backoff) (*RayServiceReconciler, *utils.FakeRayDashboardClient, *rayv1.RayService, *rayv1.RayCluster) {
	features.SetFeatureGateDuringTest(t, features.RayClusterStatusConditions, true)
	ns := "test-ns"
	pending := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-cluster", Namespace: ns, UID: types.UID("pending-cluster-uid")},
		Status: rayv1.RayClusterStatus{Conditions: []metav1.Condition{
			{Type: string(rayv1.HeadPodReady), Status: metav1.ConditionTrue},
		}},
	}
	svcName, _ := utils.GenerateHeadServiceName(utils.RayClusterCRD, rayv1.RayClusterSpec{}, "pending-cluster")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: ns},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: utils.DashboardPortName, Port: 8265}}},
	}
	s := runtime.NewScheme()
	_ = rayv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)

	dash := &utils.FakeRayDashboardClient{}
	setStatus(dash, rayv1.ApplicationStatusEnum.DEPLOY_FAILED)
	r := &RayServiceReconciler{
		Client:   clientFake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(pending, svc).Build(),
		Recorder: &events.FakeRecorder{},
		dashboardClientFunc: func(_ *rayv1.RayCluster, _ string) (dashboardclient.RayDashboardClientInterface, error) {
			return dash, nil
		},
		ServeConfigs:                  lru.New(10),
		ServeDeployFailedRetryBackoff: backoff,
	}
	rs := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: ns},
		Spec:       rayv1.RayServiceSpec{ServeConfigV2: `{"applications": [{"name": "app", "import_path": "m:app"}]}`},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus:  rayv1.RayServiceStatus{RayClusterName: "active-cluster"},
			PendingServiceStatus: rayv1.RayServiceStatus{RayClusterName: "pending-cluster"},
		},
	}
	// The config was already submitted once.
	r.cacheServeConfig(rs, "pending-cluster")
	return r, dash, rs, pending
}

func setStatus(dash *utils.FakeRayDashboardClient, status string) {
	dash.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{
		"app": {Status: status, Message: "ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory."},
	})
}

// Without the backoff, an unchanged config is never resubmitted after DEPLOY_FAILED.
func TestReconcileServe_DeployFailedWithoutRetryBackoffIsNeverResubmitted(t *testing.T) {
	ctx := context.TODO()
	r, dash, rs, pending := setupDeployFailedRetryTest(t, nil)
	for range 50 {
		isReady, apps, err := r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
		assert.False(t, isReady)
		assert.Equal(t, rayv1.ApplicationStatusEnum.DEPLOY_FAILED, apps["app"].Status)
	}
	assert.Nil(t, dash.LastUpdatedConfig)
	calculateConditions(ctx, r, rs)
	assert.True(t, meta.IsStatusConditionTrue(rs.Status.Conditions, string(rayv1.UpgradeInProgress)))
}

func TestReconcileServe_DeployFailedRetry_ResubmitsWithBackoffAndResets(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)

	submits := 0
	reconcile := func() {
		dash.LastUpdatedConfig = nil
		_, _, err := r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
		if dash.LastUpdatedConfig != nil {
			submits++
		}
	}

	// First observation of DEPLOY_FAILED: resubmit immediately.
	reconcile()
	assert.Equal(t, 1, submits)
	// Fails again quickly: nothing is resubmitted within the 30s backoff.
	for range 10 {
		fakeClock.Step(2 * time.Second)
		reconcile()
	}
	assert.Equal(t, 1, submits)
	fakeClock.Step(11 * time.Second) // 31s since last submit
	reconcile()
	assert.Equal(t, 2, submits)
	// Backoff doubled to 60s.
	fakeClock.Step(59 * time.Second)
	reconcile()
	assert.Equal(t, 2, submits)
	fakeClock.Step(2 * time.Second)
	reconcile()
	assert.Equal(t, 3, submits)

	// DEPLOYING (build in progress) never triggers a resubmit.
	setStatus(dash, rayv1.ApplicationStatusEnum.DEPLOYING)
	fakeClock.Step(time.Hour)
	reconcile()
	assert.Equal(t, 3, submits)

	// Build succeeds -> backoff is reset; a later failure retries immediately again.
	setStatus(dash, rayv1.ApplicationStatusEnum.RUNNING)
	isReady, _, err := r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	assert.True(t, isReady)
	assert.Equal(t, time.Duration(0), backoff.Get(serveDeployFailedRetryKey(rs, pending)))
	setStatus(dash, rayv1.ApplicationStatusEnum.DEPLOY_FAILED)
	reconcile()
	assert.Equal(t, 4, submits)
	assert.Contains(t, string(dash.LastUpdatedConfig), "m:app")
}

type failingUpdateDashboardClient struct {
	*utils.FakeRayDashboardClient
	updateCalls int
}

func (c *failingUpdateDashboardClient) UpdateDeployments(_ context.Context, _ []byte) error {
	c.updateCalls++
	return errors.New("dashboard unavailable")
}

// A failed resubmission still advances the backoff.
func TestReconcileServe_DeployFailedRetry_FailedPutAdvancesBackoff(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)
	failing := &failingUpdateDashboardClient{FakeRayDashboardClient: dash}
	r.dashboardClientFunc = func(_ *rayv1.RayCluster, _ string) (dashboardclient.RayDashboardClientInterface, error) {
		return failing, nil
	}
	key := serveDeployFailedRetryKey(rs, pending)

	_, _, err := r.reconcileServe(ctx, rs, pending)
	require.Error(t, err)
	assert.Equal(t, 1, failing.updateCalls)
	assert.Equal(t, 30*time.Second, backoff.Get(key))

	for range 10 {
		fakeClock.Step(2 * time.Second)
		_, _, err = r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, failing.updateCalls)

	fakeClock.Step(11 * time.Second)
	_, _, err = r.reconcileServe(ctx, rs, pending)
	require.Error(t, err)
	assert.Equal(t, 2, failing.updateCalls)
	assert.Equal(t, 60*time.Second, backoff.Get(key))
}

// A recreated RayCluster (new UID) starts with a fresh backoff.
func TestReconcileServe_DeployFailedRetry_KeyedByClusterUID(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)
	pending.UID = types.UID("uid-1")

	_, _, err := r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	require.NotNil(t, dash.LastUpdatedConfig)
	assert.Equal(t, 30*time.Second, backoff.Get("test-ns/rs/uid-1"))

	recreated := pending.DeepCopy()
	recreated.UID = types.UID("uid-2")
	dash.LastUpdatedConfig = nil
	fakeClock.Step(2 * time.Second)
	_, _, err = r.reconcileServe(ctx, rs, recreated)
	require.NoError(t, err)
	assert.NotNil(t, dash.LastUpdatedConfig, "recreated cluster must retry immediately")
	assert.Equal(t, "test-ns/rs/pending-cluster", serveDeployFailedRetryKey(rs, &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "pending-cluster"}}))
}

// A new user config resets the backoff.
func TestReconcileServe_DeployFailedRetry_NewConfigResetsBackoff(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)
	key := serveDeployFailedRetryKey(rs, pending)

	for range 3 { // build up the backoff to 120s
		_, _, err := r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
		fakeClock.Step(backoff.Get(key) + time.Second)
	}
	require.Equal(t, 120*time.Second, backoff.Get(key))

	rs.Spec.ServeConfigV2 = `{"applications": [{"name": "app", "import_path": "m:app_v2"}]}`
	dash.LastUpdatedConfig = nil
	_, _, err := r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	assert.Contains(t, string(dash.LastUpdatedConfig), "m:app_v2")
	assert.Equal(t, time.Duration(0), backoff.Get(key))

	// The new config's failure is retried right away.
	dash.LastUpdatedConfig = nil
	fakeClock.Step(2 * time.Second)
	_, _, err = r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	assert.NotNil(t, dash.LastUpdatedConfig)
}

// The retry respects skipConfigUpdate during a rollback.
func TestReconcileServe_DeployFailedRetry_RespectsSkipConfigUpdate(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)
	ctx := context.TODO()
	r, dash, rs, pending := setupDeployFailedRetryTest(t, flowcontrol.NewBackOff(30*time.Second, 10*time.Minute))
	rs.Spec.UpgradeStrategy = &rayv1.RayServiceUpgradeStrategy{Type: new(rayv1.RayServiceNewClusterWithIncrementalUpgrade)}
	setCondition(rs, rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "rolling back")
	setCondition(rs, rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "upgrading")
	_, _, err := r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	assert.Nil(t, dash.LastUpdatedConfig)
}

// The retry waits while another application is DEPLOYING.
func TestReconcileServe_DeployFailedRetry_WaitsForOtherAppsToFinishDeploying(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)
	setApps := func(c string) {
		dash.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{
			"a": {Status: rayv1.ApplicationStatusEnum.RUNNING},
			"b": {Status: rayv1.ApplicationStatusEnum.DEPLOY_FAILED},
			"c": {Status: c},
		})
	}

	setApps(rayv1.ApplicationStatusEnum.DEPLOYING)
	for range 30 {
		fakeClock.Step(2 * time.Second)
		_, _, err := r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
	}
	assert.Nil(t, dash.LastUpdatedConfig, "must not interrupt c's in-progress build")
	assert.Equal(t, time.Duration(0), backoff.Get(serveDeployFailedRetryKey(rs, pending)), "backoff is not consumed while waiting")

	setApps(rayv1.ApplicationStatusEnum.RUNNING)
	_, _, err := r.reconcileServe(ctx, rs, pending)
	require.NoError(t, err)
	assert.NotNil(t, dash.LastUpdatedConfig, "retry once the other applications have settled")
}

func TestShouldRetryDeployFailedServeApplications(t *testing.T) {
	s := rayv1.ApplicationStatusEnum
	apps := func(statuses ...string) map[string]rayv1.AppStatus {
		m := map[string]rayv1.AppStatus{}
		for i, st := range statuses {
			m[string(rune('a'+i))] = rayv1.AppStatus{Status: st}
		}
		return m
	}
	assert.False(t, shouldRetryDeployFailedServeApplications(nil))
	assert.False(t, shouldRetryDeployFailedServeApplications(apps(s.RUNNING, s.UNHEALTHY)))
	assert.True(t, shouldRetryDeployFailedServeApplications(apps(s.DEPLOY_FAILED)))
	assert.True(t, shouldRetryDeployFailedServeApplications(apps(s.RUNNING, s.DEPLOY_FAILED, s.UNHEALTHY)))
	assert.False(t, shouldRetryDeployFailedServeApplications(apps(s.RUNNING, s.DEPLOY_FAILED, s.DEPLOYING)))
}

// The delay is measured from the last attempt and starts over after 2x the max backoff.
func TestReconcileServe_DeployFailedRetry_BackoffMeasuredFromLastAttempt(t *testing.T) {
	ctx := context.TODO()
	fakeClock := clocktesting.NewFakeClock(time.Now())
	backoff := flowcontrol.NewFakeBackOff(30*time.Second, 10*time.Minute, clock.WithTicker(fakeClock))
	r, dash, rs, pending := setupDeployFailedRetryTest(t, backoff)
	key := serveDeployFailedRetryKey(rs, pending)
	submitted := func() bool {
		dash.LastUpdatedConfig = nil
		_, _, err := r.reconcileServe(ctx, rs, pending)
		require.NoError(t, err)
		return dash.LastUpdatedConfig != nil
	}
	// The rebuild started by a retry runs for `d`, then fails again.
	rebuildFor := func(d time.Duration) {
		setStatus(dash, rayv1.ApplicationStatusEnum.DEPLOYING)
		fakeClock.Step(d)
		assert.False(t, submitted())
		setStatus(dash, rayv1.ApplicationStatusEnum.DEPLOY_FAILED)
	}

	require.True(t, submitted()) // t=0
	require.Equal(t, 30*time.Second, backoff.Get(key))

	// Fails after 25s: retry 30s after the attempt, not after the failure.
	rebuildFor(25 * time.Second)
	assert.False(t, submitted())
	fakeClock.Step(4 * time.Second) // t=29s
	assert.False(t, submitted())
	fakeClock.Step(2 * time.Second) // t=31s
	assert.True(t, submitted())
	assert.Equal(t, 60*time.Second, backoff.Get(key))

	// Rebuild longer than the delay: retry as soon as it fails.
	rebuildFor(5 * time.Minute)
	assert.True(t, submitted())
	assert.Equal(t, 120*time.Second, backoff.Get(key))

	// After 2x the max backoff, the delay starts over.
	rebuildFor(25 * time.Minute)
	assert.True(t, submitted())
	assert.Equal(t, 30*time.Second, backoff.Get(key))
}

// Only the pending RayCluster is retried.
func TestReconcileServe_DeployFailedRetry_OnlyPendingCluster(t *testing.T) {
	ctx := context.TODO()
	r, dash, rs, cluster := setupDeployFailedRetryTest(t, flowcontrol.NewBackOff(30*time.Second, 10*time.Minute))
	// Make the same RayCluster the active one, with no upgrade in progress.
	rs.Status.ActiveServiceStatus.RayClusterName = cluster.Name
	rs.Status.PendingServiceStatus.RayClusterName = ""
	r.cacheServeConfig(rs, cluster.Name)
	for range 10 {
		_, _, err := r.reconcileServe(ctx, rs, cluster)
		require.NoError(t, err)
	}
	assert.Nil(t, dash.LastUpdatedConfig)

	// A new RayService's first RayCluster is pending, so it is retried.
	rs.Status.ActiveServiceStatus.RayClusterName = ""
	rs.Status.PendingServiceStatus.RayClusterName = cluster.Name
	_, _, err := r.reconcileServe(ctx, rs, cluster)
	require.NoError(t, err)
	assert.NotNil(t, dash.LastUpdatedConfig)
}
