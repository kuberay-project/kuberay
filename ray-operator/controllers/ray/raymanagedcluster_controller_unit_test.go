package ray

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

const (
	managedNamespace = "ns"
	managedName      = "c"
	nodeA            = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	nodeB            = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// managedFixture is a RayManagedCluster in a fake API server whose writes a test can fail.
type managedFixture struct {
	t          *testing.T
	client     client.Client
	reconciler *RayManagedClusterReconciler
	// Fail these: a create or delete of a worker pod with the name, unless nil.
	failCreate func(name string) error
	failDelete func(name string) error
	// Create the worker pod, then fail with this: a create whose answer was lost.
	lostCreate func(name string) error
}

func managedGroup(name string, hosts, initial int32) rayv1.RayManagedWorkerGroupSpec {
	return rayv1.RayManagedWorkerGroupSpec{
		GroupName:       name,
		InitialReplicas: new(initial),
		MaxReplicas:     ptr.To[int32](10),
		NumOfHosts:      hosts,
		RayStartParams:  map[string]string{},
		Template:        corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker", Image: "ray"}}}},
	}
}

func newManagedCluster(groups ...rayv1.RayManagedWorkerGroupSpec) *rayv1.RayManagedCluster {
	return &rayv1.RayManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: managedName, Namespace: managedNamespace, UID: "the-cluster"},
		Spec: rayv1.RayManagedClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-head", Image: "ray"}}}},
			},
			WorkerGroupSpecs: groups,
		},
	}
}

// started gives the cluster a status: KubeRay's answer, and Ray's requests.
func started(cluster *rayv1.RayManagedCluster, created map[string]int64, requests ...rayv1.RayManagedGroupRequests) *rayv1.RayManagedCluster {
	cluster.Status.Provisioning = &rayv1.RayManagedClusterProvisioning{}
	for _, group := range cluster.Spec.WorkerGroupSpecs {
		answer := rayv1.RayManagedGroupProvisioning{Name: group.GroupName, CreatedReplicas: created[group.GroupName]}
		cluster.Status.Provisioning.Groups = append(cluster.Status.Provisioning.Groups, answer)
	}
	cluster.Status.Requests = &rayv1.RayManagedClusterRequests{Groups: requests}
	return cluster
}

func newManagedFixture(t *testing.T, cluster *rayv1.RayManagedCluster, objects ...client.Object) *managedFixture {
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, rayv1.AddToScheme(s))
	f := &managedFixture{t: t}
	// Only worker pods' writes fail: the head's are made as usual.
	worker := func(obj client.Object) bool {
		pod, ok := obj.(*corev1.Pod)
		return ok && pod.Labels["ray.io/node-type"] == "worker"
	}
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if f.failCreate != nil && worker(obj) {
				if err := f.failCreate(obj.GetName()); err != nil {
					return err
				}
			}
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if f.lostCreate != nil && worker(obj) {
				return f.lostCreate(obj.GetName())
			}
			return nil
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if f.failDelete != nil && worker(obj) {
				if err := f.failDelete(obj.GetName()); err != nil {
					return err
				}
			}
			return c.Delete(ctx, obj, opts...)
		},
	}
	f.client = fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(append([]client.Object{cluster}, objects...)...).
		WithStatusSubresource(&rayv1.RayManagedCluster{}).
		WithInterceptorFuncs(funcs).
		Build()
	f.reconciler = &RayManagedClusterReconciler{Client: f.client, APIReader: f.client, Scheme: s}
	return f
}

func (f *managedFixture) reconcile() (ctrl.Result, error) {
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: managedNamespace, Name: managedName}}
	return f.reconciler.Reconcile(context.Background(), request)
}

func (f *managedFixture) cluster() *rayv1.RayManagedCluster {
	var cluster rayv1.RayManagedCluster
	require.NoError(f.t, f.client.Get(context.Background(), types.NamespacedName{Namespace: managedNamespace, Name: managedName}, &cluster))
	return &cluster
}

func (f *managedFixture) answer(group string) rayv1.RayManagedGroupProvisioning {
	for _, answer := range f.cluster().Status.Provisioning.Groups {
		if answer.Name == group {
			return answer
		}
	}
	f.t.Fatalf("no answer for group %q", group)
	return rayv1.RayManagedGroupProvisioning{}
}

func (f *managedFixture) pod(name string) *corev1.Pod {
	var pod corev1.Pod
	err := f.client.Get(context.Background(), types.NamespacedName{Namespace: managedNamespace, Name: name}, &pod)
	if apierrors.IsNotFound(err) {
		return nil
	}
	require.NoError(f.t, err)
	return &pod
}

func (f *managedFixture) workerPods() []string {
	var pods corev1.PodList
	require.NoError(f.t, f.client.List(context.Background(), &pods, client.InNamespace(managedNamespace)))
	var names []string
	for _, pod := range pods.Items {
		if pod.Labels["ray.io/node-type"] == "worker" {
			names = append(names, pod.Name)
		}
	}
	return names
}

// ownedPod is a pod of the cluster's, as a create of replica `replica`'s host 0 makes it.
func ownedPod(t *testing.T, owner *rayv1.RayManagedCluster, group string, replica int, nodeID string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workerPodName(managedName, group, int64(replica), 0),
			Namespace: managedNamespace,
			Labels: map[string]string{
				"ray.io/cluster": managedName, "ray.io/group": group, "ray.io/node-type": "worker",
				"ray.io/replica": strconv.Itoa(replica), "ray.io/host": "0", "ray.io/node-id": nodeID,
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker", Image: "ray"}}},
	}
	s := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(s))
	require.NoError(t, controllerutil.SetControllerReference(owner, pod, s))
	return pod
}

func refusedByQuota(name string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, name, errors.New("exceeded quota: pods"))
}

// A webhook that's down, as the API server answers its callers.
var errWebhookDown = errors.New("failed calling webhook: connection refused")

func TestRayManagedClusterAsksForTheInitialReplicasOnce(t *testing.T) {
	f := newManagedFixture(t, newManagedCluster(managedGroup("tpu", 2, 2)))
	_, err := f.reconcile()
	require.NoError(t, err)
	requests := f.cluster().Status.Requests.Groups
	require.Len(t, requests, 1)
	require.Len(t, requests[0].NewReplicas, 2)
	for i, replica := range requests[0].NewReplicas {
		assert.Equal(t, int64(i+1), replica.Replica, "numbered from 1")
		require.Len(t, replica.Nodes, 2, "an instance per host")
		assert.Len(t, replica.Nodes[0], 2*rayNodeIDBytes)
	}
	assert.Equal(t, int64(0), f.answer("tpu").CreatedReplicas)

	_, err = f.reconcile()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c-tpu-1-0", "c-tpu-1-1", "c-tpu-2-0", "c-tpu-2-1"}, f.workerPods())
	head := f.pod("c-head")
	require.NotNil(t, head, "the head's made along with them")
	assert.Equal(t, corev1.RestartPolicyNever, head.Spec.RestartPolicy, "a head started again would be a new cluster")
	assert.Equal(t, int64(2), f.answer("tpu").CreatedReplicas)
	initContainers := f.pod("c-tpu-1-0").Spec.InitContainers
	require.Len(t, initContainers, 1, "an initial replica waits for the head")
	assert.Equal(t, waitGCSReadyContainerName, initContainers[0].Name)
	assert.Equal(t, requests[0].NewReplicas, f.cluster().Status.Requests.Groups[0].NewReplicas, "asked for once")
}

func TestRayManagedClusterMakesAReplicaRayAsksFor(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 2, Nodes: []string{nodeA}}}}
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 1)), map[string]int64{"cpu": 1}, asked)
	f := newManagedFixture(t, cluster)
	_, err := f.reconcile()
	require.NoError(t, err)
	pod := f.pod("c-cpu-2-0")
	require.NotNil(t, pod)
	assert.Equal(t, "2", pod.Labels["ray.io/replica"])
	assert.Equal(t, "0", pod.Labels["ray.io/host"])
	assert.Equal(t, nodeA, pod.Labels["ray.io/node-id"])
	assert.Contains(t, pod.Spec.Containers[0].Env, corev1.EnvVar{Name: requestedNodeIDEnvKey, Value: nodeA})
	assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)
	assert.Empty(t, pod.Spec.InitContainers, "Ray asked from its head, which is up")
	assert.True(t, metav1.IsControlledBy(pod, f.cluster()))
	assert.Equal(t, int64(2), f.answer("cpu").CreatedReplicas)
	assert.Empty(t, f.answer("cpu").FailedReplicas)
}

func TestRayManagedClusterRetriesACreateThatMayPass(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{
		{Replica: 1, Nodes: []string{nodeA}},
		{Replica: 2, Nodes: []string{nodeB}},
	}}
	f := newManagedFixture(t, started(newManagedCluster(managedGroup("cpu", 1, 0)), nil, asked))
	for _, failure := range []error{
		apierrors.NewInternalError(errWebhookDown), // A webhook that's down.
		apierrors.NewTooManyRequests("throttled", 1),
		apierrors.NewServerTimeout(schema.GroupResource{Resource: "pods"}, "create", 1),
	} {
		f.failCreate = func(string) error { return failure }
		_, err := f.reconcile()
		require.Error(t, err)
		assert.Equal(t, int64(0), f.answer("cpu").CreatedReplicas, "nothing recorded for %v", failure)
		assert.Empty(t, f.answer("cpu").FailedReplicas)
	}
	f.failCreate = nil
	_, err := f.reconcile()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c-cpu-1-0", "c-cpu-2-0"}, f.workerPods())
	assert.Equal(t, int64(2), f.answer("cpu").CreatedReplicas)
	assert.Empty(t, f.answer("cpu").FailedReplicas)
}

func TestRayManagedClusterFindsAPodWhoseCreateTimedOut(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA}}}}
	f := newManagedFixture(t, started(newManagedCluster(managedGroup("cpu", 1, 0)), nil, asked))
	f.lostCreate = func(string) error { return apierrors.NewTimeoutError("the answer was lost", 1) }
	_, err := f.reconcile()
	require.Error(t, err)
	assert.Equal(t, int64(0), f.answer("cpu").CreatedReplicas)
	f.lostCreate = nil
	_, err = f.reconcile()
	require.NoError(t, err)
	assert.Equal(t, []string{"c-cpu-1-0"}, f.workerPods(), "the pod made is the replica's")
	assert.Equal(t, int64(1), f.answer("cpu").CreatedReplicas)
	assert.Empty(t, f.answer("cpu").FailedReplicas)
}

func TestRayManagedClusterRollsBackARefusedReplica(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "tpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA, nodeB}}}}
	f := newManagedFixture(t, started(newManagedCluster(managedGroup("tpu", 2, 0)), nil, asked))
	f.failCreate = func(name string) error {
		if name == "c-tpu-1-1" {
			return refusedByQuota(name)
		}
		return nil
	}
	_, err := f.reconcile()
	require.NoError(t, err)
	assert.Empty(t, f.workerPods(), "host 0's pod is deleted with the replica")
	answer := f.answer("tpu")
	assert.Equal(t, int64(1), answer.CreatedReplicas)
	require.Len(t, answer.FailedReplicas, 1)
	assert.Contains(t, answer.FailedReplicas[0].Message, "exceeded quota")
}

func TestRayManagedClusterFailsAReplicaOnlyOnceItsPodsAreGone(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "tpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA, nodeB}}}}
	f := newManagedFixture(t, started(newManagedCluster(managedGroup("tpu", 2, 0)), nil, asked))
	f.failCreate = func(name string) error {
		if name == "c-tpu-1-1" {
			return refusedByQuota(name)
		}
		return nil
	}
	f.failDelete = func(string) error { return apierrors.NewInternalError(errWebhookDown) }
	_, err := f.reconcile()
	require.Error(t, err)
	assert.Equal(t, []string{"c-tpu-1-0"}, f.workerPods())
	assert.Equal(t, int64(0), f.answer("tpu").CreatedReplicas, "not failed while host 0's pod runs")
	assert.Empty(t, f.answer("tpu").FailedReplicas)

	f.failDelete = nil
	_, err = f.reconcile()
	require.NoError(t, err)
	assert.Empty(t, f.workerPods())
	assert.Equal(t, int64(1), f.answer("tpu").CreatedReplicas)
	assert.Len(t, f.answer("tpu").FailedReplicas, 1)
}

func TestRayManagedClusterWaitsForItsPodThatsGoing(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA}}}}
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 0)), nil, asked)
	going := ownedPod(t, cluster, "cpu", 1, nodeA)
	going.Finalizers = []string{"e2e/holds"}
	f := newManagedFixture(t, cluster, going)
	require.NoError(t, f.client.Delete(context.Background(), going))

	result, err := f.reconcile()
	require.NoError(t, err)
	assert.Equal(t, nameTakenRetry, result.RequeueAfter)
	assert.Equal(t, int64(0), f.answer("cpu").CreatedReplicas, "a pod being deleted isn't made")

	pod := f.pod("c-cpu-1-0")
	pod.Finalizers = nil
	require.NoError(t, f.client.Update(context.Background(), pod))
	_, err = f.reconcile()
	require.NoError(t, err)
	made := f.pod("c-cpu-1-0")
	require.NotNil(t, made)
	assert.Nil(t, made.DeletionTimestamp, "made again once it's gone")
	assert.Equal(t, int64(1), f.answer("cpu").CreatedReplicas)
}

func TestRayManagedClusterWaitsForAnotherClustersPod(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA}}}}
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 0)), nil, asked)
	earlier := newManagedCluster()
	earlier.UID = "an-earlier-cluster"
	f := newManagedFixture(t, cluster, ownedPod(t, earlier, "cpu", 1, nodeB))
	result, err := f.reconcile()
	require.NoError(t, err)
	assert.Equal(t, nameTakenRetry, result.RequeueAfter)
	assert.Equal(t, int64(0), f.answer("cpu").CreatedReplicas)
	assert.Empty(t, f.answer("cpu").FailedReplicas)
}

func TestRayManagedClusterAFailingDeleteHoldsUpOnlyItself(t *testing.T) {
	cluster := started(newManagedCluster(managedGroup("a", 1, 0), managedGroup("b", 1, 0)), map[string]int64{"a": 1},
		rayv1.RayManagedGroupRequests{Name: "a", DeletePods: []rayv1.RayManagedDeletePods{{Replica: 1, Hosts: []int64{0}}}},
		rayv1.RayManagedGroupRequests{Name: "b", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeB}}}},
	)
	f := newManagedFixture(t, cluster, ownedPod(t, cluster, "a", 1, nodeA))
	f.failDelete = func(string) error { return apierrors.NewInternalError(errWebhookDown) }
	_, err := f.reconcile()
	require.Error(t, err)
	assert.NotNil(t, f.pod("c-a-1-0"), "its delete failed")
	assert.NotNil(t, f.pod("c-b-1-0"), "the other group's replica is made")
	assert.Equal(t, int64(1), f.answer("b").CreatedReplicas, "and recorded")

	f.failDelete = nil
	_, err = f.reconcile()
	require.NoError(t, err)
	assert.Nil(t, f.pod("c-a-1-0"))
}

func TestRayManagedClusterRestoredStatusAsksForNothingAgain(t *testing.T) {
	cluster := newManagedCluster(managedGroup("cpu", 1, 1), managedGroup("tpu", 2, 1))
	f := newManagedFixture(t, cluster, ownedPod(t, cluster, "cpu", 4, nodeA), ownedPod(t, cluster, "cpu", 2, nodeB))
	_, err := f.reconcile()
	require.NoError(t, err)
	assert.Empty(t, f.cluster().Status.Requests.Groups, "its pods run: no initial replicas again")
	assert.Equal(t, int64(4), f.answer("cpu").CreatedReplicas, "from its pods' highest replica")
	assert.Equal(t, int64(0), f.answer("tpu").CreatedReplicas)
	assert.ElementsMatch(t, []string{"c-cpu-4-0", "c-cpu-2-0"}, f.workerPods())
}

func (f *managedFixture) ended() *metav1.Condition {
	return meta.FindStatusCondition(f.cluster().Status.Conditions, rayv1.RayManagedClusterEnded)
}

// ask has Ray ask for more replicas, as KubeRayProvider patches status.requests.
func (f *managedFixture) ask(requests ...rayv1.RayManagedGroupRequests) {
	cluster := f.cluster()
	cluster.Status.Requests = &rayv1.RayManagedClusterRequests{Groups: requests}
	require.NoError(f.t, f.client.Status().Update(context.Background(), cluster))
}

func TestRayManagedClusterEndsWhenItsHeadFails(t *testing.T) {
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA}}}}
	f := newManagedFixture(t, started(newManagedCluster(managedGroup("cpu", 1, 0)), nil, asked))
	_, err := f.reconcile()
	require.NoError(t, err)
	require.Equal(t, []string{"c-cpu-1-0"}, f.workerPods())
	head := f.pod("c-head")
	head.Status.Phase = corev1.PodFailed
	head.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  head.Spec.Containers[0].Name,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error", Message: "Ray failed to start\n"}},
	}}
	require.NoError(t, f.client.Status().Update(context.Background(), head))

	_, err = f.reconcile()
	require.NoError(t, err)
	ended := f.ended()
	require.NotNil(t, ended)
	assert.Equal(t, metav1.ConditionTrue, ended.Status)
	assert.Equal(t, rayv1.RayManagedHeadPodFailed, ended.Reason)
	assert.Contains(t, ended.Message, "exited with code 1 (Error): Ray failed to start")
	assert.Empty(t, f.workerPods(), "its workers can't rejoin anything")
	assert.Equal(t, head.UID, f.pod("c-head").UID, "the head isn't made again")

	f.ask(rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 2, Nodes: []string{nodeB}}}})
	_, err = f.reconcile()
	require.NoError(t, err)
	assert.Empty(t, f.workerPods(), "nothing's made once it's ended")
}

func TestRayManagedClusterEndsWhenItsHeadIsDeleted(t *testing.T) {
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 0)), nil)
	f := newManagedFixture(t, cluster, ownedPod(t, cluster, "cpu", 1, nodeA))
	_, err := f.reconcile()
	require.NoError(t, err)
	require.NotNil(t, f.pod("c-head"))
	require.NoError(t, f.client.Delete(context.Background(), f.pod("c-head")))

	_, err = f.reconcile()
	require.NoError(t, err)
	ended := f.ended()
	require.NotNil(t, ended)
	assert.Equal(t, rayv1.RayManagedHeadPodDeleted, ended.Reason)
	assert.Nil(t, f.pod("c-head"), "a new head would be a new, empty cluster")
	assert.Empty(t, f.workerPods())
}

func TestRayManagedClusterTrustsTheAPIServerOverALaggingCache(t *testing.T) {
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 0)), nil)
	cluster.Status.Head = rayv1.HeadInfo{PodName: "c-head", ServiceName: "c-head-svc"}
	// The cache has the status naming the head, but not the head pod yet; the API server has it.
	f := newManagedFixture(t, cluster)
	head := ownedPod(t, cluster, "cpu", 0, "")
	head.Name, head.Labels = "c-head", map[string]string{"ray.io/cluster": managedName, "ray.io/node-type": "head"}
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	f.reconciler.APIReader = fake.NewClientBuilder().WithScheme(s).WithObjects(head).Build()

	_, err := f.reconcile()
	require.NoError(t, err)
	assert.Nil(t, f.ended(), "the head's there")
	assert.Nil(t, f.pod("c-head"), "and isn't made twice")
}

func TestRayManagedClusterWaitsForAnEarlierClustersHead(t *testing.T) {
	cluster := started(newManagedCluster(managedGroup("cpu", 1, 0)), nil)
	earlier := newManagedCluster()
	earlier.UID = "an-earlier-cluster"
	going := ownedPod(t, earlier, "cpu", 0, "")
	going.Name, going.Labels = "c-head", map[string]string{"ray.io/cluster": managedName, "ray.io/node-type": "head"}
	going.Finalizers = []string{"e2e/holds"}
	f := newManagedFixture(t, cluster, going)
	require.NoError(t, f.client.Delete(context.Background(), going))

	result, err := f.reconcile()
	require.NoError(t, err)
	assert.Equal(t, nameTakenRetry, result.RequeueAfter)
	assert.Nil(t, f.ended(), "another cluster's head going doesn't end this one")

	pod := f.pod("c-head")
	pod.Finalizers = nil
	require.NoError(t, f.client.Update(context.Background(), pod))
	_, err = f.reconcile()
	require.NoError(t, err)
	head := f.pod("c-head")
	require.NotNil(t, head)
	assert.True(t, metav1.IsControlledBy(head, f.cluster()), "its own head, once the other's gone")
	assert.Nil(t, f.ended())
}

func TestRayManagedClusterSaysHowItsHeadEnded(t *testing.T) {
	pod := func(status corev1.PodStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "c-head"},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-head"}}},
			Status:     status,
		}
	}
	exited := corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{
		Name: "ray-head", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"}},
	}}}
	assert.Equal(t, "the head pod c-head's Ray container exited with code 0 (Completed)", headExit(pod(exited)))
	evicted := corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: memory."}
	assert.Equal(t, "the head pod c-head ended (Evicted): The node was low on resource: memory.", headExit(pod(evicted)))
}

func envOf(container corev1.Container, name string) (string, bool) {
	for _, env := range container.Env {
		if env.Name == name {
			return env.Value, true
		}
	}
	return "", false
}

func (f *managedFixture) generatedSecret() *corev1.Secret {
	var secret corev1.Secret
	err := f.client.Get(context.Background(), types.NamespacedName{Namespace: managedNamespace, Name: managedName}, &secret)
	if apierrors.IsNotFound(err) {
		return nil
	}
	require.NoError(f.t, err)
	return &secret
}

// assertReadsTokenFrom checks a container reads the token from secret's file, and doesn't have it
// in its environment.
func assertReadsTokenFrom(t *testing.T, pod *corev1.Pod, container corev1.Container, secret string) {
	mode, _ := envOf(container, "RAY_AUTH_MODE")
	assert.Equal(t, "token", mode, container.Name)
	path, _ := envOf(container, "RAY_AUTH_TOKEN_PATH")
	assert.Equal(t, "/etc/ray/auth/auth_token", path, container.Name)
	_, inEnv := envOf(container, "RAY_AUTH_TOKEN")
	assert.False(t, inEnv, "%s: not in the environment every process inherits", container.Name)
	assert.Contains(t, container.VolumeMounts, corev1.VolumeMount{Name: "ray-auth-token", MountPath: "/etc/ray/auth", ReadOnly: true}, container.Name)
	var source *corev1.SecretVolumeSource
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "ray-auth-token" {
			source = volume.Secret
		}
	}
	require.NotNil(t, source, pod.Name)
	assert.Equal(t, secret, source.SecretName)
}

func TestRayManagedClusterGeneratesAToken(t *testing.T) {
	f := newManagedFixture(t, newManagedCluster(managedGroup("cpu", 1, 1)))
	for range 2 {
		_, err := f.reconcile()
		require.NoError(t, err)
	}
	secret := f.generatedSecret()
	require.NotNil(t, secret, "Ray 3 requires a token by default")
	assert.True(t, metav1.IsControlledBy(secret, f.cluster()))
	token := secret.StringData["auth_token"] + string(secret.Data["auth_token"])
	assert.NotEmpty(t, token)

	head, worker := f.pod("c-head"), f.pod("c-cpu-1-0")
	require.NotNil(t, head)
	require.NotNil(t, worker)
	assertReadsTokenFrom(t, head, head.Spec.Containers[0], "c")
	assertReadsTokenFrom(t, worker, worker.Spec.Containers[0], "c")
	require.Len(t, worker.Spec.InitContainers, 1)
	assertReadsTokenFrom(t, worker, worker.Spec.InitContainers[0], "c")

	_, err := f.reconcile()
	require.NoError(t, err)
	again := f.generatedSecret()
	assert.Equal(t, token, again.StringData["auth_token"]+string(again.Data["auth_token"]), "generated once")
}

func TestRayManagedClusterUsesTheUsersSecret(t *testing.T) {
	cluster := newManagedCluster(managedGroup("cpu", 1, 1))
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{Mode: rayv1.AuthModeToken, SecretName: new("mine")}
	f := newManagedFixture(t, cluster)
	for range 2 {
		_, err := f.reconcile()
		require.NoError(t, err)
	}
	assert.Nil(t, f.generatedSecret(), "none generated")
	worker := f.pod("c-cpu-1-0")
	require.NotNil(t, worker)
	assertReadsTokenFrom(t, worker, worker.Spec.Containers[0], "mine")
}

func TestRayManagedClusterWithAuthenticationOff(t *testing.T) {
	cluster := newManagedCluster(managedGroup("cpu", 1, 1))
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{Mode: rayv1.AuthModeDisabled}
	f := newManagedFixture(t, cluster)
	for range 2 {
		_, err := f.reconcile()
		require.NoError(t, err)
	}
	assert.Nil(t, f.generatedSecret())
	for _, name := range []string{"c-head", "c-cpu-1-0"} {
		pod := f.pod(name)
		require.NotNil(t, pod)
		mode, _ := envOf(pod.Spec.Containers[0], "RAY_AUTH_MODE")
		assert.Equal(t, "disabled", mode, "%s: said explicitly, as Ray 3 defaults to token", name)
		_, hasPath := envOf(pod.Spec.Containers[0], "RAY_AUTH_TOKEN_PATH")
		assert.False(t, hasPath, name)
		assert.False(t, hasVolume(pod, "ray-auth-token"), name)
	}
}

func TestRayManagedClusterLeavesATokenTheTemplateGives(t *testing.T) {
	group := managedGroup("cpu", 1, 0)
	group.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "RAY_AUTH_TOKEN_PATH", Value: "/mine/token"}}
	asked := rayv1.RayManagedGroupRequests{Name: "cpu", NewReplicas: []rayv1.RayManagedNewReplica{{Replica: 1, Nodes: []string{nodeA}}}}
	f := newManagedFixture(t, started(newManagedCluster(group), nil, asked))
	_, err := f.reconcile()
	require.NoError(t, err)
	worker := f.pod("c-cpu-1-0")
	require.NotNil(t, worker)
	path, _ := envOf(worker.Spec.Containers[0], "RAY_AUTH_TOKEN_PATH")
	assert.Equal(t, "/mine/token", path)
	assert.False(t, hasVolume(worker, "ray-auth-token"))
	assert.Empty(t, f.cluster().Spec.WorkerGroupSpecs[0].Template.Spec.Volumes, "the spec itself is left as it is")
}

func hasVolume(pod *corev1.Pod, name string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func TestRayManagedClusterRefusals(t *testing.T) {
	pods := schema.GroupResource{Resource: "pods"}
	assert.True(t, refused(refusedByQuota("p")))
	assert.True(t, refused(apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "p", nil)))
	assert.True(t, refused(apierrors.NewBadRequest("admission webhook denied the request")))
	assert.False(t, refused(apierrors.NewInternalError(errWebhookDown)))
	assert.False(t, refused(apierrors.NewTooManyRequests("throttled", 1)))
	assert.False(t, refused(apierrors.NewServerTimeout(pods, "create", 1)))
	assert.False(t, refused(apierrors.NewServiceUnavailable("restarting")))
	assert.False(t, refused(context.DeadlineExceeded))
}
