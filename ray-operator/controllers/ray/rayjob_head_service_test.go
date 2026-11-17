/*
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

package ray

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func headServiceTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	return scheme
}

func headServiceTestRayJob() *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "rayjob-head-service", Namespace: "default", UID: "rayjob-uid"},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
			RayClusterName:      "rayjob-head-service-raycluster",
		},
	}
}

// headServiceForRayJob sets the identity and ownership metadata written by
// reconcileServices; these cleanup tests do not need Service ports or selectors.
func headServiceForRayJob(t *testing.T, rayJob *rayv1.RayJob) *corev1.Service {
	t.Helper()
	key, err := common.RayJobHeadServiceNamespacedName(rayJob)
	require.NoError(t, err)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
			UID:       "head-service-uid",
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(rayJob, rayv1.GroupVersion.WithKind("RayJob")),
			},
			Labels: map[string]string{
				utils.RayOriginatedFromCRNameLabelKey: rayJob.Name,
				utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
			},
		},
	}
}

func TestDeleteHeadService(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes the RayJob-owned head Service", func(t *testing.T) {
		rayJob := headServiceTestRayJob()
		svc := headServiceForRayJob(t, rayJob)
		fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).Build()
		r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.deleteHeadService(ctx, rayJob))

		err := fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})
		assert.True(t, k8serrors.IsNotFound(err), "head Service should be deleted: %v", err)
	})

	t.Run("keeps a same-name Service that belongs to another RayJob", func(t *testing.T) {
		rayJob := headServiceTestRayJob()
		svc := headServiceForRayJob(t, rayJob)
		svc.Labels[utils.RayOriginatedFromCRNameLabelKey] = "another-rayjob"
		fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).Build()
		r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.deleteHeadService(ctx, rayJob))

		err := fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})
		require.NoError(t, err, "a Service owned by another RayJob must not be deleted")
	})

	t.Run("keeps a same-name Service that was not created by a RayJob", func(t *testing.T) {
		rayJob := headServiceTestRayJob()
		svc := headServiceForRayJob(t, rayJob)
		svc.Labels = map[string]string{"app": "user-managed"}
		fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).Build()
		r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.deleteHeadService(ctx, rayJob))

		err := fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})
		require.NoError(t, err, "a user-managed Service without origin labels must not be deleted")
	})

	t.Run("does nothing in clusterSelector mode", func(t *testing.T) {
		rayJob := headServiceTestRayJob()
		rayJob.Spec.ClusterSelector = map[string]string{"ray.io/cluster": "shared-cluster"}
		svc := headServiceForRayJob(t, rayJob)
		fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).Build()
		r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.deleteHeadService(ctx, rayJob))

		err := fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})
		require.NoError(t, err, "clusterSelector mode must not delete Services")
	})

	t.Run("is a no-op when the head Service is absent", func(t *testing.T) {
		rayJob := headServiceTestRayJob()
		fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).Build()
		r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, r.deleteHeadService(ctx, rayJob))
	})
}

func TestDeleteHeadServiceRequiresCurrentController(t *testing.T) {
	for _, ownerUID := range []types.UID{"", "previous-rayjob-uid"} {
		name := "no controller"
		if ownerUID != "" {
			name = "wrong controller UID"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			job := headServiceTestRayJob()
			svc := headServiceForRayJob(t, job)
			if ownerUID == "" {
				svc.OwnerReferences = nil
			} else {
				svc.OwnerReferences[0].UID = ownerUID
			}
			cli := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).Build()
			r := &RayJobReconciler{Client: cli, Recorder: events.NewFakeRecorder(10)}

			require.NoError(t, r.deleteHeadService(ctx, job))
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(svc), &corev1.Service{}),
				"matching labels must not authorize deletion of an unowned or wrong-UID Service")
			completed, err := r.isDeletionActionCompleted(ctx, job, rayv1.DeleteCluster)
			require.NoError(t, err)
			assert.True(t, completed, "an unrelated Service must not block cleanup completion")
		})
	}
}

func TestDeletionRulesHeadServiceCleanup(t *testing.T) {
	for _, state := range []string{"cluster absent", "cluster deleting", "transient Service deletion failure"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			job := headServiceTestRayJob()
			job.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Hour)}
			status := job.Status.JobDeploymentStatus
			job.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{{Policy: rayv1.DeleteCluster,
					Condition: rayv1.DeletionCondition{JobDeploymentStatus: &status}}},
			}
			svc := headServiceForRayJob(t, job)
			objects := []client.Object{svc}
			if state != "cluster absent" {
				cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
					Name: job.Status.RayClusterName, Namespace: job.Namespace,
				}}
				if state == "cluster deleting" {
					cluster.DeletionTimestamp = job.Status.EndTime.DeepCopy()
					cluster.Finalizers = []string{"test.kuberay.io/hold"}
				}
				objects = append(objects, cluster)
			}
			serviceDeletes := 0
			transientErr := errors.New("transient Service deletion failure")
			cli := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(objects...).
				WithInterceptorFuncs(interceptor.Funcs{
					Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
						if _, ok := obj.(*corev1.Service); ok {
							serviceDeletes++
							if serviceDeletes == 1 && state == "transient Service deletion failure" {
								return transientErr
							}
						}
						return c.Delete(ctx, obj, opts...)
					},
				}).Build()
			r := &RayJobReconciler{Client: cli, Recorder: events.NewFakeRecorder(10)}
			wantDeletes := 1
			if state == "transient Service deletion failure" {
				result, err := r.handleDeletionRules(ctx, job)
				require.ErrorIs(t, err, transientErr)
				assert.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter)
				require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(svc), &corev1.Service{}))
				wantDeletes = 2
			}
			_, err := r.handleDeletionRules(ctx, job)
			require.NoError(t, err)
			assert.Equal(t, wantDeletes, serviceDeletes)
			assert.True(t, k8serrors.IsNotFound(cli.Get(ctx, client.ObjectKeyFromObject(svc), &corev1.Service{})))
			result, err := r.handleDeletionRules(ctx, job)
			require.NoError(t, err)
			assert.Zero(t, result.RequeueAfter)
			assert.Equal(t, wantDeletes, serviceDeletes, "completed cleanup must be idempotent")
		})
	}
}

func TestDeleteHeadServiceProtectsReplacement(t *testing.T) {
	ctx := context.Background()
	job := headServiceTestRayJob()
	svc := headServiceForRayJob(t, job)
	cli := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				current := obj.(*corev1.Service)
				require.NoError(t, c.Delete(ctx, current))
				replacement := current.DeepCopy()
				replacement.ResourceVersion = ""
				replacement.UID = "replacement-service-uid"
				replacement.OwnerReferences = nil
				require.NoError(t, c.Create(ctx, replacement))
				// The fake client only enforces ResourceVersion preconditions;
				// emulate the API server's UID check for this replacement race.
				deleteOpts := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOpts.Preconditions)
				require.NotNil(t, deleteOpts.Preconditions.UID)
				require.Equal(t, current.UID, *deleteOpts.Preconditions.UID)
				if *deleteOpts.Preconditions.UID != replacement.UID {
					return k8serrors.NewConflict(corev1.Resource("services"), replacement.Name, errors.New("UID precondition failed"))
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).Build()
	r := &RayJobReconciler{Client: cli, Recorder: events.NewFakeRecorder(10)}

	err := r.deleteHeadService(ctx, job)
	require.True(t, k8serrors.IsConflict(err), "UID precondition must reject a replaced Service: %v", err)
	replacement := &corev1.Service{}
	require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(svc), replacement))
	assert.Equal(t, types.UID("replacement-service-uid"), replacement.UID)
	require.NoError(t, r.deleteHeadService(ctx, job), "retry must leave the unrelated replacement alone")
}

func TestHeadServiceCleanupPropagatesGetError(t *testing.T) {
	ctx := context.Background()
	job := headServiceTestRayJob()
	lookupErr := errors.New("Service lookup unavailable")
	cli := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return lookupErr
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()
	r := &RayJobReconciler{Client: cli, Recorder: events.NewFakeRecorder(10)}

	require.ErrorIs(t, r.deleteHeadService(ctx, job), lookupErr)
	completed, err := r.isDeletionActionCompleted(ctx, job, rayv1.DeleteCluster)
	require.ErrorIs(t, err, lookupErr)
	assert.False(t, completed, "a lookup failure cannot confirm completed cleanup")
}

func TestHeadServiceDeletionInProgress(t *testing.T) {
	ctx := context.Background()
	job := headServiceTestRayJob()
	svc := headServiceForRayJob(t, job)
	svc.DeletionTimestamp = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	svc.Finalizers = []string{"test.kuberay.io/hold"}
	cli := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(svc).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
				t.Fatal("an already deleting Service must not be deleted again")
				return nil
			},
		}).Build()
	r := &RayJobReconciler{Client: cli, Recorder: events.NewFakeRecorder(10)}

	require.NoError(t, r.deleteHeadService(ctx, job))
	completed, err := r.isDeletionActionCompleted(ctx, job, rayv1.DeleteCluster)
	require.NoError(t, err)
	assert.True(t, completed, "deletion requests for both resources have been accepted")
}

func TestExecuteDeletionPolicyDeleteClusterRemovesHeadService(t *testing.T) {
	ctx := context.Background()
	rayJob := headServiceTestRayJob()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: rayJob.Status.RayClusterName, Namespace: rayJob.Namespace},
	}
	svc := headServiceForRayJob(t, rayJob)
	fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(cluster, svc).Build()
	r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

	_, err := r.executeDeletionPolicy(ctx, rayJob, rayv1.DeleteCluster)
	require.NoError(t, err)

	assert.True(t, k8serrors.IsNotFound(fakeClient.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, &rayv1.RayCluster{})))
	assert.True(t, k8serrors.IsNotFound(fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})),
		"terminal DeleteCluster must also delete the RayJob-owned head Service")
}

func TestDeleteClusterResourcesKeepsHeadServiceForRetry(t *testing.T) {
	ctx := context.Background()
	rayJob := headServiceTestRayJob()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: rayJob.Status.RayClusterName, Namespace: rayJob.Namespace},
	}
	svc := headServiceForRayJob(t, rayJob)
	fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(cluster, svc).Build()
	r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

	_, err := r.deleteClusterResources(ctx, rayJob)
	require.NoError(t, err)

	assert.True(t, k8serrors.IsNotFound(fakeClient.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, &rayv1.RayCluster{})))
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{}),
		"the retry path must keep the stable head Service for the next RayCluster")
}

func TestHandleShutdownAfterJobFinishesRemovesHeadService(t *testing.T) {
	ctx := context.Background()
	t.Setenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES, "false")

	rayJob := headServiceTestRayJob()
	rayJob.Spec.ShutdownAfterJobFinishes = true
	rayJob.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Hour)}
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: rayJob.Status.RayClusterName, Namespace: rayJob.Namespace},
	}
	svc := headServiceForRayJob(t, rayJob)
	fakeClient := clientFake.NewClientBuilder().WithScheme(headServiceTestScheme(t)).WithObjects(cluster, svc).Build()
	r := &RayJobReconciler{Client: fakeClient, Recorder: events.NewFakeRecorder(10)}

	_, err := r.handleShutdownAfterJobFinishes(ctx, rayJob)
	require.NoError(t, err)

	assert.True(t, k8serrors.IsNotFound(fakeClient.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, &rayv1.RayCluster{})))
	assert.True(t, k8serrors.IsNotFound(fakeClient.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &corev1.Service{})),
		"shutdownAfterJobFinishes cleanup must also delete the RayJob-owned head Service")
}
