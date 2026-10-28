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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/expectations"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func gcsFTTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, rayv1.AddToScheme(s))
	return s
}

func newGCSStorageRayCluster(options *rayv1.GcsFaultToleranceOptions) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "cluster-uid",
		},
		Spec: rayv1.RayClusterSpec{
			GcsFaultToleranceOptions: options,
		},
	}
}

func TestReconcileGCSStoragePVC(t *testing.T) {
	ctx := context.Background()
	scheme := gcsFTTestScheme(t)

	t.Run("redis backend creates no PVC", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{RedisAddress: "redis:6379"})
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvcList := &corev1.PersistentVolumeClaimList{}
		require.NoError(t, fakeClient.List(ctx, pvcList, client.InNamespace("default")))
		assert.Empty(t, pvcList.Items)
	})

	t.Run("operator-managed PVC created with defaults and RayCluster owner", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB})
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc))
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, pvc.Spec.AccessModes)
		assert.Equal(t, resource.MustParse(utils.GCSStorageDefaultSize), pvc.Spec.Resources.Requests[corev1.ResourceStorage])
		require.Len(t, pvc.OwnerReferences, 1)
		assert.Equal(t, "RayCluster", pvc.OwnerReferences[0].Kind)
		assert.Equal(t, "test-cluster", pvc.OwnerReferences[0].Name)
	})

	t.Run("operator-managed PVC honors size and access modes", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{
				Size:        new(resource.MustParse("5Gi")),
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
			},
		})
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc))
		assert.Equal(t, []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}, pvc.Spec.AccessModes)
		assert.Equal(t, resource.MustParse("5Gi"), pvc.Spec.Resources.Requests[corev1.ResourceStorage])
	})

	t.Run("claimName creates no PVC", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{ClaimName: "my-pvc"},
		})
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvcList := &corev1.PersistentVolumeClaimList{}
		require.NoError(t, fakeClient.List(ctx, pvcList, client.InNamespace("default")))
		assert.Empty(t, pvcList.Items)
	})

	t.Run("RayService-owned cluster still owns the PVC by the RayCluster", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB})
		instance.OwnerReferences = []metav1.OwnerReference{
			{
				APIVersion: "ray.io/v1",
				Kind:       "RayService",
				Name:       "my-service",
				UID:        "service-uid",
				Controller: new(true),
			},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc))
		// The PVC is owned by the RayCluster (not the parent RayService), so it is
		// garbage-collected with the cluster and never orphaned across upgrades.
		require.Len(t, pvc.OwnerReferences, 1)
		assert.Equal(t, "RayCluster", pvc.OwnerReferences[0].Kind)
		assert.Equal(t, "test-cluster", pvc.OwnerReferences[0].Name)
	})

	t.Run("idempotent when PVC already exists", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB})
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		err := fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc)
		require.False(t, k8serrors.IsNotFound(err))
		require.NoError(t, err)
	})

	t.Run("emits a warning event when the existing PVC diverges from the desired spec", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{Size: new(resource.MustParse("5Gi"))},
		})
		// Pre-existing PVC provisioned with a smaller size (drift).
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		recorder := events.NewFakeRecorder(10)
		r := &RayClusterReconciler{Client: fakeClient, Recorder: recorder, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		select {
		case event := <-recorder.Events:
			assert.Contains(t, event, string(utils.FailedToCreatePVC))
			assert.Contains(t, event, "cannot be reconfigured in place")
		default:
			t.Fatal("expected a drift warning event, got none")
		}
	})

	t.Run("no drift event when the existing PVC matches the desired spec", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{Size: new(resource.MustParse("1Gi"))},
		})
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		recorder := events.NewFakeRecorder(10)
		r := &RayClusterReconciler{Client: fakeClient, Recorder: recorder, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		select {
		case event := <-recorder.Events:
			t.Fatalf("expected no event, got %q", event)
		default:
		}
	})

	t.Run("no drift event when storageClassName is omitted and the live PVC has a default class", func(t *testing.T) {
		// storageClassName omitted in the spec (nil); Kubernetes populates the live
		// PVC with the resolved default class. This must not be reported as drift.
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{Size: new(resource.MustParse(utils.GCSStorageDefaultSize))},
		})
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("standard"),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(utils.GCSStorageDefaultSize)},
				},
			},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		recorder := events.NewFakeRecorder(10)
		r := &RayClusterReconciler{Client: fakeClient, Recorder: recorder, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		select {
		case event := <-recorder.Events:
			t.Fatalf("expected no drift event, got %q", event)
		default:
		}
	})

	t.Run("emits a drift event when an explicitly-set storageClassName diverges", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{
				Size:             new(resource.MustParse(utils.GCSStorageDefaultSize)),
				StorageClassName: new("fast"),
			},
		})
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: new("standard"),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(utils.GCSStorageDefaultSize)},
				},
			},
		}
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		recorder := events.NewFakeRecorder(10)
		r := &RayClusterReconciler{Client: fakeClient, Recorder: recorder, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		select {
		case event := <-recorder.Events:
			assert.Contains(t, event, "storageClassName")
		default:
			t.Fatal("expected a storageClassName drift warning event, got none")
		}
	})

	t.Run("deletionPolicy Retain omits the RayCluster owner reference on create", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{DeletionPolicy: ptr.To(rayv1.RetainGCSStorageDeletionPolicy)},
		})
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc))
		assert.False(t, metav1.IsControlledBy(pvc, instance), "retained PVC must not be owned by the RayCluster")
	})

	t.Run("setting deletionPolicy Retain removes the owner reference from an existing PVC", func(t *testing.T) {
		instance := newGCSStorageRayCluster(&rayv1.GcsFaultToleranceOptions{
			Backend: rayv1.GcsFTBackendRocksDB,
			Storage: &rayv1.GcsEmbeddedStorage{DeletionPolicy: ptr.To(rayv1.RetainGCSStorageDeletionPolicy)},
		})
		existing := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-gcs-pvc", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(utils.GCSStorageDefaultSize)},
				},
			},
		}
		require.NoError(t, ctrl.SetControllerReference(instance, existing, scheme))
		fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, existing).Build()
		r := &RayClusterReconciler{Client: fakeClient, Recorder: &events.FakeRecorder{}, Scheme: scheme, rayClusterScaleExpectation: expectations.NewRayClusterScaleExpectation(fakeClient)}

		require.NoError(t, r.reconcileGCSStoragePVC(ctx, instance))

		pvc := &corev1.PersistentVolumeClaim{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-gcs-pvc", Namespace: "default"}, pvc))
		assert.False(t, metav1.IsControlledBy(pvc, instance), "owner reference should be removed once retention is enabled")
	})
}
