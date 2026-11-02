package historyserver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestGetAuthTokenForCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	clusterName := "test-cluster"
	namespace := "default"
	secretKey := "test-token-123"

	rc := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: rayv1.RayClusterSpec{
			AuthOptions: &rayv1.AuthOptions{Mode: rayv1.AuthModeToken},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Data: map[string][]byte{AuthTokenSecretKey: []byte(secretKey)},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rc, secret).
		Build()

	clientManager := &ClientManager{
		clients:      []client.Client{fakeClient},
		svcInfoCache: cache.NewLRUExpireCache(svcInfoCacheMaxSize),
	}

	// setAuthOptions reads the current RayCluster and updates its AuthOptions, so successive
	// updates carry the latest resourceVersion (the fake client enforces optimistic concurrency).
	setAuthOptions := func(opts *rayv1.AuthOptions) {
		cur := &rayv1.RayCluster{}
		require.NoError(t, fakeClient.Get(context.Background(), client.ObjectKeyFromObject(rc), cur))
		cur.Spec.AuthOptions = opts
		require.NoError(t, fakeClient.Update(context.Background(), cur))
	}

	token, err := clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, clusterName)
	require.NoError(t, err)
	assert.Equal(t, secretKey, token)

	// A rotated Secret must take effect immediately, since tokens are never cached.
	rotated := secret.DeepCopy()
	rotated.Data[AuthTokenSecretKey] = []byte("rotated-token")
	require.NoError(t, fakeClient.Update(context.Background(), rotated))
	token, err = clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, clusterName)
	require.NoError(t, err)
	assert.Equal(t, "rotated-token", token)

	// K8s-delegated token auth has no static token to inject, so it must fail explicitly rather than
	// let a live proxy request go out unauthenticated.
	enableK8sTokenAuth := true
	setAuthOptions(&rayv1.AuthOptions{Mode: rayv1.AuthModeToken, EnableK8sTokenAuth: &enableK8sTokenAuth})
	require.NoError(t, fakeClient.Delete(context.Background(), rotated))
	_, err = clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, clusterName)
	require.Error(t, err)

	// Auth enabled but the token value is empty: a misconfiguration that must error.
	setAuthOptions(&rayv1.AuthOptions{Mode: rayv1.AuthModeToken})
	emptySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Data:       map[string][]byte{AuthTokenSecretKey: []byte("")},
	}
	require.NoError(t, fakeClient.Create(context.Background(), emptySecret))
	_, err = clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, clusterName)
	require.Error(t, err)

	// Auth disabled returns empty token without error
	setAuthOptions(&rayv1.AuthOptions{Mode: rayv1.AuthModeDisabled})
	token, err = clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, clusterName)
	require.NoError(t, err)
	assert.Empty(t, token)

	// Non-existent cluster should error (spec is read fresh from K8s)
	_, err = clientManager.GetAuthTokenForRayCluster(context.Background(), namespace, "not-exists")
	assert.Error(t, err)
}

func TestGetSvcInfo(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	clusterName := "test-cluster"
	namespace := "default"
	serviceName := "test-cluster-head-svc"
	cacheKey := namespace + "/" + clusterName

	// Use a non-default port so the test covers the HeadGroupSpec override.
	portalPort := int32(9265)

	rc := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{Name: utils.DashboardPortName, ContainerPort: portalPort},
								},
							},
						},
					},
				},
			},
		},
		Status: rayv1.RayClusterStatus{
			Head: rayv1.HeadInfo{
				ServiceName: serviceName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rc).
		WithStatusSubresource(rc).
		Build()

	// Set the status on the fake client (some builders require this separately).
	_ = fakeClient.Status().Update(context.Background(), rc)

	clientManager := &ClientManager{
		clients:      []client.Client{fakeClient},
		svcInfoCache: cache.NewLRUExpireCache(svcInfoCacheMaxSize),
	}

	// First call should fetch from K8s and populate cache.
	svcInfo, err := clientManager.GetSvcInfo(clusterName, namespace)
	require.NoError(t, err)
	assert.Equal(t, serviceName, svcInfo.ServiceName)
	assert.Equal(t, namespace, svcInfo.Namespace)
	assert.Equal(t, int(portalPort), svcInfo.Port)

	// Second call should be served from cache.
	svcInfo2, err := clientManager.GetSvcInfo(clusterName, namespace)
	require.NoError(t, err)
	assert.Equal(t, svcInfo, svcInfo2)

	// Expired cache entry should trigger a re-fetch. A negative TTL makes the entry expire immediately.
	clientManager.svcInfoCache.Add(cacheKey, ServiceInfo{
		ServiceName: "stale-svc",
		Namespace:   namespace,
		Port:        int(portalPort),
	}, -1*time.Second)

	svcInfo3, err := clientManager.GetSvcInfo(clusterName, namespace)
	require.NoError(t, err)
	assert.Equal(t, serviceName, svcInfo3.ServiceName)
	assert.Equal(t, int(portalPort), svcInfo3.Port)

	// Non-existent cluster should error.
	_, err = clientManager.GetSvcInfo("not-exists", namespace)
	require.Error(t, err)

	// No clients should error.
	emptyMgr := &ClientManager{
		clients:      []client.Client{},
		svcInfoCache: cache.NewLRUExpireCache(svcInfoCacheMaxSize),
	}
	_, err = emptyMgr.GetSvcInfo(clusterName, namespace)
	assert.Error(t, err)
}
