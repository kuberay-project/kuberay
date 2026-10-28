package metrics

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayClusterInfo(t *testing.T) {
	tests := []struct {
		name            string
		clusters        []rayv1.RayCluster
		expectedMetrics []string
	}{
		{
			name: "two clusters, one with owner, one without",
			clusters: []rayv1.RayCluster{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-ray-cluster",
						Namespace: "default",
						UID:       types.UID("test-ray-cluster-uid"),
						Labels: map[string]string{
							"ray.io/originated-from-crd": "RayJob",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-ray-cluster-2",
						Namespace: "default",
						UID:       types.UID("test-ray-cluster-2-uid"),
						Labels:    map[string]string{},
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_cluster_info{name="test-ray-cluster",namespace="default",owner_kind="RayJob",uid="test-ray-cluster-uid"} 1`,
				`kuberay_cluster_info{name="test-ray-cluster-2",namespace="default",owner_kind="None",uid="test-ray-cluster-2-uid"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.clusters))
			for i := range tc.clusters {
				objs[i] = &tc.clusters[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayClusterMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			body, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)
			for _, label := range tc.expectedMetrics {
				assert.Contains(t, body, label)
			}

			if len(tc.clusters) > 0 {
				err := client.Delete(t.Context(), &tc.clusters[0])
				require.NoError(t, err)
			}

			body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)

			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, label := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, label)
			}
		})
	}
}

func TestDeleteRayClusterMetrics(t *testing.T) {
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	manager := NewRayClusterMetricsManager(context.Background(), client)
	reg := prometheus.NewRegistry()
	reg.MustRegister(manager)

	// Test case 1: Delete specific cluster metrics
	// Manually add some metrics
	manager.rayClusterProvisionedDurationSeconds.With(prometheus.Labels{"name": "cluster1", "namespace": "ns1", "uid": "uid1"}).Set(10.5)
	manager.rayClusterProvisionedDurationSeconds.With(prometheus.Labels{"name": "cluster2", "namespace": "ns2", "uid": "uid2"}).Set(20.3)
	manager.rayClusterProvisionedDurationSeconds.With(prometheus.Labels{"name": "cluster3", "namespace": "ns1", "uid": "uid3"}).Set(5.7)

	// Test deleting metrics for cluster1 in ns1
	manager.DeleteRayClusterMetrics("cluster1", "ns1")

	// Verify metrics
	body, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body, `kuberay_cluster_provisioned_duration_seconds{name="cluster1",namespace="ns1",uid="uid1"}`)
	assert.Contains(t, body, `kuberay_cluster_provisioned_duration_seconds{name="cluster2",namespace="ns2",uid="uid2"}`)
	assert.Contains(t, body, `kuberay_cluster_provisioned_duration_seconds{name="cluster3",namespace="ns1",uid="uid3"}`)

	// Test case 2: Delete with empty name
	manager.DeleteRayClusterMetrics("", "ns1")

	// Verify metrics again
	body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body2, `kuberay_cluster_provisioned_duration_seconds{name="cluster1",namespace="ns1",uid="uid1"}`)
	assert.Contains(t, body2, `kuberay_cluster_provisioned_duration_seconds{name="cluster3",namespace="ns1",uid="uid3"}`)
	assert.Contains(t, body2, `kuberay_cluster_provisioned_duration_seconds{name="cluster2",namespace="ns2",uid="uid2"}`)

	// Test case 3: Delete with empty name and namespace
	manager.DeleteRayClusterMetrics("", "")

	// Verify no metrics were deleted
	body3, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body3, `kuberay_cluster_provisioned_duration_seconds{name="cluster1",namespace="ns1",uid="uid1"}`)
	assert.Contains(t, body3, `kuberay_cluster_provisioned_duration_seconds{name="cluster3",namespace="ns1",uid="uid3"}`)
	assert.Contains(t, body3, `kuberay_cluster_provisioned_duration_seconds{name="cluster2",namespace="ns2",uid="uid2"}`)

	// Test case 4: Delete with false name and namespace
	manager.DeleteRayClusterMetrics("ns2", "cluster2")

	// Verify no metrics were deleted
	body4, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body4, `kuberay_cluster_provisioned_duration_seconds{name="cluster1",namespace="ns1",uid="uid1"}`)
	assert.Contains(t, body4, `kuberay_cluster_provisioned_duration_seconds{name="cluster3",namespace="ns1",uid="uid3"}`)
	assert.Contains(t, body4, `kuberay_cluster_provisioned_duration_seconds{name="cluster2",namespace="ns2",uid="uid2"}`)
}

func TestRayClusterConditionProvisioned(t *testing.T) {
	tests := []struct {
		name            string
		clusters        []rayv1.RayCluster
		expectedMetrics []string
	}{
		{
			name: "clusters with different provisioned status",
			clusters: []rayv1.RayCluster{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "provisioned-cluster",
						Namespace: "default",
						UID:       types.UID("provisioned-cluster-uid"),
					},
					Status: rayv1.RayClusterStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(rayv1.RayClusterProvisioned),
								Status: metav1.ConditionTrue,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "unprovisioned-cluster",
						Namespace: "default",
						UID:       types.UID("unprovisioned-cluster-uid"),
					},
					Status: rayv1.RayClusterStatus{
						Conditions: []metav1.Condition{
							{
								Type:   string(rayv1.RayClusterProvisioned),
								Status: metav1.ConditionFalse,
							},
						},
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_cluster_condition_provisioned{condition="true",name="provisioned-cluster",namespace="default",uid="provisioned-cluster-uid"} 1`,
				`kuberay_cluster_condition_provisioned{condition="false",name="unprovisioned-cluster",namespace="default",uid="unprovisioned-cluster-uid"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.clusters))
			for i := range tc.clusters {
				objs[i] = &tc.clusters[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayClusterMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			body, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)
			for _, metric := range tc.expectedMetrics {
				assert.Contains(t, body, metric)
			}

			if len(tc.clusters) > 0 {
				err := client.Delete(context.Background(), &tc.clusters[0])
				require.NoError(t, err)
			}

			body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)

			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, metric := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, metric)
			}
		})
	}
}
