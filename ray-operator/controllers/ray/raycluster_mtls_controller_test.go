package ray

import (
	"context"
	"fmt"
	"strings"
	"testing"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func newMTLSTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = rayv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	_ = certmanagerv1.AddToScheme(s)
	return s
}

func newMTLSTestCluster(name string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			UID:       "test-uid-12345678",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:      "small-group",
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "ray-worker", Image: "rayproject/ray:latest"},
							},
						},
					},
				},
			},
		},
	}
}

func newMTLSController(t *testing.T, objs ...client.Object) *RayClusterMTLSController {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	s := newMTLSTestScheme()
	runtimeObjs := make([]runtime.Object, len(objs))
	for i, obj := range objs {
		runtimeObjs[i] = obj
	}
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(runtimeObjs...).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		Build()
	return &RayClusterMTLSController{
		Client:   fakeClient,
		Scheme:   s,
		Recorder: events.NewFakeRecorder(32),
	}
}

// markMTLSCertificatesReady sets the Ready=True condition on all Certificate objects
// for the given cluster, simulating cert-manager issuing the certificates.
func markMTLSCertificatesReady(ctx context.Context, t *testing.T, r *RayClusterMTLSController, cluster *rayv1.RayCluster) {
	t.Helper()
	certNames := []string{
		utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
		utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode),
		utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode),
	}
	for _, name := range certNames {
		cert := &certmanagerv1.Certificate{}
		err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: cluster.Namespace}, cert)
		if errors.IsNotFound(err) {
			continue
		}
		require.NoError(t, err)
		cert.Status.Conditions = []certmanagerv1.CertificateCondition{
			{
				Type:               certmanagerv1.CertificateConditionReady,
				Status:             cmmeta.ConditionTrue,
				ObservedGeneration: cert.Generation,
			},
		}
		require.NoError(t, r.Status().Update(ctx, cert))
	}
}

// --- Auto-generate mode tests ---

func TestMTLSController_Disabled(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	r := newMTLSController(t, cluster)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err, "Reconcile should be a no-op when mTLS is disabled")
	assert.Equal(t, ctrl.Result{}, result)
}

func TestMTLSController_AutoGenerate_CreatesFullPKI(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	cluster.Spec.EnableInTreeAutoscaling = new(true)
	cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{Standalone: new(true)}

	r := newMTLSController(t, cluster)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	// First call creates PKI resources; certs aren't ready yet so it requeues.
	result, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsDefaultRequeueDuration, result.RequeueAfter, "should requeue when certs not ready")

	// Second call: resources already exist, certs still not ready.
	result, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsDefaultRequeueDuration, result.RequeueAfter, "should requeue when certs not ready")

	// Simulate cert-manager marking certificates as ready.
	markMTLSCertificatesReady(ctx, t, r, cluster)

	result, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsPeriodicCheckDuration, result.RequeueAfter, "should schedule periodic recheck")

	// Verify self-signed issuer was created.
	issuer := &certmanagerv1.Issuer{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetSelfSignedIssuerName(cluster.Name),
		Namespace: "default",
	}, issuer)
	require.NoError(t, err, "self-signed issuer should be created")
	assert.NotNil(t, issuer.Spec.SelfSigned)

	// Verify CA certificate was created.
	caCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetCACertName(cluster.Name),
		Namespace: "default",
	}, caCert)
	require.NoError(t, err, "CA certificate should be created")
	assert.True(t, caCert.Spec.IsCA)
	assert.Equal(t, 4096, caCert.Spec.PrivateKey.Size)

	// Verify CA issuer was created.
	caIssuer := &certmanagerv1.Issuer{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetCAIssuerName(cluster.Name),
		Namespace: "default",
	}, caIssuer)
	require.NoError(t, err, "CA issuer should be created")
	assert.NotNil(t, caIssuer.Spec.CA)

	// Verify head certificate was created with correct DNS names.
	headCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
		Namespace: "default",
	}, headCert)
	require.NoError(t, err, "head certificate should be created")
	headFQDN := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	assert.Contains(t, headCert.Spec.DNSNames, "localhost")
	assert.Contains(t, headCert.Spec.DNSNames, headFQDN)
	assert.Contains(t, headCert.Spec.IPAddresses, "127.0.0.1")

	// Verify worker certificate was created with correct DNS names.
	workerCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode),
		Namespace: "default",
	}, workerCert)
	require.NoError(t, err, "worker certificate should be created")
	assert.Contains(t, workerCert.Spec.DNSNames, "localhost")
	assert.Contains(t, workerCert.Spec.IPAddresses, "127.0.0.1")

	// Verify autoscaler certificate was created as a static client-only certificate.
	autoscalerCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode),
		Namespace: "default",
	}, autoscalerCert)
	require.NoError(t, err, "autoscaler certificate should be created")
	assert.Empty(t, autoscalerCert.Spec.DNSNames, "autoscaler client certificate needs no DNS SANs")
	assert.Empty(t, autoscalerCert.Spec.IPAddresses, "autoscaler client certificate needs no IP SANs")
	assert.Equal(t, "ray-autoscaler", autoscalerCert.Spec.CommonName)
	assert.Contains(t, autoscalerCert.Spec.Usages, certmanagerv1.UsageClientAuth)
	assert.NotContains(t, autoscalerCert.Spec.Usages, certmanagerv1.UsageServerAuth,
		"autoscaler client certificate must not be usable as a server certificate")

	// Verify all resources have correct labels.
	assert.Equal(t, cluster.Name, issuer.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, cluster.Name, caCert.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, cluster.Name, caIssuer.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, cluster.Name, headCert.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, cluster.Name, workerCert.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, cluster.Name, autoscalerCert.Labels[utils.RayClusterLabelKey])
}

func TestMTLSController_AutoGenerate_Idempotent(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	cluster.Spec.EnableInTreeAutoscaling = new(true)
	cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{Standalone: new(true)}

	r := newMTLSController(t, cluster)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	// First reconcile creates resources.
	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	markMTLSCertificatesReady(ctx, t, r, cluster)

	// Second and third reconciles should be idempotent.
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify exactly one of each resource type.
	issuerList := &certmanagerv1.IssuerList{}
	err = r.List(ctx, issuerList, client.InNamespace("default"))
	require.NoError(t, err)
	assert.Len(t, issuerList.Items, 2, "should have 2 issuers: self-signed + CA")

	certList := &certmanagerv1.CertificateList{}
	err = r.List(ctx, certList, client.InNamespace("default"))
	require.NoError(t, err)
	assert.Len(t, certList.Items, 4, "should have 4 certificates: CA + head + worker + autoscaler")
}

func TestMTLSController_ReconcileIssuer(t *testing.T) {
	cluster := newMTLSTestCluster("issuer-test")
	tests := []struct {
		name             string
		reconcile        func(*RayClusterMTLSController, context.Context, *rayv1.RayCluster) error
		issuerName       string
		component        string
		wantIssuerConfig certmanagerv1.IssuerConfig
	}{
		{
			name:       "self-signed issuer",
			reconcile:  (*RayClusterMTLSController).reconcileSelfSignedIssuer,
			issuerName: utils.GetSelfSignedIssuerName(cluster.Name),
			component:  "selfsigned-issuer",
			wantIssuerConfig: certmanagerv1.IssuerConfig{
				SelfSigned: &certmanagerv1.SelfSignedIssuer{},
			},
		},
		{
			name:       "CA issuer",
			reconcile:  (*RayClusterMTLSController).reconcileCAIssuer,
			issuerName: utils.GetCAIssuerName(cluster.Name),
			component:  "ca-issuer",
			wantIssuerConfig: certmanagerv1.IssuerConfig{
				CA: &certmanagerv1.CAIssuer{
					SecretName: utils.GetCASecretName(cluster.Name, cluster.UID),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newMTLSController(t)
			ctx := context.Background()

			require.NoError(t, tt.reconcile(r, ctx, cluster))
			require.NoError(t, tt.reconcile(r, ctx, cluster))

			issuer := &certmanagerv1.Issuer{}
			require.NoError(t, r.Get(ctx, types.NamespacedName{
				Name:      tt.issuerName,
				Namespace: cluster.Namespace,
			}, issuer))
			assert.Equal(t, tt.wantIssuerConfig, issuer.Spec.IssuerConfig)
			assert.Equal(t, tt.component, issuer.Labels["app.kubernetes.io/component"])
			assert.Equal(t, cluster.Name, issuer.Labels[utils.RayClusterLabelKey])

			controllerRef := metav1.GetControllerOf(issuer)
			require.NotNil(t, controllerRef)
			assert.Equal(t, cluster.Name, controllerRef.Name)
			assert.Equal(t, cluster.UID, controllerRef.UID)

			issuerList := &certmanagerv1.IssuerList{}
			require.NoError(t, r.List(ctx, issuerList, client.InNamespace(cluster.Namespace)))
			assert.Len(t, issuerList.Items, 1)
		})
	}
}

func TestMTLSController_AutoGenerate_UpdatesIPAddresses(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	headPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-head-0",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayClusterLabelKey:  cluster.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
			},
		},
		Status: corev1.PodStatus{PodIP: "10.244.0.5"},
	}

	r := newMTLSController(t, cluster, headPod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	markMTLSCertificatesReady(ctx, t, r, cluster)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	// Verify head certificate includes the head pod IP only.
	headCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
		Namespace: "default",
	}, headCert)
	require.NoError(t, err)
	assert.Contains(t, headCert.Spec.IPAddresses, "10.244.0.5")
	assert.Contains(t, headCert.Spec.IPAddresses, "127.0.0.1")

	// Simulate scale-up: add a worker pod.
	// A finalizer is added so that a subsequent Delete call sets DeletionTimestamp
	// without immediately removing the pod from the API server, letting us test
	// the terminating-pod filter in the SAN reconciliation.
	workerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-worker-0",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayClusterLabelKey:  cluster.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.WorkerNode),
			},
			Finalizers: []string{"test/hold"},
		},
		Status: corev1.PodStatus{PodIP: "10.244.0.6"},
	}
	require.NoError(t, r.Create(ctx, workerPod))

	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
		Namespace: "default",
	}, headCert)
	require.NoError(t, err)
	assert.Contains(t, headCert.Spec.IPAddresses, "10.244.0.5")
	assert.NotContains(t, headCert.Spec.IPAddresses, "10.244.0.6",
		"head cert should not include worker pod IPs")

	workerCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode),
		Namespace: "default",
	}, workerCert)
	require.NoError(t, err)
	assert.Contains(t, workerCert.Spec.IPAddresses, "10.244.0.6",
		"worker cert should be updated with the new worker pod IP after scale-up")

	// Simulate scale-down: deleting the pod while the finalizer is still present causes
	// envtest to set DeletionTimestamp without removing the pod from the store, so we
	// can verify that terminating pods are excluded from certificate SANs.
	require.NoError(t, r.Delete(ctx, workerPod))

	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode),
		Namespace: "default",
	}, workerCert)
	require.NoError(t, err)
	assert.NotContains(t, workerCert.Spec.IPAddresses, "10.244.0.6",
		"worker cert should not include IP of a terminating pod after scale-down")
}

func TestMTLSController_Disabled_IsNoOp(t *testing.T) {
	cluster := newMTLSTestCluster("disabled-cluster")
	cluster.Spec.TLSOptions = nil

	// Pre-existing resources should NOT be touched when mTLS is disabled.
	// The controller simply returns without requeuing.
	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: utils.GetCASecretName(cluster.Name, cluster.UID), Namespace: "default"},
	}

	r := newMTLSController(t, cluster, caSecret)
	ctx := context.Background()

	result, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "disabled mTLS should not requeue")

	// The CA secret should still exist (no cleanup on disable).
	err = r.Get(ctx, types.NamespacedName{Name: caSecret.Name, Namespace: "default"}, &corev1.Secret{})
	require.NoError(t, err, "resources should not be deleted when mTLS is simply disabled")
}

// TestMTLSController_WorkerCertHasLocalhostOnly verifies that worker certificates
// use only "localhost" as a DNS SAN. Worker-to-head communication goes via the head
// pod IP (covered by IP SANs), and worker pods don't serve DNS-addressable endpoints
// that require wildcard headless-service entries.
func TestMTLSController_WorkerCertHasLocalhostOnly(t *testing.T) {
	cluster := newMTLSTestCluster("wc-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	r := newMTLSController(t, cluster)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	markMTLSCertificatesReady(ctx, t, r, cluster)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	workerCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode),
		Namespace: "default",
	}, workerCert)
	require.NoError(t, err)

	assert.Contains(t, workerCert.Spec.DNSNames, "localhost")
	workerSvcName := fmt.Sprintf("%s-%s", cluster.Name, utils.HeadlessServiceSuffix)
	assert.NotContains(t, workerCert.Spec.DNSNames, workerSvcName,
		"GCS connects to workers via pod IP (IP SANs), not via headless service DNS")
	assert.NotContains(t, workerCert.Spec.DNSNames,
		fmt.Sprintf("*.%s.%s.svc", workerSvcName, cluster.Namespace),
		"wildcard headless-service DNS SANs are not needed")
	assert.NotContains(t, workerCert.Spec.DNSNames,
		fmt.Sprintf("*.%s.%s.svc.cluster.local", workerSvcName, cluster.Namespace),
		"wildcard headless-service DNS SANs are not needed")
}

// TestMTLSController_AutoscalerCertStaysStatic verifies that the autoscaler's client
// certificate is issued once with a static spec: even when an autoscaler Pod exists
// (with a routable IP), its IP is never added to the certificate, so rescheduling the
// autoscaler Pod never triggers a certificate reissue. Nothing dials the autoscaler;
// the certificate is only presented as a TLS client of the head GCS.
func TestMTLSController_AutoscalerCertStaysStatic(t *testing.T) {
	cluster := newMTLSTestCluster("static-client-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	cluster.Spec.EnableInTreeAutoscaling = new(true)
	cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{Standalone: new(true)}

	autoscalerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "static-client-cluster-autoscaler-abcde",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayClusterLabelKey:  cluster.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.AutoscalerNode),
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "autoscaler", Image: "rayproject/ray:latest"}}},
		Status: corev1.PodStatus{
			PodIP: "10.244.0.42",
			Phase: corev1.PodRunning,
		},
	}

	r := newMTLSController(t, cluster, autoscalerPod)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	_, err := r.Reconcile(ctx, req)
	require.NoError(t, err)

	markMTLSCertificatesReady(ctx, t, r, cluster)
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	autoscalerCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode),
		Namespace: "default",
	}, autoscalerCert)
	require.NoError(t, err)
	resourceVersionBefore := autoscalerCert.ResourceVersion

	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)

	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode),
		Namespace: "default",
	}, autoscalerCert)
	require.NoError(t, err)

	assert.Empty(t, autoscalerCert.Spec.DNSNames,
		"autoscaler client certificate must not gain DNS SANs, even with an autoscaler Pod present")
	assert.Empty(t, autoscalerCert.Spec.IPAddresses,
		"autoscaler client certificate must not gain IP SANs, even with an autoscaler Pod present")
	assert.Equal(t, resourceVersionBefore, autoscalerCert.ResourceVersion,
		"the certificate must not be rewritten by subsequent reconciles, even with an autoscaler Pod present")
}

func TestMTLSController_CertReadinessBlocksReconciliation(t *testing.T) {
	cluster := newMTLSTestCluster("ready-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	r := newMTLSController(t, cluster)
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}}

	// First reconcile adds the finalizer and requeues.
	result, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsDefaultRequeueDuration, result.RequeueAfter, "should requeue after adding finalizer")

	// Second reconcile creates resources but should requeue (certs not ready).
	result, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsDefaultRequeueDuration, result.RequeueAfter, "should requeue when certs not ready")

	// Resources should still be created.
	headCert := &certmanagerv1.Certificate{}
	err = r.Get(ctx, types.NamespacedName{
		Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
		Namespace: "default",
	}, headCert)
	require.NoError(t, err, "head certificate should be created")

	// Mark only head cert ready; should still requeue (worker not ready).
	headCert.Status.Conditions = []certmanagerv1.CertificateCondition{
		{Type: certmanagerv1.CertificateConditionReady, Status: cmmeta.ConditionTrue},
	}
	require.NoError(t, r.Status().Update(ctx, headCert))

	result, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsDefaultRequeueDuration, result.RequeueAfter, "should requeue when only head cert is ready")

	// Mark both certs ready; should now succeed with periodic check.
	markMTLSCertificatesReady(ctx, t, r, cluster)
	result, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, mtlsPeriodicCheckDuration, result.RequeueAfter, "should schedule periodic check when all ready")
}

func TestMTLSController_DeletionIsNoop(t *testing.T) {
	now := metav1.Now()
	cluster := newMTLSTestCluster("del-noop")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	cluster.DeletionTimestamp = &now
	cluster.Finalizers = []string{"some-other-finalizer"}

	r := newMTLSController(t, cluster)
	ctx := context.Background()

	result, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result, "deletion should be a no-op for the mTLS controller")
}

func TestMTLSController_SecretGetsOwnerRefToCertificate(t *testing.T) {
	cluster := newMTLSTestCluster("owner-ref-test")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	cert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
			UID:       "cert-uid-abc123",
		},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
		},
	}

	r := newMTLSController(t, cluster, cert, secret)
	ctx := context.Background()

	require.NoError(t, r.ensureSecretOwnedByCertificate(ctx, cert))

	updated := &corev1.Secret{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}, updated))

	var found bool
	for _, ref := range updated.OwnerReferences {
		if ref.UID == cert.UID {
			found = true
			assert.Equal(t, "Certificate", ref.Kind)
			assert.Equal(t, cert.Name, ref.Name)
			assert.Nil(t, ref.BlockOwnerDeletion)
		}
	}
	assert.True(t, found, "secret should have owner reference to the Certificate")
}

func TestMTLSController_SecretOwnerRefIdempotent(t *testing.T) {
	cluster := newMTLSTestCluster("owner-ref-idem")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	cert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
			UID:       "cert-uid-idem",
		},
		Spec: certmanagerv1.CertificateSpec{
			SecretName: utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{UID: cert.UID, Name: cert.Name, Kind: "Certificate", APIVersion: certmanagerv1.SchemeGroupVersion.String()},
			},
		},
	}

	r := newMTLSController(t, cluster, cert, secret)
	ctx := context.Background()

	// Call twice — should not error or duplicate the owner ref.
	require.NoError(t, r.ensureSecretOwnedByCertificate(ctx, cert))
	require.NoError(t, r.ensureSecretOwnedByCertificate(ctx, cert))

	updated := &corev1.Secret{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace}, updated))
	count := 0
	for _, ref := range updated.OwnerReferences {
		if ref.UID == cert.UID {
			count++
		}
	}
	assert.Equal(t, 1, count, "owner reference should not be duplicated")
}

// --- Utility function tests ---

func TestSetsEqual(t *testing.T) {
	tests := []struct {
		name     string
		a, b     []string
		expected bool
	}{
		{"both empty", nil, nil, true},
		{"equal same order", []string{"a", "b"}, []string{"a", "b"}, true},
		{"equal different order", []string{"b", "a"}, []string{"a", "b"}, true},
		{"different lengths", []string{"a"}, []string{"a", "b"}, false},
		{"different contents", []string{"a", "c"}, []string{"a", "b"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, setsEqual(tc.a, tc.b))
		})
	}
}

func TestCertificateNeedsUpdate(t *testing.T) {
	existing := &certmanagerv1.CertificateSpec{
		DNSNames:    []string{"b.svc", "a.svc"},
		IPAddresses: []string{"10.0.0.2", "10.0.0.1"},
	}
	desired := &certmanagerv1.CertificateSpec{
		DNSNames:    []string{"a.svc", "b.svc"},
		IPAddresses: []string{"10.0.0.1", "10.0.0.2"},
	}
	assert.False(t, certificateNeedsUpdate(existing, desired), "same sets in different order should not need update")

	desired2 := &certmanagerv1.CertificateSpec{
		DNSNames:    []string{"a.svc", "c.svc"},
		IPAddresses: []string{"10.0.0.1", "10.0.0.2"},
	}
	assert.True(t, certificateNeedsUpdate(existing, desired2), "different DNS names should need update")

	existing3 := &certmanagerv1.CertificateSpec{
		DNSNames:    []string{"a.svc"},
		IPAddresses: []string{"127.0.0.1"},
	}
	desired3 := &certmanagerv1.CertificateSpec{
		DNSNames:    []string{"a.svc"},
		IPAddresses: []string{"127.0.0.1", "10.0.0.1"},
	}
	assert.True(t, certificateNeedsUpdate(existing3, desired3), "different IP addresses should need update")
}

// --- checkMTLSSecretsReady tests (in main reconciler) ---

// newReadyCertificate creates a cert-manager Certificate with Ready=True at generation 1,
// matching the ObservedGeneration check in checkMTLSSecretsReady / isCertificateReady.
func newReadyCertificate(name string) *certmanagerv1.Certificate {
	const gen int64 = 1
	cert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  "default",
			Generation: gen,
		},
	}
	cert.Status.Conditions = []certmanagerv1.CertificateCondition{
		{
			Type:               certmanagerv1.CertificateConditionReady,
			Status:             cmmeta.ConditionTrue,
			ObservedGeneration: gen,
		},
	}
	return cert
}

func TestCheckMTLSSecretsReady_AutoGenerate_SecretsPresent(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	headCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.HeadNode))
	workerCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode))
	autoscalerCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode))
	headSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}
	workerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.WorkerNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}

	autoscalerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.AutoscalerNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}

	s := newMTLSTestScheme()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithRuntimeObjects(cluster, headCert, workerCert, autoscalerCert, headSecret, workerSecret, autoscalerSecret).
		Build()
	// Patch status so ObservedGeneration is persisted on the fake client.
	require.NoError(t, fakeClient.Status().Update(context.Background(), headCert))
	require.NoError(t, fakeClient.Status().Update(context.Background(), workerCert))
	require.NoError(t, fakeClient.Status().Update(context.Background(), autoscalerCert))
	r := &RayClusterReconciler{Client: fakeClient, Scheme: s}

	err := r.checkMTLSSecretsReady(context.Background(), cluster)
	assert.NoError(t, err, "should succeed when all secrets and ready certificates are present")
}

func TestCheckMTLSSecretsReady_AutoGenerate_CertNotReady(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	// Certificate exists but is not yet ready (e.g. reissuing after a SAN update).
	headCert := &certmanagerv1.Certificate{
		ObjectMeta: metav1.ObjectMeta{
			Name:       utils.GetTLSCertName(cluster.Name, rayv1.HeadNode),
			Namespace:  "default",
			Generation: 2,
		},
	}

	s := newMTLSTestScheme()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithRuntimeObjects(cluster, headCert).
		Build()
	r := &RayClusterReconciler{Client: fakeClient, Scheme: s}

	err := r.checkMTLSSecretsReady(context.Background(), cluster)
	require.Error(t, err, "should fail when certificate is not ready")
	assert.Contains(t, err.Error(), "not ready")
}

func TestCheckMTLSSecretsReady_AutoGenerate_SecretMissing(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	headCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.HeadNode))
	workerCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode))

	s := newMTLSTestScheme()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithRuntimeObjects(cluster, headCert, workerCert).
		Build()
	require.NoError(t, fakeClient.Status().Update(context.Background(), headCert))
	require.NoError(t, fakeClient.Status().Update(context.Background(), workerCert))
	r := &RayClusterReconciler{Client: fakeClient, Scheme: s}

	err := r.checkMTLSSecretsReady(context.Background(), cluster)
	require.Error(t, err, "should fail when secrets are missing")
	assert.Contains(t, err.Error(), "not found")
}

func TestCheckMTLSSecretsReady_AutoGenerate_SecretMissingKey(t *testing.T) {
	cluster := newMTLSTestCluster("test-cluster")
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}

	headCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.HeadNode))
	workerCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.WorkerNode))
	autoscalerCert := newReadyCertificate(utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode))
	// Head secret missing ca.crt key.
	headSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
		},
	}
	workerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.WorkerNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}

	autoscalerSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetTLSSecretName(cluster.Name, rayv1.AutoscalerNode),
			Namespace: "default",
		},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}

	s := newMTLSTestScheme()
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&certmanagerv1.Certificate{}).
		WithRuntimeObjects(cluster, headCert, workerCert, autoscalerCert, headSecret, workerSecret, autoscalerSecret).
		Build()
	require.NoError(t, fakeClient.Status().Update(context.Background(), headCert))
	require.NoError(t, fakeClient.Status().Update(context.Background(), workerCert))
	require.NoError(t, fakeClient.Status().Update(context.Background(), autoscalerCert))
	r := &RayClusterReconciler{Client: fakeClient, Scheme: s}

	err := r.checkMTLSSecretsReady(context.Background(), cluster)
	require.Error(t, err, "should fail when secret is missing a required key")
	assert.Contains(t, err.Error(), "ca.crt")
}

// --- Cluster not found test ---

func TestMTLSController_ClusterNotFound(t *testing.T) {
	r := newMTLSController(t)
	ctx := context.Background()

	result, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})
	require.NoError(t, err, "should not error on not found")
	assert.Equal(t, ctrl.Result{}, result)
}

func TestMTLSController_AutoscalerCertificateRequiredOnlyForStandalone(t *testing.T) {
	for _, tc := range []struct {
		name        string
		autoscaling *bool
		standalone  *bool
		wantCert    bool
	}{
		{name: "autoscaling unset"},
		{name: "sidecar", autoscaling: new(true)},
		{name: "explicit sidecar", autoscaling: new(true), standalone: new(false)},
		{name: "autoscaling disabled", autoscaling: new(false), standalone: new(true)},
		{name: "standalone", autoscaling: new(true), standalone: new(true), wantCert: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cluster := newMTLSTestCluster("layout-test")
			cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
			cluster.Spec.EnableInTreeAutoscaling = tc.autoscaling
			cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{Standalone: tc.standalone}
			r := newMTLSController(t, cluster)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			_, err := r.Reconcile(ctx, req)
			require.NoError(t, err)

			autoscalerCert := &certmanagerv1.Certificate{}
			certKey := client.ObjectKey{Name: utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode), Namespace: cluster.Namespace}
			err = r.Get(ctx, certKey, autoscalerCert)
			if tc.wantCert {
				require.NoError(t, err)
			} else {
				require.True(t, errors.IsNotFound(err), "unused autoscaler certificate must not be created")
			}

			// Head and worker credentials alone must suffice for the existing layouts.
			for _, nodeType := range []rayv1.RayNodeType{rayv1.HeadNode, rayv1.WorkerNode} {
				cert := &certmanagerv1.Certificate{}
				require.NoError(t, r.Get(ctx, client.ObjectKey{Name: utils.GetTLSCertName(cluster.Name, nodeType), Namespace: cluster.Namespace}, cert))
				cert.Status.Conditions = newReadyCertificate(cert.Name).Status.Conditions
				cert.Status.Conditions[0].ObservedGeneration = cert.Generation
				require.NoError(t, r.Status().Update(ctx, cert))
				require.NoError(t, r.Create(ctx, &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: utils.GetTLSSecretName(cluster.Name, nodeType), Namespace: cluster.Namespace},
					Data:       map[string][]byte{"tls.crt": []byte("cert"), "tls.key": []byte("key"), "ca.crt": []byte("ca")},
				}))
			}
			mainReconciler := &RayClusterReconciler{Client: r.Client, Scheme: r.Scheme}
			ready, err := r.checkTLSCertificatesReady(ctx, cluster)
			require.NoError(t, err)
			assert.Equal(t, !tc.wantCert, ready)
			if !tc.wantCert {
				require.NoError(t, mainReconciler.checkMTLSSecretsReady(ctx, cluster))
				result, err := r.Reconcile(ctx, req)
				require.NoError(t, err)
				assert.Equal(t, mtlsPeriodicCheckDuration, result.RequeueAfter)
				return
			}

			// The standalone layout must still wait for its client certificate and Secret.
			require.ErrorContains(t, mainReconciler.checkMTLSSecretsReady(ctx, cluster), "not ready")
			autoscalerCert.Status.Conditions = newReadyCertificate(autoscalerCert.Name).Status.Conditions
			autoscalerCert.Status.Conditions[0].ObservedGeneration = autoscalerCert.Generation
			require.NoError(t, r.Status().Update(ctx, autoscalerCert))
			require.ErrorContains(t, mainReconciler.checkMTLSSecretsReady(ctx, cluster), "not found")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: utils.GetTLSSecretName(cluster.Name, rayv1.AutoscalerNode), Namespace: cluster.Namespace},
				Data:       map[string][]byte{"tls.crt": []byte("cert"), "tls.key": []byte("key")},
			}
			require.NoError(t, r.Create(ctx, secret))
			require.ErrorContains(t, mainReconciler.checkMTLSSecretsReady(ctx, cluster), "missing required key ca.crt")
			secret.Data["ca.crt"] = []byte("ca")
			require.NoError(t, r.Update(ctx, secret))
			require.NoError(t, mainReconciler.checkMTLSSecretsReady(ctx, cluster))
			ready, err = r.checkTLSCertificatesReady(ctx, cluster)
			require.NoError(t, err)
			assert.True(t, ready)
		})
	}
}

func TestMTLSController_AutoscalerCertificateCommonNameLength(t *testing.T) {
	for _, nameLength := range []int{49, 50, 57} {
		t.Run(fmt.Sprintf("cluster-name-length-%d", nameLength), func(t *testing.T) {
			cluster := newMTLSTestCluster(strings.Repeat("a", nameLength))
			cluster.Spec.EnableInTreeAutoscaling = new(true)
			cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{Standalone: new(true)}
			r := newMTLSController(t, cluster)
			ctx := context.Background()
			require.NoError(t, r.reconcileAutoscalerCertificate(ctx, cluster))
			cert := &certmanagerv1.Certificate{}
			require.NoError(t, r.Get(ctx, client.ObjectKey{Name: utils.GetTLSCertName(cluster.Name, rayv1.AutoscalerNode), Namespace: cluster.Namespace}, cert))
			require.NotEmpty(t, cert.Spec.CommonName, "a SAN-less certificate still needs a subject")
			assert.LessOrEqual(t, len(cert.Spec.CommonName), 64, "cert-manager rejects longer common names")
			assert.Contains(t, cert.Spec.Usages, certmanagerv1.UsageClientAuth)
			assert.NotContains(t, cert.Spec.Usages, certmanagerv1.UsageServerAuth)
		})
	}
}
