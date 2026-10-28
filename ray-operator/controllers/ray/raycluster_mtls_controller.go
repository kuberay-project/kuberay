package ray

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	// Requeue intervals aligned with ODH mTLS controller.
	mtlsDefaultRequeueDuration = 30 * time.Second
	mtlsPeriodicCheckDuration  = 1 * time.Minute
)

// RayClusterMTLSController manages mTLS certificates for RayClusters using cert-manager.
// Runs as a separate controller from the main RayClusterReconciler so that certificate
// creation happens independently of pod lifecycle, avoiding the SAN race condition.
type RayClusterMTLSController struct {
	client.Client
	Scheme   *k8sruntime.Scheme
	Recorder events.EventRecorder
}

// NewRayClusterMTLSController creates a new mTLS controller instance.
func NewRayClusterMTLSController(mgr ctrl.Manager) *RayClusterMTLSController {
	return &RayClusterMTLSController{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorder("raycluster-mtls-controller"),
	}
}

// +kubebuilder:rbac:groups=ray.io,resources=rayclusters,verbs=get;list;watch;update

// +kubebuilder:rbac:groups=cert-manager.io,resources=issuers,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates/status,verbs=get
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;update;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile handles mTLS certificate lifecycle for a RayCluster.
// Cert-manager Issuers and Certificates are owned by the RayCluster via controller
// references, so Kubernetes garbage collection handles their deletion automatically.
// Each Certificate's Secret is patched with an owner reference to its Certificate so
// that Secrets are also GC'd when the Certificate is deleted (no finalizer needed).
func (r *RayClusterMTLSController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &rayv1.RayCluster{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if instance.DeletionTimestamp != nil && !instance.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if manager := utils.ManagedByExternalController(instance.Spec.ManagedBy); manager != nil {
		ctrl.LoggerFrom(ctx).Info("Skipping RayCluster managed by a custom controller", "managed-by", manager)
		return ctrl.Result{}, nil
	}

	// mTLS not enabled: nothing to do.
	if !utils.IsTLSEnabled(&instance.Spec) {
		return ctrl.Result{}, nil
	}

	return r.handleAutoGenerate(ctx, instance, ctrl.LoggerFrom(ctx))
}

// handleAutoGenerate creates the full cert-manager PKI chain (self-signed issuer,
// CA certificate, CA issuer, head/worker leaf certificates) and monitors readiness.
func (r *RayClusterMTLSController) handleAutoGenerate(ctx context.Context, instance *rayv1.RayCluster, logger logr.Logger) (ctrl.Result, error) {
	logger.Info("Reconciling auto-generated mTLS for RayCluster", "cluster", instance.Name)

	steps := []struct {
		fn   func(context.Context, *rayv1.RayCluster) error
		name string
	}{
		{r.reconcileSelfSignedIssuer, "self-signed issuer"},
		{r.reconcileCACertificate, "CA certificate"},
		{r.reconcileCAIssuer, "CA issuer"},
		{r.reconcileHeadCertificate, "head certificate"},
		{r.reconcileWorkerCertificate, "worker certificate"},
	}

	for _, step := range steps {
		if err := step.fn(ctx, instance); err != nil {
			logger.Error(err, "Failed to reconcile TLS resource", "resource", step.name)
			r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.MTLSFailedToReconcile), string(utils.ReconcileAction),
				"Failed to reconcile mTLS %s: %v", step.name, err)
			return ctrl.Result{RequeueAfter: mtlsDefaultRequeueDuration}, err
		}
	}

	// Verify certificates are ready before declaring success.
	ready, err := r.checkTLSCertificatesReady(ctx, instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: mtlsDefaultRequeueDuration}, err
	}
	if !ready {
		logger.Info("TLS certificates not ready yet, will requeue")
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.MTLSCertsNotReady), string(utils.ReconcileAction),
			"Waiting for cert-manager to issue mTLS certificates")
		return ctrl.Result{RequeueAfter: mtlsDefaultRequeueDuration}, nil
	}

	r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.MTLSPKIReady), string(utils.ReconcileAction),
		"mTLS PKI is ready: CA, head, and worker certificates issued")

	r.checkTLSCertificateExpiry(ctx, instance)

	return ctrl.Result{RequeueAfter: mtlsPeriodicCheckDuration}, nil
}

// ensureSecretOwnedByCertificate patches the Secret created by cert-manager so that it
// has an owner reference to its Certificate. This ensures the Secret is automatically
// garbage-collected when the Certificate is deleted (which itself is GC'd when the
// RayCluster is deleted), without needing an explicit cleanup finalizer.
//
// If the Secret does not yet exist (cert-manager hasn't issued it), this is a no-op;
// the next reconcile will retry.
func (r *RayClusterMTLSController) ensureSecretOwnedByCertificate(ctx context.Context, cert *certmanagerv1.Certificate) error {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: cert.Spec.SecretName, Namespace: cert.Namespace}, secret); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if err := controllerutil.SetOwnerReference(cert, secret, r.Scheme); err != nil {
		return err
	}
	return r.Update(ctx, secret)
}

// reconcileIssuer creates an issuer with the given configuration if it does not exist.
// Issuer specs are static once created, so no update is needed.
func (r *RayClusterMTLSController) reconcileIssuer(
	ctx context.Context,
	instance *rayv1.RayCluster,
	issuerName string,
	component string,
	issuerConfig certmanagerv1.IssuerConfig,
) error {
	existing := &certmanagerv1.Issuer{}
	err := r.Get(ctx, client.ObjectKey{Name: issuerName, Namespace: instance.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	issuer := &certmanagerv1.Issuer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      issuerName,
			Namespace: instance.Namespace,
			Labels:    tlsResourceLabels(instance.Name, component),
		},
		Spec: certmanagerv1.IssuerSpec{
			IssuerConfig: issuerConfig,
		},
	}
	if err := controllerutil.SetControllerReference(instance, issuer, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, issuer)
}

// reconcileSelfSignedIssuer creates the self-signed issuer used to bootstrap the CA if it does not exist.
// The issuer spec is static once created so no update is needed.
func (r *RayClusterMTLSController) reconcileSelfSignedIssuer(ctx context.Context, instance *rayv1.RayCluster) error {
	return r.reconcileIssuer(
		ctx,
		instance,
		utils.GetSelfSignedIssuerName(instance.Name),
		"selfsigned-issuer",
		certmanagerv1.IssuerConfig{
			SelfSigned: &certmanagerv1.SelfSignedIssuer{},
		},
	)
}

// reconcileCACertificate creates the root CA certificate signed by the self-signed issuer if it does not exist.
// The CA cert spec is static once created so no update is needed, avoiding spurious re-issuance due to
// reflect.DeepEqual disagreeing with cert-manager's internal field normalization.
func (r *RayClusterMTLSController) reconcileCACertificate(ctx context.Context, instance *rayv1.RayCluster) error {
	certName := utils.GetCACertName(instance.Name)
	caSecretName := utils.GetCASecretName(instance.Name, instance.UID)

	existing := &certmanagerv1.Certificate{}
	err := r.Get(ctx, client.ObjectKey{Name: certName, Namespace: instance.Namespace}, existing)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		cert := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      certName,
				Namespace: instance.Namespace,
				Labels:    tlsResourceLabels(instance.Name, "ca-certificate"),
			},
			Spec: certmanagerv1.CertificateSpec{
				SecretName: caSecretName,
				IsCA:       true,
				CommonName: fmt.Sprintf("ray-ca-%s", instance.Name),
				PrivateKey: &certmanagerv1.CertificatePrivateKey{
					Algorithm: certmanagerv1.RSAKeyAlgorithm,
					Size:      4096,
				},
				Duration: &metav1.Duration{
					Duration: 24 * time.Hour * 3650, // ~10 years
				},
				RenewBefore: &metav1.Duration{
					Duration: 24 * time.Hour * 30, // 30 days before expiry
				},
				Usages: []certmanagerv1.KeyUsage{
					certmanagerv1.UsageCertSign,
					certmanagerv1.UsageCRLSign,
				},
				IssuerRef: cmmeta.IssuerReference{
					Name:  utils.GetSelfSignedIssuerName(instance.Name),
					Kind:  "Issuer",
					Group: "cert-manager.io",
				},
			},
		}
		if err := controllerutil.SetControllerReference(instance, cert, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cert)
	}
	return r.ensureSecretOwnedByCertificate(ctx, existing)
}

// reconcileCAIssuer creates the issuer backed by the generated CA certificate's secret if it does not exist.
// The issuer spec is static once created (keyed on RayCluster UID) so no update is needed.
func (r *RayClusterMTLSController) reconcileCAIssuer(ctx context.Context, instance *rayv1.RayCluster) error {
	return r.reconcileIssuer(
		ctx,
		instance,
		utils.GetCAIssuerName(instance.Name),
		"ca-issuer",
		certmanagerv1.IssuerConfig{
			CA: &certmanagerv1.CAIssuer{
				SecretName: utils.GetCASecretName(instance.Name, instance.UID),
			},
		},
	)
}

// reconcileHeadCertificate creates or updates the head node leaf certificate.
// Only head pod IPs are added as IP SANs — worker IPs are not included, since
// workers reach the head via the service DNS name (already a DNS SAN). This
// prevents a worker scale event from triggering a head cert reissue.
func (r *RayClusterMTLSController) reconcileHeadCertificate(ctx context.Context, instance *rayv1.RayCluster) error {
	certName := utils.GetTLSCertName(instance.Name, rayv1.HeadNode)
	secretName := utils.GetTLSSecretName(instance.Name, rayv1.HeadNode)

	podIPs, err := r.getPodIPs(ctx, instance, rayv1.HeadNode)
	if err != nil {
		return err
	}

	desiredLabels := tlsResourceLabels(instance.Name, "head-certificate")
	desiredDNSNames := uniqueStrings([]string{
		"localhost",
		utils.GenerateFQDNServiceName(ctx, *instance, instance.Namespace),
	})
	sort.Strings(desiredDNSNames)
	desiredIPAddresses := normalizeIPs(podIPs)

	// Known limitation: Ray reads TLS files once at startup and does not hot-reload them.
	// When cert-manager renews this certificate (updating the Secret), running Ray pods
	// will continue using the old certificate until restarted. The RenewBefore window
	// (15 days before the 90-day expiry) provides time for a rolling restart, but no
	// automatic restart mechanism is currently implemented.
	desiredSpec := certmanagerv1.CertificateSpec{
		SecretName:  secretName,
		Duration:    &metav1.Duration{Duration: 2160 * time.Hour},
		RenewBefore: &metav1.Duration{Duration: 360 * time.Hour},
		DNSNames:    desiredDNSNames,
		IPAddresses: desiredIPAddresses,
		Usages:      leafCertKeyUsages(),
		IssuerRef: cmmeta.IssuerReference{
			Name:  utils.GetCAIssuerName(instance.Name),
			Kind:  "Issuer",
			Group: "cert-manager.io",
		},
	}

	existing := &certmanagerv1.Certificate{}
	err = r.Get(ctx, client.ObjectKey{Name: certName, Namespace: instance.Namespace}, existing)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		cert := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      certName,
				Namespace: instance.Namespace,
				Labels:    desiredLabels,
			},
			Spec: desiredSpec,
		}
		if err := controllerutil.SetControllerReference(instance, cert, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cert)
	}

	if !reflect.DeepEqual(existing.Labels, desiredLabels) || certificateNeedsUpdate(&existing.Spec, &desiredSpec) {
		existing.Labels = desiredLabels
		existing.Spec = desiredSpec
		if err := controllerutil.SetControllerReference(instance, existing, r.Scheme); err != nil {
			return err
		}
		logger := ctrl.LoggerFrom(ctx)
		logger.Info("Updating head certificate", "certificate", certName, "ipAddresses", desiredIPAddresses)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.MTLSCertificatesUpdated), string(utils.UpdateAction),
			"Updated head certificate SANs (IPs: %v)", desiredIPAddresses)
		if err := r.Update(ctx, existing); err != nil {
			return err
		}
	}
	return r.ensureSecretOwnedByCertificate(ctx, existing)
}

// reconcileWorkerCertificate creates or updates the worker node leaf certificate.
// All worker pods share this single certificate. Only worker pod IPs are included
// in the SANs; the head IP is not needed here since GCS connects to workers by
// pod IP, not by the head service address.
func (r *RayClusterMTLSController) reconcileWorkerCertificate(ctx context.Context, instance *rayv1.RayCluster) error {
	certName := utils.GetTLSCertName(instance.Name, rayv1.WorkerNode)
	secretName := utils.GetTLSSecretName(instance.Name, rayv1.WorkerNode)

	podIPs, err := r.getPodIPs(ctx, instance, rayv1.WorkerNode)
	if err != nil {
		return err
	}

	desiredLabels := tlsResourceLabels(instance.Name, "worker-certificate")
	desiredDNSNames := uniqueStrings([]string{"localhost"})
	sort.Strings(desiredDNSNames)
	desiredIPAddresses := normalizeIPs(podIPs)

	// Known limitation: Ray reads TLS files once at startup and does not hot-reload them.
	// See the equivalent comment in reconcileHeadCertificate for details.
	desiredSpec := certmanagerv1.CertificateSpec{
		SecretName:  secretName,
		Duration:    &metav1.Duration{Duration: 2160 * time.Hour},
		RenewBefore: &metav1.Duration{Duration: 360 * time.Hour},
		DNSNames:    desiredDNSNames,
		IPAddresses: desiredIPAddresses,
		Usages:      leafCertKeyUsages(),
		IssuerRef: cmmeta.IssuerReference{
			Name:  utils.GetCAIssuerName(instance.Name),
			Kind:  "Issuer",
			Group: "cert-manager.io",
		},
	}

	existing := &certmanagerv1.Certificate{}
	err = r.Get(ctx, client.ObjectKey{Name: certName, Namespace: instance.Namespace}, existing)
	if err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		cert := &certmanagerv1.Certificate{
			ObjectMeta: metav1.ObjectMeta{
				Name:      certName,
				Namespace: instance.Namespace,
				Labels:    desiredLabels,
			},
			Spec: desiredSpec,
		}
		if err := controllerutil.SetControllerReference(instance, cert, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, cert)
	}

	if !reflect.DeepEqual(existing.Labels, desiredLabels) || certificateNeedsUpdate(&existing.Spec, &desiredSpec) {
		existing.Labels = desiredLabels
		existing.Spec = desiredSpec
		if err := controllerutil.SetControllerReference(instance, existing, r.Scheme); err != nil {
			return err
		}
		logger := ctrl.LoggerFrom(ctx)
		logger.Info("Updating worker certificate", "certificate", certName, "ipAddresses", desiredIPAddresses)
		r.Recorder.Eventf(instance, nil, corev1.EventTypeNormal, string(utils.MTLSCertificatesUpdated), string(utils.UpdateAction),
			"Updated worker certificate SANs (IPs: %v)", desiredIPAddresses)
		if err := r.Update(ctx, existing); err != nil {
			return err
		}
	}
	return r.ensureSecretOwnedByCertificate(ctx, existing)
}

// getPodIPs collects pod IPs for the given node type within the RayCluster.
// Filtering by node type prevents worker IPs from appearing in the head cert
// (which would trigger a cert reissue on every worker scale event) and vice versa.
func (r *RayClusterMTLSController) getPodIPs(ctx context.Context, instance *rayv1.RayCluster, nodeType rayv1.RayNodeType) ([]string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(instance.Namespace),
		client.MatchingLabels(map[string]string{
			utils.RayClusterLabelKey:  instance.Name,
			utils.RayNodeTypeLabelKey: string(nodeType),
		})); err != nil {
		return nil, err
	}
	var podIPs []string
	for _, pod := range pods.Items {
		if !pod.DeletionTimestamp.IsZero() ||
			pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Status.PodIP != "" {
			podIPs = append(podIPs, pod.Status.PodIP)
		}
	}
	return podIPs, nil
}

// checkTLSCertificatesReady verifies that both head and worker certificates
// have been issued and are in Ready state.
func (r *RayClusterMTLSController) checkTLSCertificatesReady(ctx context.Context, instance *rayv1.RayCluster) (bool, error) {
	certNames := []string{
		utils.GetTLSCertName(instance.Name, rayv1.HeadNode),
		utils.GetTLSCertName(instance.Name, rayv1.WorkerNode),
	}
	for _, name := range certNames {
		cert := &certmanagerv1.Certificate{}
		if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: instance.Namespace}, cert); err != nil {
			return false, err
		}
		if !isCertificateReady(cert) {
			ctrl.LoggerFrom(ctx).Info("TLS certificate not ready yet", "certificate", name)
			return false, nil
		}
	}
	return true, nil
}

// checkTLSCertificateExpiry emits a warning Event for certificates expiring within 24 hours.
// The Kubernetes event recorder deduplicates identical events, so this is safe to call on
// every reconcile without flooding the event stream.
func (r *RayClusterMTLSController) checkTLSCertificateExpiry(ctx context.Context, instance *rayv1.RayCluster) {
	certNames := []string{
		utils.GetTLSCertName(instance.Name, rayv1.HeadNode),
		utils.GetTLSCertName(instance.Name, rayv1.WorkerNode),
		utils.GetCACertName(instance.Name),
	}
	for _, name := range certNames {
		cert := &certmanagerv1.Certificate{}
		if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: instance.Namespace}, cert); err != nil {
			continue
		}
		if cert.Status.NotAfter != nil {
			remaining := time.Until(cert.Status.NotAfter.Time)
			if remaining < 24*time.Hour {
				r.Recorder.Eventf(instance, nil, corev1.EventTypeWarning, string(utils.MTLSCertificateExpiringSoon), string(utils.ReconcileAction),
					"TLS certificate %s expires in %.1f hours; Ray does not hot-reload certs so a pod restart will be needed after renewal",
					name, remaining.Hours())
			}
		}
	}
}

// SetupWithManager registers the mTLS controller with the manager.
func (r *RayClusterMTLSController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("raycluster-mtls").
		For(&rayv1.RayCluster{}).
		Owns(&certmanagerv1.Issuer{}).
		Owns(&certmanagerv1.Certificate{}).
		WithOptions(controller.Options{
			LogConstructor: func(request *reconcile.Request) logr.Logger {
				logger := ctrl.Log.WithName("controllers").WithName("RayClusterMTLS")
				if request != nil {
					logger = logger.WithValues("RayCluster", request.NamespacedName)
				}
				return logger
			},
		}).
		Complete(r)
}

// --- Shared helpers (also used by tests) ---

// leafCertKeyUsages returns the key usages for head and worker leaf certificates.
func leafCertKeyUsages() []certmanagerv1.KeyUsage {
	return []certmanagerv1.KeyUsage{
		certmanagerv1.UsageDigitalSignature,
		certmanagerv1.UsageKeyEncipherment,
		certmanagerv1.UsageServerAuth,
		certmanagerv1.UsageClientAuth,
	}
}

// isCertificateReady returns true only when the Certificate's Ready condition is True
// AND the condition reflects the current generation of the spec. Checking
// ObservedGeneration on the condition prevents treating a stale Ready=True as sufficient
// immediately after a SAN update (e.g. adding a pod IP), before cert-manager has
// reissued the Secret.
func isCertificateReady(cert *certmanagerv1.Certificate) bool {
	for _, cond := range cert.Status.Conditions {
		if cond.Type == certmanagerv1.CertificateConditionReady {
			return cond.Status == cmmeta.ConditionTrue &&
				cond.ObservedGeneration == cert.Generation
		}
	}
	return false
}

// setsEqual compares two string slices as sets (order-insensitive).
func setsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	for _, v := range b {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

// certificateNeedsUpdate checks if a certificate's DNS names or IP addresses
// differ from the desired values using set-based comparison.
func certificateNeedsUpdate(existing, desired *certmanagerv1.CertificateSpec) bool {
	return !setsEqual(existing.DNSNames, desired.DNSNames) ||
		!setsEqual(existing.IPAddresses, desired.IPAddresses)
}

// normalizeIPs returns a sorted, de-duplicated set of IPs. Always includes 127.0.0.1.
func normalizeIPs(podIPs []string) []string {
	s := sets.New(podIPs...)
	s.Delete("")
	s.Insert("127.0.0.1")
	return sets.List(s)
}

// uniqueStrings returns the input with duplicates removed, preserving order.
func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// tlsResourceLabels returns standard labels for cert-manager resources owned by a RayCluster.
func tlsResourceLabels(clusterName, component string) map[string]string {
	return map[string]string{
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
		"app.kubernetes.io/component":           component,
		utils.RayClusterLabelKey:                clusterName,
	}
}
