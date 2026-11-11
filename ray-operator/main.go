package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	certmanagerv1 "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	"github.com/go-logr/zapr"
	routev1 "github.com/openshift/api/route/v1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	k8szap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/internal/managercache"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	webhooks "github.com/ray-project/kuberay/ray-operator/pkg/webhooks/v1"
)

var (
	scheme    = runtime.NewScheme()
	setupLog  = ctrl.Log.WithName("setup")
	userAgent = fmt.Sprintf("kuberay-operator/%s", utils.KUBERAY_VERSION)
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rayv1.AddToScheme(scheme))
	utilruntime.Must(routev1.Install(scheme))
	utilruntime.Must(batchv1.AddToScheme(scheme))
	utilruntime.Must(configapi.AddToScheme(scheme))
	utilruntime.Must(certmanagerv1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var leaderElectionNamespace string
	var probeAddr string
	var reconcileConcurrency int
	var watchNamespace string
	var forcedClusterUpgrade bool
	var logFile string
	var logFileEncoder string
	var logStdoutEncoder string
	var useKubernetesProxy bool
	var configFile string
	var featureGates string
	var enableBatchScheduler bool
	var batchScheduler string
	var enableMetrics bool
	var qps float64
	var burst int
	var enableNodeEventForwarder bool
	var nodeEventForwarderSources string
	var nodeEventForwarderReasons string
	var nodeEventForwarderTypes string
	var allowedNodeLabels string
	var collectorImage string

	// TODO: remove flag-based config once Configuration API graduates to v1.
	flag.StringVar(&metricsAddr, "metrics-addr", configapi.DefaultMetricsAddr, "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", configapi.DefaultProbeAddr, "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", configapi.DefaultEnableLeaderElection,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "",
		"Namespace where the leader election resource lives. Defaults to the pod namespace if not set.")
	flag.IntVar(&reconcileConcurrency, "reconcile-concurrency", configapi.DefaultReconcileConcurrency, "max concurrency for reconciling")
	flag.StringVar(
		&watchNamespace,
		"watch-namespace",
		"",
		"Specify a list of namespaces to watch for custom resources, separated by commas. If left empty, all namespaces will be watched.",
	)
	flag.BoolVar(&forcedClusterUpgrade, "forced-cluster-upgrade", false,
		"(Deprecated) Forced cluster upgrade flag")
	flag.StringVar(&logFile, "log-file-path", "",
		"Synchronize logs to local file")
	flag.StringVar(&logFileEncoder, "log-file-encoder", "json",
		"Encoder to use for log file. Valid values are 'json' and 'console'. Defaults to 'json'")
	flag.StringVar(&logStdoutEncoder, "log-stdout-encoder", "json",
		"Encoder to use for logging stdout. Valid values are 'json' and 'console'. Defaults to 'json'")
	flag.BoolVar(&enableBatchScheduler, "enable-batch-scheduler", false,
		"(Deprecated) Enable batch scheduler. Currently is volcano, which supports gang scheduler policy. Please use --batch-scheduler instead.")
	flag.StringVar(&batchScheduler, "batch-scheduler", "",
		"Batch scheduler name, supported values are volcano, yunikorn, kai-scheduler.")
	flag.StringVar(&configFile, "config", "", "Path to structured config file. Flags are ignored if config file is set.")
	flag.BoolVar(&useKubernetesProxy, "use-kubernetes-proxy", false,
		"Use Kubernetes proxy subresource when connecting to the Ray Head node.")
	flag.StringVar(&featureGates, "feature-gates", "", "A set of key=value pairs that describe feature gates. E.g. FeatureOne=true,FeatureTwo=false,...")
	flag.BoolVar(&enableMetrics, "enable-metrics", false, "Enable the emission of control plane metrics.")
	flag.Float64Var(&qps, "qps", float64(configapi.DefaultQPS), "The QPS value for the client communicating with the Kubernetes API server.")
	flag.IntVar(&burst, "burst", configapi.DefaultBurst, "The maximum burst for throttling requests from this client to the Kubernetes API server.")
	flag.BoolVar(&enableNodeEventForwarder, "enable-node-event-forwarder", false,
		"Enable the Selective Node Event Forwarder, which re-emits Kubernetes Node events onto the RayCluster custom resources whose Pods run on the affected node.")
	flag.StringVar(&nodeEventForwarderSources, "node-event-forwarder-sources", "",
		"Comma-separated list of event sources to forward Node events from, e.g. node-problem-detector. Empty means all sources.")
	flag.StringVar(&nodeEventForwarderReasons, "node-event-forwarder-reasons", "",
		"Comma-separated list of event reasons to forward, e.g. XIDError,KernelDeadlock. Empty means all reasons.")
	flag.StringVar(&nodeEventForwarderTypes, "node-event-forwarder-types", "",
		"Comma-separated list of event types to forward (Warning, Normal). Empty means all types.")
	flag.StringVar(&allowedNodeLabels, "allowed-node-labels", "",
		"Comma-separated list of node label keys worker groups may deliver as Ray node labels through workerGroupSpecs[].labelRefs. If left empty, every labelRef is rejected.")
	flag.StringVar(&collectorImage, "collector-image", configapi.DefaultCollectorImage,
		"The image of the History Server collector sidecar injected into Ray Pods. "+
			"A RayCluster can override it with spec.historyServerOptions.collectorOptions.image.")

	opts := k8szap.Options{
		TimeEncoder: zapcore.ISO8601TimeEncoder,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	var config configapi.Configuration
	if configFile != "" {
		var err error
		configData, err := os.ReadFile(configFile)
		exitOnError(err, "failed to read config file")

		config, err = decodeConfig(configData, scheme)
		exitOnError(err, "failed to decode config file")
	} else {
		config.MetricsAddr = metricsAddr
		config.ProbeAddr = probeAddr
		config.EnableLeaderElection = &enableLeaderElection
		config.LeaderElectionNamespace = leaderElectionNamespace
		config.ReconcileConcurrency = reconcileConcurrency
		config.WatchNamespace = watchNamespace
		config.LogFile = logFile
		config.LogFileEncoder = logFileEncoder
		config.LogStdoutEncoder = logStdoutEncoder
		config.EnableBatchScheduler = enableBatchScheduler
		config.BatchScheduler = batchScheduler
		config.UseKubernetesProxy = useKubernetesProxy
		config.DeleteRayJobAfterJobFinishes = os.Getenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES) == "true"
		config.EnableMetrics = enableMetrics
		config.QPS = &qps
		config.Burst = &burst
		config.NodeEventForwarder.Enabled = enableNodeEventForwarder
		config.NodeEventForwarder.Sources = splitCommaSeparated(nodeEventForwarderSources)
		config.NodeEventForwarder.Reasons = splitCommaSeparated(nodeEventForwarderReasons)
		config.NodeEventForwarder.Types = splitCommaSeparated(nodeEventForwarderTypes)
		config.AllowedNodeLabels = splitCommaSeparated(allowedNodeLabels)
		config.CollectorImage = collectorImage
	}

	stdoutEncoder, err := newLogEncoder(config.LogStdoutEncoder)
	exitOnError(err, "failed to create log encoder for stdout")
	opts.Encoder = stdoutEncoder

	if config.LogFile != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   config.LogFile,
			MaxSize:    500, // megabytes
			MaxBackups: 10,  // files
			MaxAge:     30,  // days
		}

		fileEncoder, err := newLogEncoder(config.LogFileEncoder)
		exitOnError(err, "failed to create log encoder for file")

		k8sLogger := k8szap.NewRaw(k8szap.UseFlagOptions(&opts))
		zapOpts := append(opts.ZapOpts, zap.AddCallerSkip(1))
		combineLogger := zap.New(zapcore.NewTee(
			k8sLogger.Core(),
			zapcore.NewCore(fileEncoder, zapcore.AddSync(fileWriter), zap.InfoLevel),
		)).WithOptions(zapOpts...)
		combineLoggerR := zapr.NewLogger(combineLogger)

		ctrl.SetLogger(combineLoggerR)

		// By default, the log from kubernetes/client-go is not json format.
		// This will apply the logger to kubernetes/client-go and change it to json format.
		klog.SetLogger(combineLoggerR)
	} else {
		k8sLogger := k8szap.New(k8szap.UseFlagOptions(&opts))
		ctrl.SetLogger(k8sLogger)

		// By default, the log from kubernetes/client-go is not json format.
		// This will apply the logger to kubernetes/client-go and change it to json format.
		klog.SetLogger(k8sLogger)
	}

	if forcedClusterUpgrade {
		setupLog.Info("Deprecated feature flag forced-cluster-upgrade is enabled, which has no effect.")
	}

	if err := utilfeature.DefaultMutableFeatureGate.Set(featureGates); err != nil {
		exitOnError(err, "Unable to set flag gates for known features")
	}
	features.LogFeatureGates(setupLog)

	// validate the batch scheduler configs,
	// exit with error if the configs is invalid.
	if err := configapi.ValidateBatchSchedulerConfig(setupLog, config); err != nil {
		exitOnError(err, "batch scheduler configs validation failed")
	}
	// every allowedNodeLabels entry must be a valid label key
	for _, key := range config.AllowedNodeLabels {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			exitOnError(fmt.Errorf("allowedNodeLabels entry %q: %s", key, strings.Join(errs, "; ")), "allowedNodeLabels validation failed")
		}
	}

	if features.Enabled(features.RayServiceIncrementalUpgrade) {
		utilruntime.Must(gwv1.Install(scheme))
	}

	// Although ContainerRestartPolicy is introduced in Kubernetes v1.34 as an alpha feature, which requires
	// manually enabling the ContainerRestartRules feature gate, it becomes beta in v1.35.
	// We require v1.35+ to avoid asking users to opt in to a separate K8s feature gate.
	// NOTE: this checks the API server version only. Please make sure worker nodes are also v1.35+,
	// as kubelets are allowed to be up to 3 minor versions older per the Kubernetes version skew policy.
	// If ContainerRestartRules is not enabled on a kubelet, the per-container restart policy on the
	// submitter container will be ignored. In this scenario, if the submitter container exit on failure,
	// a 30-second timeout will be applied by the operator and could incorrectly mark the RayJob as Failed
	// even if the Ray job is still running.
	if features.Enabled(features.SidecarSubmitterRestart) {
		serverVersion, err := utils.GetKubernetesVersion()
		if err != nil {
			exitOnError(err, "SidecarSubmitterRestart feature gate enabled but unable to detect K8s version. Feature requires K8s 1.35+.")
		}
		isAtLeast, err := utils.IsK8sVersionAtLeast(serverVersion, 1, 35, 0)
		if err != nil {
			exitOnError(err, "Failed to compare K8s version.")
		}
		if !isAtLeast {
			exitOnError(fmt.Errorf("current version %s is below 1.35", serverVersion.GitVersion),
				"SidecarSubmitterRestart feature gate requires K8s 1.35+")
		}
	}

	// Manager options
	options := ctrl.Options{
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{},
		},
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: config.MetricsAddr,
		},
		HealthProbeBindAddress:  config.ProbeAddr,
		LeaderElection:          *config.EnableLeaderElection,
		LeaderElectionID:        "ray-operator-leader",
		LeaderElectionNamespace: config.LeaderElectionNamespace,
	}

	// Manager Cache
	// Set the informers label selectors to narrow the scope of the resources being watched and cached.
	// This improves the scalability of the system, both for KubeRay itself by reducing the size of the
	// informers cache, and for the API server / etcd, by reducing the number of watch events.
	// For example, KubeRay is only interested in:
	// - the batch Jobs it creates when reconciling RayJobs (app.kubernetes.io/created-by=kuberay-operator), and
	// - Ray-managed Pods (ray.io/node-type in head|worker|redis-cleanup).
	// These labels are provided to the manager cache as selectors for Job and Pod resources.
	selectorsByObject, err := managercache.K8sControllerRuntimeCacheSelectors()
	exitOnError(err, "unable to build manager cache ByObject")
	if features.Enabled(features.RayNodeEventForwarder) && config.NodeEventForwarder.Enabled {
		// Scope the Event informer server-side to Node events in default and kube-system namespaces;
		// without this the event forwarder's watch would receive every Event in the cluster.
		// If types contains a single type (e.g. Warning), it is also filtered server-side.
		selectorsByObject[&corev1.Event{}] = managercache.EventForwarderCacheByObject(config.NodeEventForwarder.Types)
	}
	options.Cache.ByObject = selectorsByObject

	if watchNamespaces := strings.Split(config.WatchNamespace, ","); len(watchNamespaces) == 1 { // It is not possible for len(watchNamespaces) == 0 to be true. The length of `strings.Split("", ",")` is still 1.
		if watchNamespaces[0] == "" {
			setupLog.Info("Flag watchNamespace is not set. Watch custom resources in all namespaces.")
		} else {
			setupLog.Info("Only watch custom resources in the namespace.", "namespace", watchNamespaces[0])
			options.Cache.DefaultNamespaces[watchNamespaces[0]] = cache.Config{}
		}
	} else {
		setupLog.Info("Only watch custom resources in multiple namespaces.", "namespaces", watchNamespaces)
		for _, namespace := range watchNamespaces {
			options.Cache.DefaultNamespaces[namespace] = cache.Config{}
		}
	}

	setupLog.Info("Setup manager")
	restConfig := ctrl.GetConfigOrDie()
	restConfig.UserAgent = userAgent
	restConfig.QPS = float32(*config.QPS)
	restConfig.Burst = *config.Burst

	// Check if cert-manager API is available before registering the mTLS controller.
	// If cert-manager is not installed, the controller's Certificate/Issuer cache would
	// never sync and the manager would fail to start (e.g. in E2E environments without cert-manager).
	certManagerAvailable := certManagerAPIAvailable(restConfig)

	mgr, err := ctrl.NewManager(restConfig, options)
	exitOnError(err, "unable to start manager")

	ctx := ctrl.SetupSignalHandler()

	var rayClusterMetricsManager *metrics.RayClusterMetricsManager
	var rayJobMetricsManager *metrics.RayJobMetricsManager
	var rayServiceMetricsManager *metrics.RayServiceMetricsManager
	var batchSchedulerManager *batchscheduler.SchedulerManager
	if config.EnableMetrics {
		mgrClient := mgr.GetClient()
		rayClusterMetricsManager = metrics.NewRayClusterMetricsManager(ctx, mgrClient)
		rayJobMetricsManager = metrics.NewRayJobMetricsManager(ctx, mgrClient)
		rayServiceMetricsManager = metrics.NewRayServiceMetricsManager(ctx, mgrClient)
		ctrlmetrics.Registry.MustRegister(
			rayClusterMetricsManager,
			rayJobMetricsManager,
			rayServiceMetricsManager,
		)
	}

	batchSchedulerManager, err = batchscheduler.NewSchedulerManager(ctx, config, restConfig, mgr.GetClient())
	exitOnError(err, "unable to create batch scheduler manager")
	batchSchedulerManager.AddToScheme(mgr.GetScheme())

	// TODO: Remove isOpenShift and related reconciler options once Gateway API support is mature
	// and Route-based dashboard access is no longer needed.
	// See: https://github.com/ray-project/kuberay/pull/4365#issuecomment-4143407845
	isOpenShift, err := utils.IsOpenShiftCluster(restConfig)
	exitOnError(err, "unable to detect cluster type (OpenShift vs Kubernetes)")

	rayClusterOptions := ray.RayClusterReconcilerOptions{
		HeadSidecarContainers:    config.HeadSidecarContainers,
		WorkerSidecarContainers:  config.WorkerSidecarContainers,
		IsOpenShift:              isOpenShift,
		UseIngressOnOpenShift:    utils.ShouldUseIngressOnOpenShift(),
		RayClusterMetricsManager: rayClusterMetricsManager,
		BatchSchedulerManager:    batchSchedulerManager,
		DefaultContainerEnvs:     config.DefaultContainerEnvs,
		DefaultPodAnnotations:    config.DefaultPodAnnotations,
		DefaultPodLabels:         config.DefaultPodLabels,
		CollectorImage:           config.CollectorImage,
		CertManagerAvailable:     certManagerAvailable,
	}
	exitOnError(ray.NewReconciler(mgr, rayClusterOptions).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayCluster")

	if features.Enabled(features.RayClusterMTLS) {
		if certManagerAvailable {
			exitOnError(ray.NewRayClusterMTLSController(mgr).SetupWithManager(mgr),
				"unable to create controller", "controller", "RayClusterMTLS")
		} else {
			setupLog.Info("cert-manager API not found; mTLS controller disabled (RayClusters with tlsOptions and no CertificateSecretName will not get auto-generated certs)")
		}
	} else {
		setupLog.Info("RayClusterMTLS feature gate is disabled, skipping mTLS controller setup")
	}

	exitOnError(ray.NewRayServiceReconciler(ctx, mgr, config).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayService")

	rayJobOptions := ray.RayJobReconcilerOptions{
		RayJobMetricsManager:  rayJobMetricsManager,
		BatchSchedulerManager: batchSchedulerManager,
	}
	exitOnError(ray.NewRayJobReconciler(ctx, mgr, rayJobOptions, config).SetupWithManager(mgr, config.ReconcileConcurrency),
		"unable to create controller", "controller", "RayJob")

	if strings.ToLower(os.Getenv("ENABLE_WEBHOOKS")) == "true" {
		exitOnError(webhooks.SetupRayClusterWebhookWithManager(mgr, config),
			"unable to create webhook", "webhook", "RayCluster")
		exitOnError(webhooks.SetupRayJobWebhookWithManager(mgr, config),
			"unable to create webhook", "webhook", "RayJob")
		exitOnError(webhooks.SetupRayServiceWebhookWithManager(mgr, config),
			"unable to create webhook", "webhook", "RayService")
		// node label delivery for worker groups with labelRefs
		exitOnError(webhooks.SetupPodWebhookWithManager(mgr),
			"unable to create webhook", "webhook", "Pod")
		exitOnError(webhooks.SetupPodBindingWebhookWithManager(mgr, config),
			"unable to create webhook", "webhook", "PodBinding")
	}

	if features.Enabled(features.RayCronJob) {
		setupLog.Info("RayCronJob feature gate is enabled, starting RayCronJob controller")
		exitOnError(ray.NewRayCronJobReconciler(mgr).SetupWithManager(mgr, config.ReconcileConcurrency),
			"unable to create controller", "controller", "RayCronJob")
	} else {
		setupLog.Info("RayCronJob feature gate is disabled, skipping RayCronJob controller setup")
	}

	if features.Enabled(features.RayNodeEventForwarder) && config.NodeEventForwarder.Enabled {
		setupLog.Info("RayNodeEventForwarder is enabled, starting EventForwarder controller",
			"sources", config.NodeEventForwarder.Sources, "reasons", config.NodeEventForwarder.Reasons, "types", config.NodeEventForwarder.Types)
		if config.WatchNamespace != "" {
			setupLog.Info("Node event forwarder watches Node events in default and kube-system namespaces despite watchNamespace being set; "+
				"the operator's ServiceAccount requires Role permissions in those namespaces",
				"watchNamespace", config.WatchNamespace)
		}
		eventForwarderOptions := ray.EventForwarderOptions{
			Sources: config.NodeEventForwarder.Sources,
			Reasons: config.NodeEventForwarder.Reasons,
			Types:   config.NodeEventForwarder.Types,
		}
		eventForwarder, err := ray.NewEventForwarderReconciler(mgr, eventForwarderOptions)
		exitOnError(err, "unable to create controller", "controller", "EventForwarder")
		exitOnError(eventForwarder.SetupWithManager(mgr, config.ReconcileConcurrency),
			"unable to setup controller", "controller", "EventForwarder")
	} else {
		setupLog.Info("RayNodeEventForwarder or nodeEventForwarder.enabled is disabled, skipping EventForwarder controller setup")
	}

	if features.Enabled(features.RayClusterNetworkPolicy) {
		setupLog.Info("RayClusterNetworkPolicy feature gate is enabled, starting NetworkPolicy controller")
		networkPolicyController, err := ray.NewNetworkPolicyController(mgr)
		exitOnError(err, "unable to create controller", "controller", "NetworkPolicy")
		exitOnError(networkPolicyController.SetupWithManager(mgr, config.ReconcileConcurrency),
			"unable to setup controller", "controller", "NetworkPolicy")
	} else {
		setupLog.Info("RayClusterNetworkPolicy feature gate is disabled, skipping NetworkPolicy controller setup")
	}

	// +kubebuilder:scaffold:builder

	exitOnError(mgr.AddHealthzCheck("healthz", healthz.Ping), "unable to set up health check")
	exitOnError(mgr.AddReadyzCheck("readyz", healthz.Ping), "unable to set up ready check")

	setupLog.Info("starting manager")
	exitOnError(mgr.Start(ctx), "problem running manager")
}

// certManagerAPIAvailable returns true if the cert-manager.io/v1 API (Certificate, Issuer) is
// available in the cluster. Used to avoid registering the mTLS controller when cert-manager
// is not installed, which would cause manager startup to time out waiting for the Certificate cache to sync.
func certManagerAPIAvailable(restConfig *rest.Config) bool {
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil || discoveryClient == nil {
		return false
	}
	_, resources, err := discoveryClient.ServerGroupsAndResources()
	if err != nil {
		return false
	}
	for _, r := range resources {
		if r.GroupVersion == "cert-manager.io/v1" {
			return true
		}
	}
	return false
}

// splitCommaSeparated splits a comma-separated flag value into its non-empty,
// space-trimmed items. It returns nil for an empty value.
func splitCommaSeparated(value string) []string {
	var items []string
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

func exitOnError(err error, msg string, keysAndValues ...any) {
	if err != nil {
		setupLog.Error(err, msg, keysAndValues...)
		os.Exit(1)
	}
}

// decodeConfig decodes raw config data and returns the Configuration type.
func decodeConfig(configData []byte, scheme *runtime.Scheme) (configapi.Configuration, error) {
	cfg := configapi.Configuration{}
	codecs := serializer.NewCodecFactory(scheme)

	if err := runtime.DecodeInto(codecs.UniversalDecoder(), configData, &cfg); err != nil {
		return cfg, err
	}

	scheme.Default(&cfg)
	return cfg, nil
}

// newLogEncoder returns a zapcore.Encoder based on the encoder type ('json' or 'console')
func newLogEncoder(encoderType string) (zapcore.Encoder, error) {
	pe := zap.NewProductionEncoderConfig()
	pe.EncodeTime = zapcore.ISO8601TimeEncoder

	if encoderType == "json" || encoderType == "" {
		return zapcore.NewJSONEncoder(pe), nil
	}
	if encoderType == "console" {
		return zapcore.NewConsoleEncoder(pe), nil
	}

	return nil, fmt.Errorf("invalid encoder %q (must be 'json' or 'console')", encoderType)
}
