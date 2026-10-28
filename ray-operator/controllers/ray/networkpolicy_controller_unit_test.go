package ray

import (
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

var (
	testScheme                   *runtime.Scheme
	testNetworkPolicyController  *NetworkPolicyController
	testRayClusterBasic          *rayv1.RayCluster
	testRayClusterDenyAllIngress *rayv1.RayCluster
	testRayClusterDenyAllEgress  *rayv1.RayCluster
	testRayClusterWithRayJob     *rayv1.RayCluster
)

func setupNetworkPolicyTest(t *testing.T) {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, true)
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	testScheme = runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(testScheme))
	require.NoError(t, networkingv1.AddToScheme(testScheme))
	testNetworkPolicyController = &NetworkPolicyController{
		Scheme: testScheme,
		Client: clientFake.NewClientBuilder().
			WithScheme(testScheme).
			Build(),
	}

	// Basic cluster — DenyAll mode (default).
	testRayClusterBasic = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
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
		},
	}

	// Cluster configured with DenyAllIngress mode.
	testRayClusterDenyAllIngress = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-ingress",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllIngress),
			},
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
		},
	}

	// Cluster configured with DenyAllEgress mode.
	testRayClusterDenyAllEgress = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-egress",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllEgress),
			},
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
		},
	}

	// Cluster owned by a RayJob (K8sJobMode — has a standalone submitter pod).
	testRayClusterWithRayJob = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-rayjob",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayJobSubmissionModeLabelKey: string(rayv1.K8sJobMode),
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayJob",
					Name:       "test-job",
					UID:        "12345",
					Controller: new(true),
				},
			},
		},
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
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
		},
	}
}

// TestBuildHeadNetworkPolicy_DenyAll verifies the head NetworkPolicy in default DenyAll mode.
func TestBuildHeadNetworkPolicy_DenyAll(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterBasic, rayv1.NetworkPolicyDenyAll)

	assert.Equal(t, "test-cluster-head", policy.Name)
	assert.Equal(t, "default", policy.Namespace)

	// Labels must identify the cluster, the operator, and the head group.
	expectedLabels := map[string]string{
		utils.RayClusterLabelKey:                testRayClusterBasic.Name,
		utils.RayNodeGroupLabelKey:              utils.RayNodeHeadGroupLabelValue,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
	}
	assert.Equal(t, expectedLabels, policy.Labels)

	// PodSelector must target head pods of this cluster.
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:  "test-cluster",
		utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
	}, policy.Spec.PodSelector.MatchLabels)

	// DenyAll restricts both directions.
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)

	// 1 base ingress rule: intra-cluster only (operator access removed, injected via webhook).
	assert.Len(t, policy.Spec.Ingress, 1)

	// 1 base egress rule: intra-cluster only (DNS no longer baked in).
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildHeadNetworkPolicy_DenyAllIngress verifies that DenyAllIngress omits the Egress policy type.
func TestBuildHeadNetworkPolicy_DenyAllIngress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterDenyAllIngress, rayv1.NetworkPolicyDenyAllIngress)

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Len(t, policy.Spec.Ingress, 1)
	assert.Empty(t, policy.Spec.Egress)
}

// TestBuildHeadNetworkPolicy_DenyAllEgress verifies that DenyAllEgress only restricts egress.
func TestBuildHeadNetworkPolicy_DenyAllEgress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterDenyAllEgress, rayv1.NetworkPolicyDenyAllEgress)

	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Empty(t, policy.Spec.Ingress)
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildWorkerNetworkPolicy_DenyAll verifies the worker NetworkPolicy in DenyAll mode.
func TestBuildWorkerGroupNetworkPolicy_DenyAll(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(testRayClusterBasic, rayv1.NetworkPolicyDenyAll, "small-group")

	assert.Equal(t, "test-cluster-workers-small-group", policy.Name)
	assert.Equal(t, "default", policy.Namespace)

	// Labels must identify the cluster, the operator, and the worker group.
	expectedLabels := map[string]string{
		utils.RayClusterLabelKey:                testRayClusterBasic.Name,
		utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
		utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
		utils.RayNodeGroupLabelKey:              "small-group",
	}
	assert.Equal(t, expectedLabels, policy.Labels)

	// PodSelector must target this group's worker pods only.
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:   "test-cluster",
		utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
		utils.RayNodeGroupLabelKey: "small-group",
	}, policy.Spec.PodSelector.MatchLabels)

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)

	// Workers only allow intra-cluster ingress (no Ports = all ports).
	require.Len(t, policy.Spec.Ingress, 1)
	require.Len(t, policy.Spec.Ingress[0].From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		policy.Spec.Ingress[0].From[0].PodSelector.MatchLabels)
	assert.Empty(t, policy.Spec.Ingress[0].Ports)

	// 1 base egress rule: intra-cluster only (DNS no longer baked in).
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildWorkerGroupNetworkPolicy_DenyAllIngress verifies no egress is added for DenyAllIngress mode.
func TestBuildWorkerGroupNetworkPolicy_DenyAllIngress(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(testRayClusterDenyAllIngress, rayv1.NetworkPolicyDenyAllIngress, "small-group")

	assert.Contains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeIngress)
	assert.NotContains(t, policy.Spec.PolicyTypes, networkingv1.PolicyTypeEgress)
	assert.Len(t, policy.Spec.Ingress, 1)
	assert.Empty(t, policy.Spec.Egress)
}

// TestBuildWorkerGroupNetworkPolicy_FallsBackToWorkerRules verifies that a group
// without a workerGroups entry uses the default Worker rules.
func TestBuildWorkerGroupNetworkPolicy_FallsBackToWorkerRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	// 1 base intra-cluster + 1 custom = 2.
	require.Len(t, policy.Spec.Ingress, 2)
	require.Len(t, policy.Spec.Ingress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)
}

// TestBuildWorkerGroupNetworkPolicy_ReplacesWorkerRules verifies that a workerGroups
// entry's rules replace the default Worker rules for that group.
func TestBuildWorkerGroupNetworkPolicy_ReplacesWorkerRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		EgressRules: []networkingv1.NetworkPolicyEgressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}
	cluster.Spec.NetworkPolicy.WorkerGroups = []rayv1.WorkerGroupNetworkPolicyRules{
		{
			GroupName: "gpu-group",
			NetworkPolicyRules: rayv1.NetworkPolicyRules{
				IngressRules: []networkingv1.NetworkPolicyIngressRule{
					{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "gpu-group")

	assert.Equal(t, "test-cluster-workers-gpu-group", policy.Name)
	assert.Equal(t, map[string]string{
		utils.RayClusterLabelKey:   "test-cluster",
		utils.RayNodeTypeLabelKey:  string(rayv1.WorkerNode),
		utils.RayNodeGroupLabelKey: "gpu-group",
	}, policy.Spec.PodSelector.MatchLabels)

	// 1 base intra-cluster + 1 custom ingress from the group entry.
	require.Len(t, policy.Spec.Ingress, 2)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)

	// base intra-cluster rule only.
	assert.Len(t, policy.Spec.Egress, 1)
}

// TestBuildBaseIngressRules verifies the shared intra-cluster ingress rule used by both head and workers.
func TestBuildBaseIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildBaseIngressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.From[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster rule must allow all ports (no Ports field)")
}

// TestBuildHeadIngressRules verifies the head base ingress rules: intra-cluster only.
// The operator access rule is intentionally absent — platforms inject it via
// spec.networkPolicy.head.ingressRules (e.g. a mutating webhook).
func TestBuildHeadIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildHeadIngressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	// Rule 0: intra-cluster — no ports, pod selector matching the cluster label.
	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.From, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.From[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster rule must allow all ports (no Ports field)")
}

// TestBuildRayJobPeer verifies that buildRayJobPeer returns a peer for RayJob-owned clusters
// and nil for standalone clusters.
func TestBuildRayJobPeer(t *testing.T) {
	setupNetworkPolicyTest(t)

	peer := testNetworkPolicyController.buildRayJobPeer(testRayClusterBasic)
	assert.Nil(t, peer, "Standalone cluster should not have a RayJob peer")

	peer = testNetworkPolicyController.buildRayJobPeer(testRayClusterWithRayJob)
	require.NotNil(t, peer, "K8sJobMode RayJob-owned cluster should have a RayJob peer")
	assert.Equal(t, map[string]string{
		utils.RayOriginatedFromCRNameLabelKey: "test-job",
		utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
	}, peer.PodSelector.MatchLabels)

	// Non-K8sJobMode RayJob-owned cluster: no standalone submitter pod, so no peer.
	sidecarCluster := testRayClusterWithRayJob.DeepCopy()
	sidecarCluster.Labels[utils.RayJobSubmissionModeLabelKey] = string(rayv1.SidecarMode)
	peer = testNetworkPolicyController.buildRayJobPeer(sidecarCluster)
	assert.Nil(t, peer, "Non-K8sJobMode RayJob-owned cluster should not have a RayJob peer")
}

// TestBuildHeadIngressRules_WithRayJobOwner verifies that a RayJob-owned cluster gets
// a per-job submitter ingress rule (2 rules total: intra-cluster + submitter).
func TestBuildHeadIngressRules_WithRayJobOwner(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildHeadIngressRules(testRayClusterWithRayJob)
	require.Len(t, rules, 2)

	submitterRule := rules[1]
	require.Len(t, submitterRule.From, 1)
	assert.Equal(t, map[string]string{
		utils.RayOriginatedFromCRNameLabelKey: "test-job",
		utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
	}, submitterRule.From[0].PodSelector.MatchLabels)
	require.Len(t, submitterRule.Ports, 1)
	dashboardPort := intstr.FromInt32(utils.DefaultDashboardPort)
	assert.Equal(t, &dashboardPort, submitterRule.Ports[0].Port)
}

// TestBuildBaseEgressRules verifies the single base egress rule (intra-cluster only).
func TestBuildBaseEgressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	rules := testNetworkPolicyController.buildBaseEgressRules(testRayClusterBasic)
	require.Len(t, rules, 1)

	// Rule 0: intra-cluster egress — no ports, pod selector matching the cluster label.
	intraClusterRule := rules[0]
	require.Len(t, intraClusterRule.To, 1)
	assert.Equal(t, map[string]string{utils.RayClusterLabelKey: "test-cluster"},
		intraClusterRule.To[0].PodSelector.MatchLabels)
	assert.Empty(t, intraClusterRule.Ports, "Intra-cluster egress must allow all ports (no Ports field)")
}

// TestBuildHeadNetworkPolicy_WithRayJob verifies that a RayJob-owned cluster gets
// 2 ingress rules: intra-cluster + per-job submitter.
func TestBuildHeadNetworkPolicy_WithRayJob(t *testing.T) {
	setupNetworkPolicyTest(t)

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(testRayClusterWithRayJob, rayv1.NetworkPolicyDenyAll)

	require.Len(t, policy.Spec.Ingress, 2)
}

// TestBuildHeadNetworkPolicy_CustomIngressRules verifies that custom Head.IngressRules are appended after base rules.
func TestBuildHeadNetworkPolicy_CustomIngressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// 1 base intra-cluster + 1 custom = 2.
	require.Len(t, policy.Spec.Ingress, 2)
	require.Len(t, policy.Spec.Ingress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Ingress[1].Ports[0].Port)
}

// TestBuildHeadNetworkPolicy_CustomEgressRules verifies that custom Head.EgressRules are appended after base egress.
func TestBuildHeadNetworkPolicy_CustomEgressRules(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(8080)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		EgressRules: []networkingv1.NetworkPolicyEgressRule{
			{
				Ports: []networkingv1.NetworkPolicyPort{
					{Protocol: &tcpProto, Port: &customPort},
				},
			},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// 1 base egress + 1 custom = 2.
	require.Len(t, policy.Spec.Egress, 2)
	require.Len(t, policy.Spec.Egress[1].Ports, 1)
	assert.Equal(t, &customPort, policy.Spec.Egress[1].Ports[0].Port)
}

// TestGetHeadPort_DefaultFallback verifies the default port is returned when rayStartParams has no override.
func TestGetHeadPort_DefaultFallback(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{}

	port := testNetworkPolicyController.getHeadPort(cluster, "dashboard-port", utils.DefaultDashboardPort)
	assert.Equal(t, int32(utils.DefaultDashboardPort), port)
}

// TestGetHeadPort_CustomPort verifies that a port set via rayStartParams is returned.
func TestGetHeadPort_CustomPort(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.HeadGroupSpec.RayStartParams = map[string]string{
		"dashboard-port": "9265",
	}

	port := testNetworkPolicyController.getHeadPort(cluster, "dashboard-port", utils.DefaultDashboardPort)
	assert.Equal(t, int32(9265), port)
}

// TestBuildNetworkPolicy_LongClusterName verifies NetworkPolicy names are constructed correctly for long names.
func TestBuildNetworkPolicy_LongClusterName(t *testing.T) {
	setupNetworkPolicyTest(t)

	cluster := testRayClusterBasic.DeepCopy()
	cluster.Name = strings.Repeat("a", utils.MaxRayClusterNameLength)

	headPolicy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)
	workerPolicy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	assert.Equal(t, cluster.Name+"-head", headPolicy.Name)
	assert.Equal(t, cluster.Name+"-workers-small-group", workerPolicy.Name)
}

// TestBuildHeadNetworkPolicy_WorkerRulesNotLeaked verifies that worker-specific
// IngressRules are not applied to the head NetworkPolicy.
func TestBuildHeadNetworkPolicy_WorkerRulesNotLeaked(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Worker = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}

	policy := testNetworkPolicyController.buildHeadNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll)

	// Head NP must only have the intra-cluster base rule — no worker rules.
	require.Len(t, policy.Spec.Ingress, 1)
}

// TestBuildWorkerGroupNetworkPolicy_HeadRulesNotLeaked verifies that head-specific
// IngressRules are not applied to the worker NetworkPolicy.
func TestBuildWorkerGroupNetworkPolicy_HeadRulesNotLeaked(t *testing.T) {
	setupNetworkPolicyTest(t)

	customPort := intstr.FromInt32(9999)
	tcpProto := corev1.ProtocolTCP
	cluster := testRayClusterBasic.DeepCopy()
	cluster.Spec.NetworkPolicy.Head = &rayv1.NetworkPolicyRules{
		IngressRules: []networkingv1.NetworkPolicyIngressRule{
			{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcpProto, Port: &customPort}}},
		},
	}

	policy := testNetworkPolicyController.buildWorkerGroupNetworkPolicy(cluster, rayv1.NetworkPolicyDenyAll, "small-group")

	// Worker NP must only have the intra-cluster base rule — no head rules.
	require.Len(t, policy.Spec.Ingress, 1)
}

// TestDeleteStaleNetworkPolicies verifies that only NetworkPolicies that are both
// stale (not in desiredNames) and owned by the RayCluster are deleted. This is what
// removes a worker group's NetworkPolicy after the group is dropped from the spec.
func TestDeleteStaleNetworkPolicies(t *testing.T) {
	setupNetworkPolicyTest(t)

	instance := testRayClusterBasic.DeepCopy()
	instance.UID = "test-uid"

	newNetworkPolicy := func(name string, owned bool) *networkingv1.NetworkPolicy {
		networkPolicy := &networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: instance.Namespace,
				Labels:    networkPolicyLabels(instance, "some-group"),
			},
		}
		if owned {
			require.NoError(t, controllerutil.SetControllerReference(instance, networkPolicy, testScheme))
		}
		return networkPolicy
	}

	desired := newNetworkPolicy("test-cluster-workers-small-group", true)
	stale := newNetworkPolicy("test-cluster-workers-removed-group", true)
	foreign := newNetworkPolicy("test-cluster-workers-foreign-group", false)

	controller := &NetworkPolicyController{
		Scheme:   testScheme,
		Recorder: &events.FakeRecorder{},
		Client: clientFake.NewClientBuilder().
			WithScheme(testScheme).
			WithObjects(desired, stale, foreign).
			Build(),
	}

	err := controller.deleteStaleNetworkPolicies(t.Context(), instance, map[string]bool{desired.Name: true})
	require.NoError(t, err)

	remaining := &networkingv1.NetworkPolicyList{}
	require.NoError(t, controller.List(t.Context(), remaining, client.InNamespace(instance.Namespace)))

	names := make([]string, 0, len(remaining.Items))
	for _, item := range remaining.Items {
		names = append(names, item.Name)
	}
	// The stale owned policy is gone; the desired one and the unowned one survive.
	assert.ElementsMatch(t, []string{desired.Name, foreign.Name}, names)
}
