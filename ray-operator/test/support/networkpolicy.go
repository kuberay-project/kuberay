package support

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// NetworkPolicy rule builders for the e2e tests. Under DenyAll the operator only
// allows intra-cluster traffic, so anything else (DNS, internet, external
// ingress) has to be added by hand here. See the network-policy-deny-all sample.

// DNSEgressRule allows DNS to kube-dns on 53 (UDP+TCP). Ray pods resolve the head
// by FQDN, so without this the cluster won't come up under DenyAll/DenyAllEgress.
func DNSEgressRule() networkingv1.NetworkPolicyEgressRule {
	udp := corev1.ProtocolUDP
	tcp := corev1.ProtocolTCP
	dnsPort := intstr.FromInt32(53)
	return networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{
				NamespaceSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"kubernetes.io/metadata.name": "kube-system",
					},
				},
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"k8s-app": "kube-dns",
					},
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &udp, Port: &dnsPort},
			{Protocol: &tcp, Port: &dnsPort},
		},
	}
}

// AllHostsHTTPSEgressRule allows egress to anywhere on 443, needed when a job
// pulls its runtimeEnv working_dir or pip packages from the internet.
func AllHostsHTTPSEgressRule() networkingv1.NetworkPolicyEgressRule {
	tcp := corev1.ProtocolTCP
	httpsPort := intstr.FromInt32(443)
	return networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{
			{
				IPBlock: &networkingv1.IPBlock{
					CIDR: "0.0.0.0/0",
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &httpsPort},
		},
	}
}

// ServeIngressRule opens the Serve port to every pod in the namespace so an
// external curl pod can hit the app. Empty PodSelector = all pods in the namespace.
func ServeIngressRule(servePort int32) networkingv1.NetworkPolicyIngressRule {
	tcp := corev1.ProtocolTCP
	port := intstr.FromInt32(servePort)
	return networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{
			{
				PodSelector: &metav1.LabelSelector{},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &port},
		},
	}
}

// OperatorIngressRule lets the kuberay operator reach the given head ports. The
// RayService reconciler talks to the head directly: it pushes the Serve config
// and polls app status over the dashboard port (8265), and it probes Serve proxy
// health over the serving port (8000) by dialing the head pod IP. Both are needed
// under DenyAll or the head serve label never flips to true and serve endpoints
// stay empty. The operator runs in a different namespace than the test, so we
// match it by pod label across all namespaces (empty NamespaceSelector). We match
// on app.kubernetes.io/component=kuberay-operator because it's the same in both
// deploy paths; app.kubernetes.io/name differs (kuberay via kustomize vs
// kuberay-operator via the Helm chart).
func OperatorIngressRule(ports ...int32) networkingv1.NetworkPolicyIngressRule {
	tcp := corev1.ProtocolTCP
	npPorts := make([]networkingv1.NetworkPolicyPort, 0, len(ports))
	for _, p := range ports {
		port := intstr.FromInt32(p)
		npPorts = append(npPorts, networkingv1.NetworkPolicyPort{Protocol: &tcp, Port: &port})
	}
	return networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{
			{
				NamespaceSelector: &metav1.LabelSelector{},
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app.kubernetes.io/component": "kuberay-operator",
					},
				},
			},
		},
		Ports: npPorts,
	}
}

// SubmitterIngressRule lets the K8sJobMode submitter pod reach the head dashboard.
// The operator adds this for RayJob-owned clusters, but with clusterSelector the
// RayCluster isn't owned by the RayJob, so we add it ourselves matching the
// submitter's originated-from-cr labels.
func SubmitterIngressRule(rayJobName string, dashboardPort int32) networkingv1.NetworkPolicyIngressRule {
	tcp := corev1.ProtocolTCP
	port := intstr.FromInt32(dashboardPort)
	return networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{
			{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"ray.io/originated-from-cr-name": rayJobName,
						"ray.io/originated-from-crd":     "RayJob",
					},
				},
			},
		},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: &tcp, Port: &port},
		},
	}
}
