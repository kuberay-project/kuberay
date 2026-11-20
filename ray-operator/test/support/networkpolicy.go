package support

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// NetworkPolicy rule builders for e2e tests.

// DNSEgressRule allows Ray pods to resolve the head through kube-dns.
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

// AllHostsHTTPSEgressRule allows HTTPS egress for external dependencies.
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

// ServeIngressRule allows namespace pods to access the Serve port.
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

// OperatorIngressRule allows the operator to reach the given head ports.
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

// SubmitterIngressRule allows a RayJob submitter to reach the head dashboard.
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
