# Security policy

## Security Boundary

KubeRay relies on the Kubernetes security model and does not provide additional
security isolation beyond the default behavior. In particular:

* A Kubernetes namespace should be treated as a shared trust domain. Users and
  workloads authorized to access a namespace may be able to access or modify
  other KubeRay resources within the namespace.
* Authorized users can configure KubeRay workloads with custom container images,
  commands, and entrypoints. The ability to execute user-supplied code is an
  intended feature, not a security vulnerability.
* Cluster administrators remain responsible for creating Kubernetes
  NetworkPolicies independently and ensuring that the cluster’s networking
  implementation enforces them correctly.

## Reporting a vulnerability

Please report security issues to `security-kuberay@anyscale.com` or use the
[**Report a vulnerability**](https://github.com/ray-project/kuberay/security/advisories/new)
form.

Emails should contain:

* description of the problem
* precise and detailed steps (include screenshots) that created the problem
* the affected version(s)
* any possible mitigations, if known
