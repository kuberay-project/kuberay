package apiserversdk

import (
	"fmt"
	"net/http"
	"path"

	"k8s.io/client-go/rest"
)

type ProxyRoundTripper struct {
	Transport http.RoundTripper
}

func newProxyRoundTripper(cfg *rest.Config) (*ProxyRoundTripper, error) {
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create http RoundTripper: %w", err)
	}

	return &ProxyRoundTripper{
		Transport: transport,
	}, nil
}

// RoundTrp send the request through the Kubernetes service proxy subresource
func (rt *ProxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())

	newReq.URL.Path = path.Join(
		"/api/v1/namespaces/ray-system/services/kuberay-apiserver:8888/proxy",
		req.URL.Path,
	)

	return rt.Transport.RoundTrip(newReq)
}
