package apiserversdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	apiserversdkutil "github.com/ray-project/kuberay/apiserversdk/util"
	rayutil "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type MuxConfig struct {
	KubernetesConfig *rest.Config
	Middleware       func(http.Handler) http.Handler
}

func NewMux(config MuxConfig, clientManager manager.ClientManagerInterface) (*http.ServeMux, error) {
	u, err := url.Parse(config.KubernetesConfig.Host) // parse the K8s API server URL from the KubernetesConfig.
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s from config: %w", config.KubernetesConfig.Host, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	baseTransport, err := rest.TransportFor(config.KubernetesConfig) // rest.TransportFor provides the auth to the K8s API server.
	if err != nil {
		return nil, fmt.Errorf("failed to get transport for config: %w", err)
	}
	proxy.Transport = newRetryRoundTripper(baseTransport)
	var handler http.Handler = proxy
	if config.Middleware != nil {
		handler = config.Middleware(proxy)
	}

	mux := http.NewServeMux()
	// TODO: add template features to specify routes.
	mux.Handle("/apis/ray.io/v1/", handler)                                                                                              // forward KubeRay CR requests.
	mux.Handle("GET /apis/events.k8s.io/v1/namespaces/{namespace}/events", withFieldSelector(handler, "regarding.apiVersion=ray.io/v1")) // allow querying KubeRay CR events.

	k8sClient := kubernetes.NewForConfigOrDie(config.KubernetesConfig)

	// Compute Template middleware
	ctMiddleware := apiserversdkutil.NewComputeTemplateMiddleware(clientManager)
	mux.Handle("POST /apis/ray.io/v1/namespaces/{namespace}/rayclusters", ctMiddleware(handler))
	mux.Handle("PUT /apis/ray.io/v1/namespaces/{namespace}/rayclusters/{name}", ctMiddleware(handler))
	mux.Handle("POST /apis/ray.io/v1/namespaces/{namespace}/rayjobs", ctMiddleware(handler))
	mux.Handle("PUT /apis/ray.io/v1/namespaces/{namespace}/rayjobs/{name}", ctMiddleware(handler))
	mux.Handle("POST /apis/ray.io/v1/namespaces/{namespace}/rayservices", ctMiddleware(handler))
	mux.Handle("PUT /apis/ray.io/v1/namespaces/{namespace}/rayservices/{name}", ctMiddleware(handler))

	requireKubeRayServiceHandler := requireKubeRayService(handler, k8sClient)
	// Allow accessing KubeRay dashboards and job submissions.
	// Note: We also register "/proxy" to avoid the trailing slash redirection
	// See https://pkg.go.dev/net/http#hdr-Trailing_slash_redirection-ServeMux
	mux.Handle("/api/v1/namespaces/{namespace}/services/{service}/proxy", requireKubeRayServiceHandler)
	mux.Handle("/api/v1/namespaces/{namespace}/services/{service}/proxy/", requireKubeRayServiceHandler)

	return mux, nil
}

func withFieldSelector(handler http.Handler, selectors ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Preserve existing field selectors if any
		q.Set("fieldSelector", strings.Join(append(q["fieldSelector"], selectors...), ","))
		r.URL.RawQuery = q.Encode()
		handler.ServeHTTP(w, r)
	})
}

// requireKubeRayService verifies that the requested service has the label "app.kubernetes.io/name=kuberay".
// If the service is not found or does not have the correct label, it returns a 404 Not Found error.
func requireKubeRayService(handler http.Handler, k8sClient *kubernetes.Clientset) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace, serviceSchemeNamePort := r.PathValue("namespace"), r.PathValue("service")
		_, serviceName, _, valid := net.SplitSchemeNamePort(serviceSchemeNamePort)
		if !valid {
			http.Error(w, "invalid service format: "+serviceSchemeNamePort, http.StatusBadRequest)
			return
		}
		services, err := k8sClient.CoreV1().Services(namespace).List(r.Context(), metav1.ListOptions{
			FieldSelector: "metadata.name=" + serviceName,
			LabelSelector: "app.kubernetes.io/name=" + rayutil.ApplicationName,
		})
		if err != nil {
			http.Error(w, "failed to list kuberay services", http.StatusInternalServerError)
			return
		}
		if len(services.Items) == 0 {
			http.Error(w, "kuberay service not found", http.StatusNotFound)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

// retryRoundTripper is a custom implementation of http.RoundTripper that retries HTTP requests.
// It verifies retryable HTTP status codes and retries using exponential backoff.
type retryRoundTripper struct {
	base     http.RoundTripper
	retryCfg apiserversdkutil.RetryConfig
}

func newRetryRoundTripper(base http.RoundTripper) http.RoundTripper {
	retryCfg := apiserversdkutil.RetryConfig{
		MaxRetry:       apiserversdkutil.HTTPClientDefaultMaxRetry,
		BackoffFactor:  apiserversdkutil.HTTPClientDefaultBackoffFactor,
		InitBackoff:    apiserversdkutil.HTTPClientDefaultInitBackoff,
		MaxBackoff:     apiserversdkutil.HTTPClientDefaultMaxBackoff,
		OverallTimeout: apiserversdkutil.HTTPClientDefaultOverallTimeout,
	}

	return &retryRoundTripper{
		base:     base,
		retryCfg: retryCfg,
	}
}

func (rrt *retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	ctx, cancel := context.WithTimeout(ctx, rrt.retryCfg.OverallTimeout)
	defer cancel()

	req = req.WithContext(ctx)

	var bodyBytes []byte
	var resp *http.Response
	var err error

	if req.Body != nil {
		/* Reuse request body in each attempt */
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body for retry support: %w", err)
		}
		err = req.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close request body: %w", err)
		}
	}

	for attempt := 0; attempt <= rrt.retryCfg.MaxRetry; attempt++ {
		/* Try up to (rrt.retryCfg.MaxRetry + 1) times: initial attempt + retries */

		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}

		resp, err = rrt.base.RoundTrip(req)
		if err != nil {
			return resp, fmt.Errorf("request to %s %s failed with error: %w", req.Method, req.URL.String(), err)
		}

		if apiserversdkutil.IsSuccessfulStatusCode(resp.StatusCode) {
			if resp.Body != nil {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return nil, fmt.Errorf("failed to read response body: %w", err)
				}
				err = resp.Body.Close()
				if err != nil {
					return nil, fmt.Errorf("failed to close response body: %w", err)
				}
				resp.Body = io.NopCloser(bytes.NewReader(body))
			}
			return resp, nil
		}

		if !apiserversdkutil.IsRetryableHTTPStatusCodes(resp.StatusCode) {
			return resp, nil
		}

		if attempt == rrt.retryCfg.MaxRetry {
			return resp, nil
		}

		if resp.Body != nil {
			/* If not last attempt, drain response body */
			if _, err = io.Copy(io.Discard, resp.Body); err != nil {
				return nil, fmt.Errorf("retryRoundTripper internal failure to drain response body: %w", err)
			}
			if err = resp.Body.Close(); err != nil {
				return nil, fmt.Errorf("retryRoundTripper internal failure to close response body: %w", err)
			}
		}

		sleepDuration := apiserversdkutil.GetRetryBackoff(attempt, rrt.retryCfg.InitBackoff, rrt.retryCfg.BackoffFactor, rrt.retryCfg.MaxBackoff)

		if ok := apiserversdkutil.CheckContextDeadline(ctx, sleepDuration); !ok {
			return resp, fmt.Errorf("retry timeout exceeded context deadline")
		}

		if err = apiserversdkutil.Sleep(ctx, sleepDuration); err != nil {
			return resp, fmt.Errorf("retry canceled during backoff: %w", err)
		}
	}
	return resp, err
}
