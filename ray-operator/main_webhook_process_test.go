package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/cert"
)

func TestOperatorProcess(_ *testing.T) {
	if os.Getenv("KUBERAY_TEST_PROCESS") != "true" {
		return
	}
	os.Args = append([]string{"operator"}, flag.Args()...)
	flag.CommandLine = flag.NewFlagSet("operator", flag.ExitOnError)
	main()
}

func TestWebhookOnlyProcess(t *testing.T) {
	var apiCalls atomic.Int32
	discovery := map[string]any{
		"/api":           metav1.APIVersions{Versions: []string{"v1"}},
		"/apis":          metav1.APIGroupList{Groups: []metav1.APIGroup{{Name: "batch", Versions: []metav1.GroupVersionForDiscovery{{GroupVersion: "batch/v1", Version: "v1"}}, PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "batch/v1", Version: "v1"}}}},
		"/api/v1":        metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "pods", Kind: "Pod", Namespaced: true}, {Name: "events", Kind: "Event", Namespaced: true}, {Name: "nodes", Kind: "Node"}}},
		"/apis/batch/v1": metav1.APIResourceList{GroupVersion: "batch/v1", APIResources: []metav1.APIResource{{Name: "jobs", Kind: "Job", Namespaced: true}}},
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if response, ok := discovery[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(response); err != nil {
				t.Error(err)
			}
			return
		}
		apiCalls.Add(1)
		http.Error(w, "controller API access is unexpected", http.StatusServiceUnavailable)
	}))
	defer api.Close()
	dir := t.TempDir()
	config := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: api.URL}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test"}},
		CurrentContext: "test",
	}
	kubeconfig := filepath.Join(dir, "kubeconfig")
	require.NoError(t, clientcmd.WriteToFile(config, kubeconfig))
	certDir := filepath.Join(dir, "k8s-webhook-server", "serving-certs")
	require.NoError(t, os.MkdirAll(certDir, 0o700))
	certPEM, keyPEM, err := cert.GenerateSelfSignedCertKey("localhost", nil, nil)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.crt"), certPEM, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "tls.key"), keyPEM, 0o600))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	probeAddr := listener.Addr().String()
	require.NoError(t, listener.Close())
	logPath := filepath.Join(dir, "operator.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFile.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestOperatorProcess$", "--",
		"--metrics-addr=0", "--health-probe-bind-address="+probeAddr,
		"--enable-leader-election=true", "--enable-metrics=true", "--batch-scheduler=unsupported",
		"--feature-gates=RayCronJob=true,RayClusterMTLS=true,RayNodeEventForwarder=true,RayClusterNetworkPolicy=true",
		"--enable-node-event-forwarder=true")
	cmd.Env = append(os.Environ(), "KUBERAY_TEST_PROCESS=true", "ENABLE_CONTROLLERS=false", "ENABLE_WEBHOOKS=true", "KUBECONFIG="+kubeconfig, "TMPDIR="+dir)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		cancel()
		<-done
	}()
	httpClient := &http.Client{Timeout: time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+probeAddr+"/readyz", nil)
	require.NoError(t, err)
	ready := assert.Eventually(t, func() bool {
		response, err := httpClient.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond)
	logs, err := os.ReadFile(logPath)
	require.NoError(t, err)
	if !ready {
		t.Log(string(logs))
	}
	require.True(t, ready)
	rootCAs := x509.NewCertPool()
	require.True(t, rootCAs.AppendCertsFromPEM(certPEM))
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: rootCAs, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	admissionClient := &http.Client{Transport: transport, Timeout: time.Second}
	for _, kind := range []string{"RayCluster", "RayJob", "RayService"} {
		for _, name := range []string{"valid-name", "invalid_name"} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				object, err := json.Marshal(map[string]any{
					"apiVersion": "ray.io/v1", "kind": kind,
					"metadata": map[string]string{"name": name, "namespace": "default"},
				})
				require.NoError(t, err)
				review := admissionv1.AdmissionReview{
					TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
					Request: &admissionv1.AdmissionRequest{
						UID: "test-admission", Name: name, Namespace: "default", Operation: admissionv1.Create,
						Kind:     metav1.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: kind},
						Resource: metav1.GroupVersionResource{Group: "ray.io", Version: "v1", Resource: strings.ToLower(kind) + "s"},
						Object:   runtime.RawExtension{Raw: object},
					},
				}
				body, err := json.Marshal(review)
				require.NoError(t, err)
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://localhost:9443/validate-ray-io-v1-"+strings.ToLower(kind), bytes.NewReader(body))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				response, err := admissionClient.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				require.Equal(t, http.StatusOK, response.StatusCode)
				var result admissionv1.AdmissionReview
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				require.NotNil(t, result.Response)
				assert.Equal(t, review.Request.UID, result.Response.UID)
				assert.Equal(t, name == "valid-name", result.Response.Allowed)
			})
		}
	}
	assert.Zero(t, apiCalls.Load(), string(logs))
	assert.NotContains(t, string(logs), "Starting Controller")
}
