package apiserversdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sclient "k8s.io/client-go/kubernetes/typed/core/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	apiserverutil "github.com/ray-project/kuberay/apiserversdk/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayutil "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
)

var (
	ln                    net.Listener
	cfg                   *rest.Config
	rayClient             *rayclient.RayV1Client
	k8sClient             *k8sclient.CoreV1Client
	k8sClientWithoutProxy *k8sclient.CoreV1Client
	eventsClient          *eventsv1client.EventsV1Client
	testEnv               *envtest.Environment
	lastReq               atomic.Pointer[http.Request]
)

func TestProxy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Proxy Suite")
}

var _ = BeforeSuite(func(_ SpecContext) {
	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "ray-operator", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ToNot(HaveOccurred())
	Expect(cfg).ToNot(BeNil())

	ctrl := gomock.Controller{}
	// mock client manager
	mockClientManager := manager.NewMockClientManagerInterface(&ctrl)

	mux, err := NewMux(MuxConfig{
		KubernetesConfig: cfg,
		Middleware: func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				lastReq.Store(r.Clone(r.Context()))
				handler.ServeHTTP(w, r)
			})
		},
	}, mockClientManager)
	Expect(err).ToNot(HaveOccurred())
	Expect(mux).ToNot(BeNil())

	ln, err = net.Listen("tcp", "127.0.0.1:0")
	Expect(err).ToNot(HaveOccurred())
	Expect(ln).ToNot(BeNil())
	go func() {
		svc := &http.Server{Handler: mux, ReadHeaderTimeout: time.Minute}
		err := svc.Serve(ln)
		Expect(err).To(MatchError(net.ErrClosed))
	}()

	proxyCfg := &rest.Config{Host: "http://" + ln.Addr().String()}
	rayClient = rayclient.NewForConfigOrDie(proxyCfg)
	k8sClient = k8sclient.NewForConfigOrDie(proxyCfg)
	k8sClientWithoutProxy = k8sclient.NewForConfigOrDie(cfg)
	eventsClient = eventsv1client.NewForConfigOrDie(proxyCfg)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	_ = testEnv.Stop()
	_ = ln.Close()
})

var _ = Describe("RayCluster", Ordered, func() {
	It("Create RayCluster", func() {
		_, err := rayClient.RayClusters("default").Create(context.Background(), &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "proxy-test"},
			Spec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					RayStartParams: make(map[string]string),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "test",
									Image: "test",
								},
							},
						},
					},
				},
			},
		}, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodPost))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayclusters"))
	})
	It("Get RayCluster", func() {
		cluster, err := rayClient.RayClusters("default").Get(context.Background(), "proxy-test", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(cluster.Name).To(Equal("proxy-test"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayclusters/proxy-test"))
	})
	It("List RayCluster", func() {
		clusters, err := rayClient.RayClusters("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(clusters.Items).To(HaveLen(1))
		Expect(clusters.Items[0].Name).To(Equal("proxy-test"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayclusters"))
	})
	It("Delete RayCluster", func() {
		err := rayClient.RayClusters("default").Delete(context.Background(), "proxy-test", metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodDelete))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayclusters/proxy-test"))
	})
})

var _ = Describe("RayJob", Ordered, func() {
	It("Create RayJob", func() {
		_, err := rayClient.RayJobs("default").Create(context.Background(), &rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{Name: "proxy-test-job"},
			Spec: rayv1.RayJobSpec{
				Entrypoint: "echo hello",
				RayClusterSpec: &rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: make(map[string]string),
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "test",
										Image: "test",
									},
								},
							},
						},
					},
				},
			},
		}, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodPost))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayjobs"))
	})
	It("Get RayJob", func() {
		job, err := rayClient.RayJobs("default").Get(context.Background(), "proxy-test-job", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(job.Name).To(Equal("proxy-test-job"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayjobs/proxy-test-job"))
	})
	It("List RayJob", func() {
		jobs, err := rayClient.RayJobs("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(jobs.Items).To(HaveLen(1))
		Expect(jobs.Items[0].Name).To(Equal("proxy-test-job"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayjobs"))
	})
	It("Delete RayJob", func() {
		err := rayClient.RayJobs("default").Delete(context.Background(), "proxy-test-job", metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodDelete))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayjobs/proxy-test-job"))
	})
})

var _ = Describe("RayService", Ordered, func() {
	It("Create RayService", func() {
		_, err := rayClient.RayServices("default").Create(context.Background(), &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{Name: "proxy-test-service"},
			Spec: rayv1.RayServiceSpec{
				RayClusterSpec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: make(map[string]string),
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "test",
										Image: "test",
									},
								},
							},
						},
					},
				},
			},
		}, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodPost))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayservices"))
	})
	It("Get RayService", func() {
		service, err := rayClient.RayServices("default").Get(context.Background(), "proxy-test-service", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(service.Name).To(Equal("proxy-test-service"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayservices/proxy-test-service"))
	})
	It("Update RayService", func() {
		service, err := rayClient.RayServices("default").Get(context.Background(), "proxy-test-service", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		service.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Image = "new-image"
		_, err = rayClient.RayServices("default").Update(context.Background(), service, metav1.UpdateOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodPut))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayservices/proxy-test-service"))

		updatedSvc, err := rayClient.RayServices("default").Get(context.Background(), "proxy-test-service", metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(updatedSvc.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Image).To(Equal("new-image"))
	})
	It("List RayService", func() {
		services, err := rayClient.RayServices("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(services.Items).To(HaveLen(1))
		Expect(services.Items[0].Name).To(Equal("proxy-test-service"))
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayservices"))
	})
	It("Delete RayService", func() {
		err := rayClient.RayServices("default").Delete(context.Background(), "proxy-test-service", metav1.DeleteOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(lastReq.Load().Method).To(Equal(http.MethodDelete))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/ray.io/v1/namespaces/default/rayservices/proxy-test-service"))
	})
})

var _ = Describe("events", Ordered, func() {
	It("List events", func() {
		events, err := eventsClient.Events("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(events.Items).To(BeEmpty())
		Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
		Expect(lastReq.Load().RequestURI).To(Equal("/apis/events.k8s.io/v1/namespaces/default/events"))
	})
	It("Only GET method is allowed for events endpoint", func() {
		event := &eventsv1.Event{}
		_, err := eventsClient.Events("default").Create(context.Background(), event, metav1.CreateOptions{})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("the server does not allow this method on the requested resource")))
	})
	It("Only querying KubeRay CR events", func() {
		testEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-event-",
				Namespace:    "default",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:       "RayCluster",
				Namespace:  "default",
				Name:       "test-event",
				APIVersion: "ray.io/v1",
			},
			Type:    "Normal",
			Reason:  "Testing",
			Message: "This is a test event",
			Source: corev1.EventSource{
				Component: "test-component",
			},
		}
		testEvent2 := testEvent.DeepCopy()
		testEvent2.InvolvedObject.APIVersion = ""
		// Events created through the core API are readable through events.k8s.io/v1
		// since both APIs are backed by the same storage.
		_, err := k8sClientWithoutProxy.Events("default").Create(context.Background(), testEvent, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		_, err = k8sClientWithoutProxy.Events("default").Create(context.Background(), testEvent2, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		events, err := eventsClient.Events("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(events.Items).To(HaveLen(1))
		Expect(events.Items[0].ObjectMeta.GenerateName).To(Equal(testEvent.ObjectMeta.GenerateName))
		Expect(events.Items[0].Regarding.APIVersion).To(Equal("ray.io/v1"))
		// Test the user selector won't override "regarding.apiVersion=ray.io/v1"
		fieldSelectorString := "regarding.apiVersion="
		events, err = eventsClient.Events("default").List(context.Background(), metav1.ListOptions{FieldSelector: fieldSelectorString})
		Expect(err).ToNot(HaveOccurred())
		Expect(events.Items).To(BeEmpty())
	})
})

var _ = Describe("not match", Ordered, func() {
	It("List Pods", func() {
		_, err := k8sClient.Pods("default").List(context.Background(), metav1.ListOptions{})
		Expect(err).To(MatchError(ContainSubstring("the server could not find the requested resource")))
	})
})

var _ = Describe("kuberay service", Ordered, func() {
	svcName := "head-svc"

	AfterEach(func() {
		_ = k8sClientWithoutProxy.Services("default").Delete(context.Background(), svcName, metav1.DeleteOptions{})
	})

	Context("when a service has the KubeRay label", func() {
		BeforeEach(func() {
			_, err := k8sClientWithoutProxy.Services("default").Create(context.Background(), &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: svcName,
					Labels: map[string]string{
						rayutil.KubernetesApplicationNameLabelKey: rayutil.ApplicationName,
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "http", Port: 80, TargetPort: intstr.FromInt(80)},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).ToNot(HaveOccurred())
		})

		// Since envtest doesn't create a full K8s cluster but only the control plane, we cannot actually hit the pod.
		// So we just check the request and skip checking the error which is always a 404.
		It("should allow proxying to KubeRay-labeled service", func() {
			r := k8sClient.Services("default").ProxyGet("http", svcName, "80", "foo/bar", nil)
			_, _ = r.DoRaw(context.Background()) // Expect 404 due to envtest limitation
			Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
			Expect(lastReq.Load().RequestURI).To(Equal("/api/v1/namespaces/default/services/http:head-svc:80/proxy/foo/bar"))
		})
		It("should allow proxying to KubeRay-labeled service without trailing slash", func() {
			r := k8sClient.Services("default").ProxyGet("http", svcName, "80", "", nil)
			_, _ = r.DoRaw(context.Background()) // Expect 404 due to envtest limitation
			Expect(lastReq.Load().Method).To(Equal(http.MethodGet))
			Expect(lastReq.Load().RequestURI).To(Equal("/api/v1/namespaces/default/services/http:head-svc:80/proxy"))

			// We register both "/proxy" and "/proxy/" to prevent implicit redirects.
			// This test make sure trailing slash issue is handled correctly.
			// Without explicitly handling "/proxy", a request to it will be redirected to "/proxy/".
			// Also, a POST request to "/proxy" will be changed from POST to GET, and drops the body.
			restClient := k8sClient.RESTClient()
			_, _ = restClient.Post().
				Namespace("default").
				Resource("services").
				Name("http:head-svc:80").
				SubResource("proxy").
				DoRaw(context.Background())

			Expect(lastReq.Load().Method).To(Equal(http.MethodPost))
			Expect(lastReq.Load().RequestURI).To(Equal("/api/v1/namespaces/default/services/http:head-svc:80/proxy"))
		})
	})

	Context("when a service lacks the KubeRay label", func() {
		BeforeEach(func() {
			_, err := k8sClientWithoutProxy.Services("default").Create(context.Background(), &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: svcName,
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "http", Port: 80, TargetPort: intstr.FromInt(80)},
					},
				},
			}, metav1.CreateOptions{})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reject proxying to service without KubeRay label", func() {
			r := k8sClient.Services("default").ProxyGet("http", svcName, "80", "foo/bar", nil)
			_, err := r.DoRaw(context.Background())
			Expect(err).To(HaveOccurred())
			var statusErr *apierrors.StatusError
			ok := errors.As(err, &statusErr)
			Expect(ok).To(BeTrue())
			Expect(statusErr.Status().Details.Causes[0].Message).To(Equal("kuberay service not found"))
		})
	})
})

var _ = Describe("retryRoundTripper", func() {
	It("should not retry on successful status OK", func() {
		var attempts int32
		mock := &mockRoundTripper{
			fn: func(_ *http.Request) (*http.Response, error) {
				atomic.AddInt32(&attempts, 1)
				return &http.Response{ /* Always return OK status */
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("OK")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		req, err := http.NewRequest(http.MethodGet, "http://test", nil)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(attempts).To(Equal(int32(1)))
	})

	It("should retry failed requests and eventually succeed", func() {
		const maxFailure = 2
		var attempts int32
		mock := &mockRoundTripper{
			fn: func(_ *http.Request) (*http.Response, error) {
				count := atomic.AddInt32(&attempts, 1)
				if count <= maxFailure {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       io.NopCloser(strings.NewReader("internal error")),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		req, err := http.NewRequest(http.MethodGet, "http://test", nil)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(attempts).To(Equal(int32(maxFailure + 1)))
	})

	It("Retries exceed maximum retry counts", func() {
		var attempts int32
		mock := &mockRoundTripper{
			fn: func(_ *http.Request) (*http.Response, error) {
				atomic.AddInt32(&attempts, 1)
				return &http.Response{ /* Always return retriable status */
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("internal error")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		req, err := http.NewRequest(http.MethodGet, "http://test", nil)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusInternalServerError))
		Expect(attempts).To(Equal(int32(apiserverutil.HTTPClientDefaultMaxRetry + 1)))
	})

	It("Retries on request with body", func() {
		const testBody = "test-body"
		const maxFailure = 2
		var attempts int32
		mock := &mockRoundTripper{
			fn: func(req *http.Request) (*http.Response, error) {
				count := atomic.AddInt32(&attempts, 1)
				reqBody, err := io.ReadAll(req.Body)
				Expect(err).ToNot(HaveOccurred())
				Expect(string(reqBody)).To(Equal(testBody))

				if count <= maxFailure {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       io.NopCloser(strings.NewReader("internal error")),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		body := bytes.NewBufferString(testBody)
		req, err := http.NewRequest(http.MethodPost, "http://test", body)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(attempts).To(Equal(int32(maxFailure + 1)))
	})

	It("should not retry on non-retriable status", func() {
		var attempts int32
		mock := &mockRoundTripper{
			fn: func(_ *http.Request) (*http.Response, error) {
				atomic.AddInt32(&attempts, 1)
				return &http.Response{ /* Always return non-retriable status */
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("Not Found")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		req, err := http.NewRequest(http.MethodGet, "http://test", nil)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusNotFound))
		Expect(attempts).To(Equal(int32(1)))
	})

	It("should respect context timeout and stop retrying", func() {
		mock := &mockRoundTripper{
			fn: func(_ *http.Request) (*http.Response, error) {
				time.Sleep(100 * time.Millisecond)
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("internal error")),
				}, nil
			},
		}
		retrier := newRetryRoundTripper(mock)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://test", nil)
		Expect(err).ToNot(HaveOccurred())
		resp, err := retrier.RoundTrip(req)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("retry timeout exceeded context deadline"))
		Expect(resp).ToNot(BeNil())
	})
})

type mockRoundTripper struct {
	fn func(*http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.fn(req)
}
