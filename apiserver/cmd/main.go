package main

import (
	"context"
	"flag"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync/atomic"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/ray-project/kuberay/apiserver/pkg/interceptor"
	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/server"
	"github.com/ray-project/kuberay/apiserver/pkg/swagger"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	"github.com/ray-project/kuberay/apiserversdk"
	api "github.com/ray-project/kuberay/proto/go_client"
)

var (
	rpcPortFlag        = flag.String("rpcPortFlag", ":8887", "RPC Port")
	httpPortFlag       = flag.String("httpPortFlag", ":8888", "Http Proxy Port")
	collectMetricsFlag = flag.Bool("collectMetricsFlag", true, "Whether to collect Prometheus metrics in API server.")
	logFile            = flag.String("logFilePath", "", "Synchronize logs to local file")
	localSwaggerPath   = flag.String("localSwaggerPath", "", "Specify the root directory for `*.swagger.json` the swagger files.")
	grpcTimeout        = flag.Duration("grpc_timeout", util.GRPCServerDefaultTimeout, "gRPC server timeout duration")
	enableAPIServerV2  = flag.Bool("enable-api-server-v2", true, "Enable API server V2")
	corsAllowOrigin    = flag.String("cors-allow-origin", "", "Set the Access-Control-Allow-Origin response header for the HTTP proxy.")
	healthy            atomic.Int32
)

func main() {
	flag.Parse()

	if *logFile != "" {
		flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		klog.InitFlags(flagSet)
		_ = flagSet.Set("alsologtostderr", "true")
		_ = flagSet.Set("logtostderr", "false")
		_ = flagSet.Set("log_file", *logFile)
	}

	clientManager := manager.NewClientManager()
	resourceManager := manager.NewResourceManager(&clientManager)

	healthy.Store(1)
	klog.Infof("Setting gRPC server timeout to %v", *grpcTimeout)
	go startRPCServer(resourceManager, *grpcTimeout)
	startHttpProxy()
	// See also https://gist.github.com/enricofoltran/10b4a980cd07cb02836f70a4ab3e72d7
	quit := make(chan os.Signal, 1)
	// notify about interrupts
	signal.Notify(quit, os.Interrupt)
	// Process interrupts
	go func() {
		<-quit
		klog.Info("Unexpected interrupt")
		healthy.Store(0)
	}()
}

type RegisterHttpHandlerFromEndpoint func(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error

func startRPCServer(resourceManager *manager.ResourceManager, grpcTimeout time.Duration) {
	klog.Infof("Starting gRPC server at port %s", *rpcPortFlag)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", *rpcPortFlag)
	if err != nil {
		klog.Fatalf("Failed to start GPRC server: %v", err)
	}

	clusterServer := server.NewClusterServer(resourceManager, &server.ClusterServerOptions{CollectMetrics: *collectMetricsFlag})
	templateServer := server.NewComputeTemplateServer(resourceManager, &server.ComputeTemplateServerOptions{CollectMetrics: *collectMetricsFlag})
	jobServer := server.NewRayJobServer(resourceManager, &server.JobServerOptions{CollectMetrics: *collectMetricsFlag})
	jobSubmissionServer := server.NewRayJobSubmissionServiceServer(clusterServer, &server.RayJobSubmissionServiceServerOptions{CollectMetrics: *collectMetricsFlag})
	serveServer := server.NewRayServiceServer(resourceManager, &server.ServiceServerOptions{CollectMetrics: *collectMetricsFlag})

	s := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			interceptor.TimeoutInterceptor(grpcTimeout),
			grpc_prometheus.UnaryServerInterceptor,
			interceptor.APIServerInterceptor,
		)),
		grpc.MaxRecvMsgSize(math.MaxInt32),
	)
	api.RegisterClusterServiceServer(s, clusterServer)
	api.RegisterComputeTemplateServiceServer(s, templateServer)
	api.RegisterRayJobServiceServer(s, jobServer)
	api.RegisterRayJobSubmissionServiceServer(s, jobSubmissionServer)
	api.RegisterRayServeServiceServer(s, serveServer)

	// Register reflection service on gRPC server.
	reflection.Register(s)
	// Make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(s)
	// This is to enable `grpc_server_handling_seconds`, otherwise we won't have latency metrics.
	// see https://github.com/grpc-ecosystem/go-grpc-prometheus/blob/master/README.md#histograms for details.
	grpc_prometheus.EnableHandlingTimeHistogram()
	if err := s.Serve(listener); err != nil {
		klog.Fatalf("Failed to serve gRPC listener: %v", err)
	}

	klog.Info("gRPC server started")
}

func startHttpProxy() {
	klog.Infof("Starting Http Proxy at port %s", *httpPortFlag)

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create gRPC HTTP MUX and register services.
	runtimeMux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				UseProtoNames:  false,
				UseEnumNumbers: true,
			},
			UnmarshalOptions: protojson.UnmarshalOptions{
				DiscardUnknown: true,
			},
		}),
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)
	// Register endpoints
	registerHttpHandlerFromEndpoint(ctx, api.RegisterClusterServiceHandlerFromEndpoint, "ClusterService", runtimeMux)
	registerHttpHandlerFromEndpoint(ctx, api.RegisterComputeTemplateServiceHandlerFromEndpoint, "ComputeTemplateService", runtimeMux)
	registerHttpHandlerFromEndpoint(ctx, api.RegisterRayJobServiceHandlerFromEndpoint, "JobService", runtimeMux)
	registerHttpHandlerFromEndpoint(ctx, api.RegisterRayServeServiceHandlerFromEndpoint, "ServeService", runtimeMux)
	registerHttpHandlerFromEndpoint(ctx, api.RegisterRayJobSubmissionServiceHandlerFromEndpoint, "RayJobSubmissionService", runtimeMux)

	// Create a top level mux to include both Http gRPC servers and other endpoints like metrics
	var corsHandler func(http.Handler) http.Handler
	if *corsAllowOrigin != "" {
		klog.Info("Enabling CORS with Access-Control-Allow-Origin:", *corsAllowOrigin)
		c := cors.New(cors.Options{
			AllowedOrigins: []string{*corsAllowOrigin},
			AllowedMethods: []string{
				http.MethodGet, http.MethodPost,
				http.MethodPut, http.MethodPatch,
				http.MethodDelete, http.MethodOptions,
			},
		})
		corsHandler = c.Handler
	} else {
		klog.Info("Access-Control-Allow-Origin not set, CORS is disabled.")
		corsHandler = func(h http.Handler) http.Handler { return h }
	}

	var topMux *http.ServeMux
	if *enableAPIServerV2 {
		kubernetesConfig, err := config.GetConfig()
		if err != nil {
			klog.Fatalf("Failed to load kubeconfig: %v", err)
		}

		muxConfig := apiserversdk.MuxConfig{
			KubernetesConfig: kubernetesConfig,
			Middleware:       corsHandler, // Always set, even if it's a no-op
		}
		clientManager := manager.NewClientManager()
		topMux, err = apiserversdk.NewMux(muxConfig, &clientManager)
		if err != nil {
			klog.Fatalf("Failed to create API server mux: %v", err)
		}
	} else {
		topMux = http.NewServeMux()
	}

	// Always wrap the runtimeMux with the CORS handler (no-op if not enabled)
	topMux.Handle("/", corsHandler(runtimeMux))

	topMux.Handle("/metrics", promhttp.Handler())
	topMux.HandleFunc("/swagger/", serveSwaggerFile)
	topMux.HandleFunc("/healthz", serveHealth)
	serveSwaggerUI(topMux)

	// Create a custom HTTP server with timeouts.
	srv := &http.Server{
		Addr:         *httpPortFlag,
		Handler:      topMux,
		ReadTimeout:  0, // No timeout
		WriteTimeout: 0, // No timeout
		IdleTimeout:  0, // No timeout
	}

	// Start the server.
	if err := srv.ListenAndServe(); err != nil {
		klog.Fatal(err)
	}

	klog.Info("Http Proxy started")
}

func serveHealth(w http.ResponseWriter, _ *http.Request) {
	if healthy.Load() == 1 {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func serveSwaggerFile(w http.ResponseWriter, r *http.Request) {
	klog.Info("start serveSwaggerFile")

	if !strings.HasSuffix(r.URL.Path, "swagger.json") {
		klog.Errorf("Not Found: %s", r.URL.Path)
		http.NotFound(w, r)
		return
	}

	p := strings.TrimPrefix(r.URL.Path, "/swagger/")
	if strings.TrimSpace(*localSwaggerPath) != "" {
		// use the specified path,  for development the is  `${REPO_ROOT}/proto/swagger`.
		p = path.Join(*localSwaggerPath, "/", p)
	} else {
		// In docker images the *.swagger.json are copied to `/workspace/proto/swagger/``.
		p = path.Join("/workspace/proto/swagger/", p)
	}

	klog.Infof("Serving swagger-file: %s", p)
	http.ServeFile(w, r, p)
}

// go-bindata --nocompress --pkg swagger -o pkg/swagger/datafile.go third_party/swagger-ui/...
// We will need to copy third_party folder to `backend` folder when building images
func serveSwaggerUI(mux *http.ServeMux) {
	fileServer := http.FileServer(&assetfs.AssetFS{
		Asset:    swagger.Asset,
		AssetDir: swagger.AssetDir,
		Prefix:   "third_party/swagger-ui",
	})

	prefix := "/swagger-ui/"
	mux.Handle(prefix, http.StripPrefix(prefix, fileServer))
}

func registerHttpHandlerFromEndpoint(ctx context.Context, handler RegisterHttpHandlerFromEndpoint, serviceName string, mux *runtime.ServeMux) {
	endpoint := "localhost" + *rpcPortFlag
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32))}

	if err := handler(ctx, mux, endpoint, opts); err != nil {
		klog.Fatalf("Failed to register %v handler: %v", serviceName, err)
	}
}
