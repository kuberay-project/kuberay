package interceptor

import (
	"context"
	"time"

	"google.golang.org/grpc"
	klog "k8s.io/klog/v2"
)

// APIServerInterceptor implements UnaryServerInterceptor that provides the common wrapping logic
// to be executed before and after all API handler calls, e.g. Logging, error handling.
// For more details, see https://github.com/grpc/grpc-go/blob/master/interceptor.go
func APIServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	klog.Infof("%v handler starting", info.FullMethod)
	resp, err = handler(ctx, req)
	if err != nil {
		klog.Warning(err)
	}
	klog.Infof("%v handler finished", info.FullMethod)
	return
}

// TimeoutInterceptor implements UnaryServerInterceptor that sets the timeout for the request
func TimeoutInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return handler(ctx, req)
	}
}
