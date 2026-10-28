package interceptor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	klog "k8s.io/klog/v2"
)

// mockHandler simulates a gRPC handler for testing
type mockHandler struct {
	returnErr error
	called    bool
}

// Handle simulates the behavior of a gRPC handler with an optional delay.
// If the delay completes before the context expires, it returns "test_response" along with predefined error.
// If the context is canceled or the deadline is exceeded before the delay completes,
// it returns a corresponding gRPC status error instead.
func (h *mockHandler) Handle(ctx context.Context, _ any, delay time.Duration) (any, error) {
	h.called = true

	select {
	case <-time.After(delay):
		return "test_response", h.returnErr
	case <-ctx.Done():
		var grpcCode codes.Code
		switch ctx.Err() {
		case context.Canceled:
			grpcCode = codes.Canceled
		case context.DeadlineExceeded:
			grpcCode = codes.DeadlineExceeded
		default:
			grpcCode = codes.Unknown
		}
		return nil, status.Error(grpcCode, ctx.Err().Error())
	}
}

func TestAPIServerInterceptor(t *testing.T) {
	tests := []struct {
		expectedResp  any
		expectedError error
		handler       *mockHandler
		name          string
	}{
		{
			name:          "successful handler execution",
			handler:       &mockHandler{returnErr: nil},
			expectedResp:  "test_response",
			expectedError: nil,
		},
		{
			name:          "handler returns error",
			handler:       &mockHandler{returnErr: errors.New("handler error")},
			expectedResp:  "test_response",
			expectedError: errors.New("handler error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test context and request
			ctx := context.Background()
			req := "test_request"
			info := &grpc.UnaryServerInfo{
				FullMethod: "TestMethod",
			}

			// Call the interceptor
			resp, err := APIServerInterceptor(
				ctx,
				req,
				info,
				func(ctx context.Context, req any) (any, error) {
					return tt.handler.Handle(ctx, req, 0 /*delay*/)
				},
			)

			// Verify handler was called
			assert.True(t, tt.handler.called, "handler should have been called")

			// Verify response
			assert.Equal(t, tt.expectedResp, resp, "response should match expected")

			// Verify error
			if tt.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.expectedError.Error(), "A matching error is expected")
			}
		})
	}
}

// TestAPIServerInterceptorContextPassing ensures context is properly passed through
func TestAPIServerInterceptorContextPassing(t *testing.T) {
	type testContextKey string
	const key testContextKey = "test_key"
	ctx := context.WithValue(context.Background(), key, "test_value")
	handler := &mockHandler{}
	info := &grpc.UnaryServerInfo{FullMethod: "TestMethod"}

	_, _ = APIServerInterceptor(
		ctx,
		"test_request",
		info,
		func(receivedCtx context.Context, req any) (any, error) {
			// Verify context value is passed through
			assert.Equal(t, "test_value", receivedCtx.Value(testContextKey("test_key")))
			return handler.Handle(receivedCtx, req, 0 /*delay*/)
		},
	)
}

// TestAPIServerInterceptorLogging verifies that the interceptor logs start, finish, and warning messages.
func TestAPIServerInterceptorLogging(t *testing.T) {
	tests := []struct {
		name           string
		handlerErr     error
		expectedLogs   []string
		unexpectedLogs []string
	}{
		{
			name:       "successful execution - info logs",
			handlerErr: nil,
			expectedLogs: []string{
				"TestLoggingMethod handler starting",
				"TestLoggingMethod handler finished",
			},
			unexpectedLogs: []string{
				"handler error",
			},
		},
		{
			name:       "error execution - warning logs",
			handlerErr: errors.New("handler error"),
			expectedLogs: []string{
				"TestLoggingMethod handler starting",
				"handler error",
				"TestLoggingMethod handler finished",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Redirect stderr to capture klog output
			originalStderr := os.Stderr
			r, w, _ := os.Pipe()
			os.Stderr = w

			// Restore stderr after the test
			defer func() {
				klog.Flush()
				os.Stderr = originalStderr
			}()

			ctx := context.Background()
			handler := &mockHandler{returnErr: tt.handlerErr}
			info := &grpc.UnaryServerInfo{FullMethod: "TestLoggingMethod"}

			_, err := APIServerInterceptor(
				ctx,
				"test_request",
				info,
				func(receivedCtx context.Context, req any) (any, error) {
					return handler.Handle(receivedCtx, req, 0 /*delay*/)
				},
			)

			if tt.handlerErr != nil {
				require.EqualError(t, tt.handlerErr, err.Error(), "A matching error is expected")
			} else {
				require.NoError(t, err)
			}

			// Close the write end of the pipe and read the captured output
			w.Close()
			var buf bytes.Buffer
			_, err = io.Copy(&buf, r)
			require.NoError(t, err)
			logOutput := buf.String()

			for _, expectedLog := range tt.expectedLogs {
				assert.Contains(t,
					logOutput, expectedLog,
					"Log output should contain '%s'\nGot logs:\n%s",
					expectedLog,
					logOutput,
				)
			}

			for _, unexpectedLog := range tt.unexpectedLogs {
				assert.NotContains(t,
					logOutput, unexpectedLog,
					"Log output should not contain '%s'\nGot logs:\n%s",
					unexpectedLog,
					logOutput,
				)
			}

			assert.True(t, handler.called, "handler should have been called")
		})
	}
}

func TestTimeoutInterceptor(t *testing.T) {
	tests := []struct {
		expectedError  error
		name           string
		timeout        time.Duration
		handlerDelay   time.Duration
		expectedCalled bool
	}{
		{
			name:           "handler completes before timeout",
			timeout:        100 * time.Millisecond,
			handlerDelay:   50 * time.Millisecond,
			expectedError:  nil,
			expectedCalled: true,
		},
		{
			name:           "handler exceeds timeout",
			timeout:        50 * time.Millisecond,
			handlerDelay:   100 * time.Millisecond,
			expectedError:  status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
			expectedCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test context and request
			ctx := context.Background()
			req := "test_request"
			handler := &mockHandler{}

			// Create the interceptor with the specified timeout
			interceptor := TimeoutInterceptor(tt.timeout)

			// Call the interceptor
			resp, err := interceptor(
				ctx,
				req,
				&grpc.UnaryServerInfo{FullMethod: "TestTimeoutMethod"},
				func(ctx context.Context, req any) (any, error) {
					return handler.Handle(ctx, req, tt.handlerDelay)
				},
			)

			// Verify response and error
			if tt.expectedError == nil {
				// Verify handler was called
				assert.Equal(t, tt.expectedCalled, handler.called, "handler call status should match expected")

				require.NoError(t, err)
				assert.Equal(t, "test_response", resp, "response should match expected")
			} else {
				require.Error(t, err)
				require.Equal(t, tt.expectedError, err, "A matching error is expected")
			}
		})
	}
}
