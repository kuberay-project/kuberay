package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	rpcStatus "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/ray-project/kuberay/apiserversdk/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

type mockTransport struct {
	statusErr      *rpcStatus.Status
	body           string
	statusSequence []int
	callCount      int
	returnDoError  bool
}

// RoundTrip returns the mock HTTP response. When all statuses in
// m.statusSequence are consumed, it returns status 200 (OK).
func (m *mockTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	index := m.callCount
	m.callCount++

	if m.returnDoError {
		return nil, errors.New("mock go error")
	}

	status := http.StatusOK
	if index < len(m.statusSequence) {
		status = m.statusSequence[index]
	}

	respBody := m.body
	if status != http.StatusOK && m.statusErr != nil {
		statusMsg := fmt.Sprintf(`{"code": %d, "message": "%s"}`, m.statusErr.Code, m.statusErr.Message)
		respBody = statusMsg
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(respBody)),
	}, nil
}

func TestUnmarshalHttpResponseOK(t *testing.T) {
	retryCfg := util.RetryConfig{
		MaxRetry:       util.HTTPClientDefaultMaxRetry,
		BackoffFactor:  util.HTTPClientDefaultBackoffFactor,
		InitBackoff:    util.HTTPClientDefaultInitBackoff,
		MaxBackoff:     util.HTTPClientDefaultMaxBackoff,
		OverallTimeout: util.HTTPClientDefaultOverallTimeout,
	}

	client := NewKuberayAPIServerClient("baseurl", nil /*httpClient*/, retryCfg)
	client.ExecuteHttpRequest = func(_ *http.Request, _ string) ([]byte, *rpcStatus.Status, error) {
		resp := &api.ListClustersResponse{
			Clusters: []*api.Cluster{
				{
					Name:      "test-cluster",
					Namespace: "test-namespace",
				},
			},
		}
		bytes, err := client.marshaler.Marshal(resp)
		require.NoError(t, err)
		return bytes, nil, nil
	}

	req := &api.ListClustersRequest{}
	resp, status, err := client.ListClusters(req)
	require.NoError(t, err)
	require.Nil(t, status)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Clusters)
	require.Len(t, resp.Clusters, 1)
	require.Equal(t, "test-cluster", resp.Clusters[0].Name)
	require.Equal(t, "test-namespace", resp.Clusters[0].Namespace)
}

// Unmarshal response fails and check error returned.
func TestUnmarshalHttpResponseFails(t *testing.T) {
	retryCfg := util.RetryConfig{
		MaxRetry:       util.HTTPClientDefaultMaxRetry,
		BackoffFactor:  util.HTTPClientDefaultBackoffFactor,
		InitBackoff:    util.HTTPClientDefaultInitBackoff,
		MaxBackoff:     util.HTTPClientDefaultMaxBackoff,
		OverallTimeout: util.HTTPClientDefaultOverallTimeout,
	}

	client := NewKuberayAPIServerClient("baseurl", nil /*httpClient*/, retryCfg)
	client.ExecuteHttpRequest = func(_ *http.Request, _ string) ([]byte, *rpcStatus.Status, error) {
		// Intentionall returning a bad response.
		return []byte("helloworld"), nil, nil
	}

	req := &api.ListClustersRequest{}
	resp, status, err := client.ListClusters(req)
	require.Nil(t, status)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal", err.Error())
	require.Nil(t, resp)
}

func TestAPIServerClientError(t *testing.T) {
	httpErr := KuberayAPIServerClientError{
		HTTPStatusCode: 500,
	}
	require.Equal(t, "kuberay api server request failed with HTTP status (500: Internal Server Error)", httpErr.Error())
}

func TestAPIServerClientRetry(t *testing.T) {
	statusErr := &rpcStatus.Status{Code: 13, Message: "Internal server error"}
	succeedBody := `{"code": 0, "message": "OK"}`

	// create mock http request
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://mock/test", nil)
	require.NoError(t, err)

	tests := []struct {
		expectErr       error
		transport       *mockTransport
		expectStatus    *rpcStatus.Status
		name            string
		expectBody      []byte
		maxRetry        int
		expectCallCount int
	}{
		{
			name:     "Succeeds on first attempt when maxRetry is 0",
			maxRetry: 0,
			transport: &mockTransport{
				statusSequence: []int{http.StatusOK},
				body:           succeedBody,
			},
			expectCallCount: 1,
			expectErr:       nil,
			expectStatus:    nil,
			expectBody:      []byte(succeedBody),
		},
		{
			name:     "Retries and succeeds on third retry",
			maxRetry: 3,
			transport: &mockTransport{
				// For 4 attempts (maxRetry + 1)
				statusSequence: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
				body:           succeedBody,
			},
			expectCallCount: 4,
			expectErr:       nil,
			expectStatus:    nil,
			expectBody:      []byte(succeedBody),
		},
		{
			name:     "Fails after max retries with internal server error (retryable)",
			maxRetry: 2,
			transport: &mockTransport{
				// For 3 attempts (maxRetry + 1)
				statusSequence: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
				statusErr:      statusErr,
			},
			expectCallCount: 3,
			expectStatus:    statusErr,
			expectErr: &KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusServiceUnavailable,
			},
			expectBody: nil,
		},
		{
			name:     "Stops on non-retryable HTTP status code (403 Forbidden)",
			maxRetry: 3,
			transport: &mockTransport{
				statusSequence: []int{http.StatusForbidden},
				statusErr:      &rpcStatus.Status{Code: 7, Message: "Permission Denied"},
			},
			expectCallCount: 1,
			expectStatus:    &rpcStatus.Status{Code: 7, Message: "Permission Denied"},
			expectErr: &KuberayAPIServerClientError{
				HTTPStatusCode: http.StatusForbidden,
			},
			expectBody: nil,
		},
		{
			name:     "Stops on non-retryable error",
			maxRetry: 3,
			transport: &mockTransport{
				returnDoError: true,
			},
			expectCallCount: 1,
			expectErr:       errors.New("mock go error"),
			expectStatus:    nil,
			expectBody:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &http.Client{Transport: tt.transport}

			retryCfg := util.RetryConfig{
				MaxRetry:       tt.maxRetry,
				BackoffFactor:  util.HTTPClientDefaultBackoffFactor,
				InitBackoff:    util.HTTPClientDefaultInitBackoff,
				MaxBackoff:     util.HTTPClientDefaultMaxBackoff,
				OverallTimeout: util.HTTPClientDefaultOverallTimeout,
			}
			client := NewKuberayAPIServerClient("baseurl", mockClient, retryCfg)

			body, status, err := client.executeRequest(req, "http://mock/test")

			// Check call count is as expected
			require.Equal(t, tt.expectCallCount, tt.transport.callCount)

			if tt.expectErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectErr.Error())
			}

			if tt.expectStatus == nil {
				require.Empty(t, status)
			} else {
				partialStatus := &rpcStatus.Status{
					Code:    status.Code,
					Message: status.Message,
				}
				require.Equal(t, tt.expectStatus, partialStatus)
			}

			if tt.expectBody == nil {
				require.Empty(t, body)
			} else {
				require.Equal(t, tt.expectBody, body)
			}
		})
	}
}

func TestAPIServerClientBackoff(t *testing.T) {
	statusErr := &rpcStatus.Status{Code: 13, Message: "Internal server error"}

	mockTransport := &mockTransport{
		statusSequence: []int{
			http.StatusServiceUnavailable,
			http.StatusServiceUnavailable,
			http.StatusOK,
		},
		statusErr: statusErr,
	}

	mockClient := &http.Client{Transport: mockTransport}

	retryCfg := util.RetryConfig{
		MaxRetry:      util.HTTPClientDefaultMaxRetry,
		BackoffFactor: util.HTTPClientDefaultBackoffFactor,
		// Set short backoff time
		InitBackoff:    1 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		OverallTimeout: util.HTTPClientDefaultOverallTimeout,
	}

	client := NewKuberayAPIServerClient("baseurl", mockClient, retryCfg)

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://mock/test", nil)
	require.NoError(t, err)

	start := time.Now()
	_, _, err = client.executeRequest(req, "http://mock/test")
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, 3, mockTransport.callCount)

	// backoff: 1ms + 2ms
	expectedMin := 3 * time.Millisecond

	require.GreaterOrEqual(t, elapsed, expectedMin)
}

func TestAPIServerClientOverallTimeout(t *testing.T) {
	statusErr := &rpcStatus.Status{Code: 13, Message: "Internal server error"}

	mockTransport := &mockTransport{
		statusSequence: []int{
			http.StatusServiceUnavailable,
			http.StatusServiceUnavailable,
			http.StatusOK,
		},
		statusErr: statusErr,
	}

	mockClient := &http.Client{Transport: mockTransport}

	retryCfg := util.RetryConfig{
		MaxRetry:      util.HTTPClientDefaultMaxRetry,
		BackoffFactor: util.HTTPClientDefaultBackoffFactor,
		InitBackoff:   1 * time.Millisecond,
		MaxBackoff:    50 * time.Millisecond,
		// Set short overall timeout so that the timeout error will be raised
		OverallTimeout: 1 * time.Millisecond,
	}

	client := NewKuberayAPIServerClient("baseurl", mockClient, retryCfg)

	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, "GET", "http://mock/test", nil)
	require.NoError(t, err)

	_, _, err = client.executeRequest(req, "http://mock/test")

	// Expect a timeout error
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry timeout exceeded context deadline")
}
