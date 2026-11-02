package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clearAuthEnv makes every auth-related env var explicitly empty so tests do not
// depend on the environment they run in.
func clearAuthEnv(t *testing.T) {
	t.Helper()
	t.Setenv(RAY_AUTH_MODE_ENV_VAR, "")
	t.Setenv(RAY_AUTH_TOKEN_ENV_VAR, "")
	t.Setenv(RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR, "")
}

func TestIsRayAuthEnabled(t *testing.T) {
	tests := []struct {
		scenario string
		authMode string
		expected bool
	}{
		{scenario: "unset", authMode: "", expected: false},
		{scenario: "token mode", authMode: "token", expected: true},
		{scenario: "token mode is case insensitive", authMode: "Token", expected: true},
		{scenario: "unknown mode", authMode: "disabled", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.scenario, func(t *testing.T) {
			clearAuthEnv(t)
			t.Setenv(RAY_AUTH_MODE_ENV_VAR, tc.authMode)
			assert.Equal(t, tc.expected, isRayAuthEnabled())
		})
	}
}

func TestIsK8sTokenAuthEnabled(t *testing.T) {
	tests := []struct {
		scenario string
		value    string
		expected bool
	}{
		{scenario: "unset", value: "", expected: false},
		{scenario: "true", value: "true", expected: true},
		{scenario: "1", value: "1", expected: true},
		{scenario: "TRUE", value: "TRUE", expected: true},
		{scenario: "false", value: "false", expected: false},
		{scenario: "not a bool", value: "yes", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.scenario, func(t *testing.T) {
			clearAuthEnv(t)
			t.Setenv(RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR, tc.value)
			assert.Equal(t, tc.expected, isK8sTokenAuthEnabled())
		})
	}
}

func TestGetRayAuthToken(t *testing.T) {
	t.Run("auth disabled returns no token", func(t *testing.T) {
		clearAuthEnv(t)
		token, err := getRayAuthToken()
		require.NoError(t, err)
		assert.Empty(t, token)
	})

	t.Run("static token is trimmed", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv(RAY_AUTH_MODE_ENV_VAR, RayAuthModeToken)
		t.Setenv(RAY_AUTH_TOKEN_ENV_VAR, "  secret\n")
		token, err := getRayAuthToken()
		require.NoError(t, err)
		assert.Equal(t, "secret", token)
	})

	t.Run("static token is used even without token auth mode", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv(RAY_AUTH_TOKEN_ENV_VAR, "secret")
		token, err := getRayAuthToken()
		require.NoError(t, err)
		assert.Equal(t, "secret", token)
	})

	t.Run("token auth mode without any credential fails", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv(RAY_AUTH_MODE_ENV_VAR, RayAuthModeToken)
		_, err := getRayAuthToken()
		require.Error(t, err)
		require.ErrorIs(t, err, errRayAuthConfig)
		assert.Contains(t, err.Error(), "no credential is available")
	})

	t.Run("missing projected token fails", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv(RAY_AUTH_MODE_ENV_VAR, RayAuthModeToken)
		t.Setenv(RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR, "true")
		_, err := getRayAuthToken()
		require.Error(t, err)
		require.ErrorIs(t, err, errRayAuthConfig)
		assert.Contains(t, err.Error(), RayK8sTokenPath)
	})
}

func TestSetRayAuthHeader(t *testing.T) {
	t.Run("sets bearer header when a token exists", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv(RAY_AUTH_TOKEN_ENV_VAR, "secret")
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com", nil)
		require.NoError(t, SetRayAuthHeader(req))
		assert.Equal(t, "Bearer secret", req.Header.Get(RayAuthHeader))
	})

	t.Run("leaves header unset when auth is disabled", func(t *testing.T) {
		clearAuthEnv(t)
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com", nil)
		require.NoError(t, SetRayAuthHeader(req))
		assert.Empty(t, req.Header.Get(RayAuthHeader))
	})
}
