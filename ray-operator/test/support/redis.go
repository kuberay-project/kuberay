package support

import (
	"os"
	"strings"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/client-go/applyconfigurations/core/v1"
)

// StoreConfig defines the container image and CLI commands for a
// Redis-protocol-compatible store (Redis, Valkey, etc.).
type StoreConfig struct {
	Image     string // container image, e.g. "redis:7.4" or "valkey/valkey:8.1.8-alpine"
	ServerCmd string // server binary, e.g. "redis-server" or "valkey-server"
	CLICmd    string // CLI binary, e.g. "redis-cli" or "valkey-cli"
}

var (
	// RedisConfig is the StoreConfig for Redis.
	RedisConfig = StoreConfig{
		Image:     "redis:7.4",
		ServerCmd: "redis-server",
		CLICmd:    "redis-cli",
	}
	// ValkeyConfig is the StoreConfig for Valkey.
	ValkeyConfig = StoreConfig{
		Image:     "valkey/valkey:8.1.8-alpine",
		ServerCmd: "valkey-server",
		CLICmd:    "valkey-cli",
	}
)

// DeployRedis deploys a Redis pod and service in the given namespace.
func DeployRedis(t Test, namespace string, password string) func() string {
	return deployStore(t, namespace, password, RedisConfig)
}

// DeployValkey deploys a Valkey pod and service in the given namespace.
// Valkey (valkey.io) is a BSD-3 licensed, wire-compatible fork of Redis
// maintained by the Linux Foundation.
func DeployValkey(t Test, namespace string, password string) func() string {
	return deployStore(t, namespace, password, ValkeyConfig)
}

// DeployStore deploys the appropriate Redis-compatible store based on the
// KUBERAY_TEST_VALKEY environment variable. When KUBERAY_TEST_VALKEY=1, it
// deploys Valkey; otherwise it deploys Redis. This allows the same GCS FT
// e2e tests to validate both backends without duplicating test functions,
// following the same env-var toggle pattern used by Ray's RocksDB GCS tests
// (TEST_GCS_ROCKSDB=1 in python/ray/tests/conftest.py).
func DeployStore(t Test, namespace string, password string) func() string {
	if os.Getenv("KUBERAY_TEST_VALKEY") == "1" {
		return DeployValkey(t, namespace, password)
	}
	return DeployRedis(t, namespace, password)
}

func deployStore(t Test, namespace string, password string, cfg StoreConfig) func() string {
	t.T().Logf("deployStore: deploying %s (server=%s, cli=%s)", cfg.Image, cfg.ServerCmd, cfg.CLICmd)
	container := v1.Container().WithName("redis").WithImage(cfg.Image).
		WithPorts(v1.ContainerPort().WithContainerPort(6379))
	dbSizeCmd := []string{cfg.CLICmd, "--no-auth-warning", "DBSIZE"}
	if password != "" {
		container.WithCommand(cfg.ServerCmd, "--requirepass", password)
		dbSizeCmd = []string{cfg.CLICmd, "--no-auth-warning", "-a", password, "DBSIZE"}
	}

	pod, err := t.Client().Core().CoreV1().Pods(namespace).Apply(
		t.Ctx(),
		v1.Pod("redis", namespace).
			WithLabels(map[string]string{"app": "redis"}).
			WithSpec(v1.PodSpec().WithContainers(container)),
		TestApplyOptions,
	)
	require.NoError(t.T(), err)

	_, err = t.Client().Core().CoreV1().Services(namespace).Apply(
		t.Ctx(),
		v1.Service("redis", namespace).
			WithSpec(v1.ServiceSpec().
				WithSelector(map[string]string{"app": "redis"}).
				WithPorts(v1.ServicePort().
					WithPort(6379),
				),
			),
		TestApplyOptions,
	)
	require.NoError(t.T(), err)

	return func() string {
		stdout, stderr := ExecPodCmd(t, pod, "redis", dbSizeCmd)
		return strings.TrimSpace(stdout.String() + stderr.String())
	}
}

const (
	RedisAddress  = "redis:6379"
	RedisPassword = "5241590000000000"
)
