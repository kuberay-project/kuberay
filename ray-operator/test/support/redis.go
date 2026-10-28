package support

import (
	"strings"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/client-go/applyconfigurations/core/v1"
)

func DeployRedis(t Test, namespace string, password string) func() string {
	redisContainer := v1.Container().WithName("redis").WithImage("redis:7.4").
		WithPorts(v1.ContainerPort().WithContainerPort(6379))
	dbSizeCmd := []string{"redis-cli", "--no-auth-warning", "DBSIZE"}
	if password != "" {
		redisContainer.WithCommand("redis-server", "--requirepass", password)
		dbSizeCmd = []string{"redis-cli", "--no-auth-warning", "-a", password, "DBSIZE"}
	}

	pod, err := t.Client().Core().CoreV1().Pods(namespace).Apply(
		t.Ctx(),
		v1.Pod("redis", namespace).
			WithLabels(map[string]string{"app": "redis"}).
			WithSpec(v1.PodSpec().WithContainers(redisContainer)),
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
