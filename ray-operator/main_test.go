package main

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
)

func Test_decodeConfig(t *testing.T) {
	testcases := []struct {
		name           string
		configData     string
		errContains    string
		expectedConfig configapi.Configuration
		expectErr      bool
	}{
		{
			name: "default config file",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "config file all field set",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "config with sidecars",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
headSidecarContainers:
- name: fluentbit
  image: fluent/fluent-bit:1.9.6
workerSidecarContainers:
- name: fluentbit
  image: fluent/fluent-bit:1.9.6
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				HeadSidecarContainers: []corev1.Container{
					{
						Name:  "fluentbit",
						Image: "fluent/fluent-bit:1.9.6",
					},
				},
				WorkerSidecarContainers: []corev1.Container{
					{
						Name:  "fluentbit",
						Image: "fluent/fluent-bit:1.9.6",
					},
				},
				QPS:   ptr.To(configapi.DefaultQPS),
				Burst: ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "config with default pod metadata",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
defaultPodAnnotations:
  monitoring.example.com/scrape: "true"
  monitoring.example.com/port: "8080"
defaultPodLabels:
  app.kubernetes.io/managed-by: kuberay
  team: platform
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				DefaultPodAnnotations: map[string]string{
					"monitoring.example.com/scrape": "true",
					"monitoring.example.com/port":   "8080",
				},
				DefaultPodLabels: map[string]string{
					"app.kubernetes.io/managed-by": "kuberay",
					"team":                         "platform",
				},
				QPS:   ptr.To(configapi.DefaultQPS),
				Burst: ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "unknown filed ignored",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
unknownfield: 1
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "set QPS and Burst",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
qps: 150.5
burst: 300
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  new(150.5),
				Burst:                new(300),
			},
			expectErr: false,
		},
		{
			name: "set Burst using float",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
qps: 150
burst: 300.5
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				QPS:                  new(150.0),
				Burst:                new(300),
			},
			expectErr:   true,
			errContains: "json: cannot unmarshal number 300.5 into Go struct field Configuration.burst of type int",
		},
		{
			name: "invalid type for field",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: true
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 0,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr:   true,
			errContains: "json: cannot unmarshal bool into Go struct field Configuration.reconcileConcurrency of type int",
		},
		{
			name: "Set ReconcileConcurrency",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 100
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 100,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "Set CollectorImage",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
collectorImage: "my-registry.io/kuberay/collector:v1.7.0"
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       "my-registry.io/kuberay/collector:v1.7.0",
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "config with nodeEventForwarder",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
nodeEventForwarder:
  enabled: true
  sources:
  - node-problem-detector
  - nvidia-gpu-device-plugin
  reasons:
  - XIDError
  - KernelDeadlock
  types:
  - Warning
  - Normal
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				NodeEventForwarder: configapi.NodeEventForwarderConfiguration{
					Enabled: true,
					Sources: []string{"node-problem-detector", "nvidia-gpu-device-plugin"},
					Reasons: []string{"XIDError", "KernelDeadlock"},
					Types:   []string{"Warning", "Normal"},
				},
				QPS:   ptr.To(configapi.DefaultQPS),
				Burst: ptr.To(configapi.DefaultBurst),
			},
			expectErr: false,
		},
		{
			name: "invalid type for nodeEventForwarder",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
nodeEventForwarder: true
`,
			expectErr:   true,
			errContains: "cannot unmarshal bool into Go struct field",
		},
		{
			name: "invalid type for nodeEventForwarder.sources (space-separated string instead of list)",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
metricsAddr: ":8080"
probeAddr: ":8082"
enableLeaderElection: true
reconcileConcurrency: 1
nodeEventForwarder:
  enabled: true
  sources: "node-problem-detector nvidia-gpu-device-plugin"
`,
			expectErr:   true,
			errContains: "cannot unmarshal string into Go struct field",
		},
		{
			name: "config with allowedNodeLabels",
			configData: `apiVersion: config.ray.io/v1alpha1
kind: Configuration
allowedNodeLabels:
- topology.kubernetes.io/zone
- nvidia.com/gpu.clique
`,
			expectedConfig: configapi.Configuration{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Configuration",
					APIVersion: "config.ray.io/v1alpha1",
				},
				MetricsAddr:          ":8080",
				ProbeAddr:            ":8082",
				EnableLeaderElection: new(true),
				ReconcileConcurrency: 1,
				CollectorImage:       configapi.DefaultCollectorImage,
				QPS:                  ptr.To(configapi.DefaultQPS),
				Burst:                ptr.To(configapi.DefaultBurst),
				AllowedNodeLabels:    []string{"topology.kubernetes.io/zone", "nvidia.com/gpu.clique"},
			},
			expectErr: false,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			config, err := decodeConfig([]byte(testcase.configData), scheme)
			if testcase.expectErr {
				if err == nil {
					t.Error("expected err but got nil")
				}

				if err != nil && !strings.Contains(err.Error(), testcase.errContains) {
					t.Logf("actual error: %v", err)
					t.Logf("expected error to contain string: %q", testcase.errContains)
					t.Error("unexpected error")
				}

				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !reflect.DeepEqual(config, testcase.expectedConfig) {
				t.Logf("actual config: %v", config)
				t.Logf("expected config: %v", testcase.expectedConfig)
				t.Error("unexpected config")
			}
		})
	}
}
