package create

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/utils/ptr"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestCreateWorkerGroupSpec(t *testing.T) {
	tests := []struct {
		name     string
		options  *CreateWorkerGroupOptions
		expected rayv1.WorkerGroupSpec
	}{
		{
			name: "default worker group spec",
			options: &CreateWorkerGroupOptions{
				groupName:         "example-group",
				image:             "DEADBEEF",
				workerReplicas:    3,
				numOfHosts:        2,
				workerMinReplicas: 1,
				workerMaxReplicas: 5,
				workerCPU:         "2",
				workerMemory:      "5Gi",
				workerGPU:         "1",
				workerTPU:         "1",
				rayStartParams:    map[string]string{"dashboard-host": "0.0.0.0", "num-cpus": "2"},
				workerNodeSelectors: map[string]string{
					"worker-node-selector": "worker-node-selector-value",
				},
			},
			expected: rayv1.WorkerGroupSpec{
				RayStartParams: map[string]string{"dashboard-host": "0.0.0.0", "num-cpus": "2"},
				GroupName:      "example-group",
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-worker",
								Image: "DEADBEEF",
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:     resource.MustParse("2"),
										corev1.ResourceMemory:  resource.MustParse("5Gi"),
										util.ResourceNvidiaGPU: resource.MustParse("1"),
										util.ResourceGoogleTPU: resource.MustParse("1"),
									},
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:     resource.MustParse("2"),
										corev1.ResourceMemory:  resource.MustParse("5Gi"),
										util.ResourceNvidiaGPU: resource.MustParse("1"),
										util.ResourceGoogleTPU: resource.MustParse("1"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							"worker-node-selector": "worker-node-selector-value",
						},
					},
				},
				Replicas:    ptr.To[int32](3),
				NumOfHosts:  2,
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: ptr.To[int32](5),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, createWorkerGroupSpec(tt.options))
		})
	}
}

func TestCreateWorkerGroupCommandComplete(t *testing.T) {
	tests := []struct {
		flags         map[string]string
		expected      *CreateWorkerGroupOptions
		name          string
		image         string
		rayVersion    string
		expectedError string
		args          []string
	}{
		{
			name:       "Valid input with namespace",
			args:       []string{"example-group"},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace": "test-namespace",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:  "test-namespace",
				groupName:  "example-group",
				image:      "rayproject/ray:latest",
				rayVersion: "latest",
			},
		},
		{
			name:       "Valid input without namespace flag",
			args:       []string{"example-group"},
			rayVersion: "latest",
			expected: &CreateWorkerGroupOptions{
				namespace:  "default",
				groupName:  "example-group",
				image:      "rayproject/ray:latest",
				rayVersion: "latest",
			},
		},
		{
			name:       "Missing group name",
			args:       []string{},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace": "",
			},
			expectedError: "See ' -h' for help and examples",
		},
		{
			name:       "Empty namespace flag",
			args:       []string{"example-group"},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace": "",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:  "default",
				groupName:  "example-group",
				image:      "rayproject/ray:latest",
				rayVersion: "latest",
			},
		},
		{
			name:       "Valid input with rayStartParams",
			args:       []string{"example-group"},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace":               "test-namespace",
				"worker-ray-start-params": "dashboard-host=0.0.0.0,num-cpus=2",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:  "test-namespace",
				groupName:  "example-group",
				image:      "rayproject/ray:latest",
				rayVersion: "latest",
				rayStartParams: map[string]string{
					"dashboard-host": "0.0.0.0",
					"num-cpus":       "2",
				},
			},
		},
		{
			name:       "Empty rayStartParams",
			args:       []string{"example-group"},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace":               "test-namespace",
				"worker-ray-start-params": "",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:      "test-namespace",
				groupName:      "example-group",
				image:          "rayproject/ray:latest",
				rayVersion:     "latest",
				rayStartParams: map[string]string{},
			},
		},
		{
			name:       "Not assign rayStartParams",
			args:       []string{"example-group"},
			rayVersion: "latest",
			flags: map[string]string{
				"namespace": "test-namespace",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:      "test-namespace",
				groupName:      "example-group",
				image:          "rayproject/ray:latest",
				rayVersion:     "latest",
				rayStartParams: map[string]string{},
			},
		},
		{
			name:       "Custom Ray version",
			args:       []string{"example-group"},
			image:      defaultImageWithTag,
			rayVersion: "custom",
			flags: map[string]string{
				"namespace": "test-namespace",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:  "test-namespace",
				groupName:  "example-group",
				image:      "rayproject/ray:custom",
				rayVersion: "custom",
			},
		},
		{
			name:       "Custom Ray version with non-default custom image",
			args:       []string{"example-group"},
			image:      "custom-image",
			rayVersion: "custom",
			flags: map[string]string{
				"namespace": "test-namespace",
			},
			expected: &CreateWorkerGroupOptions{
				namespace:  "test-namespace",
				groupName:  "example-group",
				image:      "custom-image",
				rayVersion: "custom",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configFlags := genericclioptions.NewConfigFlags(true)
			cmd := &cobra.Command{}
			configFlags.AddFlags(cmd.Flags())
			cmd.Flags().String(
				"worker-ray-start-params",
				"",
				"ray start parameters for worker",
			)

			factory := cmdutil.NewFactory(configFlags)
			options := &CreateWorkerGroupOptions{
				cmdFactory: factory,
				image:      tt.image,
				rayVersion: tt.rayVersion,
			}

			for key, value := range tt.flags {
				err := cmd.Flags().Set(key, value)
				if err != nil {
					require.NoError(t, err)
				}
			}

			err := options.Complete(cmd, tt.args)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected.namespace, options.namespace)
				assert.Equal(t, tt.expected.groupName, options.groupName)
				assert.Equal(t, tt.expected.image, options.image)
				assert.Equal(t, tt.expected.rayVersion, options.rayVersion)
			}
		})
	}
}
