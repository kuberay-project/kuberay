package job

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	pluginclient "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	clienttesting "github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client/testing"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestRayJobSubmitComplete(t *testing.T) {
	tests := []struct {
		expected   *SubmitJobOptions
		name       string
		image      string
		rayVersion string
	}{
		{
			name:       "Custom Ray version",
			image:      defaultImageWithTag,
			rayVersion: "custom",
			expected: &SubmitJobOptions{
				image:      "rayproject/ray:custom",
				rayVersion: "custom",
			},
		},
		{
			name:       "Custom Ray version with non-default custom image",
			image:      "custom-image",
			rayVersion: "custom",
			expected: &SubmitJobOptions{
				image:      "custom-image",
				rayVersion: "custom",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
			configFlags := genericclioptions.NewConfigFlags(true)
			cmdFactory := cmdutil.NewFactory(configFlags)

			fakeSubmitJobOptions := NewJobSubmitOptions(cmdFactory, testStreams)
			fakeSubmitJobOptions.runtimeEnv = "././fake/path/to/env/yaml"
			fakeSubmitJobOptions.fileName = "fake/path/to/rayjob.yaml"
			fakeSubmitJobOptions.image = tc.image
			fakeSubmitJobOptions.rayVersion = tc.rayVersion

			err := fakeSubmitJobOptions.Complete()

			require.NoError(t, err)
			assert.Equal(t, "default", fakeSubmitJobOptions.namespace)
			assert.Equal(t, "fake/path/to/env/yaml", fakeSubmitJobOptions.runtimeEnv)
			assert.Equal(t, tc.expected.image, fakeSubmitJobOptions.image)
			assert.Equal(t, tc.expected.rayVersion, fakeSubmitJobOptions.rayVersion)
		})
	}
}

func TestRayJobSubmitWithYamlValidate(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	fakeDir := t.TempDir()

	tests := []struct {
		name        string
		yamlContent string
		expectError string
	}{
		{
			name: "Successful submit job validation with RayJob",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'`,
		},
		{
			name: "BackoffLimit co-exist with InteractiveMode",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'
  backoffLimit: 1`,
			expectError: "BackoffLimit is incompatible with InteractiveMode",
		},
		{
			name: "BackoffLimit is set to 0 with InteractiveMode",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'
  backoffLimit: 0`,
		},
		{
			name: "shutdownAfterJobFinishes is false and ttlSecondsAfterFinished is not zero",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: false
  ttlSecondsAfterFinished: 10
  submissionMode: 'InteractiveMode'`,
			expectError: "ttlSecondsAfterFinished is only supported when shutdownAfterJobFinishes is set to true",
		},
		{
			name: "shutdownAfterJobFinishes is true and ttlSecondsAfterFinished is not zero",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 10
  submissionMode: 'InteractiveMode'`,
		},
		{
			name: "ttlSecondsAfterFinished is less than zero",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: -10
  submissionMode: 'InteractiveMode'`,
			expectError: "ttlSecondsAfterFinished must be greater than or equal to 0",
		},
		{
			name: "ttlSecondsAfterFinished is less than zero",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 0
  submissionMode: 'InteractiveMode'`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rayJobYamlPath := filepath.Join(fakeDir, "rayjob-temp-*.yaml")

			file, err := os.Create(rayJobYamlPath)
			require.NoError(t, err)
			_, err = file.Write([]byte(tc.yamlContent))
			require.NoError(t, err)

			opts := &SubmitJobOptions{
				cmdFactory: cmdFactory,
				ioStreams:  &testStreams,
				fileName:   rayJobYamlPath,
				workingDir: "Fake/File/Path",
			}

			err = opts.Validate(&cobra.Command{})
			if tc.expectError != "" {
				require.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRayJobSubmitWithoutYamlValidate(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	test := []struct {
		name                     string
		rayjobName               string
		expectError              string
		ttlSecondsAfterFinished  int32
		shutdownAfterJobFinishes bool
	}{
		{
			name:                    "ttlSecondsAfterFinished is validate value",
			rayjobName:              "rayjob-sample",
			ttlSecondsAfterFinished: 10,
		},
		{
			name:                    "ttlSecondsAfterFinished is less than zero",
			rayjobName:              "rayjob-sample",
			ttlSecondsAfterFinished: -10,
			expectError:             "--ttl-seconds-after-finished must be greater than or equal to 0",
		},
		{
			name:                    "ttlSecondsAfterFinished is equal to zero",
			rayjobName:              "rayjob-sample",
			ttlSecondsAfterFinished: 0,
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			opts := &SubmitJobOptions{
				cmdFactory:              cmdFactory,
				ioStreams:               &testStreams,
				rayjobName:              tc.rayjobName,
				workingDir:              "Fake/File/Path",
				ttlSecondsAfterFinished: tc.ttlSecondsAfterFinished,
			}
			err := opts.Validate(&cobra.Command{})
			if tc.expectError != "" {
				require.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRayJobTerminalResult(t *testing.T) {
	tests := []struct {
		name         string
		status       rayv1.JobStatus
		jobID        string
		message      string
		wantTerminal bool
		wantError    string
	}{
		{
			name:   "pending job is not terminal",
			status: rayv1.JobStatusPending,
		},
		{
			name:         "succeeded job is successful",
			status:       rayv1.JobStatusSucceeded,
			jobID:        "job-succeeded",
			wantTerminal: true,
		},
		{
			name:         "stopped job preserves existing successful behavior",
			status:       rayv1.JobStatusStopped,
			jobID:        "job-stopped",
			wantTerminal: true,
		},
		{
			name:         "failed job returns its message",
			status:       rayv1.JobStatusFailed,
			jobID:        "job-failed",
			message:      "entrypoint failed",
			wantTerminal: true,
			wantError:    "job job-failed failed: entrypoint failed",
		},
		{
			name:         "failed job without ID uses unknown",
			status:       rayv1.JobStatusFailed,
			wantTerminal: true,
			wantError:    "job unknown failed with status FAILED",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &rayv1.RayJob{
				Status: rayv1.RayJobStatus{
					JobStatus: tc.status,
					JobId:     tc.jobID,
					Message:   tc.message,
				},
			}

			terminal, err := rayJobTerminalResult(job)
			assert.Equal(t, tc.wantTerminal, terminal)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
		})
	}
}

func TestCheckJobStatusOnSubmitError(t *testing.T) {
	const (
		namespace = "test-namespace"
		jobName   = "test-rayjob"
		jobID     = "test-job-id"
	)

	tests := []struct {
		name             string
		usingPortForward bool
		status           rayv1.JobStatus
		message          string
		wantError        string
		wantErrorParts   []string
		wantStderr       string
	}{
		{
			name:             "port-forward with succeeded RayJob returns success",
			usingPortForward: true,
			status:           rayv1.JobStatusSucceeded,
			wantStderr:       "Ray CLI exited after RayJob test-rayjob reached status SUCCEEDED; treating the submission as successful.\n",
		},
		{
			name:             "port-forward with failed RayJob returns job failure",
			usingPortForward: true,
			status:           rayv1.JobStatusFailed,
			message:          "entrypoint failed",
			wantError:        "job test-job-id failed: entrypoint failed",
		},
		{
			name:             "port-forward with running RayJob preserves submit error",
			usingPortForward: true,
			status:           rayv1.JobStatusRunning,
			wantError:        "Error occurred with Ray job submit: ray CLI exited",
		},
		{
			name:             "direct address preserves submit error without querying RayJob",
			usingPortForward: false,
			wantError:        "Error occurred with Ray job submit: ray CLI exited",
		},
		{
			name:             "RayJob get error preserves submit error and adds context",
			usingPortForward: true,
			wantErrorParts: []string{
				"Error occurred with Ray job submit: ray CLI exited",
				"failed to get RayJob test-namespace/test-rayjob while checking job status",
				`rayjobs.ray.io "test-rayjob" not found`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testStreams, _, _, stderr := genericclioptions.NewTestIOStreams()
			submitErr := errors.New("ray CLI exited")

			var k8sClient pluginclient.Client
			if tc.usingPortForward {
				rayClient := clienttesting.NewRayClientset()
				if tc.status != "" {
					rayClient = clienttesting.NewRayClientset(&rayv1.RayJob{
						ObjectMeta: metav1.ObjectMeta{
							Name:      jobName,
							Namespace: namespace,
						},
						Status: rayv1.RayJobStatus{
							JobStatus: tc.status,
							JobId:     jobID,
							Message:   tc.message,
						},
					})
				}
				k8sClient = pluginclient.NewClientForTesting(nil, rayClient)
			} else {
				// A nil Ray client makes this test panic if direct-address mode
				// unexpectedly tries to check the job status through Kubernetes.
				k8sClient = pluginclient.NewClientForTesting(nil, nil)
			}

			options := &SubmitJobOptions{
				ioStreams: &testStreams,
				namespace: namespace,
				RayJob: &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
				}},
			}

			err := options.checkJobStatusOnSubmitError(
				context.Background(),
				k8sClient,
				tc.usingPortForward,
				submitErr,
			)

			if tc.wantError == "" && len(tc.wantErrorParts) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				if tc.wantError != "" {
					require.EqualError(t, err, tc.wantError)
				}
				for _, part := range tc.wantErrorParts {
					require.ErrorContains(t, err, part)
				}
			}
			assert.Equal(t, tc.wantStderr, stderr.String())
		})
	}
}

func TestRayJobSubmit_AddressValidation(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	test := []struct {
		name             string
		address          string
		expectError      string
		expectNormalized string
		flagChanged      bool
	}{
		{
			name:        "address flag not set: port-forward mode",
			address:     "",
			flagChanged: false,
		},
		{
			name:        "address flag set but empty: error",
			address:     "",
			flagChanged: true,
			expectError: "--address was provided but is empty",
		},
		{
			name:        "valid https address: OK",
			address:     "https://ingress.example.com",
			flagChanged: true,
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			opts := &SubmitJobOptions{
				cmdFactory: cmdFactory,
				ioStreams:  &testStreams,
				rayjobName: "rayjob-sample",
				workingDir: "Fake/File/Path",
			}

			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&opts.address, "address", "", "Ray Dashboard base URL")

			if tc.flagChanged {
				require.NoError(t, cmd.Flags().Set("address", tc.address))
			} else {
				opts.address = tc.address
			}

			err := opts.Validate(cmd)
			if tc.expectError != "" {
				require.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
				if tc.expectNormalized != "" {
					require.Equal(t, tc.expectNormalized, opts.address)
				}
			}
		})
	}
}

func TestRayJobSubmitCmdFlagsOverrideYaml(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	fakeDir := t.TempDir()

	tests := []struct {
		name        string
		yamlContent string
		flagMap     map[string]any
		expectSpec  map[string]any
		expectError string
	}{
		{
			name: "Both shutdownAfterJobFinishes and ttlSecondsAfterFinished are not set in yaml",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 20,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(20),
			},
		},
		{
			name: "Both shutdownAfterJobFinishes and ttlSecondsAfterFinished are set in yaml with wrong values",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: false
  ttlSecondsAfterFinished: -10
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 20,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(20),
			},
		},
		{
			name: "Only shutdownAfterJobFinishes is set in yaml",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: false
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 20,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(20),
			},
		},
		{
			name: "Only ttlSecondsAfterFinished is set in yaml",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  ttlSecondsAfterFinished: 10
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 20,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(20),
			},
		},
		{
			name: "Override only ttlSecondsAfterFinished",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 100
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 200,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(200),
			},
		},
		{
			name: "Set ttl-seconds-after-finished to zero",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": 0,
			},
			expectSpec: map[string]any{
				"ShutdownAfterJobFinishes": true,
				"TTLSecondsAfterFinished":  int32(0),
			},
		},
		{
			name: "Override only ttlSecondsAfterFinished to negative and cause error",
			yamlContent: `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 10
  submissionMode: 'InteractiveMode'`,
			flagMap: map[string]any{
				"ttl-seconds-after-finished": -10,
			},
			expectError: "--ttl-seconds-after-finished must be greater than or equal to 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rayJobYamlPath := filepath.Join(fakeDir, "rayjob-temp-*.yaml")

			file, err := os.Create(rayJobYamlPath)
			require.NoError(t, err)
			_, err = file.Write([]byte(tc.yamlContent))
			require.NoError(t, err)

			opts := &SubmitJobOptions{
				cmdFactory: cmdFactory,
				ioStreams:  &testStreams,
				fileName:   rayJobYamlPath,
				workingDir: "Fake/File/Path",
			}
			cmd := &cobra.Command{}
			cmd.Flags().Int32Var(&opts.ttlSecondsAfterFinished, "ttl-seconds-after-finished", 0, "")

			args := []string{}
			for flag, value := range tc.flagMap {
				if v, ok := value.(bool); ok && v {
					args = append(args, fmt.Sprintf("--%s", flag))
				} else {
					args = append(args, fmt.Sprintf("--%s=%v", flag, value))
				}
			}

			cmd.SetArgs(args)
			err = cmd.ParseFlags(args)
			require.NoError(t, err)

			err = opts.Validate(cmd)
			if tc.expectError != "" {
				require.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
			}

			if tc.expectSpec != nil {
				for field, expected := range tc.expectSpec {
					actual := reflect.ValueOf(opts.RayJob.Spec).FieldByName(field).Interface()
					require.Equal(t, expected, actual)
				}
			}
		})
	}
}

func TestDecodeRayJobYaml(t *testing.T) {
	rayjobtmpfile, err := os.CreateTemp("./", "rayjob-temp-*.yaml")
	require.NoError(t, err)

	defer os.Remove(rayjobtmpfile.Name())

	rayYaml := `apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  submissionMode: 'InteractiveMode'`
	_, err = rayjobtmpfile.Write([]byte(rayYaml))
	require.NoError(t, err)

	rayJobYamlActual, err := decodeRayJobYaml(filepath.Join("./", rayjobtmpfile.Name()))
	require.NoError(t, err)

	assert.Equal(t, "rayjob-sample", rayJobYamlActual.GetName())

	submissionMode := rayJobYamlActual.Spec.SubmissionMode
	assert.Equal(t, rayv1.InteractiveMode, submissionMode)
}

func TestRuntimeEnvHasWorkingDir(t *testing.T) {
	runtimeEnvFile, err := os.CreateTemp("./", "runtime-env-*.yaml")
	require.NoError(t, err)

	defer os.Remove(runtimeEnvFile.Name())

	runTimeEnv := `pip:
  - requests==2.26.0
  - pendulum==2.1.2
env_vars:
  counter_name: "test_counter"
working_dir: /fake/dir/ray_working_dir/
`
	_, err = runtimeEnvFile.Write([]byte(runTimeEnv))
	require.NoError(t, err)

	runtimeEnvActual, err := runtimeEnvHasWorkingDir(filepath.Join("./", runtimeEnvFile.Name()))
	require.NoError(t, err)

	assert.NotEmpty(t, runtimeEnvActual)
	assert.Equal(t, "/fake/dir/ray_working_dir/", runtimeEnvActual)
}

func TestRaySubmitCmd(t *testing.T) {
	testStreams, _, _, _ := genericclioptions.NewTestIOStreams()
	cmdFactory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))
	fakeSubmitJobOptions := NewJobSubmitOptions(cmdFactory, testStreams)

	fakeSubmitJobOptions.runtimeEnv = "/fake-runtime/path"
	fakeSubmitJobOptions.runtimeEnvJson = "{\"env_vars\":{\"counter_name\":\"test_counter\"}"
	fakeSubmitJobOptions.submissionID = "fake-submission-id12345"
	fakeSubmitJobOptions.entryPointCPU = 2.0
	fakeSubmitJobOptions.entryPointGPU = 1.0
	fakeSubmitJobOptions.entryPointMemory = 600
	fakeSubmitJobOptions.entryPointResource = "{\"fake-resource\":{\"the-fake-resource\"}"
	fakeSubmitJobOptions.noWait = true
	fakeSubmitJobOptions.headers = "{\"requestHeaders\": {\"header\": \"header\"}}"
	fakeSubmitJobOptions.verify = "True"
	fakeSubmitJobOptions.workingDir = "/fake/working/dir"
	fakeSubmitJobOptions.entryPoint = "python fake_python_script.py"
	fakeSubmitJobOptions.address = dashboardAddr

	actualCmd, err := fakeSubmitJobOptions.raySubmitCmd()
	require.NoError(t, err)
	expectedCmd := []string{
		"ray",
		"job",
		"submit",
		"--address",
		dashboardAddr,
		"--runtime-env",
		"/fake-runtime/path",
		"--runtime-env-json",
		"{\"env_vars\":{\"counter_name\":\"test_counter\"}",
		"--submission-id",
		"fake-submission-id12345",
		"--entrypoint-num-cpus",
		"2.000000",
		"--entrypoint-num-gpus",
		"1.000000",
		"--entrypoint-memory",
		"600",
		"--entrypoint-resource",
		"{\"fake-resource\":{\"the-fake-resource\"}",
		"--no-wait",
		"--headers",
		"{\"requestHeaders\": {\"header\": \"header\"}}",
		"--verify",
		"True",
		"--working-dir",
		"/fake/working/dir",
		"--",
		"python",
		"fake_python_script.py",
	}

	assert.Equal(t, expectedCmd, actualCmd)
}

func TestRayJobSubmit_FlagsHaveDefaults(t *testing.T) {
	streams, _, _, _ := genericclioptions.NewTestIOStreams()
	factory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))
	opts := NewJobSubmitOptions(factory, streams)

	cmd := NewJobSubmitCommand(factory, streams)
	require.NoError(t, cmd.ParseFlags([]string{}))

	assert.InDelta(t, float32(0), opts.entryPointCPU, 1e-6, "default entrypoint-num-cpus should be 0")
	assert.InDelta(t, float32(0), opts.entryPointGPU, 1e-6, "default entrypoint-num-gpus should be 0")
	assert.Equal(t, 0, opts.entryPointMemory, "default entrypoint-memory should be 0")
	assert.False(t, opts.noWait, "default no-wait should be false")
}

func TestRaySubmitCmd_AddressSelection(t *testing.T) {
	streams, _, _, _ := genericclioptions.NewTestIOStreams()
	factory := cmdutil.NewFactory(genericclioptions.NewConfigFlags(true))

	makeCmd := func(addr string) ([]string, error) {
		opts := NewJobSubmitOptions(factory, streams)
		opts.workingDir = "/fake/working/dir"
		opts.entryPoint = "python fake.py"
		opts.address = addr
		return opts.raySubmitCmd()
	}

	tests := []struct {
		name         string
		address      string
		expectedAddr string
	}{
		{
			name:         "no address provided: falls back to dashboardAddr",
			address:      "",
			expectedAddr: dashboardAddr,
		},
		{
			name:         "custom address provided: uses custom",
			address:      "https://ingress.example.com",
			expectedAddr: "https://ingress.example.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := makeCmd(tc.address)
			require.NoError(t, err)

			require.GreaterOrEqual(t, len(cmd), 5, "command too short")
			assert.Equal(t, "ray", cmd[0])
			assert.Equal(t, "job", cmd[1])
			assert.Equal(t, "submit", cmd[2])

			assert.Equal(t, "--address", cmd[3])
			assert.Equal(t, tc.expectedAddr, cmd[4])

			require.Contains(t, cmd, "--working-dir")
			require.Contains(t, cmd, "--")
		})
	}
}
