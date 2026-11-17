package v1alpha1

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"

	kaischeduler "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kai-scheduler"
	schedulerPlugins "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/scheduler-plugins"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/volcano"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/yunikorn"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestValidateBatchSchedulerConfig(t *testing.T) {
	type args struct {
		logger logr.Logger
		config Configuration
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "legacy option, enable-batch-scheduler=false",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					EnableBatchScheduler: false,
				},
			},
			wantErr: false,
		},
		{
			name: "legacy option, enable-batch-scheduler=true",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					EnableBatchScheduler: true,
				},
			},
			wantErr: false,
		},
		{
			name: "valid option, batch-scheduler=yunikorn",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					BatchScheduler: yunikorn.GetPluginName(),
				},
			},
			wantErr: false,
		},
		{
			name: "valid option, batch-scheduler=volcano",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					BatchScheduler: volcano.GetPluginName(),
				},
			},
			wantErr: false,
		},
		{
			name: "valid option, batch-scheduler=scheduler-plugins",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					BatchScheduler: schedulerPlugins.GetPluginName(),
				},
			},
			wantErr: false,
		},
		{
			name: "valid option, batch-scheduler=kai-scheduler",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					BatchScheduler: kaischeduler.GetPluginName(),
				},
			},
			wantErr: false,
		},
		{
			name: "invalid option, batch-scheduler=kubernetes-was-v1alpha3 requires the KubernetesWAS feature gate",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					BatchScheduler: "kubernetes-was-v1alpha3",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid option, invalid scheduler name",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       "unknown-scheduler-name",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid option, invalid scheduler name default",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       "default",
				},
			},
			wantErr: true,
		},
		{
			name: "both enable-batch-scheduler and batch-scheduler are set",
			args: args{
				logger: testr.New(t),
				config: Configuration{
					EnableBatchScheduler: true,
					BatchScheduler:       "volcano",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateBatchSchedulerConfig(tt.args.logger, tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("ValidateBatchSchedulerConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateBatchSchedulerConfigKubernetesWAS(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.KubernetesWAS, true)

	require.NoError(t, ValidateBatchSchedulerConfig(testr.New(t), Configuration{}))
	require.Error(t, ValidateBatchSchedulerConfig(testr.New(t), Configuration{BatchScheduler: volcano.GetPluginName()}))
	require.Error(t, ValidateBatchSchedulerConfig(testr.New(t), Configuration{EnableBatchScheduler: true}))
}
