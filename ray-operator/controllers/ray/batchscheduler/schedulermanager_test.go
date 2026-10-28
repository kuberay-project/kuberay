package batchscheduler

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	kaischeduler "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kai-scheduler"
	kuberneteswas "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kubernetes-was"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/volcano"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/yunikorn"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestGetSchedulerFactory(t *testing.T) {
	type args struct {
		rayConfigs v1alpha1.Configuration
	}
	tests := []struct {
		want           reflect.Type
		name           string
		expectedErrMsg string
		args           args
	}{
		{
			name: "enableBatchScheduler=false, batchScheduler=''",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       "",
				},
			},
			want: reflect.TypeFor[*schedulerinterface.DefaultBatchSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler=false, batchScheduler not set",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
				},
			},
			want: reflect.TypeFor[*schedulerinterface.DefaultBatchSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler=false, batchScheduler set to yunikorn",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       yunikorn.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*yunikorn.YuniKornSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler=false, batchScheduler set to volcano",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       volcano.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*volcano.VolcanoBatchSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler=false, batchScheduler set to kai-scheduler",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       kaischeduler.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*kaischeduler.KaiSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler=false, batchScheduler set to kubernetes-was-v1alpha3 is rejected without the gate",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: false,
					BatchScheduler:       "kubernetes-was-v1alpha3",
				},
			},
			expectedErrMsg: "the scheduler is not supported, name=kubernetes-was-v1alpha3",
		},
		{
			name: "enableBatchScheduler not set, batchScheduler set to yunikorn",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					BatchScheduler: yunikorn.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*yunikorn.YuniKornSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler not set, batchScheduler set to volcano",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					BatchScheduler: volcano.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*volcano.VolcanoBatchSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler not set, batchScheduler set to kai-scheduler",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					BatchScheduler: kaischeduler.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*kaischeduler.KaiSchedulerFactory](),
		},
		{
			name: "enableBatchScheduler not set, batchScheduler set to unknown value",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					BatchScheduler: "unknown-scheduler-name",
				},
			},
			expectedErrMsg: "the scheduler is not supported, name=unknown-scheduler-name",
		},
		{
			// for backwards compatibility, if enableBatchScheduler=true, always use volcano
			name: "enableBatchScheduler=true, batchScheduler set to yunikorn",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: true,
					BatchScheduler:       yunikorn.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*volcano.VolcanoBatchSchedulerFactory](),
		},
		{
			// for backwards compatibility, if enableBatchScheduler=true, always use volcano
			name: "enableBatchScheduler=true, batchScheduler set to volcano",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: true,
					BatchScheduler:       volcano.GetPluginName(),
				},
			},
			want: reflect.TypeFor[*volcano.VolcanoBatchSchedulerFactory](),
		},
		{
			// for backwards compatibility, if enableBatchScheduler=true, always use volcano
			name: "enableBatchScheduler=true, batchScheduler set to volcano",
			args: args{
				rayConfigs: v1alpha1.Configuration{
					EnableBatchScheduler: true,
					BatchScheduler:       "",
				},
			},
			want: reflect.TypeFor[*volcano.VolcanoBatchSchedulerFactory](),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getSchedulerFactory(tt.args.rayConfigs)
			if len(tt.expectedErrMsg) > 0 {
				require.Errorf(t, err, tt.expectedErrMsg)
				return
			}

			if reflect.TypeOf(got) != tt.want {
				t.Errorf("getSchedulerFactory() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSchedulerFactoryKubernetesWAS(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.KubernetesWAS, true)

	factory, err := getSchedulerFactory(v1alpha1.Configuration{})
	require.NoError(t, err)
	require.IsType(t, &kuberneteswas.SchedulerFactory{}, factory)
}
