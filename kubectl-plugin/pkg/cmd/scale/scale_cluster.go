package scale

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
)

type ScaleClusterOptions struct {
	cmdFactory  cmdutil.Factory
	ioStreams   *genericclioptions.IOStreams
	replicas    *int32
	minReplicas *int32
	maxReplicas *int32
	namespace   string
	workerGroup string
	cluster     string
}

var (
	scaleLong = templates.LongDesc(`
		Scale a Ray cluster by worker group
	`)

	scaleExample = templates.Examples(`
		# Scale a Ray cluster by setting one of its worker groups to 3 replicas
		kubectl ray scale cluster my-cluster --worker-group my-group --replicas 3

		# Scale the maximum replicas for a worker group to 10
		kubectl ray scale cluster my-cluster --worker-group my-group --max-replicas 10

		# Scale both minimum and maximum replicas for a worker group
  		kubectl ray scale cluster my-cluster --worker-group my-group --min-replicas 2 --max-replicas 6

		# Scale the worker group to 5 replicas and update its min and max bounds at the same time
  		kubectl ray scale cluster my-cluster --worker-group my-group --replicas 5 --min-replicas 3 --max-replicas 7
	`)
)

func NewScaleClusterOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *ScaleClusterOptions {
	return &ScaleClusterOptions{
		cmdFactory:  cmdFactory,
		ioStreams:   &streams,
		replicas:    new(int32),
		minReplicas: new(int32),
		maxReplicas: new(int32),
	}
}

func NewScaleClusterCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewScaleClusterOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:          "cluster (RAYCLUSTER) (-w/--worker-group WORKERGROUP) (-r/--replicas N)",
		Short:        "Scale a Ray cluster",
		Long:         scaleLong,
		Example:      scaleExample,
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(args); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			return options.Run(cmd.Context(), k8sClient, os.Stdout)
		},
	}

	cmd.Flags().StringVarP(&options.workerGroup, "worker-group", "w", "", "worker group")
	cobra.CheckErr(cmd.MarkFlagRequired("worker-group"))
	cmd.Flags().Int32VarP(options.replicas, "replicas", "r", -1, "Desired number of replicas in worker group")
	cmd.Flags().Int32VarP(options.minReplicas, "min-replicas", "", -1, "Minimum desired number of replicas for worker group")
	cmd.Flags().Int32VarP(options.maxReplicas, "max-replicas", "", -1, "Maximum desired number of replicas for worker group")
	return cmd
}

func (options *ScaleClusterOptions) Complete(args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace
	options.cluster = args[0]

	return nil
}

func (options *ScaleClusterOptions) Validate() error {
	minSet := options.minReplicas != nil && *options.minReplicas != -1
	maxSet := options.maxReplicas != nil && *options.maxReplicas != -1
	desiredSet := options.replicas != nil && *options.replicas != -1

	if options.workerGroup == "" {
		return fmt.Errorf("must specify -w/--worker-group")
	}

	// Ensure that at least one scaling parameter is specified
	if !minSet && !maxSet && !desiredSet {
		return fmt.Errorf("must specify at least one of --replicas, --min-replicas, or --max-replicas (non-negative integers)")
	}

	// Validate that each parameter value is non-negative
	if minSet && *options.minReplicas < 0 {
		return fmt.Errorf("--min-replicas must be a non-negative integer")
	}
	if maxSet && *options.maxReplicas < 0 {
		return fmt.Errorf("--max-replicas must be a non-negative integer")
	}
	if desiredSet && *options.replicas < 0 {
		return fmt.Errorf("--replicas must be a non-negative integer")
	}

	// Validate the logical relationships between min, max, and desired replicas
	if minSet && maxSet && *options.minReplicas > *options.maxReplicas {
		return fmt.Errorf("--min-replicas (%d) cannot be greater than --max-replicas (%d)", *options.minReplicas, *options.maxReplicas)
	}
	if desiredSet {
		if minSet && *options.replicas < *options.minReplicas {
			return fmt.Errorf("--replicas (%d) cannot be less than --min-replicas (%d)", *options.replicas, *options.minReplicas)
		}
		if maxSet && *options.replicas > *options.maxReplicas {
			return fmt.Errorf("--replicas (%d) cannot be greater than --max-replicas (%d)", *options.replicas, *options.maxReplicas)
		}
	}

	return nil
}

func (options *ScaleClusterOptions) Run(ctx context.Context, k8sClient client.Client, writer io.Writer) error {
	cluster, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).Get(ctx, options.cluster, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to scale worker group %s in Ray cluster %s in namespace %s: %w", options.workerGroup, options.cluster, options.namespace, err)
	}

	// find the index of the worker group
	var workerGroups []string
	workerGroupIndex := -1
	for i, workerGroup := range cluster.Spec.WorkerGroupSpecs {
		workerGroups = append(workerGroups, workerGroup.GroupName)
		if workerGroup.GroupName == options.workerGroup {
			workerGroupIndex = i
		}
	}
	if workerGroupIndex == -1 {
		return fmt.Errorf("worker group %s not found in Ray cluster %s in namespace %s. Available worker groups: %s", options.workerGroup, options.cluster, options.namespace, strings.Join(workerGroups, ", "))
	}

	currentReplicas := int32(0)
	if cluster.Spec.WorkerGroupSpecs[workerGroupIndex].Replicas != nil {
		currentReplicas = *cluster.Spec.WorkerGroupSpecs[workerGroupIndex].Replicas
	}

	currentMinReplicas := int32(0)
	currentMaxReplicas := int32(math.MaxInt32)

	if cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MinReplicas != nil {
		currentMinReplicas = *cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MinReplicas
	}
	if cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MaxReplicas != nil {
		currentMaxReplicas = *cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MaxReplicas
	}

	finalMinReplicas := currentMinReplicas
	finalMaxReplicas := currentMaxReplicas
	finalReplicas := currentReplicas
	hasChanges := false
	var changes []string

	if options.minReplicas != nil && *options.minReplicas >= 0 {
		finalMinReplicas = *options.minReplicas
		if finalMinReplicas != currentMinReplicas {
			cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MinReplicas = &finalMinReplicas
			changes = append(changes, fmt.Sprintf("Scaled minReplicas: %d to %d", currentMinReplicas, *options.minReplicas))
			hasChanges = true
		}
	}
	if options.maxReplicas != nil && *options.maxReplicas >= 0 {
		finalMaxReplicas = *options.maxReplicas
		if finalMaxReplicas != currentMaxReplicas {
			cluster.Spec.WorkerGroupSpecs[workerGroupIndex].MaxReplicas = &finalMaxReplicas
			changes = append(changes, fmt.Sprintf("Scaled maxReplicas: %d to %d", currentMaxReplicas, *options.maxReplicas))
			hasChanges = true
		}
	}
	if options.replicas != nil && *options.replicas >= 0 {
		finalReplicas = *options.replicas
		if finalReplicas != currentReplicas {
			cluster.Spec.WorkerGroupSpecs[workerGroupIndex].Replicas = &finalReplicas
			changes = append(changes, fmt.Sprintf("Scaled Replicas: %d to %d", currentReplicas, *options.replicas))
			hasChanges = true
		}
	}

	// Validate the final state
	if finalMinReplicas > finalMaxReplicas {
		return fmt.Errorf("cannot set --min-replicas (%d) greater than --max-replicas (%d)",
			finalMinReplicas, finalMaxReplicas)
	}
	if finalReplicas < finalMinReplicas {
		return fmt.Errorf("cannot set --replicas (%d) less than --min-replicas (%d)",
			finalReplicas, finalMinReplicas)
	}
	if finalReplicas > finalMaxReplicas {
		return fmt.Errorf("cannot set --replicas (%d) greater than --max-replicas (%d)",
			finalReplicas, finalMaxReplicas)
	}

	if hasChanges {
		_, err = k8sClient.RayClient().RayV1().RayClusters(options.namespace).
			Update(ctx, cluster, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update worker group %s in Ray cluster %s in namespace %s: %w",
				options.workerGroup, options.cluster, options.namespace, err)
		}

		fmt.Fprintf(writer, "Updated worker group %s in Ray cluster %s in namespace %s (%s)\n",
			options.workerGroup, options.cluster, options.namespace, strings.Join(changes, ", "))
	} else {
		fmt.Fprintf(writer, "Worker group %s in Ray cluster %s in namespace %s already matches the requested configuration. Skipping.\n",
			options.workerGroup, options.cluster, options.namespace)
	}

	return nil
}
