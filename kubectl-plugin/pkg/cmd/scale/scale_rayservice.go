package scale

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type ScaleRayServiceOptions struct {
	cmdFactory  cmdutil.Factory
	ioStreams   *genericclioptions.IOStreams
	minReplicas *int32
	maxReplicas *int32
	namespace   string
	workerGroup string
	service     string
}

// A worker group's replica bounds live on both the RayService (durable, applied to
// clusters created later) and its live RayCluster (takes effect now). This command
// writes both in one invocation; patching either object alone leaves the change
// either non-durable or ineffective until the next cluster replacement.
var (
	scaleRayServiceLong = templates.LongDesc(`
		Scale a Ray service by worker group.

		Updates the worker group's min/max replicas on both the RayService (durable — the
		bounds survive cluster replacement) and its live RayCluster (immediate effect).
		The replica count itself is managed by the Ray autoscaler for a Ray service; to
		set a RayCluster's replicas directly, use "kubectl ray scale cluster".
	`)

	scaleRayServiceExample = templates.Examples(`
		# Scale the minimum replicas for a worker group to 2
		kubectl ray scale rayservice my-service --worker-group my-group --min-replicas 2

		# Scale the maximum replicas for a worker group to 10
		kubectl ray scale rayservice my-service --worker-group my-group --max-replicas 10

		# Scale both minimum and maximum replicas for a worker group
		kubectl ray scale rayservice my-service --worker-group my-group --min-replicas 2 --max-replicas 6
	`)
)

// patchOperation is one RFC 6902 operation. "add" is used for the bound fields because
// MinReplicas/MaxReplicas are optional (omitempty) and may be absent from the stored
// object; "add" sets the member whether or not it already exists.
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value"`
}

func NewScaleRayServiceOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *ScaleRayServiceOptions {
	return &ScaleRayServiceOptions{
		cmdFactory:  cmdFactory,
		ioStreams:   &streams,
		minReplicas: new(int32),
		maxReplicas: new(int32),
	}
}

func NewScaleRayServiceCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewScaleRayServiceOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:          "rayservice (RAYSERVICE) (-w/--worker-group WORKERGROUP) (--min-replicas N) (--max-replicas N)",
		Short:        "Scale a Ray service",
		Long:         scaleRayServiceLong,
		Example:      scaleRayServiceExample,
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
	cmd.Flags().Int32VarP(options.minReplicas, "min-replicas", "", -1, "Minimum desired number of replicas for worker group")
	cmd.Flags().Int32VarP(options.maxReplicas, "max-replicas", "", -1, "Maximum desired number of replicas for worker group")
	return cmd
}

func (options *ScaleRayServiceOptions) Complete(args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace
	options.service = args[0]

	return nil
}

func (options *ScaleRayServiceOptions) Validate() error {
	minSet := options.minReplicas != nil && *options.minReplicas != -1
	maxSet := options.maxReplicas != nil && *options.maxReplicas != -1

	if options.workerGroup == "" {
		return fmt.Errorf("must specify -w/--worker-group")
	}

	// Ensure that at least one scaling parameter is specified
	if !minSet && !maxSet {
		return fmt.Errorf("must specify at least one of --min-replicas or --max-replicas (non-negative integers)")
	}

	// Validate that each parameter value is non-negative
	if minSet && *options.minReplicas < 0 {
		return fmt.Errorf("--min-replicas must be a non-negative integer")
	}
	if maxSet && *options.maxReplicas < 0 {
		return fmt.Errorf("--max-replicas must be a non-negative integer")
	}

	// Validate the logical relationship between min and max replicas
	if minSet && maxSet && *options.minReplicas > *options.maxReplicas {
		return fmt.Errorf("--min-replicas (%d) cannot be greater than --max-replicas (%d)", *options.minReplicas, *options.maxReplicas)
	}

	return nil
}

func (options *ScaleRayServiceOptions) Run(ctx context.Context, k8sClient client.Client, writer io.Writer) error {
	rayService, err := k8sClient.RayClient().RayV1().RayServices(options.namespace).Get(ctx, options.service, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to scale worker group %s in Ray service %s in namespace %s: %w", options.workerGroup, options.service, options.namespace, err)
	}

	if pendingCluster := rayService.Status.PendingServiceStatus.RayClusterName; pendingCluster != "" {
		return fmt.Errorf("an incremental upgrade is in progress for Ray service %s (pending cluster %s), so the scaling target is ambiguous. Retry after the upgrade completes", options.service, pendingCluster)
	}

	serviceIndex, err := findWorkerGroup(rayService.Spec.RayClusterSpec.WorkerGroupSpecs, options.workerGroup)
	if err != nil {
		return fmt.Errorf("worker group %s not found in Ray service %s in namespace %s: %w", options.workerGroup, options.service, options.namespace, err)
	}

	serviceOps, serviceChanges, _, _, err := scaleBoundChanges(
		rayService.Spec.RayClusterSpec.WorkerGroupSpecs[serviceIndex],
		options.minReplicas, options.maxReplicas,
		fmt.Sprintf("/spec/rayClusterConfig/workerGroupSpecs/%d", serviceIndex),
	)
	if err != nil {
		return err
	}

	clusterName := rayService.Status.ActiveServiceStatus.RayClusterName
	if clusterName == "" {
		return fmt.Errorf("Ray service %s has no active Ray cluster yet (status.activeServiceStatus.rayClusterName is empty), so there is nothing to scale. Retry once the service is running", options.service)
	}

	rayCluster, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).Get(ctx, clusterName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get the live Ray cluster %s for Ray service %s in namespace %s: %w", clusterName, options.service, options.namespace, err)
	}

	clusterIndex, err := findWorkerGroup(rayCluster.Spec.WorkerGroupSpecs, options.workerGroup)
	if err != nil {
		return fmt.Errorf("worker group %s not found in the live Ray cluster %s in namespace %s: %w", options.workerGroup, clusterName, options.namespace, err)
	}

	clusterOps, clusterChanges, _, _, err := scaleBoundChanges(
		rayCluster.Spec.WorkerGroupSpecs[clusterIndex],
		options.minReplicas, options.maxReplicas,
		fmt.Sprintf("/spec/workerGroupSpecs/%d", clusterIndex),
	)
	if err != nil {
		return err
	}

	if len(serviceOps) == 0 && len(clusterOps) == 0 {
		fmt.Fprintf(writer, "Worker group %s in Ray service %s in namespace %s already matches the requested configuration. Skipping.\n",
			options.workerGroup, options.service, options.namespace)
		return nil
	}

	if len(serviceOps) > 0 {
		if err := patchWorkerGroupBounds(ctx, k8sClient, options.namespace, options.service, rayService.ResourceVersion, serviceOps, true); err != nil {
			return fmt.Errorf("failed to update worker group %s in Ray service %s in namespace %s: %w", options.workerGroup, options.service, options.namespace, err)
		}
		fmt.Fprintf(writer, "Updated worker group %s in Ray service %s in namespace %s (%s)\n",
			options.workerGroup, options.service, options.namespace, strings.Join(serviceChanges, ", "))
	} else {
		fmt.Fprintf(writer, "Worker group %s in Ray service %s in namespace %s already matches the requested configuration. Skipping.\n",
			options.workerGroup, options.service, options.namespace)
	}

	if len(clusterOps) > 0 {
		if err := patchWorkerGroupBounds(ctx, k8sClient, options.namespace, clusterName, rayCluster.ResourceVersion, clusterOps, false); err != nil {
			fmt.Fprintf(writer, "The new bounds were recorded in Ray service %s, but the live Ray cluster %s could not be updated (%v). They will take effect when the cluster is next replaced; re-run this command to apply them to the live cluster now.\n",
				options.service, clusterName, err)
			return fmt.Errorf("failed to update worker group %s in the live Ray cluster %s in namespace %s: %w", options.workerGroup, clusterName, options.namespace, err)
		}
		fmt.Fprintf(writer, "Updated worker group %s in the live Ray cluster %s in namespace %s (%s)\n",
			options.workerGroup, clusterName, options.namespace, strings.Join(clusterChanges, ", "))
	} else {
		fmt.Fprintf(writer, "Worker group %s in the live Ray cluster %s in namespace %s already matches the requested configuration. Skipping.\n",
			options.workerGroup, clusterName, options.namespace)
	}

	return nil
}

// findWorkerGroup resolves a worker group's index by groupName, since group order is not
// guaranteed.
func findWorkerGroup(specs []rayv1.WorkerGroupSpec, groupName string) (int, error) {
	var groupNames []string
	groupIndex := -1
	for i, spec := range specs {
		groupNames = append(groupNames, spec.GroupName)
		if spec.GroupName == groupName {
			groupIndex = i
		}
	}
	if groupIndex == -1 {
		return -1, fmt.Errorf("worker group %s not found. Available worker groups: %s", groupName, strings.Join(groupNames, ", "))
	}
	return groupIndex, nil
}

// scaleBoundChanges builds the patch operations for one object's worker group from the
// current spec and the requested bounds, and validates the object's final state. Fields
// whose value does not change produce no operation, so re-running the command touches
// only the object that still differs.
func scaleBoundChanges(spec rayv1.WorkerGroupSpec, minFlag, maxFlag *int32, basePath string) ([]patchOperation, []string, int32, int32, error) {
	currentMin := int32(0)
	if spec.MinReplicas != nil {
		currentMin = *spec.MinReplicas
	}
	currentMax := int32(math.MaxInt32)
	if spec.MaxReplicas != nil {
		currentMax = *spec.MaxReplicas
	}

	finalMin, finalMax := currentMin, currentMax
	var ops []patchOperation
	var changes []string

	if minFlag != nil {
		finalMin = *minFlag
		if finalMin != currentMin {
			ops = append(ops, patchOperation{Op: "add", Path: basePath + "/minReplicas", Value: finalMin})
			changes = append(changes, fmt.Sprintf("Scaled minReplicas: %d to %d", currentMin, finalMin))
		}
	}
	if maxFlag != nil {
		finalMax = *maxFlag
		if finalMax != currentMax {
			ops = append(ops, patchOperation{Op: "add", Path: basePath + "/maxReplicas", Value: finalMax})
			changes = append(changes, fmt.Sprintf("Scaled maxReplicas: %d to %d", currentMax, finalMax))
		}
	}

	if finalMin > finalMax {
		return nil, nil, 0, 0, fmt.Errorf("cannot set --min-replicas (%d) greater than --max-replicas (%d)", finalMin, finalMax)
	}

	return ops, changes, finalMin, finalMax, nil
}

// patchWorkerGroupBounds applies the operations guarded by a resourceVersion test, so a
// concurrent edit fails loudly instead of being clobbered.
func patchWorkerGroupBounds(ctx context.Context, k8sClient client.Client, namespace, name, resourceVersion string, boundOps []patchOperation, isService bool) error {
	ops := make([]patchOperation, 0, len(boundOps)+1)
	ops = append(ops, patchOperation{Op: "test", Path: "/metadata/resourceVersion", Value: resourceVersion})
	ops = append(ops, boundOps...)

	patchBytes, err := json.Marshal(ops)
	if err != nil {
		return err
	}

	if isService {
		_, err = k8sClient.RayClient().RayV1().RayServices(namespace).Patch(ctx, name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	} else {
		_, err = k8sClient.RayClient().RayV1().RayClusters(namespace).Patch(ctx, name, types.JSONPatchType, patchBytes, metav1.PatchOptions{})
	}
	return err
}
