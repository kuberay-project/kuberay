package create

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type CreateWorkerGroupOptions struct {
	cmdFactory          cmdutil.Factory
	ioStreams           *genericclioptions.IOStreams
	namespace           string
	clusterName         string
	groupName           string
	rayStartParams      map[string]string
	workerNodeSelectors map[string]string
	rayVersion          string
	image               string
	workerCPU           string
	workerGPU           string
	workerTPU           string
	workerMemory        string
	workerReplicas      int32
	numOfHosts          int32
	workerMinReplicas   int32
	workerMaxReplicas   int32
}

var (
	createWorkerGroupLong = templates.LongDesc(`
	Adds a worker group to an existing Ray cluster.
	`)

	createWorkerGroupExample = templates.Examples(fmt.Sprintf(`
		# Create a worker group in an existing Ray cluster with defaults
		kubectl ray create workergroup example-group --ray-cluster sample-cluster

		# Create a worker group in an existing Ray cluster
		kubectl ray create workergroup example-group --ray-cluster sample-cluster --image %s --worker-cpu 2 --worker-memory 5Gi

		# Create a worker group in an existing Ray cluster with TPU
		kubectl ray create workergroup example-tpu-group --ray-cluster sample-cluster --worker-tpu 1 --worker-node-selectors %s=tpu-v5-lite-podslice,%s=1x1

		# For more details on TPU-related node selectors like %s and %s, refer to:
		#https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#availability
	`, util.RayImage, util.NodeSelectorGKETPUAccelerator, util.NodeSelectorGKETPUTopology, util.NodeSelectorGKETPUAccelerator, util.NodeSelectorGKETPUTopology))
)

func NewCreateWorkerGroupOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *CreateWorkerGroupOptions {
	return &CreateWorkerGroupOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewCreateWorkerGroupCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewCreateWorkerGroupOptions(cmdFactory, streams)
	// Silence warnings to avoid messages like 'unknown field "spec.headGroupSpec.template.metadata.creationTimestamp"'
	// See https://github.com/kubernetes/kubernetes/issues/67610 for more details.
	rest.SetDefaultWarningHandler(rest.NoWarnings{})

	cmd := &cobra.Command{
		Use:          "workergroup (WORKERGROUP) [(-r|--ray-cluster) RAYCLUSTER]",
		Short:        "Create worker group in an existing Ray cluster",
		Long:         createWorkerGroupLong,
		Example:      createWorkerGroupExample,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Validate(); err != nil {
				return err
			}

			if err := options.Complete(cmd, args); err != nil {
				return err
			}
			return options.Run(cmd.Context(), cmdFactory)
		},
	}

	cmd.Flags().StringVarP(&options.clusterName, "ray-cluster", "c", "", "Ray cluster to add a worker group to")
	cobra.CheckErr(cmd.MarkFlagRequired("ray-cluster"))
	cmd.Flags().StringVar(&options.rayVersion, "ray-version", util.RayVersion, "Ray version to use")
	cmd.Flags().StringVar(&options.image, "image", defaultImageWithTag, "container image to use")
	cmd.Flags().Int32Var(&options.workerReplicas, "worker-replicas", 1, "desired replicas")
	cmd.Flags().Int32Var(&options.numOfHosts, "num-of-hosts", 1, "number of hosts in the worker group per replica")
	cmd.Flags().Int32Var(&options.workerMinReplicas, "worker-min-replicas", 1, "minimum number of replicas")
	cmd.Flags().Int32Var(&options.workerMaxReplicas, "worker-max-replicas", 10, "maximum number of replicas")
	cmd.Flags().StringVar(&options.workerCPU, "worker-cpu", "2", "number of CPUs in each replica")
	cmd.Flags().StringVar(&options.workerGPU, "worker-gpu", "0", "number of GPUs in each replica")
	cmd.Flags().StringVar(&options.workerTPU, "worker-tpu", "0", "number of TPUs in each replica")
	cmd.Flags().StringVar(&options.workerMemory, "worker-memory", "4Gi", "amount of memory in each replica")
	cmd.Flags().StringToStringVar(&options.rayStartParams, "worker-ray-start-params", make(map[string]string), "a map of arguments to the Ray workers' 'ray start' entrypoint, e.g. '--worker-ray-start-params metrics-export-port=8080,num-cpus=2'")
	cmd.Flags().StringToStringVar(&options.workerNodeSelectors, "worker-node-selectors", make(map[string]string), "Node selectors to apply to all worker pods in this worker group (e.g. --worker-node-selectors cloud.google.com/gke-accelerator=nvidia-l4,cloud.google.com/gke-nodepool=my-node-pool)")

	return cmd
}

func (options *CreateWorkerGroupOptions) Complete(cmd *cobra.Command, args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace

	if options.rayStartParams == nil {
		options.rayStartParams = map[string]string{}
	}

	if len(args) != 1 {
		return cmdutil.UsageErrorf(cmd, "%s", cmd.Use)
	}
	options.groupName = args[0]

	if options.image == "" {
		options.image = fmt.Sprintf("rayproject/ray:%s", options.rayVersion)
	}

	// If the image is the default, align its tag with the configured Ray version.
	if options.image == defaultImageWithTag {
		options.image = fmt.Sprintf("%s:%s", defaultImage, options.rayVersion)
	}

	return nil
}

func (options *CreateWorkerGroupOptions) Validate() error {
	if err := util.ValidateTPU(&options.workerTPU, &options.numOfHosts, options.workerNodeSelectors); err != nil {
		return fmt.Errorf("%w", err)
	}
	return nil
}

func (options *CreateWorkerGroupOptions) Run(ctx context.Context, factory cmdutil.Factory) error {
	k8sClient, err := client.NewClient(factory)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	rayCluster, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).Get(ctx, options.clusterName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error getting Ray cluster: %w", err)
	}

	newRayCluster := rayCluster.DeepCopy()

	newRayCluster.Spec.WorkerGroupSpecs = append(newRayCluster.Spec.WorkerGroupSpecs, createWorkerGroupSpec(options))

	newRayCluster, err = k8sClient.RayClient().RayV1().RayClusters(options.namespace).Update(ctx, newRayCluster, metav1.UpdateOptions{FieldManager: util.FieldManager})
	if err != nil {
		return fmt.Errorf("error updating Ray cluster with new worker group: %w", err)
	}

	fmt.Printf("Updated Ray cluster %s/%s with new worker group\n", newRayCluster.Namespace, newRayCluster.Name)
	return nil
}

func createWorkerGroupSpec(options *CreateWorkerGroupOptions) rayv1.WorkerGroupSpec {
	podTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "ray-worker",
					Image: options.image,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(options.workerCPU),
							corev1.ResourceMemory: resource.MustParse(options.workerMemory),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(options.workerCPU),
							corev1.ResourceMemory: resource.MustParse(options.workerMemory),
						},
					},
				},
			},
			NodeSelector: options.workerNodeSelectors,
		},
	}

	gpuResource := resource.MustParse(options.workerGPU)
	if !gpuResource.IsZero() {
		podTemplate.Spec.Containers[0].Resources.Requests[corev1.ResourceName(util.ResourceNvidiaGPU)] = gpuResource
		podTemplate.Spec.Containers[0].Resources.Limits[corev1.ResourceName(util.ResourceNvidiaGPU)] = gpuResource
	}

	tpuResource := resource.MustParse(options.workerTPU)
	if !tpuResource.IsZero() {
		podTemplate.Spec.Containers[0].Resources.Requests[corev1.ResourceName(util.ResourceGoogleTPU)] = tpuResource
		podTemplate.Spec.Containers[0].Resources.Limits[corev1.ResourceName(util.ResourceGoogleTPU)] = tpuResource
	}
	return rayv1.WorkerGroupSpec{
		GroupName:      options.groupName,
		Replicas:       &options.workerReplicas,
		NumOfHosts:     options.numOfHosts,
		MinReplicas:    &options.workerMinReplicas,
		MaxReplicas:    &options.workerMaxReplicas,
		RayStartParams: options.rayStartParams,
		Template:       podTemplate,
	}
}
