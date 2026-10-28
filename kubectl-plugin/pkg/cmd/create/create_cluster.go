package create

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/generation"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	rayclient "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned"
)

type CreateClusterOptions struct {
	cmdFactory             cmdutil.Factory
	ioStreams              *genericclioptions.IOStreams
	labels                 map[string]string
	annotations            map[string]string
	workerRayStartParams   map[string]string
	headRayStartParams     map[string]string
	headNodeSelectors      map[string]string
	workerNodeSelectors    map[string]string
	rayClusterConfig       *generation.RayClusterConfig
	headMemory             string
	workerMemory           string
	rayVersion             string
	image                  string
	headCPU                string
	namespace              string
	headEphemeralStorage   string
	headGPU                string
	workerCPU              string
	clusterName            string
	workerEphemeralStorage string
	workerGPU              string
	workerTPU              string
	configFile             string
	autoscaler             generation.AutoscalerVersion
	timeout                time.Duration
	numOfHosts             int32
	workerReplicas         int32
	dryRun                 bool
	wait                   bool
}

var (
	defaultProvisionedTimeout = 5 * time.Minute
	defaultImage              = "rayproject/ray"
	defaultImageWithTag       = fmt.Sprintf("%s:%s", defaultImage, util.RayVersion)

	createClusterLong = templates.LongDesc(`
	Create a Ray cluster with the given name and options.
	`)

	createClusterExample = templates.Examples(fmt.Sprintf(`
		# Create a Ray cluster using default values
		kubectl ray create cluster sample-cluster

		# Create a Ray cluster from flags input
		kubectl ray create cluster sample-cluster --ray-version %s --image %s --head-cpu 1 --head-memory 5Gi --head-ephemeral-storage 10Gi --worker-replicas 3 --worker-cpu 1 --worker-memory 5Gi --worker-ephemeral-storage 10Gi

		# Create a Ray cluster with K8s labels and annotations
		kubectl ray create cluster sample-cluster --labels app=ray,env=dev --annotations ttl-hours=24,owner=chthulu

		# Create a Ray cluster with TPU in default worker group
		kubectl ray create cluster sample-cluster --worker-tpu 1 --worker-node-selectors %s=tpu-v5-lite-podslice,%s=1x1

		# For more details on TPU-related node selectors like %s and %s, refer to:
		# https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#availability

		# Create a Ray cluster from a YAML configuration file
		kubectl ray create cluster sample-cluster --file ray-cluster-config.yaml
	`, util.RayVersion, util.RayImage, util.NodeSelectorGKETPUAccelerator, util.NodeSelectorGKETPUTopology, util.NodeSelectorGKETPUAccelerator, util.NodeSelectorGKETPUTopology))
)

func NewCreateClusterOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *CreateClusterOptions {
	return &CreateClusterOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewCreateClusterCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewCreateClusterOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:          "cluster [CLUSTERNAME]",
		Short:        "Create Ray cluster",
		Long:         createClusterLong,
		Example:      createClusterExample,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, args); err != nil {
				return err
			}
			if err := options.Validate(cmd); err != nil {
				return err
			}

			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			return options.Run(cmd.Context(), k8sClient)
		},
	}

	cmd.Flags().StringToStringVar(&options.labels, "labels", nil, "K8s labels (e.g. --labels app=ray,env=dev)")
	cmd.Flags().StringToStringVar(&options.annotations, "annotations", nil, "K8s annotations (e.g. --annotations ttl-hours=24,owner=chthulu)")
	cmd.Flags().StringVar(&options.rayVersion, "ray-version", util.RayVersion, "Ray version to use")
	cmd.Flags().StringVar(&options.image, "image", defaultImageWithTag, "container image to use")
	cmd.Flags().StringVar(&options.headCPU, "head-cpu", util.DefaultHeadCPU, "number of CPUs in the Ray head")
	cmd.Flags().StringVar(&options.headMemory, "head-memory", util.DefaultHeadMemory, "amount of memory in the Ray head")
	cmd.Flags().StringVar(&options.headGPU, "head-gpu", util.DefaultHeadGPU, "number of GPUs in the Ray head")
	cmd.Flags().StringVar(&options.headEphemeralStorage, "head-ephemeral-storage", util.DefaultHeadEphemeralStorage, "amount of ephemeral storage in the Ray head")
	cmd.Flags().StringToStringVar(&options.headRayStartParams, "head-ray-start-params", make(map[string]string), "a map of arguments to the Ray head's 'ray start' entrypoint, e.g. '--head-ray-start-params dashboard-host=0.0.0.0,num-cpus=2'")
	cmd.Flags().StringToStringVar(&options.headNodeSelectors, "head-node-selectors", make(map[string]string), "Node selectors to apply to all head pods in the cluster (e.g. --head-node-selector=cloud.google.com/gke-accelerator=nvidia-l4,cloud.google.com/gke-nodepool=my-node-pool)")
	cmd.Flags().Int32Var(&options.workerReplicas, "worker-replicas", util.DefaultWorkerReplicas, "desired worker group replicas")
	cmd.Flags().StringVar(&options.workerCPU, "worker-cpu", util.DefaultWorkerCPU, "number of CPUs in each worker group replica")
	cmd.Flags().StringVar(&options.workerMemory, "worker-memory", util.DefaultWorkerMemory, "amount of memory in each worker group replica")
	cmd.Flags().StringVar(&options.workerGPU, "worker-gpu", util.DefaultWorkerGPU, "number of GPUs in each worker group replica")
	cmd.Flags().StringVar(&options.workerEphemeralStorage, "worker-ephemeral-storage", util.DefaultWorkerEphemeralStorage, "amount of ephemeral storage in each worker group replica")
	cmd.Flags().StringVar(&options.workerTPU, "worker-tpu", util.DefaultWorkerTPU,
		fmt.Sprintf(
			"number of TPUs in each worker group replica. If greater than 0, you must also set %s and %s in --worker-node-selectors.",
			util.NodeSelectorGKETPUAccelerator,
			util.NodeSelectorGKETPUTopology,
		),
	)
	cmd.Flags().StringToStringVar(&options.workerRayStartParams, "worker-ray-start-params", make(map[string]string), "a map of arguments to the Ray workers' 'ray start' entrypoint, e.g. '--worker-ray-start-params metrics-export-port=8080,num-cpus=2'")
	cmd.Flags().StringToStringVar(&options.workerNodeSelectors, "worker-node-selectors", make(map[string]string), "Node selectors to apply to all worker pods in the cluster (e.g. --worker-node-selector=cloud.google.com/gke-accelerator=nvidia-l4,cloud.google.com/gke-nodepool=my-node-pool)")
	cmd.Flags().Int32Var(&options.numOfHosts, "num-of-hosts", util.DefaultNumOfHosts, "number of hosts in default worker group per replica")
	cmd.Flags().Var(&options.autoscaler, "autoscaler", fmt.Sprintf("autoscaler to use, supports: %q, %q", generation.AutoscalerV1, generation.AutoscalerV2))
	cmd.Flags().StringVar(&options.configFile, "file", "", "path to a YAML file containing Ray cluster configuration")
	cmd.Flags().BoolVar(&options.dryRun, "dry-run", false, "print the generated YAML instead of creating the cluster")
	cmd.Flags().BoolVar(&options.wait, "wait", false, "wait for the cluster to be provisioned before returning. Returns an error if the cluster is not provisioned by the timeout specified")
	cmd.Flags().DurationVar(&options.timeout, "timeout", defaultProvisionedTimeout, "the timeout for --wait")

	return cmd
}

func (options *CreateClusterOptions) Complete(cmd *cobra.Command, args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace

	if len(args) != 1 {
		return cmdutil.UsageErrorf(cmd, "%s", cmd.Use)
	}
	options.clusterName = args[0]

	if options.image == "" {
		options.image = fmt.Sprintf("%s:%s", defaultImage, options.rayVersion)
	}

	// If the image is the default but the ray version is not the default, set the image to use the specified ray version
	if options.image == defaultImageWithTag && options.rayVersion != util.RayVersion {
		options.image = fmt.Sprintf("%s:%s", defaultImage, options.rayVersion)
	}

	return nil
}

func (options *CreateClusterOptions) Validate(cmd *cobra.Command) error {
	if options.configFile != "" {
		if err := flagsIncompatibleWithConfigFilePresent(cmd); err != nil {
			return err
		}

		// If a cluster config file is provided, check it can be parsed into a RayClusterConfig object
		rayClusterConfig, err := generation.ParseConfigFile(options.configFile)
		if err != nil {
			return fmt.Errorf("failed to parse config file: %w", err)
		}

		if err := generation.ValidateConfig(rayClusterConfig); err != nil {
			return fmt.Errorf("failed to validate config file: %w", err)
		}

		// store the returned RayClusterConfig object for use in Run()
		options.rayClusterConfig = rayClusterConfig
	} else {
		resourceFields := map[string]string{
			"head-cpu":                 options.headCPU,
			"head-gpu":                 options.headGPU,
			"head-memory":              options.headMemory,
			"head-ephemeral-storage":   options.headEphemeralStorage,
			"worker-cpu":               options.workerCPU,
			"worker-gpu":               options.workerGPU,
			"worker-tpu":               options.workerTPU,
			"worker-memory":            options.workerMemory,
			"worker-ephemeral-storage": options.workerEphemeralStorage,
		}

		for name, value := range resourceFields {
			if value != "" || cmd.Flags().Changed(name) {
				if err := util.ValidateResourceQuantity(value, name); err != nil {
					return fmt.Errorf("%w", err)
				}
			}
		}
	}

	if err := util.ValidateTPU(&options.workerTPU, &options.numOfHosts, options.workerNodeSelectors); err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

// resolveClusterName resolves the cluster name from the CLI flag and the config file
func (options *CreateClusterOptions) resolveClusterName() (string, error) {
	var name string

	if options.rayClusterConfig.Name != nil && *options.rayClusterConfig.Name != "" && options.clusterName != "" && options.clusterName != *options.rayClusterConfig.Name {
		return "", fmt.Errorf("the cluster name in the config file %q does not match the cluster name %q. You must use the same name to perform this operation", *options.rayClusterConfig.Name, options.clusterName)
	}

	if options.clusterName != "" {
		name = options.clusterName
	} else if options.rayClusterConfig.Name != nil && *options.rayClusterConfig.Name != "" {
		name = *options.rayClusterConfig.Name
	} else {
		return "", fmt.Errorf("the cluster name is required")
	}

	return name, nil
}

// resolveNamespace resolves the namespace from the CLI flag and the config file
func (options *CreateClusterOptions) resolveNamespace() (string, error) {
	namespace, cliNamespace, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return "", fmt.Errorf("failed to get current namespace: %w", err)
	}

	if cliNamespace && options.rayClusterConfig.Namespace != nil && *options.rayClusterConfig.Namespace != "" && namespace != *options.rayClusterConfig.Namespace {
		return "", fmt.Errorf("the namespace in the config file %q does not match the namespace %q. You must pass --namespace=%s to perform this operation", *options.rayClusterConfig.Namespace, namespace, *options.rayClusterConfig.Namespace)
	}

	if options.rayClusterConfig.Namespace != nil && *options.rayClusterConfig.Namespace != "" {
		options.namespace = *options.rayClusterConfig.Namespace
		namespace = *options.rayClusterConfig.Namespace
	}

	return namespace, nil
}

func (options *CreateClusterOptions) Run(ctx context.Context, k8sClient client.Client) error {
	var rayClusterac *rayv1ac.RayClusterApplyConfiguration
	var err error

	// If options.rayClusterConfig is set, use it exclusively because it means the user provided a config file
	if options.rayClusterConfig != nil {
		name, err := options.resolveClusterName()
		if err != nil {
			return err
		}
		options.rayClusterConfig.Name = &name

		namespace, err := options.resolveNamespace()
		if err != nil {
			return err
		}
		options.rayClusterConfig.Namespace = &namespace
	} else {
		options.rayClusterConfig = &generation.RayClusterConfig{
			Namespace:   &options.namespace,
			Name:        &options.clusterName,
			Labels:      options.labels,
			Annotations: options.annotations,
			RayVersion:  &options.rayVersion,
			Image:       &options.image,
			Head: &generation.Head{
				CPU:              &options.headCPU,
				Memory:           &options.headMemory,
				EphemeralStorage: &options.headEphemeralStorage,
				GPU:              &options.headGPU,
				RayStartParams:   options.headRayStartParams,
				NodeSelectors:    options.headNodeSelectors,
			},
			WorkerGroups: []generation.WorkerGroup{
				{
					Name:             new("default-group"),
					Replicas:         options.workerReplicas,
					NumOfHosts:       &options.numOfHosts,
					CPU:              &options.workerCPU,
					Memory:           &options.workerMemory,
					EphemeralStorage: &options.workerEphemeralStorage,
					GPU:              &options.workerGPU,
					TPU:              &options.workerTPU,
					RayStartParams:   options.workerRayStartParams,
					NodeSelectors:    options.workerNodeSelectors,
				},
			},
			Autoscaler: &generation.Autoscaler{
				Version: options.autoscaler,
			},
		}
	}

	rayClusterac = options.rayClusterConfig.GenerateRayClusterApplyConfig()

	// If dry run is enabled, it will call the YAML converter and print out the YAML
	if options.dryRun {
		rayClusterYaml, err := generation.ConvertRayClusterApplyConfigToYaml(rayClusterac)
		if err != nil {
			return fmt.Errorf("error creating RayCluster YAML: %w", err)
		}
		fmt.Printf("%s\n", rayClusterYaml)
		return nil
	}

	if clusterExists(k8sClient.RayClient(), *options.rayClusterConfig.Namespace, *options.rayClusterConfig.Name) {
		return fmt.Errorf("the Ray cluster %s in namespace %s already exists", *options.rayClusterConfig.Name, *options.rayClusterConfig.Namespace)
	}

	result, err := k8sClient.RayClient().RayV1().RayClusters(*options.rayClusterConfig.Namespace).Apply(ctx, rayClusterac, metav1.ApplyOptions{FieldManager: util.FieldManager})
	if err != nil {
		return fmt.Errorf("failed to create Ray cluster: %w", err)
	}
	fmt.Printf("Created Ray cluster: %s\n", result.GetName())

	if options.wait {
		err = k8sClient.WaitRayClusterProvisioned(ctx, *options.rayClusterConfig.Namespace, result.GetName(), options.timeout)
		if err != nil {
			return err
		}
		fmt.Printf("Ray cluster %s is provisioned\n", result.GetName())
	}

	return nil
}

// clusterExists checks if a RayCluster with the given name exists in the given namespace
func clusterExists(client rayclient.Interface, namespace, name string) bool {
	if _, err := client.RayV1().RayClusters(namespace).Get(context.Background(), name, metav1.GetOptions{}); err == nil {
		return true
	}
	return false
}

// flagsIncompatibleWithConfigFilePresent returns an error if there are command-line flags that are incompatible with a config file
func flagsIncompatibleWithConfigFilePresent(cmd *cobra.Command) error {
	incompatibleFlagsUsed := []string{}

	// Define which flags are allowed to be used with --file.
	// These are typically flags that modify the command's behavior but not the cluster configuration.
	allowedWithFile := map[string]bool{
		"file":      true,
		"context":   true,
		"namespace": true,
		"dry-run":   true,
		"wait":      true,
		"timeout":   true,
	}

	// Check all flags to see if any incompatible flags are set
	cmd.Flags().Visit(func(f *pflag.Flag) {
		if !allowedWithFile[f.Name] {
			incompatibleFlagsUsed = append(incompatibleFlagsUsed, f.Name)
		}
	})

	if len(incompatibleFlagsUsed) > 0 {
		return fmt.Errorf("the following flags are incompatible with --file: %v", incompatibleFlagsUsed)
	}

	return nil
}
