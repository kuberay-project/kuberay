package log

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
)

const filePathInPod = "/tmp/ray/session_latest/logs/"

type nodeTypeEnum string

const (
	allNodeType    nodeTypeEnum = "all"
	headNodeType   nodeTypeEnum = "head"
	workerNodeType nodeTypeEnum = "worker"
)

// String is used both by fmt.Print and by Cobra in help text
func (e *nodeTypeEnum) String() string {
	return string(*e)
}

// Set must have pointer receiver so it doesn't change the value of a copy
func (e *nodeTypeEnum) Set(v string) error {
	val := strings.ToLower(v)

	switch val {
	case string(allNodeType), string(headNodeType), string(workerNodeType):
		*e = nodeTypeEnum(val)
		return nil
	default:
		return fmt.Errorf("must be one of %q, %q, or %q", allNodeType, headNodeType, workerNodeType)
	}
}

// Type is only used in help text
func (e *nodeTypeEnum) Type() string {
	return "enum"
}

func nodeTypeCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		fmt.Sprintf("%s\tdownload logs from %s Ray nodes", allNodeType, allNodeType),
		fmt.Sprintf("%s\tdownload logs from %s Ray nodes", headNodeType, headNodeType),
		fmt.Sprintf("%s\tdownload logs from %s Ray nodes", workerNodeType, workerNodeType),
	}, cobra.ShellCompDirectiveDefault
}

type ClusterLogOptions struct {
	cmdFactory   cmdutil.Factory
	ioStreams    *genericclioptions.IOStreams
	Executor     RemoteExecutor
	namespace    string
	outputDir    string
	nodeType     nodeTypeEnum
	ResourceName string
	ResourceType util.ResourceType
	link         string
}

var (
	logLong = templates.LongDesc(`
		Download logs from a Ray cluster and save them to a directory
	`)

	logExample = templates.Examples(`
		# Download logs from a Ray cluster and save them to a directory with the Ray cluster's name. Retrieves 'all' logs
		kubectl ray log my-raycluster

		# Download logs from a Ray cluster and save them to a directory named /path/to/dir
		kubectl ray log my-raycluster --out-dir /path/to/dir

		# Download logs from a Ray cluster, but only for the head node
		kubectl ray log my-raycluster --node-type head

		# Download logs from a Ray cluster, but only for the worker nodes
		kubectl ray log my-raycluster --node-type worker

		# Download all (worker node and head node) the logs from a Ray cluster
		kubectl ray log my-raycluster --node-type all

		# Print a link to Google Cloud Console to view logs for a Ray cluster
		kubectl ray log my-raycluster --link gke
	`)

	// flag to check if output directory is generated and needs to be deleted
	deleteOutputDir = false
)

func NewClusterLogOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *ClusterLogOptions {
	return &ClusterLogOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
		Executor:   &DefaultRemoteExecutor{},
		nodeType:   allNodeType,
	}
}

func NewClusterLogCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewClusterLogOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:          "log (RAYCLUSTER | TYPE/NAME) [--out-dir DIR_PATH] [--node-type all|head|worker] [--link gke]",
		Short:        "Get Ray cluster logs",
		Long:         logLong,
		Example:      logExample,
		Aliases:      []string{"logs"},
		SilenceUsage: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return cmdutil.UsageErrorf(cmd, "accepts 1 arg, received %d\n%s", len(args), cmd.Use)
			}
			return nil
		},
		ValidArgsFunction: completion.RayClusterResourceNameCompletionFunc(cmdFactory),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, args); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			return options.Run(cmd.Context(), cmdFactory)
		},
	}
	cmd.Flags().StringVar(&options.outputDir, "out-dir", "", "directory to save the logs to")
	cmd.Flags().Var(&options.nodeType, "node-type", "type of Ray node from which to download logs, supports: 'worker', 'head', or 'all'")
	cmd.Flags().StringVar(&options.link, "link", "", "print a link to a cloud console to view logs, supports: 'gke'")

	cobra.CheckErr(cmd.RegisterFlagCompletionFunc("node-type", nodeTypeCompletion))
	cobra.CheckErr(cmd.RegisterFlagCompletionFunc("link", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"gke"}, cobra.ShellCompDirectiveNoFileComp
	}))

	return cmd
}

func (options *ClusterLogOptions) Complete(cmd *cobra.Command, args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace

	typeAndName := strings.Split(args[0], "/")
	if len(typeAndName) == 1 {
		options.ResourceType = util.RayCluster
		options.ResourceName = typeAndName[0]
	} else {
		if len(typeAndName) != 2 || typeAndName[1] == "" {
			return cmdutil.UsageErrorf(cmd, "invalid resource type/name: %s", args[0])
		}

		switch strings.ToLower(typeAndName[0]) {
		case string(util.RayCluster):
			options.ResourceType = util.RayCluster
		case string(util.RayJob):
			options.ResourceType = util.RayJob
		case string(util.RayService):
			options.ResourceType = util.RayService
		default:
			return cmdutil.UsageErrorf(cmd, "unsupported resource type: %s", typeAndName[0])
		}

		options.ResourceName = typeAndName[1]
	}

	return nil
}

func (options *ClusterLogOptions) Validate() error {
	switch options.nodeType {
	case "all":
		fmt.Fprintln(options.ioStreams.Out, "Command set to retrieve both head and worker node logs.")
	case "head":
		fmt.Fprintln(options.ioStreams.Out, "Command set to retrieve only head node logs.")
	case "worker":
		fmt.Fprintln(options.ioStreams.Out, "Command set to retrieve only worker node logs.")
	default:
		return fmt.Errorf("unknown node type `%s`", options.nodeType)
	}

	if options.link != "" {
		if options.link != "gke" {
			return fmt.Errorf("unsupported link type %q, only %q is supported", options.link, "gke")
		}
		if options.outputDir != "" {
			return fmt.Errorf("--out-dir is incompatible with --link")
		}

		// No more validation for the output dir is needed if printing a link.
		return nil
	}

	if options.outputDir == "" {
		fmt.Fprintln(options.ioStreams.Out, "No output directory specified, creating dir under current directory using resource name.")
		options.outputDir = options.ResourceName
		err := os.MkdirAll(options.outputDir, 0o755)
		if err != nil {
			return fmt.Errorf("could not create directory with cluster name %s: %w", options.outputDir, err)
		}
		deleteOutputDir = true
	}

	info, err := os.Stat(options.outputDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("Directory does not exist. Failed with: %w", err)
	} else if err != nil {
		return fmt.Errorf("Error occurred will checking directory: %w", err)
	} else if !info.IsDir() {
		return fmt.Errorf("Path is not a directory. Please input a directory and try again")
	}

	return nil
}

func (options *ClusterLogOptions) Run(ctx context.Context, factory cmdutil.Factory) error {
	clientSet, err := client.NewClient(factory)
	if err != nil {
		return fmt.Errorf("failed to retrieve Kubernetes client set: %w", err)
	}

	// Retrieve RayCluster name for the non RayCluster type node
	var clusterName string
	switch options.ResourceType {
	case util.RayCluster:
		clusterName = options.ResourceName
	case util.RayJob:
		rayJob, err := clientSet.RayClient().RayV1().RayJobs(options.namespace).Get(ctx, options.ResourceName, v1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve Ray job info for %s: %w", options.ResourceName, err)
		}
		clusterName = rayJob.Status.RayClusterName
	case util.RayService:
		rayService, err := clientSet.RayClient().RayV1().RayServices(options.namespace).Get(ctx, options.ResourceName, v1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to retrieve Ray job info for %s: %w", options.ResourceName, err)
		}
		clusterName = rayService.Status.ActiveServiceStatus.RayClusterName
	default:
		return fmt.Errorf("unsupported resource type: %s", options.ResourceType)
	}

	// If a cloud link is requested, break out and generate a link to query for the
	// cluster name.
	if options.link == "gke" {
		return options.gcpLink(ctx, clientSet, clusterName)
	}

	// set the list options for the specified nodetype
	var listopts v1.ListOptions
	switch options.nodeType {
	case "all":
		listopts = v1.ListOptions{
			LabelSelector: fmt.Sprintf("ray.io/cluster=%s", clusterName),
		}
	case "head":
		listopts = v1.ListOptions{
			LabelSelector: fmt.Sprintf("ray.io/node-type=head, ray.io/cluster=%s", clusterName),
		}
	case "worker":
		listopts = v1.ListOptions{
			LabelSelector: fmt.Sprintf("ray.io/node-type=worker, ray.io/cluster=%s", clusterName),
		}
	default:
		return fmt.Errorf("Unknown ray resource node type: %s", options.nodeType)
	}

	// Get list of nodes that are considered the specified node type
	rayNodes, err := clientSet.KubernetesClient().CoreV1().Pods(options.namespace).List(ctx, listopts)
	if err != nil {
		return fmt.Errorf("failed to retrieve head node for Ray cluster %s: %w", clusterName, err)
	}
	if len(rayNodes.Items) == 0 {
		// Clean up the empty directory if the directory was generated. Since it will always be in current dir, only Remove() is used.
		if deleteOutputDir {
			os.Remove(options.outputDir)
		}
		return fmt.Errorf("No Ray nodes found for resource %s", clusterName)
	}

	// Get a list of logs of the Ray nodes.
	var logList []*bytes.Buffer
	for _, rayNode := range rayNodes.Items {
		// Since the first container is always the Ray container, we will retrieve the first container logs
		containerName := rayNode.Spec.Containers[0].Name
		request := clientSet.KubernetesClient().CoreV1().Pods(rayNode.Namespace).GetLogs(rayNode.Name, &corev1.PodLogOptions{Container: containerName})

		podLogs, err := request.Stream(ctx)
		if err != nil {
			return fmt.Errorf("Error retrieving log for Ray cluster node %s: %w", rayNode.Name, err)
		}
		defer podLogs.Close()

		// Get current logs:
		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, podLogs)
		if err != nil {
			return fmt.Errorf("Failed to get read current logs for Ray cluster Node %s: %w", rayNode.Name, err)
		}

		logList = append(logList, buf)
	}

	// Pod file name format is name of the Ray node
	for ind, logList := range logList {
		curFilePath := filepath.Join(options.outputDir, rayNodes.Items[ind].Name, "stdout.log")
		dirPath := filepath.Join(options.outputDir, rayNodes.Items[ind].Name)
		err := os.MkdirAll(dirPath, 0o755)
		if err != nil {
			return fmt.Errorf("failed to create directory within path %s: %w", dirPath, err)
		}
		file, err := os.OpenFile(curFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("failed to create/open file for kuberay-head with path: %s: %w", curFilePath, err)
		}
		defer file.Close()

		_, err = logList.WriteTo(file)
		if err != nil {
			return fmt.Errorf("failed to write to file for kuberay-head: %s: %w", rayNodes.Items[ind].Name, err)
		}

		containerName := rayNodes.Items[ind].Spec.Containers[0].Name
		req := clientSet.KubernetesClient().CoreV1().RESTClient().
			Get().
			Namespace(rayNodes.Items[ind].Namespace).
			Resource("pods").
			Name(rayNodes.Items[ind].Name).
			SubResource("exec").
			Param("container", containerName).
			VersionedParams(&corev1.PodExecOptions{
				Command: []string{"tar", "--warning=no-file-changed", "-cf", "-", "-C", filePathInPod, "."},
				Stdin:   true,
				Stdout:  true,
				Stderr:  true,
				TTY:     false,
			}, clientgoscheme.ParameterCodec)

		restconfig, err := factory.ToRESTConfig()
		if err != nil {
			return fmt.Errorf("failed to get restconfig: %w", err)
		}

		exec, err := options.Executor.CreateExecutor(restconfig, req.URL())
		if err != nil {
			return fmt.Errorf("failed to create executor with error: %w", err)
		}

		err = options.downloadRayLogFiles(ctx, exec, rayNodes.Items[ind])
		if err != nil {
			return fmt.Errorf("failed to download ray head log files with error: %w", err)
		}
	}
	return nil
}

// RemoteExecutor creates the executor for executing exec on the pod - provided for testing purposes
type RemoteExecutor interface {
	CreateExecutor(restConfig *rest.Config, url *url.URL) (remotecommand.Executor, error)
}

type DefaultRemoteExecutor struct{}

// CreateExecutor returns the executor created by NewSPDYExecutor
func (dre *DefaultRemoteExecutor) CreateExecutor(restConfig *rest.Config, url *url.URL) (remotecommand.Executor, error) {
	return remotecommand.NewSPDYExecutor(restConfig, "POST", url)
}

// downloadRayLogFiles will use to the executor and retrieve the logs file from the inputted Ray head
func (options *ClusterLogOptions) downloadRayLogFiles(ctx context.Context, exec remotecommand.Executor, rayNode corev1.Pod) error {
	outreader, outStream := io.Pipe()
	go func() {
		defer outStream.Close()
		err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdin:  options.ioStreams.In,
			Stdout: outStream,
			Stderr: options.ioStreams.ErrOut,
			Tty:    false,
		})
		if err != nil {
			log.Fatalf("Error occurred while calling remote command: %v", err)
		}
	}()

	// Goes through the tar and create/copy them one by one into the destination dir
	tarReader := tar.NewReader(outreader)
	header, err := tarReader.Next()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("error will extracting head tar file for Ray head %s: %w", rayNode.Name, err)
	}

	fmt.Fprintf(options.ioStreams.Out, "Downloading log for Ray Node %s\n", rayNode.Name)
	for !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("Error reading tar archive: %w", err)
		}

		// Construct the full local path and a directory for the tmp file logs
		localFilePath := filepath.Join(path.Clean(options.outputDir), path.Clean(rayNode.Name), path.Clean(header.Name))

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(localFilePath, 0o755); err != nil {
				return fmt.Errorf("Error creating directory: %w", err)
			}
		case tar.TypeReg:
			// Check for overflow: G115
			if header.Mode < 0 || header.Mode > math.MaxUint32 {
				fmt.Fprintf(options.ioStreams.Out, "file mode out side of acceptable value %d skipping file\n", header.Mode)
			}
			// Create file and write contents
			outFile, err := os.OpenFile(localFilePath, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode)) //nolint:gosec // overflow is guarded by bounds check above
			if err != nil {
				return fmt.Errorf("Error creating file: %w", err)
			}
			defer outFile.Close()
			// This is to limit the copy size for a decompression bomb, currently set arbitrarily
			for {
				n, err := io.CopyN(outFile, tarReader, 1000000)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					return fmt.Errorf("failed while writing to file: %w", err)
				}
				if n == 0 {
					break
				}
			}
		default:
			fmt.Printf("Ignoring unsupported file type: %b", header.Typeflag)
		}

		header, err = tarReader.Next()
		if header == nil && err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("error while extracting tar file with error: %w", err)
		}
	}

	return nil
}

func (options *ClusterLogOptions) gcpLink(ctx context.Context, clientSet client.Client, clusterName string) error {
	// Fetch the head pod to check for fluentbit container.
	headPods, err := clientSet.KubernetesClient().CoreV1().Pods(options.namespace).List(ctx, v1.ListOptions{
		LabelSelector: fmt.Sprintf("ray.io/node-type=head,ray.io/cluster=%s", clusterName),
	})
	if err == nil && len(headPods.Items) > 0 {
		headPod := headPods.Items[0]
		hasFluentbit := false
		for _, container := range headPod.Spec.Containers {
			if container.Name == "fluentbit" {
				hasFluentbit = true
				break
			}
		}
		if !hasFluentbit {
			fmt.Fprintln(options.ioStreams.ErrOut, "Warning: The head pod does not have a 'fluentbit' container. Logs might not be exported to Google Cloud Logging.")
		}
	}

	queryLines := []string{
		`resource.type="k8s_container"`,
		fmt.Sprintf(`resource.labels.namespace_name="%s"`, options.namespace),
		fmt.Sprintf(`labels."k8s-pod/ray_io/cluster"="%s"`, clusterName),
	}

	switch options.nodeType {
	case headNodeType, workerNodeType:
		queryLines = append(queryLines, fmt.Sprintf(`labels."k8s-pod/ray_io/node-type"="%s"`, options.nodeType))
	default:
		queryLines = append(queryLines, `labels."k8s-pod/ray_io/is-ray-node"="yes"`)
	}

	query := strings.Join(queryLines, "\n")
	gcpURL := fmt.Sprintf("https://console.cloud.google.com/logs/query;query=%s", url.QueryEscape(query))

	config, err := options.cmdFactory.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return fmt.Errorf("cannot determine current kubectl context to infer GCP project: %w", err)
	}
	if strings.HasPrefix(config.CurrentContext, "gke_") {
		// GKE context names are usually in the format: gke_PROJECT_LOCATION_NAME.
		_, rest, _ := strings.Cut(config.CurrentContext, "_")
		project, _, _ := strings.Cut(rest, "_")
		gcpURL += fmt.Sprintf("?project=%s", url.QueryEscape(project))
	} else {
		fmt.Fprintln(options.ioStreams.ErrOut, "Warning: The current kubectl context does not appear to be a GKE cluster. The generated link may not work.")
	}

	fmt.Fprintln(options.ioStreams.Out, gcpURL)
	return nil
}
