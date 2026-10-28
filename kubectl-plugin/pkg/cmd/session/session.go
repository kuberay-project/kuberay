package session

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
)

type appPort struct {
	name string
	port int
}

type SessionOptions struct {
	cmdFactory     cmdutil.Factory
	ioStreams      *genericiooptions.IOStreams
	currentContext string
	ResourceType   util.ResourceType
	ResourceName   string
	namespace      string
	Verbose        bool
}

const reconnectDelay = 3 * time.Second

var (
	dashboardPort = appPort{
		name: "Ray Dashboard",
		port: 8265,
	}
	clientPort = appPort{
		name: "Ray Interactive Client",
		port: 10001,
	}
	servePort = appPort{
		name: "Ray Serve",
		port: 8000,
	}
)

var (
	sessionLong = templates.LongDesc(`
		Forward local ports to the Ray resources.

		Forward different local ports depending on the resource type: RayCluster, RayJob, or RayService.
	`)

	sessionExample = templates.Examples(`
		# Without specifying the resource type, forward local ports to the Ray cluster
		kubectl ray session my-raycluster

		# Forward local ports to the Ray cluster
		kubectl ray session raycluster/my-raycluster

		# Forward local ports to the Ray cluster used for the Ray job
		kubectl ray session rayjob/my-rayjob

		# Forward local ports to the Ray cluster used for the RayService resource
		kubectl ray session rayservice/my-rayservice
	`)
)

func NewSessionOptions(cmdFactory cmdutil.Factory, streams genericiooptions.IOStreams) *SessionOptions {
	return &SessionOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewSessionCommand(cmdFactory cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := NewSessionOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:     "session (RAYCLUSTER | TYPE/NAME)",
		Short:   "Forward local ports to the Ray resources.",
		Long:    sessionLong,
		Example: sessionExample,
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
			return options.Run(cmd.Context(), cmdFactory)
		},
	}

	cmd.Flags().BoolVarP(&options.Verbose, "verbose", "v", false, "verbose output")

	return cmd
}

func (options *SessionOptions) Complete(cmd *cobra.Command, args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace

	context, err := cmd.Flags().GetString("context")
	if err != nil || context == "" {
		config, err := options.cmdFactory.ToRawKubeConfigLoader().RawConfig()
		if err != nil {
			return fmt.Errorf("error retrieving raw config: %w", err)
		}
		context = config.CurrentContext
	}
	options.currentContext = context

	typeAndName := strings.Split(args[0], "/")
	if len(typeAndName) == 1 {
		options.ResourceType = util.RayCluster
		options.ResourceName = typeAndName[0]
	} else {
		if len(typeAndName) != 2 || typeAndName[1] == "" {
			return cmdutil.UsageErrorf(cmd, "invalid resource type/name: %s", args[0])
		}

		switch typeAndName[0] {
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

func (options *SessionOptions) Run(ctx context.Context, factory cmdutil.Factory) error {
	// Cancel on SIGINT/SIGTERM so the port-forward child is torn down along with
	// the session rather than being reparented to init.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	k8sClient, err := client.NewClient(factory)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	svcName, err := k8sClient.GetRayHeadSvcName(ctx, options.namespace, options.ResourceType, options.ResourceName)
	if err != nil {
		return err
	}
	fmt.Printf("Forwarding ports to service %s\n", svcName)

	var appPorts []appPort
	switch options.ResourceType {
	case util.RayCluster:
		appPorts = []appPort{dashboardPort, clientPort}
	case util.RayJob:
		appPorts = []appPort{dashboardPort}
	case util.RayService:
		appPorts = []appPort{dashboardPort, servePort}
	default:
		return fmt.Errorf("unsupported resource type: %s", options.ResourceType)
	}

	var kubectlArgs []string
	if options.currentContext != "" {
		kubectlArgs = append(kubectlArgs, "--context", options.currentContext)
	}
	// TODO (dxia): forward more global kubectl flags like --server, --insecure-skip-tls-verify, --token, --tls-server-name, --user, --password, etc?

	kubectlArgs = append(kubectlArgs, "port-forward", "-n", options.namespace, "service/"+svcName)
	for _, appPort := range appPorts {
		kubectlArgs = append(kubectlArgs, fmt.Sprintf("%d:%d", appPort.port, appPort.port))
	}

	for _, appPort := range appPorts {
		fmt.Printf("%s: http://localhost:%d\n", appPort.name, appPort.port)
	}
	fmt.Println()

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			portforwardCmd := exec.CommandContext(ctx, "kubectl", kubectlArgs...)
			portforwardCmd.Stdout = options.ioStreams.Out
			portforwardCmd.Stderr = options.ioStreams.ErrOut

			if options.Verbose {
				fmt.Printf("Running: %s\n", strings.Join(portforwardCmd.Args, " "))
			}

			if err = portforwardCmd.Run(); err == nil {
				return
			}

			// The session was terminated: exec.CommandContext has already killed
			// the port-forward child, so stop trying to reconnect.
			if ctx.Err() != nil {
				return
			}

			fmt.Printf("failed to port-forward: %v. Retrying in %v ...\n\n", err, reconnectDelay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(reconnectDelay):
			}
		}
	})

	wg.Wait()
	return nil
}
