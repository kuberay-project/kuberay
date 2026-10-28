package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ray-project/kuberay/podpool/manager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	logruslogger "github.com/virtual-kubelet/virtual-kubelet/log/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	buildVersion = "N/A"
	k8sVersion   = "v1.35.0"
)

func kubeletVersion() string {
	return strings.Join([]string{k8sVersion, "pod-pool", buildVersion}, "-")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	log.L = logruslogger.FromLogrus(logrus.NewEntry(logrus.StandardLogger()))

	rootCmd := &cobra.Command{
		Use:           filepath.Base(os.Args[0]),
		Short:         "Virtual Kubelet for PodPool",
		Long:          "Virtual Kubelet provider for managing PodPool resources",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVirtualKubelet(ctx, cmd)
		},
	}

	// Add flags
	var logLevel string

	rootCmd.Flags().String("kubeconfig", "", "path to kubeconfig file")
	rootCmd.Flags().String("nodename", "virtual-kubelet", "name of the virtual node")
	rootCmd.Flags().String("operating-system", "Linux", "operating system of the node")
	rootCmd.Flags().String("architecture", "amd64", "architecture of the node")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", `set the log level, e.g. "debug", "info", "warn", "error"`)

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if logLevel != "" {
			lvl, err := logrus.ParseLevel(logLevel)
			if err != nil {
				return fmt.Errorf("could not parse log level: %w", err)
			}
			logrus.SetLevel(lvl)
		}
		return nil
	}

	if err := rootCmd.Execute(); err != nil && !errors.Is(err, context.Canceled) {
		log.G(ctx).Fatal(err)
	}
}

func runVirtualKubelet(ctx context.Context, cmd *cobra.Command) error {
	kubeconfig, _ := cmd.Flags().GetString("kubeconfig")
	nodeName, _ := cmd.Flags().GetString("nodename")
	operatingOS, _ := cmd.Flags().GetString("operating-system")
	arch, _ := cmd.Flags().GetString("architecture")

	// Build Kubernetes client config
	var config *rest.Config
	var err error
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return fmt.Errorf("failed to create kubernetes config: %w", err)
	}

	// Create Kubernetes client
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// Create the pod manager (provider)
	podManager, err := manager.NewCachePodManager(nodeName, operatingOS, arch, clientset)
	if err != nil {
		return fmt.Errorf("failed to create pod manager: %w", err)
	}

	// Start the pod manager
	if err := podManager.Start(ctx); err != nil {
		return fmt.Errorf("failed to start pod manager: %w", err)
	}

	// Create the virtual node object
	virtualNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				"type":                   "virtual-kubelet",
				"kubernetes.io/role":     "agent",
				"kubernetes.io/hostname": nodeName,
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "virtual-kubelet.io/provider",
					Value:  "podpool",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				OperatingSystem: operatingOS,
				Architecture:    arch,
				KubeletVersion:  kubeletVersion(),
			},
			Capacity:    corev1.ResourceList{},
			Allocatable: corev1.ResourceList{},
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
				{
					Type:   corev1.NodeMemoryPressure,
					Status: corev1.ConditionFalse,
				},
				{
					Type:   corev1.NodeDiskPressure,
					Status: corev1.ConditionFalse,
				},
				{
					Type:   corev1.NodePIDPressure,
					Status: corev1.ConditionFalse,
				},
				{
					Type:   corev1.NodeNetworkUnavailable,
					Status: corev1.ConditionFalse,
				},
			},
			Addresses:       []corev1.NodeAddress{},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{},
		},
	}

	// Create node controller
	nodeRunner, err := node.NewNodeController(
		podManager,
		virtualNode,
		clientset.CoreV1().Nodes(),
	)
	if err != nil {
		return fmt.Errorf("failed to create node controller: %w", err)
	}

	log.G(ctx).Infof("Starting virtual kubelet for node: %s", nodeName)
	if err := nodeRunner.Run(ctx); err != nil {
		return fmt.Errorf("node controller exited with error: %w", err)
	}

	return nil
}
