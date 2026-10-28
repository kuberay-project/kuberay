package main

import (
	"os"

	flag "github.com/spf13/pflag"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/cmd"
)

func main() {
	flags := flag.NewFlagSet("kubectl-ray", flag.ExitOnError)
	flag.CommandLine = flags
	ioStreams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}

	root := cmd.NewRayCommand(ioStreams)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
