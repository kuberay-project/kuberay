package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	rayjobsubmitter "github.com/ray-project/kuberay/ray-operator/rayjob-submitter"
)

func main() {
	var (
		runtimeEnvJson      string
		metadataJson        string
		entrypointResources string
		entrypointNumCpus   float32
		entrypointNumGpus   float32
	)

	flag.StringVar(&runtimeEnvJson, "runtime-env-json", "", "JSON-serialized runtime_env dictionary.")
	flag.StringVar(&metadataJson, "metadata-json", "", "JSON-serialized dictionary of metadata to attach to the job.")
	flag.StringVar(&entrypointResources, "entrypoint-resources", "", "a JSON-serialized dictionary mapping resource name to resource quantity describing resources to reserve for the entrypoint command, separately from any tasks or actors that are launched by it.")
	flag.Float32Var(&entrypointNumCpus, "entrypoint-num-cpus", 0.0, "the quantity of CPU cores to reserve for the entrypoint command, separately from any tasks or actors that are launched by it.")
	flag.Float32Var(&entrypointNumGpus, "entrypoint-num-gpus", 0.0, "the quantity of GPU cores to reserve for the entrypoint command, separately from any tasks or actors that are launched by it.")
	flag.Parse()

	address := os.Getenv("RAY_DASHBOARD_ADDRESS")
	if address == "" {
		exitOnError(fmt.Errorf("missing RAY_DASHBOARD_ADDRESS in environment variables"))
	}
	submissionId := os.Getenv("RAY_JOB_SUBMISSION_ID")
	if submissionId == "" {
		exitOnError(fmt.Errorf("missing RAY_JOB_SUBMISSION_ID in environment variables"))
	}

	req := utiltypes.RayJobRequest{
		Entrypoint:   strings.Join(flag.Args(), " "),
		SubmissionId: submissionId,
		NumCpus:      entrypointNumCpus,
		NumGpus:      entrypointNumGpus,
	}
	if len(runtimeEnvJson) > 0 {
		if err := json.Unmarshal([]byte(runtimeEnvJson), &req.RuntimeEnv); err != nil {
			exitOnError(err)
		}
	}
	if len(metadataJson) > 0 {
		if err := json.Unmarshal([]byte(metadataJson), &req.Metadata); err != nil {
			exitOnError(err)
		}
	}
	if len(entrypointResources) > 0 {
		if err := json.Unmarshal([]byte(entrypointResources), &req.Resources); err != nil {
			exitOnError(err)
		}
	}
	rayDashboardClient := &dashboardclient.RayDashboardClient{}
	address = rayjobsubmitter.JobSubmissionURL(address)
	authToken := os.Getenv("RAY_AUTH_TOKEN")
	rayDashboardClient.InitClient(&http.Client{Timeout: time.Second * 10}, address, authToken)
	submissionId, err := rayDashboardClient.SubmitJobReq(context.Background(), &req)
	if err != nil {
		if strings.Contains(err.Error(), "Please use a different submission_id") {
			fmt.Fprintf(os.Stdout, "INFO -- Job '%s' has already been submitted, tailing logs.\n", submissionId)
		} else {
			exitOnError(err)
		}
	} else {
		fmt.Fprintf(os.Stdout, "SUCC -- Job '%s' submitted successfully\n", submissionId)
	}
	fmt.Fprintf(os.Stdout, "INFO -- Tailing logs until the job finishes:\n")
	err = rayjobsubmitter.TailJobLogs(address, submissionId, authToken, os.Stdout)
	exitOnError(err)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR -- ", err)
		os.Exit(1)
	}
}
