// This file handles displaying the job status of a rayjob.
import { ColorPaletteProp } from "@mui/joy";
import { keyframes } from "@emotion/react";
import AutorenewRoundedIcon from "@mui/icons-material/AutorenewRounded";
import BlockIcon from "@mui/icons-material/Block";
import CheckRoundedIcon from "@mui/icons-material/CheckRounded";
import PendingRoundedIcon from "@mui/icons-material/PendingRounded";
import { ClusterStatus } from "@/types/v2/raycluster";

// There are two statues: JobStatus and JobDeploymentStatus. According to Ray CRD
// https://github.com/ray-project/kuberay/blob/master/ray-operator/apis/ray/v1/rayjob_types.go
// These are the enums:
/*
  JobStatusNew       JobStatus = ""
  JobStatusPending   JobStatus = "PENDING"
  JobStatusRunning   JobStatus = "RUNNING"
  JobStatusStopped   JobStatus = "STOPPED"
  JobStatusSucceeded JobStatus = "SUCCEEDED"
  JobStatusFailed    JobStatus = "FAILED"

  JobDeploymentStatusNew          JobDeploymentStatus = ""
  JobDeploymentStatusInitializing JobDeploymentStatus = "Initializing"
  JobDeploymentStatusRunning      JobDeploymentStatus = "Running"
  JobDeploymentStatusComplete     JobDeploymentStatus = "Complete"
  JobDeploymentStatusFailed       JobDeploymentStatus = "Failed"
  JobDeploymentStatusSuspending   JobDeploymentStatus = "Suspending"
  JobDeploymentStatusSuspended    JobDeploymentStatus = "Suspended"
*/
// However, it seems like we also have WaitForDashboardReady status... which
// is not in the Kuberay codebase.
// The plan is to only display the JobStatus in the list table. The Deployment status
// can be included in the details page.

export const capitalize = (status: string) =>
  status.charAt(0).toUpperCase() + status.toLowerCase().slice(1);

export const getClusterStatusDisplay = (status: ClusterStatus) => {
  return capitalize(status);
};

export const getClusterStatusColor = (
  status: ClusterStatus,
): ColorPaletteProp => {
  return {
    READY: "primary",
    PENDING: "warning",
    SUSPENDING: "warning",
    FAILED: "danger",
    SUSPENDED: "neutral",
    UNKNOWN: "neutral",
  }[status] as ColorPaletteProp;
};

export const getClusterStatusIcon = (status: string) => {
  const spin = keyframes`
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  `;

  return {
    READY: <CheckRoundedIcon />,
    PENDING: <PendingRoundedIcon />,
  }[status];
};
