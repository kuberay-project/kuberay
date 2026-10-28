// This file handles displaying the job status of a rayjob.
import { ColorPaletteProp } from "@mui/joy";
import { keyframes } from "@emotion/react";
import AutorenewRoundedIcon from "@mui/icons-material/AutorenewRounded";
import BlockIcon from "@mui/icons-material/Block";
import CheckRoundedIcon from "@mui/icons-material/CheckRounded";
import PendingRoundedIcon from "@mui/icons-material/PendingRounded";
import { JobStatus } from "@/types/v2/rayjob";

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

const capitalize = (status: string) =>
  status.charAt(0) + status.toLowerCase().slice(1);

// Return the jobStatus.
export const getJobStatusDisplay = (status: JobStatus) => {
  return capitalize(status);
};

export const getJobStatusColor = (status: JobStatus): ColorPaletteProp => {
  const statusColorMap: Record<string, ColorPaletteProp> = {
    PENDING: "warning",
    SUCCEEDED: "success",
    FAILED: "danger",
    RUNNING: "primary",
    STOPPED: "danger",
    WAITING: "warning",
  };
  return statusColorMap[status] ?? "neutral";
};

export const getJobStatusIcon = (status: string) => {
  const spin = keyframes`
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  `;

  return {
    SUCCEEDED: <CheckRoundedIcon />,
    PENDING: <PendingRoundedIcon />,
    STOPPED: <BlockIcon />,
    FAILED: <BlockIcon />,
    UNKNOWN: <PendingRoundedIcon />,
    RUNNING: (
      <AutorenewRoundedIcon
        sx={{
          animation: `${spin} 2s linear infinite`,
        }}
      />
    ),
  }[status];
};
