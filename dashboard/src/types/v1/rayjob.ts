import { ClusterSpec } from "./raycluster";

export interface Job {
  name: string;
  namespace: string;
  user: string;
  entrypoint: string;
  jobId?: string;
  clusterSpec: ClusterSpec;
  createdAt: string;
  jobStatus: string;
  jobDeploymentStatus?: string;
  message?: string;
  shutdownAfterJobFinishes: boolean;
  ttlSecondsAfterFinished: number;
  version: string;
  jobSubmitter: {
    image: string;
  };
  logsLink: string;
  rayGrafanaDashboardLink: string;
  rayHeadDashboardLink: string;
  startTime: string;
  endTime: string;
}

export type Status = "SUCCEEDED" | "FAILED" | "RUNNING" | "PENDING" | "STOPPED";

export type JobType = "All" | "Batch API" | "Ray";

export type Jobs = Job[];

export interface JobLinks {
  rayDashboard: string;
  metrics: string;
  logs: string;
}

// A JobRow is a row that is used for displaying in a table.
// It contains a subset of the fields in a Job object
export interface JobRow {
  name: string;
  jobStatus: {
    jobStatus: string;
    jobDeploymentStatus: string;
  };
  createdAt: string;
  message: string;
  links: {
    rayGrafanaDashboardLink: string;
    logsLink: string;
    rayHeadDashboardLink: string;
  };
}
