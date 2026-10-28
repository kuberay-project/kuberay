import { V1Condition } from "@kubernetes/client-node";
import { ClusterStatus, RayClusterSpec } from "./v2/raycluster";
import { JobStatus, JobDeploymentStatus } from "./v2/rayjob";

export type Order = "asc" | "desc";

export interface ClusterRow {
  name: string;
  namespace: string;
  rayVersion: string;
  clusterState: ClusterStatus;
  createdAt?: Date;
  lastUpdateTime?: string;
  availableWorkerReplicas?: number;
  desiredWorkerReplicas?: number;
  desiredCPU?: string;
  desiredMemory?: string;
  desiredGPU?: string;
  headPodName?: string;
  headServiceIP?: string;
  endpoints: {
    client?: string;
    dashboard?: string;
    gcsServer?: string;
    metrics?: string;
  };
  links: {
    rayHeadDashboardLink: string;
    rayGrafanaDashboardLink?: string;
    notebookLink?: string;
  };
  conditions?: V1Condition[];
  labels?: Record<string, string>;
  annotations?: Record<string, string>;
}

export interface JobRow {
  name: string;
  namespace: string;
  jobStatus: {
    jobStatus: JobStatus;
    jobDeploymentStatus: JobDeploymentStatus;
  };
  createdAt: Date;
  message: string;
  links: {
    rayHeadDashboardLink?: string;
    rayGrafanaDashboardLink?: string;
    logsLink?: string;
  };
  rayClusterName: string;
  submissionMode: string;
  rayVersion: string;
  clusterSpec?: RayClusterSpec;
}
