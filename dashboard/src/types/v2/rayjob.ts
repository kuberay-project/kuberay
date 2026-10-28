import { V1PodTemplateSpec } from "@kubernetes/client-node";
import { RayClusterSpec, RayClusterStatus } from "./raycluster";

export interface RayJobSpec {
  submitterPodTemplate: V1PodTemplateSpec;
  metadata: Record<string, string>;
  rayClusterSpec: RayClusterSpec;
  clusterSelector: Record<string, string>;
  entrypoint: string;
  runtimeEnvYAML: string;
  jobId: string;
  entrypointResources: string;
  ttlSecondsAfterFinished: number;
  entrypointNumCpus: number;
  entrypointNumGpus: number;
  shutdownAfterJobFinishes: boolean;
  suspend: boolean;
}

export interface RayJobStatus {
  jobId: string;
  rayClusterName: string;
  dashboardURL: string;
  jobStatus: JobStatus;
  jobDeploymentStatus: JobDeploymentStatus;
  message: string;
  startTime: string;
  endTime: string;
  rayClusterStatus: RayClusterStatus;
  observedGeneration: number;
}

export type JobStatus =
  | "PENDING"
  | "RUNNING"
  | "STOPPED"
  | "SUCCEEDED"
  | "FAILED";

export type JobDeploymentStatus =
  | ""
  | "Initializing"
  | "Running"
  | "Complete"
  | "Failed"
  | "Suspending"
  | "Suspended"
  | "Retrying"
  | "Waiting";

export type JobType = "All" | "Batch API" | "Ray";
