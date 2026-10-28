import { DeletionStrategy } from "@/types/common";
import { SubmitterConfig } from "../../v2/raycluster";
import { RayJobSpec, RayJobStatus } from "../../v2/rayjob";
import { V1ObjectMeta } from "@kubernetes/client-node";

export interface RayJobListResponse {
  apiVersion: string;
  items: RayJobItem[];
  kind: string;
  metadata: {
    continue: string;
    resourceVersion: string;
  };
}

type JobSubmissionMode =
  | "K8sJobMode"
  | "HTTPMode"
  | "InteractiveMode"
  | "SidecarMode";

interface RayJobAPISpec extends RayJobSpec {
  activeDeadlineSeconds: number;
  backoffLimit: number;
  submitterConfig: SubmitterConfig;
  managedBy: string;
  deletionStrategy: DeletionStrategy;
  submissionMode: JobSubmissionMode;
}

interface RayJobStatusInfo {
  startTime?: string;
  endTime?: string;
}

type JobFailedReason =
  | "SubmissionFailed"
  | "DeadlineExceeded"
  | "AppFailed"
  | "JobDeploymentStatusTransitionGracePeriodExceeded";

interface RayJobAPIStatus extends RayJobStatus {
  rayJobInfo: RayJobStatusInfo;
  reason: JobFailedReason;
  succeeded: number;
  failed: number;
}

export interface RayJobItem {
  apiVersion: string;
  kind: string;
  metadata: V1ObjectMeta;
  spec: RayJobAPISpec;
  status?: RayJobAPIStatus;
}

export interface CreateRayJobConfig {
  jobName: string;
  dockerImage: string;
  entrypoint: string;
  headResources: {
    cpu: string;
    memory: string;
    gpu: string;
  };
  workerResources: {
    replicas: number;
    minReplicas: number;
    maxReplicas: number;
    cpu: string;
    memory: string;
    gpu: string;
  };
}
