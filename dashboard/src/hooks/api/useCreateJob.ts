import { mutate } from "swr";
import React from "react";
import {
  V1Container,
  V1ContainerPort,
  V1ResourceRequirements,
} from "@kubernetes/client-node";
import { useSnackBar } from "@/components/SnackBarProvider";
import { config } from "@/utils/constants";
import { CreateRayJobConfig } from "@/types/v2/api/rayjob";
import { ResourceKey } from "@/types/common";

// Constants
const WORKER_GROUP_NAME = "small-wg";

// Types
interface ResourceRequirements {
  cpu: string;
  memory: string;
  gpu?: string;
}

// Helper function to build container resources
function buildContainerResources(
  resources: ResourceRequirements,
): V1ResourceRequirements {
  const limits: Record<string, string> = {
    cpu: resources.cpu,
    memory: resources.memory,
  };

  const requests: Record<string, string> = {
    cpu: resources.cpu,
    memory: resources.memory,
  };

  // Add GPU if specified
  if (resources.gpu && resources.gpu !== "0") {
    limits[ResourceKey.GPU] = resources.gpu;
    requests[ResourceKey.GPU] = resources.gpu;
  }

  return { limits, requests };
}

// Build head container spec
function buildHeadContainer(jobConfig: CreateRayJobConfig): V1Container {
  const ports: V1ContainerPort[] = [
    { containerPort: 6379, name: "gcs-server" },
    { containerPort: 8265, name: "dashboard" },
    { containerPort: 10001, name: "client" },
  ];

  const headContainer: V1Container = {
    name: "ray-head",
    image: jobConfig.dockerImage,
    ports,
    resources: buildContainerResources(jobConfig.headResources),
  };

  return headContainer;
}

// Build worker container spec
function buildWorkerContainer(jobConfig: CreateRayJobConfig): V1Container {
  const workerContainer: V1Container = {
    name: "ray-worker",
    image: jobConfig.dockerImage,
    resources: buildContainerResources(jobConfig.workerResources),
  };

  return workerContainer;
}

// Parse error response
async function parseErrorResponse(response: Response): Promise<string> {
  const errorText = await response.text();
  try {
    const errorJson = JSON.parse(errorText);
    return errorJson.message || errorJson.error || errorText;
  } catch {
    return errorText;
  }
}

// Create RayJob using Kubernetes native CRD format
async function createRayJobRequest(jobConfig: CreateRayJobConfig) {
  // Use the new proxy endpoint: POST /apis/ray.io/v1/namespaces/{namespace}/rayjobs
  // Note: config.rayApiUrl already includes /apis/ray.io/v1
  const namespace = jobConfig.namespace;
  const url = `${config.rayApiUrl}/namespaces/${namespace}/rayjobs`;
  console.log(`Creating RayJob at URL: ${url}`);
  console.log(`Job config:`, JSON.stringify(jobConfig, null, 2));

  const headContainer = buildHeadContainer(jobConfig);
  const workerContainer = buildWorkerContainer(jobConfig);
  // Use Kubernetes native RayJob CRD format based on official samples
  const data = {
    apiVersion: "ray.io/v1",
    kind: "RayJob",
    metadata: {
      name: jobConfig.jobName,
      namespace,
    },
    spec: {
      // Entrypoint command to run the Ray job
      entrypoint: jobConfig.entrypoint,

      // RayCluster specification
      rayClusterSpec: {
        // Head node configuration
        headGroupSpec: {
          rayStartParams: {},
          template: {
            spec: {
              containers: [headContainer],
            },
          },
        },

        // Worker nodes configuration
        workerGroupSpecs: [
          {
            groupName: WORKER_GROUP_NAME,
            replicas: jobConfig.workerResources.replicas,
            minReplicas: jobConfig.workerResources.minReplicas,
            maxReplicas: jobConfig.workerResources.maxReplicas,
            rayStartParams: {},
            template: {
              spec: {
                containers: [workerContainer],
              },
            },
          },
        ],
      },
    },
  };

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(data),
  });

  if (response.ok) {
    return await response.json();
  }

  // Handle error response
  console.error(
    `Failed to create RayJob: ${response.status} ${response.statusText}`,
  );

  const errorMessage = await parseErrorResponse(response);
  throw new Error(errorMessage);
}

export const useCreateJob = () => {
  const [creating, setCreating] = React.useState(false);
  const snackBar = useSnackBar();

  const createJob = React.useCallback(
    async (jobConfig: CreateRayJobConfig) => {
      setCreating(true);
      try {
        // Create the job and use "mutate" to force refresh the cache.
        // We don't want to use the result of creation to populate cache.
        // Just fetch from remote.
        await mutate(
          `/namespaces/${jobConfig.namespace}/rayjobs`,
          createRayJobRequest(jobConfig),
          { populateCache: false },
        );
        await mutate("/rayjobs");

        snackBar.showSnackBar(
          "Success",
          `Job ${jobConfig.jobName} created successfully`,
          "success",
        );

        return true;
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        snackBar.showSnackBar(
          `Failed to create job ${jobConfig.jobName}`,
          `Error: ${errorMessage}`,
          "danger",
        );
        return false;
      } finally {
        setCreating(false);
      }
    },
    [snackBar],
  );

  return {
    creating,
    createJob,
  };
};
