import { useNamespace } from "@/components/NamespaceProvider";
import { apiServerFetcher } from "@/utils/fetch";
import useSWR from "swr";

import { RayClusterListResponse } from "@/types/v2/api/raycluster";
import { ClusterRow } from "@/types/table";
import { ClusterStatus } from "@/types/v2/raycluster";
import { V1Condition } from "@kubernetes/client-node";
import { ALL_NAMESPACES } from "@/utils/config-defaults";
import { config } from "@/utils/constants";

export const useListClusters = (
  refreshInterval: number = 5000,
  version: string,
) => {
  const namespace = useNamespace();
  // We could use "isValidating" to indicate to the user that we are refreshing,
  // however, when isValidating is used, the component gets re-rendered even if
  // the data doesn't change, which we don't want.
  const { data, error, isLoading } = useSWR<RayClusterListResponse | any>(
    // if no namespace is loaded yet, skip fetching.
    // Also, you can't reach all_namespaces in the kubeflow UI, but you can do it
    // when viewing the standalone app.
    namespace
      ? `${namespace == ALL_NAMESPACES ? `` : `/namespaces/${namespace}`}/rayclusters`
      : null,
    apiServerFetcher,
    { refreshInterval },
  );

  let clusters;
  if (version === "v1") {
    clusters = data?.clusters ? data.clusters : [];
  } else {
    clusters = data
      ? transformRayClusterResponse(data as RayClusterListResponse)
      : [];
  }

  return {
    clusters,
    isLoading,
    error,
  };
};

const parseClusterStatus = (conditions: V1Condition[]): ClusterStatus => {
  const headPodReady = conditions.find((c) => c.type === "HeadPodReady");
  const provisioned = conditions.find(
    (c) => c.type === "RayClusterProvisioned",
  );
  const suspended = conditions.find((c) => c.type === "RayClusterSuspended");
  const suspending = conditions.find((c) => c.type === "RayClusterSuspending");

  if (suspended?.status === "True") {
    return ClusterStatus.SUSPENDED;
  }
  if (suspending?.status === "True") {
    return ClusterStatus.SUSPENDING;
  }

  if (headPodReady?.status === "True" && provisioned?.status !== "False") {
    return ClusterStatus.READY;
  }

  if (headPodReady?.status === "False" || provisioned?.status === "False") {
    return ClusterStatus.FAILED;
  }

  return ClusterStatus.PENDING;
};

const transformRayClusterResponse = (
  response: RayClusterListResponse,
): ClusterRow[] => {
  return response.items.map((item) => {
    const clusterState = parseClusterStatus(item.status.conditions ?? []);
    const namespace = item.metadata.namespace!;

    const generateLinks = () => {
      const serviceName = item.status.head?.serviceName ?? "";
      const dashboardPort =
        item.spec.headGroupSpec?.template?.spec?.containers?.[0].ports?.find(
          (port) => port.name === "dashboard",
        )?.containerPort;

      if (!serviceName || !dashboardPort) {
        return {
          rayHeadDashboardLink: "",
        };
      }
      return {
        rayHeadDashboardLink: `${config.coreApiUrl}/namespaces/${namespace}/services/${serviceName}:${dashboardPort}/proxy/#/cluster`,
      };
    };

    return {
      name: item.metadata.name!,
      namespace,
      rayVersion: item.spec.rayVersion,
      clusterState,
      createdAt: item.metadata.creationTimestamp,
      lastUpdateTime: item.status.lastUpdateTime,
      availableWorkerReplicas: item.status.availableWorkerReplicas,
      desiredWorkerReplicas: item.status.desiredWorkerReplicas,
      desiredCPU: item.status.desiredCPU,
      desiredMemory: item.status.desiredMemory,
      desiredGPU: item.status.desiredGPU,
      headPodName: item.status.head!.podName,
      headServiceIP: item.status.head!.serviceIP,
      endpoints: {
        client: item.status.endpoints?.client,
        dashboard: item.status.endpoints?.dashboard,
        gcsServer: item.status.endpoints?.["gcs-server"],
        metrics: item.status.endpoints?.metrics,
      },
      links: generateLinks(),
      conditions: item.status.conditions,
      labels: item.metadata.labels,
      annotations: item.metadata.annotations,
    };
  });
};
