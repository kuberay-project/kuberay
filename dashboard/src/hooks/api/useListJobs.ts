import { useNamespace } from "@/components/NamespaceProvider";
import { apiServerFetcher } from "@/utils/fetch";
import useSWR from "swr";
import { RayJobListResponse, RayJobItem } from "@/types/v2/api/rayjob";
import { JobRow } from "@/types/table";
import { ALL_NAMESPACES } from "@/utils/config-defaults";
import { config } from "@/utils/constants";

export const useListJobs = (
  refreshInterval: number = 5000,
  version: "v1" | "v2" = "v2",
) => {
  const namespace = useNamespace();
  // We could use "isValidating" to indicate to the user that we are refreshing,
  // however, when isValidating is used, the component gets re-rendered even if
  // the data doesn't change, which we don't want.
  const { data, error, isLoading, mutate } = useSWR<RayJobListResponse | any>(
    // if no namespace is loaded yet, skip fetching.
    // Also, you can't reach all_namespaces in the kubeflow UI, but you can do it
    // when viewing the standalone app.
    namespace
      ? `${namespace == ALL_NAMESPACES ? `` : `/namespaces/${namespace}`}/rayjobs`
      : null,
    apiServerFetcher,
    {
      refreshInterval,
    },
  );

  let jobs;
  if (version === "v1") {
    jobs = data?.jobs ? data.jobs : [];
  } else {
    jobs = data?.items ? data.items.map(convertRayJobItemToJobRow) : [];
  }

  return {
    jobs,
    isLoading,
    error,
    mutate,
  };
};

const convertRayJobItemToJobRow = (item: RayJobItem): JobRow => {
  const namespace = item.metadata.namespace!;
  const generateLinks = () => {
    const serviceName = item.status?.rayClusterStatus?.head?.serviceName;
    const dashboardPort =
      item.spec?.rayClusterSpec?.headGroupSpec?.template?.spec?.containers?.[0]?.ports?.find(
        (port) => port.name === "dashboard",
      )?.containerPort;
    if (!serviceName || !dashboardPort || !config.coreApiUrl) {
      return {};
    }
    const rayHeadDashboardLink = `${config.coreApiUrl}/namespaces/${namespace}/services/${serviceName}:${dashboardPort}/proxy/#/jobs`;
    const rayJobId = item.status?.jobId;
    return {
      rayHeadDashboardLink,
      logsLink: `${rayHeadDashboardLink}/${rayJobId}`,
    };
  };
  return {
    name: item.metadata.name!,
    namespace: item.metadata.namespace!,
    jobStatus: item.status ?? { jobStatus: "PENDING", jobDeploymentStatus: "" },
    createdAt: item.metadata.creationTimestamp!,
    message: item.status?.message ?? "",
    links: generateLinks(),
    rayClusterName: item.status?.rayClusterName ?? "",
    submissionMode: item.spec?.submissionMode,
    rayVersion: item.spec?.rayClusterSpec?.rayVersion,
    clusterSpec: item.spec?.rayClusterSpec,
  };
};
