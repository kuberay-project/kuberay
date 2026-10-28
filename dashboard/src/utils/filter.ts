import { apiVersion } from "./constants";
import * as v2 from "./v2/filter";
import * as v1 from "./v1/filter";

function exportFuncByVersion(apiVersion: "v1" | "v2") {
  if (apiVersion === "v1") {
    return {
      filterJobs: v1.filterJobs,
      filterCluster: v1.filterCluster,
      clusterIsRayJob: v1.clusterIsRayJob,
      clusterIsNotebook: v1.clusterIsNotebook,
    };
  }
  return {
    filterJobs: v2.filterJobs,
    filterCluster: v2.filterCluster,
    clusterIsRayJob: v2.clusterIsRayJob,
    clusterIsNotebook: undefined,
  };
}

export const { filterJobs, filterCluster, clusterIsRayJob, clusterIsNotebook } =
  exportFuncByVersion(apiVersion);
