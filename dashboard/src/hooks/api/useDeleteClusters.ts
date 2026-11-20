import { mutate } from "swr";
import React from "react";
import { useSnackBar } from "@/components/SnackBarProvider";
import { useNamespace } from "@/components/NamespaceProvider";
import { config } from "@/utils/constants";
import { ClusterRow } from "@/types/table";
import { ALL_NAMESPACES } from "@/utils/config-defaults";

async function _deleteCluster(namespace: string, clusterName: string) {
  const baseUrl = `${config.rayApiUrl}/namespaces/${namespace}/rayclusters/`;
  const response = await fetch(`${baseUrl}${clusterName}`, {
    method: "DELETE",
  });
  if (response.ok) {
    return await response.json();
  }
  console.log(response.status, response.statusText);
  const err = await response.json();
  throw new Error(err.message);
}

export const useDeleteClusters = () => {
  const [deleting, setDeleting] = React.useState(false);
  const namespace = useNamespace();
  const snackBar = useSnackBar();

  // Use the namespace of each row so that deletion works no matter which
  // namespace the dashboard is currently listing (including "all").
  const deleteClusters = async (clusters: readonly ClusterRow[]) => {
    setDeleting(true);
    // Delete the cluster and use "mutate" to force refresh the cache.
    // We don't want to use the result of deletion to populate cache, since the
    // response is empty.
    try {
      await mutate(
        `${namespace == ALL_NAMESPACES ? `` : `/namespaces/${namespace}`}/rayclusters`,
        Promise.all(
          clusters.map((cluster) =>
            _deleteCluster(cluster.namespace, cluster.name),
          ),
        ),
        { populateCache: false },
      );
      snackBar.showSnackBar(
        `${clusters.length} cluster${clusters.length > 1 ? "s" : ""} deleted`,
        `Deleted ${clusters.map((cluster) => cluster.name).join(", ")}`,
        "success",
      );
    } catch (err) {
      snackBar.showSnackBar(
        "Failed to delete clusters",
        "Error: " + err,
        "danger",
      );
    }
    setDeleting(false);
  };

  return { deleting, deleteClusters };
};
